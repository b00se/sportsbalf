"""Leakage-safe archived QB/RB component candidate evaluation.

This module evaluates the already-approved component projectors on immutable
weekly observations.  Fold targets contain identity and calendar columns only;
the target observations are used solely after projection to score predictions.
Receiver positions are deliberately reported as unsupported until route input
is available to the receiver projector.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Literal

import numpy as np
import pandas as pd

from src.nfl.models.archived_component_adapter import (
    ArchivedComponentFrames,
    adapt_archived_weekly_components,
)
from src.nfl.models.fantasy_scoring import derive_fantasy_points
from src.nfl.models.qb_components import COMPONENTS as QB_COMPONENTS
from src.nfl.models.qb_components import project_qb_components
from src.nfl.models.rb_components import RB_COMPONENTS, project_rb_components

Mode = Literal["weekly", "season"]
SUPPORTED_POSITIONS = ("QB", "RB")
RANKING_TOP_K: Final[int] = 3
METRIC_DEFINITIONS: Final[tuple[tuple[str, str], ...]] = (
    (
        "spearman_rank",
        "mean per-fold Spearman correlation of predicted and actual half-PPR points; "
        "folds with fewer than two distinct values are ineligible",
    ),
    (
        "top_k_recall",
        "mean per-fold recall of the actual top-k half-PPR players in the predicted "
        "top-k, where k=min(3,n) for a slate; ties are resolved by player_id "
        "ascending",
    ),
    (
        "calibration",
        "unavailable: deterministic point forecasts expose neither predictive "
        "intervals nor probabilities, so empirical calibration cannot be computed",
    ),
)
_ADAPTER_RECEIVER_DEFAULTS = (
    "routes",
    "rare_rush_attempts",
    "rare_rush_yards",
    "rare_rush_tds",
)


@dataclass(frozen=True, slots=True)
class ComponentEvaluationResult:
    """Predictions, metrics, and eligibility for one fold protocol."""

    predictions: pd.DataFrame
    metrics: pd.DataFrame
    status: str
    mode: Mode
    unsupported_positions: tuple[str, ...] = ("WR", "TE")
    metric_definitions: tuple[tuple[str, str], ...] = METRIC_DEFINITIONS


def _validate_calendar(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"player_id", "position", "season", "week"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"weekly outcomes missing required columns: {missing}")
    work = frame.copy(deep=True)
    work["position"] = work["position"].astype(str).str.upper().str.strip()
    work = work[work["position"].isin({"QB", "RB"})].copy()
    if work.empty:
        raise ValueError("weekly outcomes contain no QB/RB rows")
    for column, lower, upper in (("season", 1920, 9999), ("week", 1, 22)):
        raw = work[column]
        if raw.map(lambda value: isinstance(value, (bool, np.bool_))).any():
            raise ValueError(f"{column} must not contain booleans")
        values = pd.to_numeric(raw, errors="coerce")
        if (
            values.isna().any()
            or (~np.isfinite(values)).any()
            or (values % 1 != 0).any()
            or (values < lower).any()
            or (values > upper).any()
        ):
            raise ValueError(f"{column} must be a finite integer in range")
        work[column] = values.astype(int)
    if (
        work["player_id"].isna().any()
        or work["player_id"].astype(str).str.strip().eq("").any()
    ):
        raise ValueError("player_id must be non-empty")
    work["player_id"] = work["player_id"].astype(str)
    if work.duplicated(["player_id", "season", "week"]).any():
        raise ValueError("duplicate player/calendar rows")
    needed = (set(QB_COMPONENTS) | set(RB_COMPONENTS)) - {"rush_attempts"}
    missing_stats = sorted(needed - set(work.columns))
    if missing_stats:
        raise ValueError(f"weekly outcomes missing component columns: {missing_stats}")
    signed_yards = {
        "passing_yards",
        "rushing_yards",
        "receiving_yards",
        "rare_rush_yards",
    }
    validated_columns = needed | (signed_yards & set(work.columns))
    for column in validated_columns:
        raw = work[column]
        if raw.map(lambda value: isinstance(value, (bool, np.bool_))).any():
            raise ValueError(f"{column} must not contain booleans")
        values = pd.to_numeric(raw, errors="coerce")
        if values.isna().any() or (~np.isfinite(values)).any():
            raise ValueError(f"{column} must contain finite numeric values")
        if column not in signed_yards and (values < 0).any():
            raise ValueError(f"{column} must be nonnegative")
        work[column] = values.astype(float)
    # The shared adapter also validates an empty receiver partition.  These
    # streams are intentionally not modeled here; zero-filled compatibility
    # columns keep the adapter contract explicit without pretending routes are
    # available for candidate projections.
    for column in _ADAPTER_RECEIVER_DEFAULTS:
        if column not in work:
            work[column] = 0.0
    return work.sort_values(
        ["season", "week", "player_id"], kind="mergesort"
    ).reset_index(drop=True)


def _key_frame(frame: pd.DataFrame) -> pd.Series:
    return frame["season"] * 100 + frame["week"]


def _folds(
    work: pd.DataFrame, mode: Mode
) -> list[tuple[tuple[int, int] | tuple[int], pd.Series, pd.Series]]:
    keys = work[["season", "week"]].drop_duplicates().sort_values(["season", "week"])
    result = []
    if mode == "weekly":
        for row in keys.itertuples(index=False):
            key = (int(row.season), int(row.week))
            test_mask = work["season"].eq(key[0]) & work["week"].eq(key[1])
            train_mask = _key_frame(work) < key[0] * 100 + key[1]
            result.append((key, train_mask, test_mask))
    elif mode == "season":
        for season in sorted(work["season"].unique()):
            test_mask = work["season"].eq(int(season))
            train_mask = work["season"] < int(season)
            result.append(((int(season),), train_mask, test_mask))
    else:
        raise ValueError("mode must be 'weekly' or 'season'")
    return result


def _project_fold(
    adapted: ArchivedComponentFrames,
    test: pd.DataFrame,
    fold: tuple[int, int] | tuple[int],
) -> pd.DataFrame:
    boundary = fold[0] * 100 + (fold[1] if len(fold) == 2 else 0)
    outputs: list[pd.DataFrame] = []
    for position, projector_frame, id_column in (
        ("QB", adapted.qb, "qb_id"),
        ("RB", adapted.rb, "rb_id"),
    ):
        history = projector_frame.loc[
            (projector_frame["season"] * 100 + projector_frame["week"])
            < (boundary if len(fold) == 2 else fold[0] * 100)
        ]
        target = test.loc[
            test["position"].eq(position), ["player_id", "season", "week"]
        ].copy()
        if target.empty:
            continue
        target = target.rename(columns={"player_id": id_column})
        if position == "QB":
            projected = project_qb_components(history, target)
            projected = projected.rename(
                columns={"qb_id": "player_id", "rushing_attempts": "rush_attempts"}
            )
        else:
            projected = project_rb_components(history, target)
            projected = projected.rename(columns={"rb_id": "player_id"})
        projected["position"] = position
        outputs.append(projected)
    if not outputs:
        return pd.DataFrame()
    return pd.concat(outputs, ignore_index=True, sort=False)


def _score_projected(projected: pd.DataFrame) -> pd.Series:
    """Score mixed-position projections, treating absent streams as zero."""
    scoring = projected.copy(deep=True)
    component_columns = [
        "pass_attempts",
        "completions",
        "passing_yards",
        "passing_tds",
        "interceptions",
        "rushing_attempts",
        "rush_attempts",
        "rushing_yards",
        "rushing_tds",
        "targets",
        "receptions",
        "receiving_yards",
        "receiving_tds",
    ]
    # Resolve the QB/RB spelling difference before invoking the strict scorer;
    # passing both aliases with different mixed-position values is rejected by
    # the scorer as an ambiguous input.
    if "rushing_attempts" not in scoring:
        scoring["rushing_attempts"] = scoring["rush_attempts"]
    elif "rush_attempts" in scoring:
        scoring["rushing_attempts"] = scoring["rushing_attempts"].fillna(
            scoring["rush_attempts"]
        )
    scoring = scoring.drop(columns=["rush_attempts"], errors="ignore")
    for column in component_columns:
        if column == "rush_attempts":
            continue
        if column not in scoring:
            scoring[column] = 0.0
        else:
            scoring[column] = scoring[column].fillna(0.0)
    return derive_fantasy_points(scoring, "half_ppr")["fantasy_points"]


def _metric_rows(predictions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    component_columns = [
        "pass_attempts",
        "completions",
        "passing_yards",
        "passing_tds",
        "interceptions",
        "rush_attempts",
        "rushing_yards",
        "rushing_tds",
        "targets",
        "receptions",
        "receiving_yards",
        "receiving_tds",
    ]
    for scope, group_key, group in [
        ("overall", None, predictions),
        *[("position", p, g) for p, g in predictions.groupby("position", sort=True)],
    ]:
        outer_folds = int(group["fold"].nunique())
        aggregate_eligible = outer_folds >= 3
        aggregate_status = "eligible" if aggregate_eligible else "inconclusive"
        aggregate_reason = (
            "" if aggregate_eligible else "fewer than three distinct outer folds"
        )
        for component in component_columns:
            actual = f"actual_{component}"
            predicted = f"predicted_{component}"
            if actual not in group or predicted not in group:
                continue
            error = group[predicted] - group[actual]
            rows.append(
                {
                    "scope": scope,
                    "group": group_key,
                    "metric": "component_mae",
                    "component": component,
                    "fold": None,
                    "aggregation": "aggregate",
                    "rows": len(group),
                    "outer_folds": outer_folds,
                    "valid_folds": outer_folds,
                    "folds": outer_folds,
                    "eligible": aggregate_eligible,
                    "parameter": None,
                    "definition": "mean absolute component error",
                    "valid": aggregate_eligible,
                    "status": aggregate_status,
                    "reason": aggregate_reason,
                    "value": float(error.abs().mean())
                    if aggregate_eligible
                    else float("nan"),
                }
            )
        error = group["predicted_fantasy_points"] - group["actual_fantasy_points"]
        rows.extend(
            {
                "scope": scope,
                "group": group_key,
                "metric": metric,
                "component": "fantasy_points",
                "fold": None,
                "aggregation": "aggregate",
                "rows": len(group),
                "outer_folds": outer_folds,
                "valid_folds": outer_folds,
                "folds": outer_folds,
                "eligible": aggregate_eligible,
                "parameter": None,
                "definition": (
                    "mean absolute point-forecast error; point-forecast CRPS equals "
                    "MAE"
                ),
                "valid": aggregate_eligible,
                "status": aggregate_status,
                "reason": aggregate_reason,
                "value": float(error.abs().mean())
                if aggregate_eligible
                else float("nan"),
            }
            for metric in ("fantasy_point_mae", "fantasy_point_crps")
        )
        # Ranking metrics are computed within each outer fold.  This prevents
        # cross-slate scale differences from masquerading as ranking quality
        # and gives every reported slate equal weight in the aggregate.
        fold_values: dict[str, list[float]] = {
            "spearman_rank": [],
            "top_k_recall": [],
        }
        for fold, fold_group in group.groupby("fold", sort=True):
            predicted = pd.to_numeric(
                fold_group["predicted_fantasy_points"], errors="coerce"
            )
            actual = pd.to_numeric(
                fold_group["actual_fantasy_points"], errors="coerce"
            )
            if (
                len(fold_group) >= 2
                and predicted.nunique(dropna=True) >= 2
                and actual.nunique(dropna=True) >= 2
            ):
                correlation = predicted.corr(actual, method="spearman")
                if pd.notna(correlation):
                    fold_values["spearman_rank"].append(float(correlation))
                    spearman_value = float(correlation)
                    spearman_valid = True
                    spearman_reason = ""
                else:
                    spearman_value = float("nan")
                    spearman_valid = False
                    spearman_reason = "undefined Spearman correlation"
            else:
                spearman_value = float("nan")
                spearman_valid = False
                spearman_reason = (
                    "requires at least two rows and two distinct predicted and "
                    "actual values"
                )
            rows.append(
                {
                    "scope": scope,
                    "group": group_key,
                    "metric": "spearman_rank",
                    "component": "fantasy_points",
                    "fold": fold,
                    "aggregation": "fold",
                    "rows": len(fold_group),
                    "outer_folds": 1,
                    "valid_folds": int(spearman_valid),
                    "folds": int(spearman_valid),
                    "eligible": False,
                    "parameter": None,
                    "definition": dict(METRIC_DEFINITIONS)["spearman_rank"],
                    "valid": spearman_valid,
                    "status": "inconclusive",
                    "reason": spearman_reason,
                    "value": spearman_value,
                }
            )

            if len(fold_group) > 0:
                k = min(RANKING_TOP_K, len(fold_group))
                predicted_order = fold_group.sort_values(
                    ["predicted_fantasy_points", "player_id"],
                    ascending=[False, True],
                    kind="mergesort",
                )
                actual_order = fold_group.sort_values(
                    ["actual_fantasy_points", "player_id"],
                    ascending=[False, True],
                    kind="mergesort",
                )
                predicted_top = set(predicted_order.head(k)["player_id"])
                actual_top = set(actual_order.head(k)["player_id"])
                top_k_value = float(len(predicted_top & actual_top) / k)
                fold_values["top_k_recall"].append(top_k_value)
                top_k_valid = True
                top_k_reason = ""
            else:
                top_k_value = float("nan")
                top_k_valid = False
                top_k_reason = "requires at least one row"
            rows.append(
                {
                    "scope": scope,
                    "group": group_key,
                    "metric": "top_k_recall",
                    "component": "fantasy_points",
                    "fold": fold,
                    "aggregation": "fold",
                    "rows": len(fold_group),
                    "outer_folds": 1,
                    "valid_folds": int(top_k_valid),
                    "folds": int(top_k_valid),
                    "eligible": False,
                    "parameter": f"k={RANKING_TOP_K}",
                    "definition": dict(METRIC_DEFINITIONS)["top_k_recall"],
                    "valid": top_k_valid,
                    "status": "inconclusive",
                    "reason": top_k_reason,
                    "value": top_k_value,
                }
            )

        definitions = dict(METRIC_DEFINITIONS)
        parameters = {
            "spearman_rank": None,
            "top_k_recall": f"k={RANKING_TOP_K}",
            "calibration": "unavailable",
        }
        for metric, values in fold_values.items():
            eligible = len(values) >= 3
            rows.append(
                {
                    "scope": scope,
                    "group": group_key,
                    "metric": metric,
                    "component": "fantasy_points",
                    "fold": None,
                    "aggregation": "aggregate",
                    "rows": len(group),
                    "outer_folds": outer_folds,
                    "valid_folds": len(values),
                    "folds": len(values),
                    "eligible": eligible,
                    "parameter": parameters[metric],
                    "definition": definitions[metric],
                    "valid": bool(values),
                    "status": "eligible" if eligible else "inconclusive",
                    "reason": "" if eligible else "fewer than three valid outer folds",
                    "value": float(np.mean(values)) if eligible else float("nan"),
                }
            )
        rows.append(
            {
                "scope": scope,
                "group": group_key,
                "metric": "calibration",
                "component": "fantasy_points",
                "fold": None,
                "aggregation": "aggregate",
                "rows": len(group),
                "outer_folds": outer_folds,
                "valid_folds": 0,
                "folds": 0,
                "eligible": False,
                "parameter": parameters["calibration"],
                "definition": definitions["calibration"],
                "valid": False,
                "status": "inconclusive",
                "reason": definitions["calibration"],
                "value": float("nan"),
            }
        )
    return pd.DataFrame(
        rows,
        columns=[
            "scope",
            "group",
            "metric",
            "component",
            "fold",
            "aggregation",
            "rows",
            "outer_folds",
            "valid_folds",
            "folds",
            "eligible",
            "parameter",
            "definition",
            "valid",
            "status",
            "reason",
            "value",
        ],
    )


def evaluate_archived_components(
    frame: pd.DataFrame, *, mode: Mode = "weekly"
) -> ComponentEvaluationResult:
    """Evaluate QB/RB projectors with weekly or season-held-out origins.

    The input is canonical archived weekly data with QB/RB component columns.
    Each projection target is reconstructed from only ``player_id``,
    ``position``, ``season``, and ``week``.  A result with fewer than three
    outer folds is inconclusive and is never promotable.
    """
    work = _validate_calendar(frame)
    # Adapt the immutable archive once.  This is a pure schema/value
    # normalization; each projector still receives only the strict prefix
    # selected below, never the fold's target-week rows.
    adapted = adapt_archived_weekly_components(work)
    scored = derive_fantasy_points(work, "half_ppr")
    predictions: list[pd.DataFrame] = []
    for fold, train_mask, test_mask in _folds(work, mode):
        train, test = work.loc[train_mask], work.loc[test_mask]
        if train.empty or test.empty:
            continue
        projected = _project_fold(adapted, test, fold)
        if projected.empty:
            continue
        actual_columns = [
            "player_id",
            "position",
            "season",
            "week",
            "fantasy_points",
            "pass_attempts",
            "completions",
            "passing_yards",
            "passing_tds",
            "interceptions",
            "rushing_attempts",
            "rushing_yards",
            "rushing_tds",
            "targets",
            "receptions",
            "receiving_yards",
            "receiving_tds",
        ]
        actual = scored.loc[test.index, actual_columns].copy()
        actual = actual.rename(columns={"fantasy_points": "actual_fantasy_points"})
        actual = actual.rename(columns={"rushing_attempts": "actual_rush_attempts"})
        actual = actual.rename(
            columns={
                column: f"actual_{column}"
                for column in actual.columns
                if column not in {"player_id", "position", "season", "week"}
                and not column.startswith("actual_")
            }
        )
        predicted_points = _score_projected(projected)
        projected = projected.rename(
            columns={
                column: f"predicted_{column}"
                for column in (set(QB_COMPONENTS) | set(RB_COMPONENTS))
                if column in projected
            }
        )
        projected = projected.rename(
            columns={"predicted_rushing_attempts": "predicted_rush_attempts"}
        )
        merged = projected.merge(
            actual,
            on=["player_id", "position", "season", "week"],
            how="inner",
            validate="one_to_one",
        )
        merged["predicted_fantasy_points"] = predicted_points.to_numpy()
        merged["fold"] = "-".join(map(str, fold))
        predictions.append(merged)
    prediction_frame = (
        pd.concat(predictions, ignore_index=True, sort=False)
        if predictions
        else pd.DataFrame()
    )
    fold_columns = ["season", "week"] if mode == "weekly" else ["season"]
    fold_count = (
        prediction_frame[fold_columns].drop_duplicates().shape[0]
        if not prediction_frame.empty
        else 0
    )
    status = "eligible_for_candidate_comparison" if fold_count >= 3 else "inconclusive"
    return ComponentEvaluationResult(
        prediction_frame,
        _metric_rows(prediction_frame)
        if not prediction_frame.empty
        else pd.DataFrame(),
        status,
        mode,
    )


def evaluate_archived_component_candidates(
    frame: pd.DataFrame, *, mode: Mode = "weekly"
) -> ComponentEvaluationResult:
    """Compatibility alias naming the output as a non-promoting candidate."""
    return evaluate_archived_components(frame, mode=mode)


__all__ = [
    "ComponentEvaluationResult",
    "METRIC_DEFINITIONS",
    "RANKING_TOP_K",
    "evaluate_archived_components",
    "evaluate_archived_component_candidates",
]
