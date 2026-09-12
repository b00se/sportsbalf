"""Leakage-safe archived WR/TE component candidate evaluation.

The receiver projector needs route observations, while the canonical weekly
archive does not contain a trustworthy route count.  This module keeps those
inputs separate: route rows are validated and aggregated first, then joined
to *prior* canonical observations for projector history.  Fold targets always
contain identity and calendar columns only.  Target-week route rows are joined
back only after projection, for scoring and coverage accounting.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Literal

import numpy as np
import pandas as pd

from src.nfl.models.fantasy_scoring import derive_fantasy_points
from src.nfl.models.receiver_components import (
    RECEIVER_COMPONENTS,
    project_receiver_components,
)

Mode = Literal["weekly", "season"]
RANKING_TOP_K: Final[int] = 3
METRIC_COLUMNS: Final[tuple[str, ...]] = (
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
)
METRIC_DEFINITIONS: Final[tuple[tuple[str, str], ...]] = (
    ("component_mae", "mean absolute error for the named receiver component"),
    ("fantasy_point_mae", "mean absolute half-PPR fantasy-point error"),
    (
        "fantasy_point_crps",
        "mean absolute point-forecast error; deterministic point-forecast CRPS "
        "equals MAE",
    ),
    (
        "spearman_rank",
        "mean per-fold Spearman correlation of predicted and actual half-PPR "
        "points; folds with fewer than two distinct values are ineligible",
    ),
    (
        "top_k_recall",
        "mean per-fold recall of actual top-k half-PPR players in predicted "
        "top-k, where k=min(3,n); ties resolve by player_id ascending",
    ),
    (
        "calibration",
        "unavailable: deterministic point forecasts expose neither predictive "
        "intervals nor probabilities",
    ),
)
_DEFINITIONS = dict(METRIC_DEFINITIONS)
_REQUIRED_ROUTE_COLUMNS: Final[frozenset[str]] = frozenset(
    {"player_id", "position", "season", "week", "team", "identified_receiver_routes"}
)
_REQUIRED_OUTCOME_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "player_id",
        "position",
        "season",
        "week",
        "targets",
        "receptions",
        "receiving_yards",
        "receiving_tds",
    }
)
_SIGNED_YARDS: Final[frozenset[str]] = frozenset(
    {"receiving_yards", "rushing_yards"}
)


@dataclass(frozen=True, slots=True)
class ReceiverComponentEvaluationResult:
    """Predictions, metrics, and conservative route-coverage metadata."""

    predictions: pd.DataFrame
    metrics: pd.DataFrame
    status: str
    mode: Mode
    matched_history_rows: int
    target_rows: int
    route_rows: int
    target_route_rows: int
    matched_history_coverage: float
    target_coverage: float
    metric_definitions: tuple[tuple[str, str], ...] = METRIC_DEFINITIONS

    @property
    def history_coverage(self) -> float:
        """Alias for the fraction of canonical WR/TE rows with route history."""
        return self.matched_history_coverage


def _strict_calendar(frame: pd.DataFrame, name: str) -> pd.DataFrame:
    result = frame.copy(deep=True)
    for column, lower, upper in (("season", 1920, 9999), ("week", 1, 22)):
        raw = result[column]
        if raw.map(lambda value: isinstance(value, (bool, np.bool_))).any():
            raise ValueError(f"{name}.{column} must not contain booleans")
        values = pd.to_numeric(raw, errors="coerce")
        if (
            values.isna().any()
            or (~np.isfinite(values)).any()
            or (values % 1 != 0).any()
            or (values < lower).any()
            or (values > upper).any()
        ):
            raise ValueError(f"{name}.{column} must be a finite integer in range")
        result[column] = values.astype(int)
    return result


def _strict_numbers(
    frame: pd.DataFrame, columns: set[str], *, name: str
) -> pd.DataFrame:
    result = frame.copy(deep=True)
    for column in columns:
        raw = result[column]
        if raw.map(lambda value: isinstance(value, (bool, np.bool_))).any():
            raise ValueError(f"{name}.{column} must not contain booleans")
        values = pd.to_numeric(raw, errors="coerce")
        if values.isna().any() or (~np.isfinite(values)).any():
            raise ValueError(f"{name}.{column} must contain finite numeric values")
        if column not in _SIGNED_YARDS and (values < 0).any():
            raise ValueError(f"{name}.{column} must be nonnegative")
        result[column] = values.astype(float)
    return result


def _validate_outcomes(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate and retain only canonical WR/TE outcomes."""
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("weekly outcomes must be a pandas DataFrame")
    missing = sorted(_REQUIRED_OUTCOME_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"weekly outcomes missing required columns: {missing}")
    result = frame.copy(deep=True)
    result["position"] = result["position"].astype("string").str.strip().str.upper()
    result = result[result["position"].isin(["WR", "TE"])].copy()
    if result.empty:
        raise ValueError("weekly outcomes contain no WR/TE rows")
    result["player_id"] = result["player_id"].astype("string").str.strip()
    if result["player_id"].isna().any() or result["player_id"].eq("").any():
        raise ValueError("weekly outcomes player_id must be non-empty")
    result = _strict_calendar(result, "weekly outcomes")
    if result.duplicated(["player_id", "season", "week"]).any():
        raise ValueError("duplicate WR/TE player-week rows")
    numeric = {
        "targets",
        "receptions",
        "receiving_yards",
        "receiving_tds",
    }
    aliases = {
        "rushing_attempts": "rush_attempts",
        "rushing_yards": "rush_yards",
        "rushing_tds": "rush_tds",
    }
    for canonical, alias in aliases.items():
        if canonical in result.columns and alias in result.columns:
            left = pd.to_numeric(result[canonical], errors="coerce")
            right = pd.to_numeric(result[alias], errors="coerce")
            conflicting = left.notna() & right.notna() & ~left.eq(right)
            if conflicting.any():
                raise ValueError(f"conflicting aliases for {canonical}")
            result[canonical] = left.fillna(right)
        elif canonical not in result.columns and alias in result.columns:
            result[canonical] = result[alias]
    for column in ("rushing_attempts", "rushing_yards", "rushing_tds"):
        if column not in result.columns:
            result[column] = 0.0
        numeric.add(column)
    for column in ("fumbles_lost", "two_point_conversions"):
        if column in result.columns:
            raw = result[column]
            if raw.map(lambda value: isinstance(value, (bool, np.bool_))).any():
                raise ValueError(f"weekly outcomes.{column} must not contain booleans")
            values = pd.to_numeric(raw, errors="coerce")
            invalid = values.isna() & raw.notna()
            if invalid.any() or (~np.isfinite(values.dropna())).any():
                raise ValueError(
                    f"weekly outcomes.{column} must contain finite numeric values"
                )
            if (values.dropna() < 0).any():
                raise ValueError(f"weekly outcomes.{column} must be nonnegative")
            result[column] = values.fillna(0.0).astype(float)
    result = _strict_numbers(result, numeric, name="weekly outcomes")
    if (result["receptions"] > result["targets"]).any():
        raise ValueError("receptions cannot exceed targets")
    if (result["rushing_tds"] > result["rushing_attempts"]).any():
        raise ValueError("rushing touchdowns cannot exceed attempts")
    return result.sort_values(
        ["season", "week", "player_id"], kind="mergesort"
    ).reset_index(drop=True)


def _validate_routes(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate route rows and sum only explicitly identified multi-team rows."""
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("route artifact must be a pandas DataFrame")
    missing = sorted(_REQUIRED_ROUTE_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"route artifact missing required columns: {missing}")
    result = frame.copy(deep=True)
    for column in ("player_id", "team"):
        result[column] = result[column].astype("string").str.strip()
        if result[column].isna().any() or result[column].eq("").any():
            raise ValueError(f"route artifact {column} must be non-empty")
    result["position"] = result["position"].astype("string").str.strip().str.upper()
    if result["position"].isna().any() or (
        ~result["position"].isin(["WR", "TE"])
    ).any():
        raise ValueError("route artifact position must be WR or TE")
    result = _strict_calendar(result, "route artifact")
    result = _strict_numbers(
        result, {"identified_receiver_routes"}, name="route artifact"
    )
    exact_key = ["player_id", "season", "week", "team"]
    if result.duplicated(exact_key).any():
        raise ValueError("duplicate route key")
    conflict_key = ["player_id", "season", "week"]
    position_counts = result.groupby(conflict_key, sort=False)["position"].nunique()
    if (position_counts > 1).any():
        raise ValueError("conflicting positions for player-week route rows")
    return (
        result.groupby(conflict_key + ["position"], as_index=False, sort=True)
        .agg(
            identified_receiver_routes=("identified_receiver_routes", "sum"),
            teams=("team", "nunique"),
        )
        .drop(columns="teams")
    )


def _receiver_history(outcomes: pd.DataFrame, routes: pd.DataFrame) -> pd.DataFrame:
    """Build projector history from the inner join of outcomes and routes."""
    joined = outcomes.merge(
        routes,
        on=["player_id", "position", "season", "week"],
        how="inner",
        validate="one_to_one",
    )
    history = joined[
        [
            "player_id",
            "position",
            "season",
            "week",
            "identified_receiver_routes",
            "targets",
            "receptions",
            "receiving_yards",
            "receiving_tds",
            "rushing_attempts",
            "rushing_yards",
            "rushing_tds",
        ]
    ].rename(
        columns={
            "player_id": "receiver_id",
            "identified_receiver_routes": "routes",
            "rushing_attempts": "rare_rush_attempts",
            "rushing_yards": "rare_rush_yards",
            "rushing_tds": "rare_rush_tds",
        }
    )
    return history


def _folds(
    outcomes: pd.DataFrame, mode: Mode
) -> list[tuple[str, pd.Series, pd.Series]]:
    keys = outcomes[["season", "week"]].drop_duplicates().sort_values(
        ["season", "week"]
    )
    folds: list[tuple[str, pd.Series, pd.Series]] = []
    if mode == "weekly":
        for row in keys.itertuples(index=False):
            season, week = int(row.season), int(row.week)
            test = outcomes["season"].eq(season) & outcomes["week"].eq(week)
            train = (outcomes["season"] < season) | (
                outcomes["season"].eq(season) & outcomes["week"].lt(week)
            )
            folds.append((f"{season}-{week}", train, test))
    elif mode == "season":
        for season in sorted(outcomes["season"].unique()):
            season = int(season)
            folds.append(
                (
                    str(season),
                    outcomes["season"].lt(season),
                    outcomes["season"].eq(season),
                )
            )
    else:
        raise ValueError("mode must be 'weekly' or 'season'")
    return folds


def _score_projected(projected: pd.DataFrame) -> pd.Series:
    scoring = projected.copy(deep=True)
    for column in (
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
    ):
        if column not in scoring:
            scoring[column] = 0.0
    return derive_fantasy_points(scoring, "half_ppr")["fantasy_points"]


def _score_actual(outcomes: pd.DataFrame) -> pd.Series:
    """Score receiver outcomes after filling absent non-receiver streams."""
    scoring = outcomes[
        [
            "player_id",
            "position",
            "season",
            "week",
            "targets",
            "receptions",
            "receiving_yards",
            "receiving_tds",
            "rushing_attempts",
            "rushing_yards",
            "rushing_tds",
        ]
    ].copy()
    for column in (
        "pass_attempts",
        "completions",
        "passing_yards",
        "passing_tds",
        "interceptions",
        "fumbles_lost",
        "two_point_conversions",
    ):
        if column not in outcomes:
            scoring[column] = 0.0
        else:
            scoring[column] = outcomes[column].fillna(0.0).to_numpy()
    return derive_fantasy_points(scoring, "half_ppr")["fantasy_points"]


def _top_recall(group: pd.DataFrame) -> float:
    if group.empty:
        return float("nan")
    k = min(RANKING_TOP_K, len(group))
    predicted = set(
        group.sort_values(
            ["predicted_fantasy_points", "player_id"],
            ascending=[False, True],
            kind="mergesort",
        ).head(k)["player_id"]
    )
    actual = set(
        group.sort_values(
            ["actual_fantasy_points", "player_id"],
            ascending=[False, True],
            kind="mergesort",
        ).head(k)["player_id"]
    )
    return float(len(predicted & actual) / k)


def _metric_rows(predictions: pd.DataFrame) -> pd.DataFrame:
    """Emit aggregate and fold rows under the shared R3 metric contract."""
    if predictions.empty:
        return pd.DataFrame(columns=METRIC_COLUMNS)
    rows: list[dict[str, object]] = []
    component_names = [*RECEIVER_COMPONENTS]
    groups = [("overall", None, predictions)] + [
        ("position", position, group)
        for position, group in predictions.groupby("position", sort=True)
    ]
    for scope, key, group in groups:
        outer_folds = int(group["outer_fold"].nunique())
        basic_eligible = outer_folds >= 3
        basic_reason = "" if basic_eligible else "fewer than three distinct outer folds"

        def add(
            metric: str,
            component: str,
            value: float,
            *,
            valid_folds: int,
            rows_count: int,
            parameter: object = None,
            definition: str,
            eligible: bool,
            reason: str,
        ) -> None:
            rows.append(
                {
                    "scope": scope,
                    "group": key,
                    "metric": metric,
                    "component": component,
                    "fold": None,
                    "aggregation": "aggregate",
                    "rows": rows_count,
                    "outer_folds": outer_folds,
                    "valid_folds": valid_folds,
                    "folds": valid_folds,
                    "eligible": eligible,
                    "parameter": parameter,
                    "definition": definition,
                    "valid": valid_folds > 0,
                    "status": "eligible" if eligible else "inconclusive",
                    "reason": reason,
                    "value": value if eligible else float("nan"),
                }
            )

        fp_error = group["predicted_fantasy_points"] - group["actual_fantasy_points"]
        for metric in ("fantasy_point_mae", "fantasy_point_crps"):
            add(
                metric,
                "fantasy_points",
                float(fp_error.abs().mean()),
                valid_folds=outer_folds,
                rows_count=len(group),
                definition=_DEFINITIONS[metric],
                eligible=basic_eligible,
                reason=basic_reason,
            )
        for component in component_names:
            actual_name = f"actual_{component}"
            predicted_name = f"predicted_{component}"
            valid = group[actual_name].notna() & group[predicted_name].notna()
            values = group.loc[valid]
            valid_folds = int(values["outer_fold"].nunique())
            error = values[predicted_name] - values[actual_name]
            eligible = basic_eligible and valid_folds >= 3 and not error.empty
            reason = "" if eligible else (
                "fewer than three valid outer folds"
                if valid_folds < 3
                else "no valid observations"
            )
            add(
                "component_mae",
                component,
                float(error.abs().mean()) if not error.empty else float("nan"),
                valid_folds=valid_folds,
                rows_count=len(values),
                definition=_DEFINITIONS["component_mae"],
                eligible=eligible,
                reason=reason,
            )

        rank_values: dict[str, list[float]] = {
            "spearman_rank": [],
            "top_k_recall": [],
        }
        for fold, fold_group in group.groupby("outer_fold", sort=True):
            pred = pd.to_numeric(
                fold_group["predicted_fantasy_points"], errors="coerce"
            )
            actual = pd.to_numeric(
                fold_group["actual_fantasy_points"], errors="coerce"
            )
            if len(fold_group) >= 2 and pred.nunique() >= 2 and actual.nunique() >= 2:
                spearman = pred.corr(actual, method="spearman")
            else:
                spearman = float("nan")
            spearman_valid = pd.notna(spearman)
            if spearman_valid:
                rank_values["spearman_rank"].append(float(spearman))
            rows.append(
                {
                    "scope": scope,
                    "group": key,
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
                    "definition": _DEFINITIONS["spearman_rank"],
                    "valid": bool(spearman_valid),
                    "status": "inconclusive",
                    "reason": "" if spearman_valid else "requires two distinct values",
                    "value": float(spearman) if spearman_valid else float("nan"),
                }
            )
            top = _top_recall(fold_group)
            top_valid = pd.notna(top)
            if top_valid:
                rank_values["top_k_recall"].append(float(top))
            rows.append(
                {
                    "scope": scope,
                    "group": key,
                    "metric": "top_k_recall",
                    "component": "fantasy_points",
                    "fold": fold,
                    "aggregation": "fold",
                    "rows": len(fold_group),
                    "outer_folds": 1,
                    "valid_folds": int(top_valid),
                    "folds": int(top_valid),
                    "eligible": False,
                    "parameter": f"k={RANKING_TOP_K}",
                    "definition": _DEFINITIONS["top_k_recall"],
                    "valid": bool(top_valid),
                    "status": "inconclusive",
                    "reason": "" if top_valid else "requires at least one row",
                    "value": float(top) if top_valid else float("nan"),
                }
            )
        for metric, values in rank_values.items():
            eligible = basic_eligible and len(values) >= 3
            add(
                metric,
                "fantasy_points",
                float(np.mean(values)) if values else float("nan"),
                valid_folds=len(values),
                rows_count=len(group),
                parameter=f"k={RANKING_TOP_K}" if metric == "top_k_recall" else None,
                definition=_DEFINITIONS[metric],
                eligible=eligible,
                reason="" if eligible else "fewer than three valid outer folds",
            )
        add(
            "calibration",
            "fantasy_points",
            float("nan"),
            valid_folds=0,
            rows_count=len(group),
            parameter="unavailable",
            definition=_DEFINITIONS["calibration"],
            eligible=False,
            reason=_DEFINITIONS["calibration"],
        )
    return pd.DataFrame(rows, columns=METRIC_COLUMNS)


def _project_fold(
    history: pd.DataFrame,
    test: pd.DataFrame,
    fold: str,
    mode: Mode,
) -> pd.DataFrame:
    # The projector has bounded fallbacks for a first fold.  Keeping that
    # fold in the result preserves the requirement that every canonical WR/TE
    # target identity is scored; an empty history still contains the complete
    # projector schema and therefore cannot leak target outcomes.
    if test.empty:
        return pd.DataFrame()
    target = test[["player_id", "position", "season", "week"]].rename(
        columns={"player_id": "receiver_id"}
    )
    projected = project_receiver_components(history, target)
    projected = projected.rename(columns={"receiver_id": "player_id"})
    projected["outer_fold"] = fold
    projected["mode"] = mode
    return projected


def evaluate_archived_receiver_components(
    weekly_outcomes: pd.DataFrame,
    route_artifact: pd.DataFrame,
    *,
    mode: Mode = "weekly",
) -> ReceiverComponentEvaluationResult:
    """Evaluate WR/TE projections using strict rolling-origin folds.

    ``route_artifact`` is aggregated across teams only after validating that
    each source team key is unique and that a player-week has one position.
    The target frame passed to :func:`project_receiver_components` contains
    exactly ``receiver_id``, ``position``, ``season``, and ``week``.
    """
    outcomes = _validate_outcomes(weekly_outcomes)
    routes = _validate_routes(route_artifact)
    all_route_join = outcomes.merge(
        routes,
        on=["player_id", "position", "season", "week"],
        how="inner",
        validate="one_to_one",
    )
    history_all = _receiver_history(outcomes, routes)
    predictions: list[pd.DataFrame] = []
    for fold, train_mask, test_mask in _folds(outcomes, mode):
        train = outcomes.loc[train_mask]
        test = outcomes.loc[test_mask]
        if test.empty:
            continue
        history = history_all.merge(
            train[["player_id", "position", "season", "week"]].rename(
                columns={"player_id": "receiver_id"}
            ),
            on=["receiver_id", "position", "season", "week"],
            how="inner",
            validate="one_to_one",
        )
        projected = _project_fold(history, test, fold, mode)
        if projected.empty:
            continue
        scored = _score_actual(test)
        actual = test[
            [
                "player_id",
                "position",
                "season",
                "week",
                "targets",
                "receptions",
                "receiving_yards",
                "receiving_tds",
            ]
        ].copy()
        actual = actual.rename(
            columns={
                "targets": "actual_targets",
                "receptions": "actual_receptions",
                "receiving_yards": "actual_receiving_yards",
                "receiving_tds": "actual_receiving_tds",
            }
        )
        actual["actual_rare_rush_attempts"] = test["rushing_attempts"].to_numpy()
        actual["actual_rare_rush_yards"] = test["rushing_yards"].to_numpy()
        actual["actual_rare_rush_tds"] = test["rushing_tds"].to_numpy()
        actual["actual_routes"] = actual.merge(
            all_route_join[
                [
                    "player_id",
                    "position",
                    "season",
                    "week",
                    "identified_receiver_routes",
                ]
            ],
            on=["player_id", "position", "season", "week"],
            how="left",
            validate="one_to_one",
        )["identified_receiver_routes"].to_numpy()
        actual["actual_fantasy_points"] = scored.to_numpy()
        predicted_points = _score_projected(projected)
        for component in RECEIVER_COMPONENTS:
            projected = projected.rename(columns={component: f"predicted_{component}"})
        projected["predicted_fantasy_points"] = predicted_points.to_numpy()
        merged = projected.merge(
            actual,
            on=["player_id", "position", "season", "week"],
            how="inner",
            validate="one_to_one",
        )
        predictions.append(merged)
    prediction_frame = (
        pd.concat(predictions, ignore_index=True, sort=False)
        if predictions
        else pd.DataFrame()
    )
    outer_folds = (
        int(prediction_frame["outer_fold"].nunique())
        if not prediction_frame.empty
        else 0
    )
    status = (
        "eligible_for_candidate_comparison" if outer_folds >= 3 else "inconclusive"
    )
    target_rows = len(prediction_frame)
    target_route_rows = (
        int(prediction_frame["actual_routes"].notna().sum())
        if target_rows
        else 0
    )
    outcome_count = len(outcomes)
    matched_history_rows = len(all_route_join)
    return ReceiverComponentEvaluationResult(
        predictions=prediction_frame,
        metrics=_metric_rows(prediction_frame),
        status=status,
        mode=mode,
        matched_history_rows=matched_history_rows,
        target_rows=target_rows,
        route_rows=len(routes),
        target_route_rows=target_route_rows,
        matched_history_coverage=(
            matched_history_rows / outcome_count if outcome_count else 0.0
        ),
        target_coverage=target_route_rows / target_rows if target_rows else 0.0,
    )


def evaluate_archived_receiver_component_candidates(
    weekly_outcomes: pd.DataFrame,
    route_artifact: pd.DataFrame,
    *,
    mode: Mode = "weekly",
) -> ReceiverComponentEvaluationResult:
    """Compatibility alias that emphasizes the result is not promoted."""
    return evaluate_archived_receiver_components(
        weekly_outcomes, route_artifact, mode=mode
    )


__all__ = [
    "METRIC_COLUMNS",
    "METRIC_DEFINITIONS",
    "RANKING_TOP_K",
    "ReceiverComponentEvaluationResult",
    "evaluate_archived_receiver_components",
    "evaluate_archived_receiver_component_candidates",
]
