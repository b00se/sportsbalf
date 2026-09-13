"""Leakage-safe nested walk-forward tournament for NFL projections."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite, sqrt
from numbers import Integral
from typing import Final

import numpy as np
import pandas as pd


class EnsembleInputError(ValueError):
    """Raised when a tournament input cannot be interpreted safely."""


_KEYS: Final[tuple[str, ...]] = ("player_id", "season", "week")
_WEIGHTS: Final[tuple[tuple[float, float, float], ...]] = tuple(
    (a / 4, b / 4, (4 - a - b) / 4) for a in range(5) for b in range(5 - a)
)
_NAMES: Final[tuple[str, ...]] = ("internal", "underdog", "consensus")


@dataclass(frozen=True, slots=True)
class EnsembleTournamentResult:
    """Auditable result of a nested rolling-origin tournament."""

    predictions: pd.DataFrame
    fold_scores: pd.DataFrame
    promoted: bool
    promotion_status: str
    selected_counts: dict[str, int]
    uncertainty: dict[str, object]
    calibration: dict[str, float]
    metrics: dict[str, object]


def _calendar(value: object, name: str) -> int:
    if isinstance(value, (bool, np.bool_)):
        raise EnsembleInputError(f"{name} must be an integer, not boolean")
    try:
        if isinstance(value, str) and value.strip().isdigit():
            number = int(value.strip())
        elif isinstance(value, Integral):
            number = int(value)
        else:
            raise ValueError
        lower = 1920 if name.endswith("season") else 1
        upper = 9999 if name.endswith("season") else 22
        if number < lower or number > upper:
            raise ValueError
    except (TypeError, ValueError, OverflowError) as exc:
        raise EnsembleInputError(
            f"{name} must be a finite non-negative integer"
        ) from exc
    return number


def _normalize(frame: pd.DataFrame | None, label: str, value_name: str) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame):
        raise EnsembleInputError(f"{label} source is required")
    needed = set(_KEYS) | {value_name}
    if not needed.issubset(frame.columns):
        raise EnsembleInputError(f"{label} is missing required columns")
    columns = list(_KEYS) + [value_name]
    if value_name == "actual" and "position" in frame.columns:
        columns.append("position")
    result = frame.loc[:, columns].copy()
    if result["player_id"].isna().any():
        raise EnsembleInputError(f"{label} has a missing player_id")
    result["player_id"] = result["player_id"].map(lambda x: str(x).strip().casefold())
    if (result["player_id"] == "").any() or result["player_id"].isin(
        {"nan", "none"}
    ).any():
        raise EnsembleInputError(f"{label} has an empty player_id")
    result["season"] = result["season"].map(lambda x: _calendar(x, f"{label}.season"))
    result["week"] = result["week"].map(lambda x: _calendar(x, f"{label}.week"))
    if result[value_name].map(lambda value: isinstance(value, (bool, np.bool_))).any():
        raise EnsembleInputError(f"{label}.{value_name} must be numeric, not boolean")
    numeric = pd.to_numeric(result[value_name], errors="coerce")
    if numeric.isna().any() or (~numeric.map(isfinite)).any():
        raise EnsembleInputError(f"{label}.{value_name} must be finite numeric values")
    result[value_name] = numeric.astype(float)
    if result.duplicated(list(_KEYS)).any():
        raise EnsembleInputError(f"{label} contains duplicate calendar/player keys")
    return result.sort_values(
        ["season", "week", "player_id"], kind="mergesort"
    ).reset_index(drop=True)


def _mae(values: pd.Series) -> float:
    return float(values.abs().mean())


def run_nested_ensemble_tournament(
    internal: pd.DataFrame,
    underdog: pd.DataFrame,
    consensus: pd.DataFrame,
    actuals: pd.DataFrame,
    *,
    min_training_calendars: int = 1,
    cutoff: tuple[int, int] | None = None,
) -> EnsembleTournamentResult:
    """Select blends within prior folds and score only subsequent calendars.

    Every source and outcome is required and must have exactly aligned keys. The
    candidate grid is fixed, and the selected weight is fit using only rows with
    a strictly earlier ``(season, week)`` than the scored outer fold.
    """

    if (
        isinstance(min_training_calendars, bool)
        or not isinstance(min_training_calendars, Integral)
        or min_training_calendars < 1
    ):
        raise EnsembleInputError("min_training_calendars must be positive")
    if cutoff is not None:
        if len(cutoff) != 2:
            raise EnsembleInputError("cutoff must be a (season, week) pair")
        cutoff = (
            _calendar(cutoff[0], "cutoff.season"),
            _calendar(cutoff[1], "cutoff.week"),
        )
    frames = {
        "internal": _normalize(internal, "internal", "prediction"),
        "underdog": _normalize(underdog, "underdog", "prediction"),
        "consensus": _normalize(consensus, "consensus", "prediction"),
    }
    actual = _normalize(actuals, "actuals", "actual")
    expected = set(map(tuple, actual[list(_KEYS)].to_records(index=False)))
    for name, frame in frames.items():
        keys = set(map(tuple, frame[list(_KEYS)].to_records(index=False)))
        if keys != expected:
            raise EnsembleInputError(f"{name} keys are missing or misaligned")
    merged = actual.rename(columns={"actual": "target"})
    for name, frame in frames.items():
        merged = merged.merge(
            frame.rename(columns={"prediction": name}),
            on=list(_KEYS),
            how="inner",
            validate="one_to_one",
        )
    if cutoff is not None:
        merged = merged.loc[
            merged[["season", "week"]]
            .apply(tuple, axis=1)
            .map(lambda key: key <= cutoff)
        ].reset_index(drop=True)
    calendars = sorted(set(zip(merged.season, merged.week)))
    if len(calendars) <= min_training_calendars:
        return EnsembleTournamentResult(
            pd.DataFrame(),
            pd.DataFrame(),
            False,
            "inconclusive_insufficient_outer_folds",
            {},
            {
                "status": "inconclusive",
                "edge_claim": False,
                "mean_absolute_error": None,
                "lower": None,
                "upper": None,
            },
            {},
            {
                "component_loss": {},
                "fantasy_point_mae": None,
                "crps": None,
                "spearman_rank_correlation": None,
                "top_k_recall": None,
                "calibration_by_position": {},
            },
        )
    scored: list[dict[str, object]] = []
    fold_scores: list[dict[str, object]] = []
    counts: dict[str, int] = {}
    for index, calendar in enumerate(calendars):
        if index < max(2, min_training_calendars):
            continue
        prior = (
            merged[["season", "week"]]
            .apply(tuple, axis=1)
            .map(lambda key: key < calendar)
        )
        test = merged.loc[
            merged["season"].eq(calendar[0]) & merged["week"].eq(calendar[1])
        ]
        train = merged.loc[prior]
        if train.empty:
            continue
        train_calendars = sorted(set(zip(train.season, train.week)))
        inner_results: dict[str, list[float]] = {}
        weight_by_name: dict[str, tuple[float, float, float]] = {}
        candidates = [
            (name, tuple(float(x == name) for x in _NAMES)) for name in _NAMES
        ]
        candidates += [
            (
                "blend:"
                + "+".join(
                    f"{name}={weight:g}"
                    for name, weight in zip(_NAMES, weights)
                    if weight
                ),
                weights,
            )
            for weights in _WEIGHTS
        ]
        for name, weights in candidates:
            weight_by_name[name] = weights
            inner_results[name] = []
        for inner_index, inner_calendar in enumerate(train_calendars[1:], 1):
            inner_train = train.loc[
                train[["season", "week"]]
                .apply(tuple, axis=1)
                .map(lambda key: key < inner_calendar)
            ]
            inner_test = train.loc[
                train["season"].eq(inner_calendar[0])
                & train["week"].eq(inner_calendar[1])
            ]
            if inner_train.empty:
                continue
            for name, weights in candidates:
                prediction = sum(
                    inner_test[source] * weight
                    for source, weight in zip(_NAMES, weights)
                )
                inner_results[name].append(_mae(prediction - inner_test.target))
        if not any(inner_results.values()):
            continue
        selected_name = min(
            (name for name, values in inner_results.items() if values),
            key=lambda name: (
                sum(inner_results[name]) / len(inner_results[name]),
                name,
            ),
        )
        weights = weight_by_name[selected_name]
        best_blend_mae = min(
            sum(values) / len(values)
            for name, values in inner_results.items()
            if name.startswith("blend:") and values
        )
        counts[selected_name] = counts.get(selected_name, 0) + len(test)
        pred = sum(test[name] * weight for name, weight in zip(_NAMES, weights))
        baseline_errors = {name: (test[name] - test.target).abs() for name in _NAMES}
        selected_errors = (pred - test.target).abs()
        actual_rank = test.target.rank(method="average")
        prediction_rank = pd.Series(pred, index=test.index).rank(method="average")
        fold_spearman = (
            actual_rank.corr(prediction_rank)
            if actual_rank.nunique() > 1 and prediction_rank.nunique() > 1
            else 0.0
        )
        fold_spearman = float(fold_spearman) if pd.notna(fold_spearman) else 0.0
        k = max(1, int(len(test) * 0.1))
        top_actual = set(test.nlargest(k, "target").index)
        top_pred = set(pd.Series(pred, index=test.index).nlargest(k).index)
        for row, prediction, error in zip(
            test.itertuples(index=False), pred, selected_errors
        ):
            scored.append(
                {
                    "player_id": row.player_id,
                    "season": row.season,
                    "week": row.week,
                    "prediction": float(prediction),
                    "actual": float(row.target),
                    "selected_model": selected_name,
                    "source_identity": "internal+underdog+consensus",
                    "selection_scope": "prior_calendar_only",
                    "absolute_error": float(error),
                    **({"position": row.position} if hasattr(row, "position") else {}),
                }
            )
        fold_scores.append(
            {
                "season": calendar[0],
                "week": calendar[1],
                "selected_mae": float(selected_errors.mean()),
                "internal_mae": float(baseline_errors["internal"].mean()),
                "underdog_mae": float(baseline_errors["underdog"].mean()),
                "consensus_mae": float(baseline_errors["consensus"].mean()),
                "best_blend_training_mae": float(best_blend_mae),
                "selected_model": selected_name,
                "training_rows": len(train),
                "inner_validation_folds": len(train_calendars) - 1,
                "spearman_rank_correlation": fold_spearman,
                "top_k_recall": float(len(top_actual & top_pred) / len(top_actual)),
            }
        )
    if not scored:
        raise EnsembleInputError("nested evaluation produced no outer test folds")
    scored_frame = pd.DataFrame(scored)
    scores = pd.DataFrame(fold_scores)
    best_baseline = float(scores[[f"{name}_mae" for name in _NAMES]].mean().min())
    selected_mae = float(scores["selected_mae"].mean())
    promoted = selected_mae <= best_baseline + 1e-12
    errors = scored_frame["absolute_error"].to_numpy()
    mean = float(errors.mean())
    if len(scores) < 3:
        uncertainty = {
            "status": "inconclusive",
            "edge_claim": False,
            "mean_absolute_error": mean,
            "lower": None,
            "upper": None,
        }
    else:
        stderr = float(errors.std(ddof=1) / sqrt(len(errors)))
        uncertainty = {
            "status": "reported",
            "edge_claim": bool(promoted and mean + 1.96 * stderr <= best_baseline),
            "mean_absolute_error": mean,
            "lower": max(0.0, mean - 1.96 * stderr),
            "upper": mean + 1.96 * stderr,
        }
    calibration = {
        "mean_signed_error": float(
            (scored_frame["prediction"] - scored_frame["actual"]).mean()
        ),
        "mean_absolute_error": mean,
    }
    if len(scores) < 3:
        promoted = False
        status = "inconclusive_insufficient_outer_rows"
    else:
        status = (
            "promoted_tie_or_better" if promoted else "not_promoted_baseline_better"
        )
    component_loss = {name: float(scores[f"{name}_mae"].mean()) for name in _NAMES}
    spearman = float(scores["spearman_rank_correlation"].mean())
    top_recall = float(scores["top_k_recall"].mean())
    if "position" in scored_frame:
        position_calibration = {
            str(position).strip().upper(): {
                "mean_signed_error": float(
                    (group["prediction"] - group["actual"]).mean()
                ),
                "mean_absolute_error": float(group["absolute_error"].mean()),
            }
            for position, group in scored_frame.groupby("position", dropna=False)
        }
    else:
        position_calibration = {"ALL": calibration}
    metrics: dict[str, object] = {
        "component_loss": component_loss,
        "fantasy_point_mae": mean,
        "crps": mean,
        "crps_definition": (
            "deterministic point-forecast empirical CRPS equals absolute error"
        ),
        "spearman_rank_correlation": spearman,
        "top_k_recall": top_recall,
        "calibration_by_position": position_calibration,
    }
    return EnsembleTournamentResult(
        scored_frame,
        scores,
        promoted,
        status,
        counts,
        uncertainty,
        calibration,
        metrics,
    )


__all__ = [
    "EnsembleInputError",
    "EnsembleTournamentResult",
    "run_nested_ensemble_tournament",
]
