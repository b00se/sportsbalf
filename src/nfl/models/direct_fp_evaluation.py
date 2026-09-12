"""Leakage-safe rolling-origin evaluation for the direct fantasy-point benchmark."""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

from .baseline_evaluation import BaselineEvaluationResult, _build_metrics, _validate
from .fantasy_scoring import direct_fantasy_points_benchmark

Mode = Literal["weekly", "season"]


def evaluate_direct_fantasy_points(
    frame: pd.DataFrame,
    *,
    target_col: str = "fantasy_points",
    mode: Mode = "weekly",
    window: int = 6,
) -> BaselineEvaluationResult:
    """Evaluate direct trailing fantasy points on strict outer folds.

    Each fold calls :func:`direct_fantasy_points_benchmark` with only rows
    strictly before the fold.  Results are intentionally marked eligible or
    inconclusive only; this evaluator never promotes a candidate.
    """
    if mode not in ("weekly", "season"):
        raise ValueError("mode must be 'weekly' or 'season'")
    if (
        isinstance(window, (bool, np.bool_))
        or not isinstance(window, int)
        or window < 1
    ):
        raise ValueError("window must be a positive integer")
    work = _validate(frame, target_col)
    keys = list(
        work[["season", "week"]].drop_duplicates().itertuples(index=False, name=None)
    )
    seasons = sorted(work["season"].unique())
    folds = keys if mode == "weekly" else [(season, None) for season in seasons]
    predictions: list[pd.DataFrame] = []
    for season, week in folds:
        if mode == "weekly":
            train = work[
                (work["season"] < season)
                | ((work["season"] == season) & (work["week"] < week))
            ]
            test = work[(work["season"] == season) & (work["week"] == week)].copy()
            fold_id = f"{season}-{week}"
        else:
            train = work[work["season"] < season]
            test = work[work["season"] == season].copy()
            fold_id = str(season)
        if train.empty or test.empty:
            continue
        history = train[["player_id", "season", "week", target_col]].rename(
            columns={target_col: "fantasy_points"}
        )
        targets = test[["player_id", "season", "week"]]
        candidate = direct_fantasy_points_benchmark(
            history, targets, window=window
        )
        actual = test[
            ["season", "week", "player_id", "position", target_col]
        ].rename(columns={target_col: "actual"})
        candidate = candidate.merge(
            actual, on=["season", "week", "player_id"], how="left"
        )
        candidate["outer_fold"] = fold_id
        candidate["baseline"] = "direct_fantasy_points"
        candidate["prediction"] = candidate["direct_fantasy_points"].astype(float)
        candidate["window"] = window
        candidate["fantasy_points"] = candidate["actual"]
        predictions.append(
            candidate[[
                "season", "week", "player_id", "position", "fantasy_points",
                "actual", "baseline", "prediction", "outer_fold", "window",
                "fallback_used", "fallback_reason",
            ]]
        )
    prediction_frame = (
        pd.concat(predictions, ignore_index=True)
        if predictions
        else pd.DataFrame(
            columns=[
                "season",
                "week",
                "player_id",
                "position",
                "fantasy_points",
                "actual",
                "baseline",
                "prediction",
                "outer_fold",
                "window",
            ]
        )
    )
    outer_folds = (
        prediction_frame["outer_fold"].nunique() if not prediction_frame.empty else 0
    )
    status = "eligible_for_candidate_comparison" if outer_folds >= 3 else "inconclusive"
    metrics = (
        _build_metrics(prediction_frame, "actual")
        if not prediction_frame.empty
        else pd.DataFrame()
    )
    return BaselineEvaluationResult(prediction_frame, metrics, status, mode)


def evaluate_weekly_direct_fantasy_points(
    frame: pd.DataFrame, *, target_col: str = "fantasy_points", window: int = 6
) -> BaselineEvaluationResult:
    """Evaluate the direct-FP benchmark with weekly outer folds."""
    return evaluate_direct_fantasy_points(
        frame, target_col=target_col, mode="weekly", window=window
    )


def evaluate_season_direct_fantasy_points(
    frame: pd.DataFrame, *, target_col: str = "fantasy_points", window: int = 6
) -> BaselineEvaluationResult:
    """Evaluate the direct-FP benchmark with season-held-out outer folds."""
    return evaluate_direct_fantasy_points(
        frame, target_col=target_col, mode="season", window=window
    )


__all__ = [
    "evaluate_direct_fantasy_points",
    "evaluate_weekly_direct_fantasy_points",
    "evaluate_season_direct_fantasy_points",
]
