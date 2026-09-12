"""Leakage-safe rolling-origin evaluation for the direct fantasy-point benchmark."""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

from .baseline_evaluation import BaselineEvaluationResult, _build_metrics, _validate

Mode = Literal["weekly", "season"]


def evaluate_direct_fantasy_points(
    frame: pd.DataFrame,
    *,
    target_col: str = "fantasy_points",
    mode: Mode = "weekly",
    window: int = 6,
) -> BaselineEvaluationResult:
    """Evaluate direct trailing fantasy points on strict outer folds.

    The trailing means are precomputed from rows strictly before each
    calendar fold, with the same semantics as
    :func:`direct_fantasy_points_benchmark`. Results are intentionally marked
    eligible or inconclusive only; this evaluator never promotes a candidate.
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
    calendar_key = work["season"] * 100 + work["week"]
    ordered = work.assign(_calendar_key=calendar_key).sort_values(
        ["player_id", "_calendar_key"], kind="mergesort"
    )
    if mode == "weekly":
        ordered["_direct_prediction"] = (
            ordered.groupby("player_id", sort=False)[target_col]
            .transform(
                lambda values: values.shift(1).rolling(
                    window, min_periods=1
                ).mean()
            )
            .fillna(0.0)
        )
    predictions: list[pd.DataFrame] = []
    for season, week in folds:
        if mode == "weekly":
            cutoff = season * 100 + week
            if not (ordered["_calendar_key"] < cutoff).any():
                continue
            test = ordered[
                (ordered["season"] == season) & (ordered["week"] == week)
            ].copy()
            fold_id = f"{season}-{week}"
        else:
            train = work[work["season"] < season]
            test = work[work["season"] == season].copy()
            fold_id = str(season)
        if test.empty or (mode == "season" and train.empty):
            continue
        if mode == "season":
            history = train.sort_values(
                ["season", "week", "player_id"], kind="mergesort"
            )
            trailing = history.groupby("player_id", sort=False).tail(window)
            means = trailing.groupby("player_id")[target_col].mean()
            candidate_prediction = test["player_id"].map(means).fillna(0.0)
        else:
            candidate_prediction = test["_direct_prediction"]
        candidate = test[["season", "week", "player_id", "position"]].copy()
        candidate["actual"] = test[target_col].astype(float)
        candidate["baseline"] = "direct_fantasy_points"
        candidate["prediction"] = candidate_prediction.astype(float)
        candidate["window"] = window
        candidate["fantasy_points"] = candidate["actual"]
        candidate["outer_fold"] = fold_id
        history_players = (
            train["player_id"]
            if mode == "season"
            else ordered.loc[
                ordered["_calendar_key"] < season * 100 + week, "player_id"
            ]
        )
        candidate["fallback_used"] = ~candidate["player_id"].isin(history_players)
        candidate["fallback_reason"] = candidate["fallback_used"].map(
            {True: "no_player_history", False: "none"}
        )
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
    metrics = _build_metrics(prediction_frame, "actual")
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
