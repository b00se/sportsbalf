"""Leakage-safe baseline evaluation for canonical NFL player-week outcomes.

The evaluator deliberately contains no model fitting.  It provides useful,
reproducible reference points before more elaborate component models are
compared with the same walk-forward protocol.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

Mode = Literal["weekly", "season"]


@dataclass(frozen=True, slots=True)
class BaselineEvaluationResult:
    """Predictions, metrics, and promotion status for one evaluation run."""

    predictions: pd.DataFrame
    metrics: pd.DataFrame
    status: str
    mode: Mode


def _validate(frame: pd.DataFrame, target_col: str) -> pd.DataFrame:
    required = {"season", "week", "player_id", "position", target_col}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"missing required columns: {missing}")
    work = frame.copy(deep=True)
    for col in ("season", "week"):
        if work[col].map(lambda value: isinstance(value, (bool, np.bool_))).any():
            raise ValueError(f"{col} must not contain boolean values")
        vals = pd.to_numeric(work[col], errors="coerce")
        if vals.isna().any() or (vals % 1 != 0).any():
            raise ValueError(f"{col} must contain integer calendar values")
        work[col] = vals.astype(int)
    if (
        (work["season"] < 1900).any()
        or (work["week"] < 1).any()
        or (work["week"] > 22).any()
    ):
        raise ValueError("invalid season/week calendar values; NFL week must be 1-22")
    for col in ("player_id", "position"):
        if work[col].isna().any() or work[col].astype(str).str.strip().eq("").any():
            raise ValueError(f"{col} must be non-empty")
    target = pd.to_numeric(work[target_col], errors="coerce")
    if target.isna().any() or (~np.isfinite(target)).any():
        raise ValueError("outcomes must be finite numeric values")
    work[target_col] = target.astype(float)
    if work.duplicated(["season", "week", "player_id"]).any():
        raise ValueError("duplicate player-week keys")
    return work.sort_values(
        ["season", "week", "player_id"], kind="mergesort"
    ).reset_index(drop=True)


def _spearman(pred: pd.Series, actual: pd.Series) -> float:
    if len(pred) < 2 or pred.nunique() < 2 or actual.nunique() < 2:
        return float("nan")
    return float(pred.rank(method="average").corr(actual.rank(method="average")))


def _top_recall(
    pred: pd.Series, actual: pd.Series, ids: pd.Series, k: int = 5
) -> float:
    n = min(k, len(pred))
    if n == 0:
        return float("nan")
    return float(
        len(
            set(ids.loc[pred.nlargest(n).index])
            & set(ids.loc[actual.nlargest(n).index])
        )
        / n
    )


def _build_metrics(pred: pd.DataFrame, target_col: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    groups = [("overall", None, pred)] + [
        ("position", p, g) for p, g in pred.groupby("position", sort=True)
    ]
    for scope, key, group in groups:
        for baseline, b in group.groupby("baseline", sort=True):
            err = b["prediction"] - b[target_col]
            rows.append(
                {
                    "scope": scope,
                    "group": key,
                    "baseline": baseline,
                    "rows": len(b),
                    "mae": float(err.abs().mean()),
                    "crps": float(err.abs().mean()),
                    "mean_error": float(err.mean()),
                    "coverage": float((err.abs() <= err.abs().mean()).mean()),
                    "spearman_rank": _spearman(b["prediction"], b[target_col]),
                    "top_k_recall": _top_recall(
                        b["prediction"], b[target_col], b["player_id"]
                    ),
                }
            )
    weekly = pred.groupby(["season", "week"], sort=True)
    for (season, week), slate in weekly:
        for baseline, b in slate.groupby("baseline", sort=True):
            err = b["prediction"] - b[target_col]
            rows.append(
                {
                    "scope": "week",
                    "group": f"{season}-{week}",
                    "baseline": baseline,
                    "rows": len(b),
                    "mae": float(err.abs().mean()),
                    "crps": float(err.abs().mean()),
                    "mean_error": float(err.mean()),
                    "coverage": float((err.abs() <= err.abs().mean()).mean()),
                    "spearman_rank": _spearman(b["prediction"], b[target_col]),
                    "top_k_recall": _top_recall(
                        b["prediction"], b[target_col], b["player_id"]
                    ),
                }
            )
    return pd.DataFrame(rows)


def evaluate_baselines(
    frame: pd.DataFrame, *, target_col: str = "fantasy_points", mode: Mode = "weekly"
) -> BaselineEvaluationResult:
    """Evaluate fixed naive baselines using only strictly earlier observations.

    ``weekly`` uses expanding rolling-origin folds (one calendar week at a
    time); ``season`` holds each season out and trains only on earlier seasons.
    """
    if mode not in ("weekly", "season"):
        raise ValueError("mode must be 'weekly' or 'season'")
    work = _validate(frame, target_col)
    keys = list(
        work[["season", "week"]].drop_duplicates().itertuples(index=False, name=None)
    )
    seasons = sorted(work["season"].unique())
    preds: list[pd.DataFrame] = []
    for key in keys if mode == "weekly" else [(s, None) for s in seasons]:
        if mode == "weekly":
            season, week = key
            train = work[
                (work["season"] < season)
                | ((work["season"] == season) & (work["week"] < week))
            ]
            test = work[(work["season"] == season) & (work["week"] == week)].copy()
        else:
            season = key[0]
            train = work[work["season"] < season]
            test = work[work["season"] == season].copy()
        if train.empty or test.empty:
            continue
        player = train.groupby("player_id")[target_col].mean()
        pos = train.groupby("position")[target_col].mean()
        global_mean = float(train[target_col].mean())
        test["player_trailing_mean"] = (
            test["player_id"]
            .map(player)
            .fillna(test["position"].map(pos))
            .fillna(global_mean)
        )
        test["position_historical_mean"] = test["position"].map(pos).fillna(global_mean)
        long = test.melt(
            id_vars=["season", "week", "player_id", "position", target_col],
            value_vars=["player_trailing_mean", "position_historical_mean"],
            var_name="baseline",
            value_name="prediction",
        )
        preds.append(long)
    prediction_frame = (
        pd.concat(preds, ignore_index=True)
        if preds
        else pd.DataFrame(
            columns=[
                "season",
                "week",
                "player_id",
                "position",
                target_col,
                "baseline",
                "prediction",
            ]
        )
    )
    outer_weeks = prediction_frame[["season", "week"]].drop_duplicates().shape[0]
    status = (
        "eligible_for_candidate_comparison"
        if outer_weeks >= 3
        else "inconclusive"
    )
    return BaselineEvaluationResult(
        prediction_frame,
        _build_metrics(prediction_frame, target_col)
        if not prediction_frame.empty
        else pd.DataFrame(),
        status,
        mode,
    )


def evaluate_weekly_baselines(
    frame: pd.DataFrame, *, target_col: str = "fantasy_points"
) -> BaselineEvaluationResult:
    """Evaluate expanding weekly baselines."""
    return evaluate_baselines(frame, target_col=target_col, mode="weekly")


def evaluate_season_baselines(
    frame: pd.DataFrame, *, target_col: str = "fantasy_points"
) -> BaselineEvaluationResult:
    """Evaluate season-held-out baselines."""
    return evaluate_baselines(frame, target_col=target_col, mode="season")


__all__ = [
    "BaselineEvaluationResult",
    "evaluate_baselines",
    "evaluate_weekly_baselines",
    "evaluate_season_baselines",
]
