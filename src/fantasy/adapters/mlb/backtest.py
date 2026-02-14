"""Walk-forward backtest helpers for Phase 1.5 MLB fantasy projections."""

from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True, slots=True)
class WalkForwardFold:
    """Single walk-forward fold definition."""

    train_seasons: tuple[int, ...]
    test_season: int


def generate_walk_forward_folds(
    seasons: tuple[int, ...]
) -> tuple[WalkForwardFold, ...]:
    """Generate `(train through N-1, test on N)` folds."""

    ordered = tuple(sorted({int(season) for season in seasons}))
    if len(ordered) < 2:
        return ()

    folds: list[WalkForwardFold] = []
    for index in range(1, len(ordered)):
        train = ordered[:index]
        test = ordered[index]
        folds.append(WalkForwardFold(train_seasons=train, test_season=test))
    return tuple(folds)


def aggregate_metric_scores(predictions: pd.DataFrame) -> pd.DataFrame:
    """Aggregate MAE/RMSE/bias by metric id from row-level predictions."""

    required = {"metric_id", "prediction", "actual"}
    if not required.issubset(predictions.columns):
        return pd.DataFrame(columns=["metric_id", "mae", "rmse", "bias", "n"])
    if predictions.empty:
        return pd.DataFrame(columns=["metric_id", "mae", "rmse", "bias", "n"])

    frame = predictions.copy()
    frame["prediction"] = pd.to_numeric(frame["prediction"], errors="coerce")
    frame["actual"] = pd.to_numeric(frame["actual"], errors="coerce")
    frame = frame.dropna(subset=["prediction", "actual"]).copy()
    if frame.empty:
        return pd.DataFrame(columns=["metric_id", "mae", "rmse", "bias", "n"])

    frame["error"] = frame["prediction"] - frame["actual"]
    frame["abs_error"] = frame["error"].abs()
    frame["sq_error"] = frame["error"] ** 2

    grouped = frame.groupby("metric_id", dropna=False)
    summary = grouped.agg(
        mae=("abs_error", "mean"),
        mse=("sq_error", "mean"),
        bias=("error", "mean"),
        n=("error", "size"),
    ).reset_index()
    summary["rmse"] = summary["mse"].map(lambda value: math.sqrt(float(value)))
    return summary[["metric_id", "mae", "rmse", "bias", "n"]]
