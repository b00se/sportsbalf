"""Walk-forward backtesting utilities for NFL pass attempts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor

from src.nfl.models.predict import DEFAULT_PARAMS


@dataclass(frozen=True, slots=True)
class WalkForwardConfig:
    """Configuration for NFL walk-forward evaluation."""

    min_train_weeks: int = 32
    step_weeks: int = 1
    max_folds: int | None = None
    model_params: Mapping[str, float | int] | None = None


def _sorted_week_keys(frame: pd.DataFrame) -> list[tuple[int, int]]:
    """Return sorted unique ``(season, week)`` keys."""

    keys = (
        frame[["season", "week"]]
        .dropna()
        .drop_duplicates()
        .astype({"season": int, "week": int})
        .sort_values(["season", "week"])
    )
    return [(int(row.season), int(row.week)) for row in keys.itertuples(index=False)]


def _sanitize_xy(
    frame: pd.DataFrame,
    *,
    features: Sequence[str],
    target_col: str,
) -> tuple[pd.DataFrame, pd.Series]:
    """Return model matrix/target with invalid rows removed."""

    x = frame.loc[:, list(features)].apply(pd.to_numeric, errors="coerce")
    y = pd.to_numeric(frame[target_col], errors="coerce")
    mask = x.notna().all(axis=1) & y.notna()
    return x.loc[mask], y.loc[mask]


def _fit_and_score(
    train: pd.DataFrame,
    test: pd.DataFrame,
    *,
    features: Sequence[str],
    target_col: str,
    model_params: Mapping[str, float | int] | None,
) -> dict[str, float]:
    """Fit a model on train and return test metrics."""

    x_train, y_train = _sanitize_xy(train, features=features, target_col=target_col)
    x_test, y_test = _sanitize_xy(test, features=features, target_col=target_col)
    if x_train.empty or x_test.empty:
        return {
            "rows_train": float(len(x_train)),
            "rows_test": float(len(x_test)),
            "rmse": float("nan"),
            "mae": float("nan"),
            "r2": float("nan"),
            "baseline_rmse": float("nan"),
            "baseline_mae": float("nan"),
            "baseline_r2": float("nan"),
        }

    params: dict[str, float | int] = dict(DEFAULT_PARAMS)
    if model_params:
        params.update(dict(model_params))
    model = XGBRegressor(**params)
    model.fit(x_train, y_train)

    pred = pd.Series(model.predict(x_test), index=x_test.index)
    baseline = pd.Series(float(y_train.mean()), index=y_test.index)

    return {
        "rows_train": float(len(x_train)),
        "rows_test": float(len(x_test)),
        "rmse": float(np.sqrt(mean_squared_error(y_test, pred))),
        "mae": float(mean_absolute_error(y_test, pred)),
        "r2": float(r2_score(y_test, pred)),
        "baseline_rmse": float(np.sqrt(mean_squared_error(y_test, baseline))),
        "baseline_mae": float(mean_absolute_error(y_test, baseline)),
        "baseline_r2": float(r2_score(y_test, baseline)),
    }


def run_walk_forward_backtest(
    frame: pd.DataFrame,
    *,
    features: Sequence[str],
    target_col: str = "pass_attempts",
    config: WalkForwardConfig | None = None,
) -> pd.DataFrame:
    """Run leakage-safe walk-forward folds keyed by ``(season, week)``.

    Args:
        frame: NFL model dataset.
        features: Feature column list.
        target_col: Target column name.
        config: Walk-forward configuration.

    Returns:
        Fold-level metrics DataFrame.
    """

    cfg = config or WalkForwardConfig()
    if cfg.min_train_weeks < 1:
        raise ValueError("min_train_weeks must be >= 1")
    if cfg.step_weeks < 1:
        raise ValueError("step_weeks must be >= 1")

    work = frame.copy()
    work["season"] = pd.to_numeric(work["season"], errors="coerce")
    work["week"] = pd.to_numeric(work["week"], errors="coerce")
    work = work.dropna(subset=["season", "week"])
    work["season"] = work["season"].astype(int)
    work["week"] = work["week"].astype(int)

    keys = _sorted_week_keys(work)
    if len(keys) <= cfg.min_train_weeks:
        return pd.DataFrame()

    folds: list[dict[str, Any]] = []
    fold_id = 0

    for train_end_idx in range(cfg.min_train_weeks, len(keys), cfg.step_weeks):
        if cfg.max_folds is not None and fold_id >= cfg.max_folds:
            break

        train_keys = keys[:train_end_idx]
        test_keys = keys[train_end_idx : train_end_idx + cfg.step_weeks]
        if not test_keys:
            break

        train_index = pd.MultiIndex.from_tuples(train_keys, names=["season", "week"])
        test_index = pd.MultiIndex.from_tuples(test_keys, names=["season", "week"])

        key_index = pd.MultiIndex.from_frame(work[["season", "week"]])
        train = work.loc[key_index.isin(train_index)].copy()
        test = work.loc[key_index.isin(test_index)].copy()

        metrics = _fit_and_score(
            train,
            test,
            features=features,
            target_col=target_col,
            model_params=cfg.model_params,
        )
        row: dict[str, Any] = {
            "fold": fold_id,
            "train_start_season": train_keys[0][0],
            "train_start_week": train_keys[0][1],
            "train_end_season": train_keys[-1][0],
            "train_end_week": train_keys[-1][1],
            "test_start_season": test_keys[0][0],
            "test_start_week": test_keys[0][1],
            "test_end_season": test_keys[-1][0],
            "test_end_week": test_keys[-1][1],
        }
        row.update(metrics)
        folds.append(row)
        fold_id += 1

    return pd.DataFrame(folds)


__all__ = ["WalkForwardConfig", "run_walk_forward_backtest"]
