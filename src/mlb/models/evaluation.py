"""Walk-forward evaluation helpers for MLB strikeout model tournaments."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from src.mlb.models.registry import SIMPLE_MODEL_PREFERENCE, ModelSpec
from src.mlb.models.trainers import fit_estimator, predict_estimator


@dataclass(frozen=True, slots=True)
class ChampionSelection:
    """Selected champion model with supporting aggregate metrics."""

    model_name: str
    mean_mae: float
    mean_rmse: float
    mean_r2: float


def build_walk_forward_splits(
    frame: pd.DataFrame,
    *,
    date_col: str = "game_date",
) -> list[tuple[int, pd.Index, pd.Index]]:
    """Build season-based walk-forward splits.

    Each split trains on all seasons <= N-1 and tests on season N.

    Args:
        frame: Dataset containing historical games.
        date_col: Date column used to derive season year.

    Returns:
        List of ``(test_season, train_index, test_index)`` tuples.
    """

    if frame.empty:
        return []

    dated = frame.copy()
    dated[date_col] = pd.to_datetime(dated[date_col], errors="coerce")
    dated = dated[dated[date_col].notna()].copy()
    dated["season"] = dated[date_col].dt.year

    seasons = sorted(int(year) for year in dated["season"].dropna().unique())
    if len(seasons) <= 1:
        return []

    splits: list[tuple[int, pd.Index, pd.Index]] = []
    for season in seasons[1:]:
        train_idx = dated[dated["season"] < season].index
        test_idx = dated[dated["season"] == season].index
        if len(train_idx) == 0 or len(test_idx) == 0:
            continue
        splits.append((season, train_idx, test_idx))
    return splits


def _score_predictions(actual: pd.Series, predicted: pd.Series) -> dict[str, float]:
    """Compute model evaluation metrics for one fold."""

    mae = float(mean_absolute_error(actual, predicted))
    rmse = float(np.sqrt(mean_squared_error(actual, predicted)))
    r2 = float(r2_score(actual, predicted))
    return {"mae": mae, "rmse": rmse, "r2": r2}


def run_walk_forward_tournament(
    frame: pd.DataFrame,
    *,
    specs: Sequence[ModelSpec],
    features: list[str],
    target_col: str = "strikeouts",
    date_col: str = "game_date",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run a model tournament across identical walk-forward splits.

    Args:
        frame: Modeling frame.
        specs: Candidate model specs.
        features: Ordered features used by all candidates.
        target_col: Target column name.
        date_col: Date column used to derive split seasons.

    Returns:
        Tuple of ``(fold_metrics, leaderboard)`` dataframes.
    """

    splits = build_walk_forward_splits(frame, date_col=date_col)
    if not splits:
        raise ValueError("Not enough seasonal history to build walk-forward splits.")

    fold_rows: list[dict[str, float | int | str]] = []
    for season, train_idx, test_idx in splits:
        train_df = frame.loc[train_idx]
        test_df = frame.loc[test_idx]

        for spec in specs:
            model = fit_estimator(
                train_df,
                spec=spec,
                features=features,
                target_col=target_col,
            )
            preds = predict_estimator(test_df, model=model, features=features)
            scores = _score_predictions(test_df[target_col], preds)
            fold_rows.append(
                {
                    "model": spec.name,
                    "test_season": season,
                    "mae": scores["mae"],
                    "rmse": scores["rmse"],
                    "r2": scores["r2"],
                    "train_size": int(len(train_df)),
                    "test_size": int(len(test_df)),
                }
            )

    fold_metrics = pd.DataFrame(fold_rows)
    leaderboard = (
        fold_metrics.groupby("model", as_index=False)
        .agg(
            mean_mae=("mae", "mean"),
            median_mae=("mae", "median"),
            std_mae=("mae", "std"),
            mean_rmse=("rmse", "mean"),
            mean_r2=("r2", "mean"),
            folds=("test_season", "nunique"),
        )
        .sort_values(
            ["mean_mae", "mean_rmse", "mean_r2"],
            ascending=[True, True, False],
        )
        .reset_index(drop=True)
    )
    leaderboard["std_mae"] = leaderboard["std_mae"].fillna(0.0)
    return fold_metrics, leaderboard


def select_champion(
    leaderboard: pd.DataFrame,
    *,
    epsilon: float = 1e-6,
    simplicity_order: Sequence[str] | None = None,
) -> ChampionSelection:
    """Select champion model using deterministic tie-break rules.

    Rules:
    1. Lowest mean MAE
    2. Within epsilon: lower mean RMSE
    3. Within epsilon: higher mean R^2
    4. Within epsilon: simpler model using fixed preference order

    Args:
        leaderboard: Aggregated model leaderboard.
        epsilon: Tolerance for tie comparisons.
        simplicity_order: Ordered model preference from simplest to most complex.

    Returns:
        Champion selection payload.
    """

    if leaderboard.empty:
        raise ValueError("Cannot select champion from empty leaderboard.")

    candidates = leaderboard.copy()
    min_mae = float(candidates["mean_mae"].min())
    candidates = candidates[candidates["mean_mae"] <= min_mae + epsilon]

    min_rmse = float(candidates["mean_rmse"].min())
    candidates = candidates[candidates["mean_rmse"] <= min_rmse + epsilon]

    max_r2 = float(candidates["mean_r2"].max())
    candidates = candidates[candidates["mean_r2"] >= max_r2 - epsilon]

    ranking = list(simplicity_order) if simplicity_order else SIMPLE_MODEL_PREFERENCE
    rank_map = {name: idx for idx, name in enumerate(ranking)}
    candidates["simplicity_rank"] = candidates["model"].map(
        lambda name: rank_map.get(str(name), len(rank_map) + 1)
    )

    winner = candidates.sort_values("simplicity_rank", ascending=True).iloc[0]
    return ChampionSelection(
        model_name=str(winner["model"]),
        mean_mae=float(winner["mean_mae"]),
        mean_rmse=float(winner["mean_rmse"]),
        mean_r2=float(winner["mean_r2"]),
    )
