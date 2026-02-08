"""Walk-forward evaluation helpers for MLB strikeout model tournaments."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from src.core.model_selection import SelectionPolicy, apply_metric_filters
from src.mlb.models.buckets import SegmentationConfig, fit_bucket_model
from src.mlb.models.registry import (
    SIMPLE_MODEL_PREFERENCE,
    ModelSpec,
    resolve_trial_params,
)
from src.mlb.models.trainers import fit_estimator, predict_estimator


@dataclass(frozen=True, slots=True)
class ChampionSelection:
    """Selected champion model with supporting aggregate metrics."""

    model_name: str
    mean_mae: float
    mean_rmse: float
    mean_r2: float
    strategy_name: str = "global"
    trial_id: int = 0
    params: dict[str, float | int] | None = None


def build_walk_forward_splits(
    frame: pd.DataFrame,
    *,
    date_col: str = "game_date",
) -> list[tuple[int, pd.Index, pd.Index]]:
    """Build season-based walk-forward splits.

    Each split trains on all seasons <= N-1 and tests on season N.
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


def probability_calibration_report(
    frame: pd.DataFrame,
    *,
    actual_col: str,
    probability_col: str,
    bins: int = 10,
) -> tuple[dict[str, float], pd.DataFrame]:
    """Compute probability calibration summary and reliability table.

    Args:
        frame: Input rows with actual outcomes and model probabilities.
        actual_col: Binary outcome column (0/1).
        probability_col: Predicted probability column in [0, 1].
        bins: Number of equal-width bins for reliability table.

    Returns:
        Tuple of ``(summary, by_bin)`` where summary contains brier score,
        log loss, expected calibration error (ece), and row count.
    """

    clipped_bins = max(2, int(bins))
    subset = frame[[actual_col, probability_col]].copy()
    subset[actual_col] = pd.to_numeric(subset[actual_col], errors="coerce")
    subset[probability_col] = pd.to_numeric(subset[probability_col], errors="coerce")
    subset = subset.dropna(subset=[actual_col, probability_col]).copy()
    subset = subset[
        subset[actual_col].isin([0.0, 1.0])
        & subset[probability_col].between(0.0, 1.0, inclusive="both")
    ].copy()

    if subset.empty:
        empty = pd.DataFrame(
            columns=[
                "bin",
                "count",
                "mean_probability",
                "observed_rate",
                "abs_calibration_gap",
            ]
        )
        return {
            "rows": 0.0,
            "brier_score": np.nan,
            "log_loss": np.nan,
            "ece": np.nan,
        }, empty

    y = subset[actual_col].to_numpy(dtype=float)
    p = subset[probability_col].to_numpy(dtype=float)
    p_clip = np.clip(p, 1e-6, 1.0 - 1e-6)

    brier = float(np.mean((p - y) ** 2))
    log_loss = float(-(y * np.log(p_clip) + (1.0 - y) * np.log(1.0 - p_clip)).mean())

    subset["bin"] = pd.cut(
        subset[probability_col],
        bins=np.linspace(0.0, 1.0, clipped_bins + 1),
        include_lowest=True,
        labels=False,
    )
    by_bin = (
        subset.groupby("bin", as_index=False)
        .agg(
            count=(actual_col, "size"),
            mean_probability=(probability_col, "mean"),
            observed_rate=(actual_col, "mean"),
        )
        .sort_values("bin", kind="stable")
    )
    by_bin["abs_calibration_gap"] = (
        by_bin["mean_probability"] - by_bin["observed_rate"]
    ).abs()
    ece = float(
        (
            by_bin["abs_calibration_gap"]
            * (by_bin["count"] / max(float(by_bin["count"].sum()), 1.0))
        ).sum()
    )

    summary = {
        "rows": float(len(subset)),
        "brier_score": brier,
        "log_loss": log_loss,
        "ece": ece,
    }
    return summary, by_bin.reset_index(drop=True)


def _resolve_strategy_labels(
    strategy: str,
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    *,
    segmentation: SegmentationConfig,
) -> tuple[str, pd.Series, pd.Series, object | None]:
    """Return effective strategy name and bucket labels for train/test rows."""

    if strategy == "global":
        return (
            "global",
            pd.Series("global", index=train_df.index),
            pd.Series("global", index=test_df.index),
            None,
        )

    try:
        model = fit_bucket_model(strategy, train_df, settings=segmentation)
        return strategy, model.assign(train_df), model.assign(test_df), model
    except ValueError:
        if strategy == "kmeans":
            # Decision-locked fallback: kmeans failures degrade to quantile buckets.
            fallback = fit_bucket_model("quantile3", train_df, settings=segmentation)
            return (
                "quantile3",
                fallback.assign(train_df),
                fallback.assign(test_df),
                fallback,
            )
        return (
            "global",
            pd.Series("global", index=train_df.index),
            pd.Series("global", index=test_df.index),
            None,
        )


def _predict_for_strategy(
    *,
    spec: ModelSpec,
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    features: list[str],
    target_col: str,
    train_labels: pd.Series,
    test_labels: pd.Series,
    min_bucket_size: int,
    params: dict[str, float | int] | None = None,
) -> pd.Series:
    """Fit/predict for one model under one strategy assignment."""

    if (train_labels == "global").all() and (test_labels == "global").all():
        model = fit_estimator(
            train_df,
            spec=spec,
            features=features,
            target_col=target_col,
            params=params,
        )
        return predict_estimator(test_df, model=model, features=features)

    preds = pd.Series(index=test_df.index, dtype=float)
    global_model = fit_estimator(
        train_df,
        spec=spec,
        features=features,
        target_col=target_col,
        params=params,
    )

    for bucket in sorted(test_labels.unique()):
        bucket_test_idx = test_labels[test_labels == bucket].index
        bucket_train_idx = train_labels[train_labels == bucket].index

        if len(bucket_train_idx) < min_bucket_size:
            bucket_preds = predict_estimator(
                test_df.loc[bucket_test_idx],
                model=global_model,
                features=features,
            )
            preds.loc[bucket_test_idx] = bucket_preds.values
            continue

        bucket_model = fit_estimator(
            train_df.loc[bucket_train_idx],
            spec=spec,
            features=features,
            target_col=target_col,
            params=params,
        )
        bucket_preds = predict_estimator(
            test_df.loc[bucket_test_idx],
            model=bucket_model,
            features=features,
        )
        preds.loc[bucket_test_idx] = bucket_preds.values

    return preds


def run_walk_forward_tournament(
    frame: pd.DataFrame,
    *,
    specs: Sequence[ModelSpec],
    features: list[str],
    target_col: str = "strikeouts",
    date_col: str = "game_date",
    strategies: Sequence[str] | None = None,
    segmentation: SegmentationConfig | None = None,
    max_trials_per_model: int = 1,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run a model tournament across identical walk-forward splits.

    Returns ``(fold_metrics, leaderboard)`` where leaderboard aggregates by
    strategy+model when segmented strategies are enabled.
    """

    splits = build_walk_forward_splits(frame, date_col=date_col)
    if not splits:
        raise ValueError("Not enough seasonal history to build walk-forward splits.")

    segmentation_cfg = segmentation or SegmentationConfig(enabled=False)
    strategy_list = list(strategies or ["global"])

    fold_rows: list[dict[str, float | int | str]] = []
    for season, train_idx, test_idx in splits:
        train_df = frame.loc[train_idx]
        test_df = frame.loc[test_idx]

        for strategy in strategy_list:
            effective_strategy, train_labels, test_labels, _ = _resolve_strategy_labels(
                strategy,
                train_df,
                test_df,
                segmentation=segmentation_cfg,
            )

            for spec in specs:
                for trial_id, params in enumerate(
                    resolve_trial_params(spec, max_trials=max_trials_per_model)
                ):
                    preds = _predict_for_strategy(
                        spec=spec,
                        train_df=train_df,
                        test_df=test_df,
                        features=features,
                        target_col=target_col,
                        train_labels=train_labels,
                        test_labels=test_labels,
                        min_bucket_size=segmentation_cfg.min_bucket_size,
                        params=params,
                    )
                    scores = _score_predictions(test_df[target_col], preds)
                    fold_rows.append(
                        {
                            "requested_strategy": strategy,
                            "effective_strategy": effective_strategy,
                            "model": spec.name,
                            "trial_id": int(trial_id),
                            "params_json": json.dumps(params, sort_keys=True),
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
        fold_metrics.groupby(
            ["effective_strategy", "model", "trial_id", "params_json"], as_index=False
        )
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
    leaderboard.rename(columns={"effective_strategy": "strategy"}, inplace=True)
    leaderboard["std_mae"] = leaderboard["std_mae"].fillna(0.0)
    return fold_metrics, leaderboard


def select_champion(
    leaderboard: pd.DataFrame,
    *,
    primary_metric: str = "mae",
    tie_breakers: Sequence[str] | None = None,
    epsilon: float = 1e-6,
    simplicity_order: Sequence[str] | None = None,
) -> ChampionSelection:
    """Select champion model/strategy using deterministic tie-break rules."""

    if leaderboard.empty:
        raise ValueError("Cannot select champion from empty leaderboard.")

    policy = SelectionPolicy(
        primary_metric=primary_metric,
        tie_breakers=tuple(tie_breakers or ["rmse", "r2"]),
        epsilon=epsilon,
    )
    candidates = apply_metric_filters(leaderboard, policy=policy)

    ranking = list(simplicity_order) if simplicity_order else SIMPLE_MODEL_PREFERENCE
    rank_map = {name: idx for idx, name in enumerate(ranking)}
    candidates = candidates.copy()
    candidates["simplicity_rank"] = candidates["model"].map(
        lambda name: rank_map.get(str(name), len(rank_map) + 1)
    )

    winner = candidates.sort_values("simplicity_rank", ascending=True).iloc[0]
    return ChampionSelection(
        model_name=str(winner["model"]),
        strategy_name=str(winner.get("strategy", "global")),
        trial_id=int(winner.get("trial_id", 0)),
        params=json.loads(str(winner.get("params_json", "{}"))),
        mean_mae=float(winner["mean_mae"]),
        mean_rmse=float(winner["mean_rmse"]),
        mean_r2=float(winner["mean_r2"]),
    )
