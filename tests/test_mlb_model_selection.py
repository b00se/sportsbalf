from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from src.mlb.models.buckets import SegmentationConfig, fit_bucket_model
from src.mlb.models.evaluation import (
    build_walk_forward_splits,
    run_walk_forward_tournament,
    select_champion,
)
from src.mlb.models.predict import FEATURES
from src.mlb.models.registry import DEFAULT_CANDIDATES, resolve_model_specs
from src.mlb.models.strategy import predict_with_strategy_artifact
from src.mlb.models.trainers import fit_estimator, predict_estimator
from src.mlb.pipeline import _train_or_load_serving_model


def _synthetic_training_frame() -> pd.DataFrame:
    rows: list[dict[str, float | int | str]] = []
    for season in (2022, 2023, 2024, 2025):
        for idx in range(1, 7):
            rows.append(
                {
                    "game_date": f"{season}-05-{idx:02d}",
                    "rolling_K_avg_3": 5.0 + idx * 0.2,
                    "rolling_K_avg_5": 5.1 + idx * 0.2,
                    "rolling_pitch_count_5": 85.0 + idx,
                    "rolling_K_rate": 0.22 + idx * 0.005,
                    "opponent_k_pct": 0.20 + idx * 0.003,
                    "opponent_k_rate": 0.21 + idx * 0.003,
                    "park_factor_K": 0.95 + idx * 0.01,
                    "rest_days": 4 + (idx % 3),
                    "strikeouts": 3 + (idx % 5),
                }
            )
    return pd.DataFrame(rows)


def test_registry_candidates_fit_and_predict() -> None:
    frame = _synthetic_training_frame()
    specs = resolve_model_specs(DEFAULT_CANDIDATES)

    for spec in specs:
        model = fit_estimator(frame, spec=spec, features=FEATURES)
        preds = predict_estimator(frame.head(5), model=model, features=FEATURES)

        assert len(preds) == 5
        assert preds.notna().all()


def test_walk_forward_splits_consistent_across_models() -> None:
    frame = _synthetic_training_frame()
    specs = resolve_model_specs(["poisson", "elastic_net", "xgboost"])

    fold_metrics, _ = run_walk_forward_tournament(frame, specs=specs, features=FEATURES)

    expected = {
        (
            int(row["test_season"]),
            int(row["train_size"]),
            int(row["test_size"]),
        )
        for _, row in fold_metrics[fold_metrics["model"] == "poisson"].iterrows()
    }

    for model_name in fold_metrics["model"].unique():
        observed = {
            (
                int(row["test_season"]),
                int(row["train_size"]),
                int(row["test_size"]),
            )
            for _, row in fold_metrics[fold_metrics["model"] == model_name].iterrows()
        }
        assert observed == expected

    splits = build_walk_forward_splits(frame)
    assert len(splits) == 3


def test_walk_forward_includes_quantile_and_kmeans_strategy_rows() -> None:
    frame = _synthetic_training_frame()
    specs = resolve_model_specs(["poisson", "xgboost"])
    segmentation = SegmentationConfig(enabled=True, min_bucket_size=2)

    fold_metrics, leaderboard = run_walk_forward_tournament(
        frame,
        specs=specs,
        features=FEATURES,
        strategies=["global", "quantile3", "kmeans"],
        segmentation=segmentation,
    )

    assert {"global", "quantile3", "kmeans"}.issubset(
        set(leaderboard["strategy"].unique())
    )
    assert "effective_strategy" in fold_metrics.columns


def test_walk_forward_respects_max_trials_per_model() -> None:
    frame = _synthetic_training_frame()
    specs = resolve_model_specs(["poisson"])
    segmentation = SegmentationConfig(enabled=False)

    _, leaderboard = run_walk_forward_tournament(
        frame,
        specs=specs,
        features=FEATURES,
        strategies=["global"],
        segmentation=segmentation,
        max_trials_per_model=2,
    )

    poisson_rows = leaderboard[leaderboard["model"] == "poisson"]
    assert set(poisson_rows["trial_id"].tolist()) == {0, 1}
    assert poisson_rows["params_json"].nunique() == 2


def test_kmeans_bucket_falls_back_to_quantile_when_degenerate() -> None:
    frame = _synthetic_training_frame()

    with pytest.raises(ValueError, match="min_bucket_size"):
        fit_bucket_model(
            "kmeans",
            frame,
            settings=SegmentationConfig(enabled=True, min_bucket_size=1_000),
        )


def test_champion_selection_uses_deterministic_tiebreakers() -> None:
    leaderboard = pd.DataFrame(
        [
            {
                "strategy": "global",
                "model": "elastic_net",
                "mean_mae": 1.0000001,
                "mean_rmse": 1.10,
                "mean_r2": 0.30,
            },
            {
                "strategy": "global",
                "model": "poisson",
                "mean_mae": 1.0000002,
                "mean_rmse": 1.10,
                "mean_r2": 0.30,
            },
            {
                "strategy": "global",
                "model": "xgboost",
                "mean_mae": 1.40,
                "mean_rmse": 1.50,
                "mean_r2": 0.20,
            },
        ]
    )

    winner = select_champion(leaderboard, epsilon=1e-6)
    assert winner.model_name == "poisson"


def test_champion_selection_honors_configured_primary_metric() -> None:
    leaderboard = pd.DataFrame(
        [
            {
                "strategy": "global",
                "model": "random_forest",
                "mean_mae": 2.90,
                "mean_rmse": 3.90,
                "mean_r2": 0.48,
            },
            {
                "strategy": "global",
                "model": "xgboost",
                "mean_mae": 2.95,
                "mean_rmse": 3.70,
                "mean_r2": 0.44,
            },
        ]
    )

    winner = select_champion(
        leaderboard,
        primary_metric="rmse",
        tie_breakers=["mae", "r2"],
        epsilon=1e-6,
    )
    assert winner.model_name == "xgboost"


def test_champion_selection_rejects_unknown_metric_name() -> None:
    leaderboard = pd.DataFrame(
        [
            {
                "strategy": "global",
                "model": "poisson",
                "mean_mae": 2.95,
                "mean_rmse": 4.00,
                "mean_r2": 0.45,
            }
        ]
    )

    with pytest.raises(ValueError, match="Unsupported metric"):
        select_champion(leaderboard, primary_metric="mape")


def test_pipeline_champion_model_roundtrip_predicts_with_strategy(
    tmp_path: Path,
) -> None:
    frame = _synthetic_training_frame()
    section = {
        "model_path": str(tmp_path / "xgb_baseline.joblib"),
        "model_selection": {
            "enabled": True,
            "candidates": ["poisson", "elastic_net", "hist_gradient_boosting"],
            "champion_model_path": str(tmp_path / "champion.joblib"),
            "champion_metadata_path": str(tmp_path / "champion.json"),
            "leaderboard_path": str(tmp_path / "leaderboard.csv"),
            "segmentation": {
                "enabled": True,
                "bucket_methods": ["quantile3", "kmeans"],
                "min_bucket_size": 2,
            },
        },
    }

    model, model_name, strategy_name = _train_or_load_serving_model(
        frame,
        section=section,
        retrain=True,
    )
    preds = predict_with_strategy_artifact(
        frame.head(4),
        artifact=model,
        features=FEATURES,
    )

    assert model_name in {"poisson", "elastic_net", "hist_gradient_boosting"}
    assert strategy_name in {"global", "quantile3", "kmeans"}
    assert preds.notna().all()
    assert (tmp_path / "champion.joblib").exists()
    assert (tmp_path / "champion.json").exists()
    assert (tmp_path / "leaderboard.csv").exists()
