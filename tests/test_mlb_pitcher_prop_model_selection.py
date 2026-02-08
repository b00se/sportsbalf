from __future__ import annotations

from pathlib import Path

import pandas as pd
from src.mlb.models.strategy import predict_with_strategy_artifact
from src.mlb.pitcher_props.descriptors import get_stat_descriptor
from src.mlb.pitcher_props.pipeline import _model_features, _train_or_load


def _synthetic_pitcher_prop_frame(target_col: str) -> pd.DataFrame:
    rows: list[dict[str, float | int | str]] = []
    for season in (2022, 2023, 2024, 2025):
        for idx in range(1, 7):
            rows.append(
                {
                    "game_date": f"{season}-05-{idx:02d}",
                    "rolling_K_avg_3": 4.5 + idx * 0.2,
                    "rolling_K_avg_5": 4.8 + idx * 0.2,
                    "rolling_pitch_count_5": 82.0 + idx,
                    "rolling_K_rate": 0.18 + idx * 0.004,
                    "rest_days": 4 + (idx % 3),
                    "rolling_on_base_events_allowed_5": 0.25 + idx * 0.01,
                    "rolling_hard_contact_allowed_5": 0.33 + idx * 0.005,
                    "opponent_out_rate": 14.0 + idx * 0.2,
                    "park_factor_outs": 0.95 + idx * 0.01,
                    target_col: 12 + (idx % 5),
                }
            )
    return pd.DataFrame(rows)


def test_pitcher_props_model_selection_roundtrip_predicts_with_strategy(
    tmp_path: Path,
) -> None:
    descriptor = get_stat_descriptor("outs_recorded")
    frame = _synthetic_pitcher_prop_frame(descriptor.target_col)
    section = {
        "model_path": str(tmp_path / "outs_baseline.joblib"),
        "model_selection": {
            "enabled": True,
            "candidates": ["poisson", "elastic_net", "hist_gradient_boosting"],
            "champion_model_path": str(tmp_path / "outs_champion.joblib"),
            "champion_metadata_path": str(tmp_path / "outs_champion.json"),
            "leaderboard_path": str(tmp_path / "outs_leaderboard.csv"),
            "segmentation": {
                "enabled": True,
                "bucket_methods": ["quantile3", "kmeans"],
                "min_bucket_size": 2,
            },
        },
    }

    model, model_name, strategy_name = _train_or_load(
        frame,
        section=section,
        descriptor=descriptor,
        retrain=True,
    )
    preds = predict_with_strategy_artifact(
        frame.head(4),
        artifact=model,
        features=_model_features(descriptor),
    )

    assert model_name in {"poisson", "elastic_net", "hist_gradient_boosting"}
    assert strategy_name in {"global", "quantile3", "kmeans"}
    assert preds.notna().all()
    assert (tmp_path / "outs_champion.joblib").exists()
    assert (tmp_path / "outs_champion.json").exists()
    assert (tmp_path / "outs_leaderboard.csv").exists()


def test_pitcher_props_model_selection_falls_back_to_baseline_when_insufficient_history(
    tmp_path: Path,
) -> None:
    descriptor = get_stat_descriptor("outs_recorded")
    frame = _synthetic_pitcher_prop_frame(descriptor.target_col)
    frame["game_date"] = "2025-05-01"
    section = {
        "model_path": str(tmp_path / "outs_baseline.joblib"),
        "model_selection": {
            "enabled": True,
            "champion_model_path": str(tmp_path / "outs_champion.joblib"),
            "champion_metadata_path": str(tmp_path / "outs_champion.json"),
            "leaderboard_path": str(tmp_path / "outs_leaderboard.csv"),
        },
    }

    model, model_name, strategy_name = _train_or_load(
        frame,
        section=section,
        descriptor=descriptor,
        retrain=True,
    )
    preds = predict_with_strategy_artifact(
        frame.head(3),
        artifact=model,
        features=_model_features(descriptor),
    )

    assert model_name == "xgboost"
    assert strategy_name == "global"
    assert preds.notna().all()
    assert (tmp_path / "outs_baseline.joblib").exists()
