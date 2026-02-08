from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from src.core.contracts import PipelineConfig
from src.mlb.models.strategy import predict_with_strategy_artifact
from src.mlb.pitcher_props.descriptors import get_stat_descriptor
from src.mlb.pitcher_props.pipeline import (
    _model_features,
    _persist_label_quality_report,
    _train_or_load,
    run_mlb_pitcher_prop_pipeline,
)


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
            "final_holdout": {
                "enabled": True,
                "seasons": 1,
                "baseline_model": "xgboost",
                "report_path": str(tmp_path / "outs_final_holdout.csv"),
            },
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
    report = pd.read_csv(tmp_path / "outs_final_holdout.csv")
    assert {"baseline", "champion"} == set(report["model_role"].tolist())


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


def test_label_quality_report_tracks_earned_runs_fallback_share(tmp_path: Path) -> None:
    descriptor = get_stat_descriptor("earned_runs")
    games = pd.DataFrame(
        {
            "game_date": ["2024-04-01", "2024-04-02", "2025-04-01"],
            "earned_runs": [1.0, 2.0, 0.0],
            "earned_runs_fallback_used": [0, 1, 1],
            "earned_runs_high_fidelity_used": [1, 0, 0],
        }
    )
    output = tmp_path / "earned_runs_label_quality.csv"

    _persist_label_quality_report(
        games,
        descriptor=descriptor,
        report_path=str(output),
    )

    report = pd.read_csv(output).sort_values("season").reset_index(drop=True)
    assert report["fallback_rows"].tolist() == [1, 1]
    assert report["rows"].tolist() == [2, 1]
    assert report["high_fidelity_rows"].tolist() == [1, 0]


def test_pipeline_retrains_when_loaded_artifact_is_incompatible(monkeypatch) -> None:
    descriptor = get_stat_descriptor("outs_recorded")
    model_frame = _synthetic_pitcher_prop_frame(descriptor.target_col).copy()
    model_frame["game_date"] = pd.to_datetime(model_frame["game_date"])
    features = _model_features(descriptor)
    for column in features:
        if column not in model_frame.columns:
            model_frame[column] = 0.0

    train_or_load_calls: list[bool] = []

    def _fake_train_or_load(frame, *, section, descriptor, retrain):
        train_or_load_calls.append(bool(retrain))
        if len(train_or_load_calls) == 1:
            return "incompatible", "xgboost", "global"
        return "compatible", "xgboost", "global"

    def _fake_predict(frame, *, artifact, features, name="prediction"):
        if artifact == "incompatible":
            raise ValueError("feature mismatch")
        return pd.Series(np.full(len(frame), 10.0), index=frame.index, name=name)

    monkeypatch.setattr(
        "src.mlb.pitcher_props.pipeline._build_training_games",
        lambda section, descriptor: model_frame.copy(),
    )
    monkeypatch.setattr(
        "src.mlb.pitcher_props.pipeline._persist_label_quality_report",
        lambda games, descriptor, report_path: None,
    )
    monkeypatch.setattr(
        "src.mlb.pitcher_props.pipeline._train_or_load",
        _fake_train_or_load,
    )
    monkeypatch.setattr(
        "src.mlb.pitcher_props.pipeline.predict_with_strategy_artifact",
        _fake_predict,
    )
    monkeypatch.setattr(
        "src.mlb.pitcher_props.pipeline.load_pitcher_prop_lines",
        lambda _path, line_col: pd.DataFrame(
            [
                {
                    "player": "123",
                    line_col: 15.5,
                    "over_decimal_price": 1.9,
                    "under_decimal_price": 1.9,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        "src.mlb.pitcher_props.pipeline._build_prediction_rows",
        lambda lines, games, descriptor, target_date: pd.DataFrame(
            [
                {
                    "player": "123",
                    "pitcher_id": 123,
                    descriptor.line_col: 15.5,
                    "prediction": np.nan,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        "src.mlb.pitcher_props.pipeline.apply_simulations",
        lambda lines, **kwargs: lines.assign(
            prob_over=0.5,
            prob_under=0.5,
            prob_push=0.0,
            ev_over=0.0,
            ev_under=0.0,
            edge_over=0.0,
            edge_under=0.0,
            simulated_mean=10.0,
            simulated_std=1.0,
            simulated_median=10.0,
        ),
    )

    config = PipelineConfig(
        config_path=Path("config/mlb.yaml"),
        sport="mlb",
        stat="outs_recorded",
        raw={},
        section={
            "model_path": "models/tmp.joblib",
            "lines_path": "tests/testdata/outs_lines.csv",
            "fallback_std": 1.0,
        },
    )
    result = run_mlb_pitcher_prop_pipeline(config, retrain=False)

    assert train_or_load_calls == [False, True]
    assert "predicted_outs_recorded" in result.columns


def test_strikeouts_pipeline_applies_live_context_enrichment(monkeypatch) -> None:
    descriptor = get_stat_descriptor("strikeouts")
    model_frame = _synthetic_pitcher_prop_frame(descriptor.target_col).copy()
    model_frame["game_date"] = pd.to_datetime(model_frame["game_date"])
    model_frame["opponent_k_rate"] = 0.22
    model_frame["park_factor_K"] = 1.0

    monkeypatch.setattr(
        "src.mlb.pitcher_props.pipeline._build_training_games",
        lambda section, descriptor: model_frame.copy(),
    )
    monkeypatch.setattr(
        "src.mlb.pitcher_props.pipeline._persist_label_quality_report",
        lambda games, descriptor, report_path: None,
    )
    monkeypatch.setattr(
        "src.mlb.pitcher_props.pipeline._train_or_load",
        lambda frame, *, section, descriptor, retrain: (
            "compatible",
            "xgboost",
            "global",
        ),
    )
    monkeypatch.setattr(
        "src.mlb.pitcher_props.pipeline.predict_with_strategy_artifact",
        lambda frame, *, artifact, features, name="prediction": pd.Series(
            np.full(len(frame), 9.0),
            index=frame.index,
            name=name,
        ),
    )
    monkeypatch.setattr(
        "src.mlb.pitcher_props.pipeline.load_pitcher_prop_lines",
        lambda _path, line_col: pd.DataFrame(
            [
                {
                    "player": "123",
                    line_col: 6.5,
                    "over_decimal_price": 1.9,
                    "under_decimal_price": 1.9,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        "src.mlb.pitcher_props.pipeline._build_prediction_rows",
        lambda lines, games, descriptor, target_date: pd.DataFrame(
            [
                {
                    "player": "123",
                    "pitcher_id": 123,
                    "opponent_team": "NYY",
                    descriptor.line_col: 6.5,
                }
            ]
        ),
    )

    class _FakeLiveService:
        def __init__(self, config):
            self.config = config

        def fetch(self, rows, target_date):
            del target_date
            frame = rows[["pitcher_id", "opponent_team"]].copy()
            frame["humidity_pct"] = 66.0
            frame["weather_known_flag"] = 1
            frame["roof_state"] = "open"
            return type(
                "LiveResult",
                (),
                {
                    "frame": frame,
                    "metadata": {
                        "live_feature_set_version": "v1",
                        "live_feature_sources": ["primary", "secondary"],
                        "live_fetch_timestamp": "2026-02-08T00:00:00Z",
                        "cache_age_hours": 0.0,
                        "stale_cache_usage_pct": 0.0,
                        "cache_status": "fresh",
                    },
                },
            )()

    monkeypatch.setattr(
        "src.mlb.pitcher_props.pipeline.LiveContextService",
        _FakeLiveService,
    )
    monkeypatch.setattr(
        "src.mlb.pitcher_props.pipeline.apply_simulations",
        lambda lines, **kwargs: lines.assign(
            prob_over=0.5,
            prob_under=0.5,
            prob_push=0.0,
            ev_over=0.0,
            ev_under=0.0,
            edge_over=0.0,
            edge_under=0.0,
            simulated_mean=9.0,
            simulated_std=1.0,
            simulated_median=9.0,
        ),
    )

    config = PipelineConfig(
        config_path=Path("config/mlb.yaml"),
        sport="mlb",
        stat="strikeouts",
        raw={},
        section={
            "model_path": "models/tmp.joblib",
            "lines_path": "tests/testdata/lines_with_odds.csv",
            "fallback_std": 1.0,
            "live_features": {"enabled": True},
        },
    )

    result = run_mlb_pitcher_prop_pipeline(config, retrain=False)

    assert "predicted_strikeouts" in result.columns
    assert "live_feature_set_version" in result.columns
    assert "live_feature_sources" in result.columns
    assert "live_fetch_timestamp" in result.columns
    assert "cache_age_hours" in result.columns
    assert "stale_cache_usage_pct" in result.columns
    assert float(result.loc[0, "humidity_pct"]) == 66.0
