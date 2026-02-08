from __future__ import annotations

from pathlib import Path

import pandas as pd
import scripts.backtest_mlb_strikeouts as backtest_script


def test_prepare_training_frame_rebuilds_historical_live_features_globally(
    monkeypatch,
) -> None:
    frames = {
        "current.csv": pd.DataFrame(
            [
                {
                    "pitcher": 99,
                    "pitcher_id": 99,
                    "game_date": "2024-03-31",
                    "strikeouts": 3,
                    "umpire": "U0",
                }
            ]
        ),
        "hist_a.csv": pd.DataFrame(
            [
                {
                    "pitcher": 1,
                    "pitcher_id": 1,
                    "game_date": "2024-04-01",
                    "strikeouts": 5,
                    "umpire": "UA",
                }
            ]
        ),
        "hist_b.csv": pd.DataFrame(
            [
                {
                    "pitcher": 2,
                    "pitcher_id": 2,
                    "game_date": "2024-04-02",
                    "strikeouts": 7,
                    "umpire": "UA",
                }
            ]
        ),
    }
    for key in frames:
        frames[key]["game_date"] = pd.to_datetime(frames[key]["game_date"])

    monkeypatch.setattr(
        backtest_script,
        "read_csv",
        lambda path: frames[str(path)].copy(),
    )
    monkeypatch.setattr(
        backtest_script,
        "_load_or_create_park_factors",
        lambda *_args, **_kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(
        backtest_script,
        "aggregate_pitcher_games",
        lambda df: df.copy(),
    )
    monkeypatch.setattr(
        backtest_script,
        "add_rolling_features",
        lambda df: df.copy(),
    )
    monkeypatch.setattr(
        backtest_script, "add_park_factor", lambda df, _park: df.copy()
    )
    monkeypatch.setattr(backtest_script, "add_opponent_k_rate", lambda df: df.copy())
    monkeypatch.setattr(
        backtest_script,
        "_normalize_opponent_feature_columns",
        lambda df: df.copy(),
    )
    monkeypatch.setattr(backtest_script, "_clean_for_model", lambda df: df.copy())

    section: dict[str, object] = {
        "pitch_data_path": "current.csv",
        "park_factors_path": str(Path("unused_park.csv")),
        "training_data_paths": ["hist_a.csv", "hist_b.csv"],
    }

    result = backtest_script._prepare_training_frame(section)
    sample_sizes = result.sort_values("game_date")["umpire_sample_size"].tolist()

    assert sample_sizes == [0.0, 1.0]


def test_run_feature_set_comparison_reports_positive_mae_improvement(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        backtest_script,
        "load_pipeline_config",
        lambda *_args, **_kwargs: type(
            "Cfg",
            (),
            {
                "section": {
                    "model_selection": {
                        "candidates": ["random_forest"],
                    }
                }
            },
        )(),
    )
    monkeypatch.setattr(
        backtest_script,
        "_prepare_training_frame",
        lambda _section: pd.DataFrame(
            {
                "game_date": pd.to_datetime(["2024-04-01", "2024-04-02"]),
                "weather_known_flag": [1, 0],
                "roof_state": ["open", "unknown"],
                "umpire_known_flag": [1, 1],
            }
        ),
    )

    def _fake_run_tournament(frame, *, selection_cfg, features):
        del frame, selection_cfg
        mae = 1.5 if len(features) == len(backtest_script.BASELINE_FEATURES) else 1.2
        fold = pd.DataFrame([{"fold": 0, "mae": mae}])
        leaderboard = pd.DataFrame(
            [{"model": "random_forest", "strategy": "global", "mean_mae": mae}]
        )
        champion = type(
            "Winner",
            (),
            {
                "model_name": "random_forest",
                "strategy_name": "global",
                "mean_mae": mae,
                "mean_rmse": mae + 0.2,
                "mean_r2": 0.1,
            },
        )()
        return fold, leaderboard, champion

    monkeypatch.setattr(backtest_script, "_run_tournament", _fake_run_tournament)

    comparison, summary = backtest_script.run_feature_set_comparison("config/mlb.yaml")

    assert set(comparison["variant"]) == {"baseline", "enriched"}
    assert summary["mae_gate_passed"] is True
    assert float(summary["mae_improvement_vs_baseline"]) > 0.0
