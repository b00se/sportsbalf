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
