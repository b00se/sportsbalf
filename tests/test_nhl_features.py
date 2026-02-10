from __future__ import annotations

import pandas as pd
from src.nhl.features.shots_on_goal import build_sog_inference_features


def _history_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "season": 2024,
                "game_id": "g1",
                "game_date": "2024-10-05",
                "player_id": "8478402",
                "player_name": "Player One",
                "team": "NYR",
                "opponent": "BOS",
                "shots_on_goal": 4.0,
                "time_on_ice_minutes": 18.5,
            },
            {
                "season": 2024,
                "game_id": "g2",
                "game_date": "2024-10-06",
                "player_id": "8478402",
                "player_name": "Player One",
                "team": "NYR",
                "opponent": "MTL",
                "shots_on_goal": 3.0,
                "time_on_ice_minutes": 19.0,
            },
            {
                "season": 2024,
                "game_id": "g3",
                "game_date": "2024-10-07",
                "player_id": "8478402",
                "player_name": "Player One",
                "team": "NYR",
                "opponent": "TOR",
                "shots_on_goal": 2.0,
                "time_on_ice_minutes": 17.3,
            },
            {
                "season": 2024,
                "game_id": "g4",
                "game_date": "2024-10-08",
                "player_id": "8478402",
                "player_name": "Player One",
                "team": "NYR",
                "opponent": "NJD",
                "shots_on_goal": 5.0,
                "time_on_ice_minutes": 20.1,
            },
            {
                "season": 2024,
                "game_id": "g5",
                "game_date": "2024-10-09",
                "player_id": "8478402",
                "player_name": "Player One",
                "team": "NYR",
                "opponent": "OTT",
                "shots_on_goal": 1.0,
                "time_on_ice_minutes": 16.0,
            },
            {
                "season": 2024,
                "game_id": "g6",
                "game_date": "2024-10-10",
                "player_id": "8478402",
                "player_name": "Player One",
                "team": "NYR",
                "opponent": "BUF",
                "shots_on_goal": 4.0,
                "time_on_ice_minutes": 18.8,
            },
            {
                "season": 2024,
                "game_id": "g7",
                "game_date": "2024-10-11",
                "player_id": "8478402",
                "player_name": "Player One",
                "team": "NYR",
                "opponent": "CAR",
                "shots_on_goal": 3.0,
                "time_on_ice_minutes": 19.2,
            },
            {
                "season": 2024,
                "game_id": "g8",
                "game_date": "2024-10-05",
                "player_id": "8471214",
                "player_name": "Player Two",
                "team": "TOR",
                "opponent": "MTL",
                "shots_on_goal": 2.0,
                "time_on_ice_minutes": 17.0,
            },
            {
                "season": 2024,
                "game_id": "g9",
                "game_date": "2024-10-06",
                "player_id": "8471214",
                "player_name": "Player Two",
                "team": "TOR",
                "opponent": "OTT",
                "shots_on_goal": 3.0,
                "time_on_ice_minutes": 18.2,
            },
            {
                "season": 2024,
                "game_id": "g10",
                "game_date": "2024-10-07",
                "player_id": "8471214",
                "player_name": "Player Two",
                "team": "TOR",
                "opponent": "BOS",
                "shots_on_goal": 4.0,
                "time_on_ice_minutes": 19.3,
            },
        ]
    )


def test_build_sog_inference_features_builds_rolling_averages_and_prediction() -> None:
    inference = pd.DataFrame(
        [
            {"player_id": "8478402", "player_name": "Player One"},
            {"player_id": "8471214", "player_name": "Player Two"},
        ]
    )

    featured = build_sog_inference_features(
        inference_rows=inference,
        skater_games=_history_frame(),
        rolling_windows=[5, 10],
        fallback_prediction=2.5,
    )

    player_one = featured.loc[featured["player_id"] == "8478402"].iloc[0]
    player_two = featured.loc[featured["player_id"] == "8471214"].iloc[0]

    assert player_one["sog_avg_last_5"] == 3.0
    assert round(float(player_one["sog_avg_last_10"]), 6) == round(22.0 / 7.0, 6)
    assert round(float(player_one["sog_avg_season"]), 6) == round(22.0 / 7.0, 6)
    assert round(float(player_one["predicted_shots_on_goal"]), 6) == round(
        0.5 * 3.0 + 0.3 * (22.0 / 7.0) + 0.2 * (22.0 / 7.0), 6
    )

    assert player_two["sog_avg_last_5"] == 3.0
    assert player_two["sog_avg_last_10"] == 3.0
    assert player_two["sog_avg_season"] == 3.0
    assert player_two["predicted_shots_on_goal"] == 3.0


def test_build_sog_inference_features_uses_fallback_for_missing_history() -> None:
    inference = pd.DataFrame(
        [{"player_id": "missing-player", "player_name": "Missing"}]
    )

    featured = build_sog_inference_features(
        inference_rows=inference,
        skater_games=_history_frame(),
        rolling_windows=[5, 10],
        fallback_prediction=2.5,
    )

    row = featured.iloc[0]
    assert pd.isna(row["sog_avg_last_5"])
    assert pd.isna(row["sog_avg_last_10"])
    assert pd.isna(row["sog_avg_season"])
    assert row["predicted_shots_on_goal"] == 2.5
