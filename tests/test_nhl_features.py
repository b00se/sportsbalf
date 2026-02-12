from __future__ import annotations

import pandas as pd
from src.nhl.features.shots_on_goal import (
    build_sog_inference_features,
    build_sog_training_features,
)


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


def test_build_sog_inference_features_builds_richer_features_and_baseline() -> None:
    inference = pd.DataFrame(
        [
            {
                "player_id": "8478402",
                "player_name": "Player One",
                "team": "NYR",
                "opponent": "BOS",
            },
            {
                "player_id": "8471214",
                "player_name": "Player Two",
                "team": "TOR",
                "opponent": "MTL",
            },
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
    assert round(float(player_one["sog_avg_season_to_date"]), 6) == round(22.0 / 7.0, 6)
    assert round(float(player_one["baseline_predicted_shots_on_goal"]), 6) == round(
        0.5 * 3.0 + 0.3 * (22.0 / 7.0) + 0.2 * (22.0 / 7.0),
        6,
    )

    assert player_two["sog_avg_last_5"] == 3.0
    assert player_two["sog_avg_last_10"] == 3.0
    assert player_two["sog_avg_season_to_date"] == 3.0
    assert player_two["predicted_shots_on_goal"] == 3.0


def test_build_sog_inference_features_uses_fallback_for_missing_history() -> None:
    inference = pd.DataFrame(
        [
            {
                "player_id": "missing-player",
                "player_name": "Missing",
                "team": "SEA",
                "opponent": "ANA",
            }
        ]
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
    assert pd.isna(row["sog_avg_season_to_date"])
    assert row["predicted_shots_on_goal"] == 2.5


def test_build_sog_training_features_lags_player_history_for_leakage_safety() -> None:
    training = build_sog_training_features(_history_frame(), [5, 10])
    player_one = training.loc[training["player_id"] == "8478402"].sort_values(
        "game_date"
    )

    first_row = player_one.iloc[0]
    second_row = player_one.iloc[1]

    assert pd.isna(first_row["sog_avg_last_5"])
    assert pd.isna(first_row["sog_avg_season_to_date"])
    assert second_row["sog_avg_last_5"] == 4.0
    assert second_row["sog_avg_season_to_date"] == 4.0
    assert second_row["games_played_to_date"] == 1.0


def test_training_team_and_opponent_context_do_not_leak_same_game_rows() -> None:
    history = pd.DataFrame(
        [
            {
                "season": 2024,
                "game_id": "g1",
                "game_date": "2024-10-01",
                "player_id": "a1",
                "player_name": "A One",
                "team": "A",
                "opponent": "B",
                "shots_on_goal": 1.0,
                "time_on_ice_minutes": 15.0,
            },
            {
                "season": 2024,
                "game_id": "g1",
                "game_date": "2024-10-01",
                "player_id": "a2",
                "player_name": "A Two",
                "team": "A",
                "opponent": "B",
                "shots_on_goal": 3.0,
                "time_on_ice_minutes": 16.0,
            },
            {
                "season": 2024,
                "game_id": "g1",
                "game_date": "2024-10-01",
                "player_id": "b1",
                "player_name": "B One",
                "team": "B",
                "opponent": "A",
                "shots_on_goal": 2.0,
                "time_on_ice_minutes": 15.0,
            },
            {
                "season": 2024,
                "game_id": "g1",
                "game_date": "2024-10-01",
                "player_id": "b2",
                "player_name": "B Two",
                "team": "B",
                "opponent": "A",
                "shots_on_goal": 2.0,
                "time_on_ice_minutes": 16.0,
            },
            {
                "season": 2024,
                "game_id": "g2",
                "game_date": "2024-10-02",
                "player_id": "a1",
                "player_name": "A One",
                "team": "A",
                "opponent": "B",
                "shots_on_goal": 4.0,
                "time_on_ice_minutes": 17.0,
            },
            {
                "season": 2024,
                "game_id": "g2",
                "game_date": "2024-10-02",
                "player_id": "a2",
                "player_name": "A Two",
                "team": "A",
                "opponent": "B",
                "shots_on_goal": 6.0,
                "time_on_ice_minutes": 18.0,
            },
            {
                "season": 2024,
                "game_id": "g2",
                "game_date": "2024-10-02",
                "player_id": "b1",
                "player_name": "B One",
                "team": "B",
                "opponent": "A",
                "shots_on_goal": 1.0,
                "time_on_ice_minutes": 15.0,
            },
            {
                "season": 2024,
                "game_id": "g2",
                "game_date": "2024-10-02",
                "player_id": "b2",
                "player_name": "B Two",
                "team": "B",
                "opponent": "A",
                "shots_on_goal": 1.0,
                "time_on_ice_minutes": 15.0,
            },
        ]
    )

    training = build_sog_training_features(history, [5, 10])
    team_a_game2 = training.loc[
        (training["team"] == "A") & (training["game_id"] == "g2")
    ].sort_values("player_id")

    assert team_a_game2.shape[0] == 2
    # Team A game 1 average is (1 + 3) / 2 = 2.
    assert team_a_game2["team_sog_for_avg_last_5"].tolist() == [2.0, 2.0]
    # Opponent B allowed average in game 1 is also 2.
    assert team_a_game2["opponent_sog_allowed_avg_last_5"].tolist() == [2.0, 2.0]


def test_inference_team_and_opponent_context_use_game_level_aggregates() -> None:
    history = pd.DataFrame(
        [
            {
                "season": 2024,
                "game_id": "g1",
                "game_date": "2024-10-01",
                "player_id": "a1",
                "player_name": "A One",
                "team": "A",
                "opponent": "B",
                "shots_on_goal": 0.0,
                "time_on_ice_minutes": 15.0,
            },
            {
                "season": 2024,
                "game_id": "g1",
                "game_date": "2024-10-01",
                "player_id": "b1",
                "player_name": "B One",
                "team": "B",
                "opponent": "A",
                "shots_on_goal": 2.0,
                "time_on_ice_minutes": 16.0,
            },
            {
                "season": 2024,
                "game_id": "g2",
                "game_date": "2024-10-02",
                "player_id": "a2",
                "player_name": "A Two",
                "team": "A",
                "opponent": "B",
                "shots_on_goal": 10.0,
                "time_on_ice_minutes": 15.0,
            },
            {
                "season": 2024,
                "game_id": "g2",
                "game_date": "2024-10-02",
                "player_id": "a3",
                "player_name": "A Three",
                "team": "A",
                "opponent": "B",
                "shots_on_goal": 10.0,
                "time_on_ice_minutes": 15.0,
            },
            {
                "season": 2024,
                "game_id": "g2",
                "game_date": "2024-10-02",
                "player_id": "a4",
                "player_name": "A Four",
                "team": "A",
                "opponent": "B",
                "shots_on_goal": 10.0,
                "time_on_ice_minutes": 15.0,
            },
            {
                "season": 2024,
                "game_id": "g2",
                "game_date": "2024-10-02",
                "player_id": "a5",
                "player_name": "A Five",
                "team": "A",
                "opponent": "B",
                "shots_on_goal": 10.0,
                "time_on_ice_minutes": 15.0,
            },
            {
                "season": 2024,
                "game_id": "g2",
                "game_date": "2024-10-02",
                "player_id": "b2",
                "player_name": "B Two",
                "team": "B",
                "opponent": "A",
                "shots_on_goal": 2.0,
                "time_on_ice_minutes": 16.0,
            },
            {
                "season": 2024,
                "game_id": "g3",
                "game_date": "2024-10-03",
                "player_id": "a6",
                "player_name": "A Six",
                "team": "A",
                "opponent": "B",
                "shots_on_goal": 0.0,
                "time_on_ice_minutes": 15.0,
            },
            {
                "season": 2024,
                "game_id": "g3",
                "game_date": "2024-10-03",
                "player_id": "b3",
                "player_name": "B Three",
                "team": "B",
                "opponent": "A",
                "shots_on_goal": 2.0,
                "time_on_ice_minutes": 16.0,
            },
        ]
    )
    inference = pd.DataFrame(
        [
            {
                "player_id": "a1",
                "player_name": "A One",
                "team": "A",
                "opponent": "B",
            }
        ]
    )

    featured = build_sog_inference_features(
        inference_rows=inference,
        skater_games=history,
        rolling_windows=[5, 10],
        fallback_prediction=2.5,
    )
    row = featured.iloc[0]

    # Team A game-level averages over g1,g2,g3 are [0,10,0] => 10/3.
    assert row["team_sog_for_avg_last_5"] == 10.0 / 3.0
    # Opponent B allowed game-level averages are also [0,10,0] => 10/3.
    assert row["opponent_sog_allowed_avg_last_5"] == 10.0 / 3.0


def test_inference_player_tail_features_are_based_on_game_date_order() -> None:
    history_unsorted = pd.DataFrame(
        [
            {
                "season": 2024,
                "game_id": "g3",
                "game_date": "2024-10-03",
                "player_id": "p1",
                "player_name": "P One",
                "team": "A",
                "opponent": "B",
                "shots_on_goal": 9.0,
                "time_on_ice_minutes": 30.0,
            },
            {
                "season": 2024,
                "game_id": "g1",
                "game_date": "2024-10-01",
                "player_id": "p1",
                "player_name": "P One",
                "team": "A",
                "opponent": "B",
                "shots_on_goal": 1.0,
                "time_on_ice_minutes": 10.0,
            },
            {
                "season": 2024,
                "game_id": "g2",
                "game_date": "2024-10-02",
                "player_id": "p1",
                "player_name": "P One",
                "team": "A",
                "opponent": "B",
                "shots_on_goal": 5.0,
                "time_on_ice_minutes": 20.0,
            },
        ]
    )
    inference = pd.DataFrame(
        [{"player_id": "p1", "player_name": "P One", "team": "A", "opponent": "B"}]
    )

    featured = build_sog_inference_features(
        inference_rows=inference,
        skater_games=history_unsorted,
        rolling_windows=[2, 10],
        fallback_prediction=2.5,
    )
    row = featured.iloc[0]

    # Chronological last-2 shots are 5 and 9.
    assert row["sog_avg_last_5"] == 7.0
    # Chronological last-2 TOI values are 20 and 30.
    assert row["toi_avg_last_5"] == 25.0


def test_inference_team_context_is_based_on_game_date_order() -> None:
    history_unsorted = pd.DataFrame(
        [
            {
                "season": 2024,
                "game_id": "m",
                "game_date": "2024-10-03",
                "player_id": "a3",
                "player_name": "A Three",
                "team": "A",
                "opponent": "B",
                "shots_on_goal": 9.0,
                "time_on_ice_minutes": 15.0,
            },
            {
                "season": 2024,
                "game_id": "z",
                "game_date": "2024-10-01",
                "player_id": "a1",
                "player_name": "A One",
                "team": "A",
                "opponent": "B",
                "shots_on_goal": 1.0,
                "time_on_ice_minutes": 15.0,
            },
            {
                "season": 2024,
                "game_id": "a",
                "game_date": "2024-10-02",
                "player_id": "a2",
                "player_name": "A Two",
                "team": "A",
                "opponent": "B",
                "shots_on_goal": 5.0,
                "time_on_ice_minutes": 15.0,
            },
            {
                "season": 2024,
                "game_id": "m",
                "game_date": "2024-10-03",
                "player_id": "b3",
                "player_name": "B Three",
                "team": "B",
                "opponent": "A",
                "shots_on_goal": 2.0,
                "time_on_ice_minutes": 16.0,
            },
            {
                "season": 2024,
                "game_id": "z",
                "game_date": "2024-10-01",
                "player_id": "b1",
                "player_name": "B One",
                "team": "B",
                "opponent": "A",
                "shots_on_goal": 2.0,
                "time_on_ice_minutes": 16.0,
            },
            {
                "season": 2024,
                "game_id": "a",
                "game_date": "2024-10-02",
                "player_id": "b2",
                "player_name": "B Two",
                "team": "B",
                "opponent": "A",
                "shots_on_goal": 2.0,
                "time_on_ice_minutes": 16.0,
            },
        ]
    )
    inference = pd.DataFrame(
        [{"player_id": "a1", "player_name": "A One", "team": "A", "opponent": "B"}]
    )

    featured = build_sog_inference_features(
        inference_rows=inference,
        skater_games=history_unsorted,
        rolling_windows=[2, 10],
        fallback_prediction=2.5,
    )
    row = featured.iloc[0]

    # Team A chronological game means are [1,5,9]; last-2 mean is 7.
    assert row["team_sog_for_avg_last_5"] == 7.0
    # Team B allowed chronological means are [1,5,9]; last-2 mean is 7.
    assert row["opponent_sog_allowed_avg_last_5"] == 7.0
