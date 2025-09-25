import pandas as pd

from src.nfl.data.qb_attempts import prepare_qb_attempts_dataset


def test_prepare_qb_attempts_dataset_merges_ud_lines_and_features():
    weekly = pd.DataFrame(
        [
            {
                "position": "QB",
                "season": 2023,
                "week": 1,
                "game_id": 1,
                "player_id": "A",
                "player_display_name": "Kyler Murray",
                "recent_team": "ARI",
                "opponent_team": "SEA",
                "attempts": 30,
            },
            {
                "position": "QB",
                "season": 2023,
                "week": 2,
                "game_id": 2,
                "player_id": "A",
                "player_display_name": "Kyler Murray",
                "recent_team": "ARI",
                "opponent_team": "DAL",
                "attempts": 28,
            },
            {
                "position": "QB",
                "season": 2023,
                "week": 1,
                "game_id": 3,
                "player_id": "B",
                "player_display_name": "Josh Allen",
                "recent_team": "BUF",
                "opponent_team": "NYJ",
                "attempts": 40,
            },
        ]
    )

    schedule = pd.DataFrame(
        [
            {
                "game_id": 1,
                "season": 2023,
                "week": 1,
                "home_team": "ARI",
                "away_team": "SEA",
                "spread_line": -1.5,
                "total_line": 47.5,
            },
            {
                "game_id": 2,
                "season": 2023,
                "week": 2,
                "home_team": "DAL",
                "away_team": "ARI",
                "spread_line": -3.0,
                "total_line": 45.0,
            },
            {
                "game_id": 3,
                "season": 2023,
                "week": 1,
                "home_team": "NYJ",
                "away_team": "BUF",
                "spread_line": 2.5,
                "total_line": 48.5,
            },
        ]
    )

    ud_lines = pd.DataFrame(
        [
            {"player_name": "Kyler Murray", "game_id": 1, "line": 31.5},
            {"player_name": "Kyler Murray", "game_id": 2, "line": 30.5},
            {"player_name": "Josh Allen", "game_id": 3, "line": 38.5},
        ]
    )

    result = prepare_qb_attempts_dataset(weekly, schedule, ud_lines)

    kyler_week1 = result[(result["qb_id"] == "A") & (result["week"] == 1)].iloc[0]
    assert kyler_week1["ud_line"] == 31.5
    assert bool(kyler_week1["home"])
    assert pd.isna(kyler_week1["prev_attempts"])

    kyler_week2 = result[(result["qb_id"] == "A") & (result["week"] == 2)].iloc[0]
    assert kyler_week2["ud_line"] == 30.5
    assert kyler_week2["prev_attempts"] == 30
    assert pd.isna(kyler_week2["rolling3_attempts"])

    josh = result[result["qb_id"] == "B"].iloc[0]
    assert not bool(josh["home"])
    assert josh["ud_line"] == 38.5


def test_prepare_qb_attempts_dataset_normalizes_team_names():
    weekly = pd.DataFrame(
        [
            {
                "position": "QB",
                "season": 2016,
                "week": 5,
                "game_id": 10,
                "player_id": "C",
                "player_display_name": "Derek Carr",
                "recent_team": "OAK",
                "opponent_team": "SD",
                "attempts": 45,
            }
        ]
    )

    schedule = pd.DataFrame(
        [
            {
                "game_id": 10,
                "season": 2016,
                "week": 5,
                "home_team": "OAK",
                "away_team": "SD",
                "spread_line": -2.0,
                "total_line": 49.5,
            }
        ]
    )

    result = prepare_qb_attempts_dataset(weekly, schedule, ud_lines=None)
    row = result.iloc[0]

    assert row["team"] == "LV"
    assert row["opponent"] == "LAC"
    assert bool(row["home"])
    assert pd.isna(row["ud_line"])
