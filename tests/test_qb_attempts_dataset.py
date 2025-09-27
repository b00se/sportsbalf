import warnings
from urllib.error import HTTPError

import pandas as pd

import src.nfl.data.qb_attempts as qb_attempts
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
                "gameday": "2023-09-10",
                "div_game": 0,
                "home_team": "ARI",
                "away_team": "SEA",
                "spread_line": -1.5,
                "total_line": 47.5,
            },
            {
                "game_id": 2,
                "season": 2023,
                "week": 2,
                "gameday": "2023-09-17",
                "div_game": 1,
                "home_team": "DAL",
                "away_team": "ARI",
                "spread_line": -3.0,
                "total_line": 45.0,
            },
            {
                "game_id": 3,
                "season": 2023,
                "week": 1,
                "gameday": "2023-09-10",
                "div_game": 0,
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

    pbp = pd.DataFrame(
        [
            {
                "season": 2023,
                "week": 1,
                "game_id": 1,
                "posteam": "ARI",
                "defteam": "SEA",
                "pass_attempt": 1,
                "rush_attempt": 0,
                "score_differential": 3,
                "qtr": 1,
                "passer_player_id": "A",
                "rusher_player_id": "A",
                "qb_dropback": 1,
                "epa": 0.5,
                "cpoe": 0.2,
                "air_yards": 6.0,
            },
            {
                "season": 2023,
                "week": 2,
                "game_id": 2,
                "posteam": "ARI",
                "defteam": "DAL",
                "pass_attempt": 1,
                "rush_attempt": 0,
                "score_differential": -3,
                "qtr": 2,
                "passer_player_id": "A",
                "rusher_player_id": "A",
                "qb_dropback": 1,
                "epa": 0.3,
                "cpoe": 0.1,
                "air_yards": 5.0,
            },
            {
                "season": 2023,
                "week": 1,
                "game_id": 3,
                "posteam": "BUF",
                "defteam": "NYJ",
                "pass_attempt": 1,
                "rush_attempt": 0,
                "score_differential": 7,
                "qtr": 2,
                "passer_player_id": "B",
                "rusher_player_id": "B",
                "qb_dropback": 1,
                "epa": 0.4,
                "cpoe": -0.1,
                "air_yards": 7.0,
            },
        ]
    )

    ngs = pd.DataFrame(
        [
            {
                "season": 2023,
                "week": 1,
                "player_gsis_id": "A",
                "avg_time_to_throw": 2.5,
                "avg_intended_air_yards": 7.1,
                "completion_percentage_above_expectation": 0.05,
            },
            {
                "season": 2023,
                "week": 1,
                "player_gsis_id": "B",
                "avg_time_to_throw": 2.7,
                "avg_intended_air_yards": 8.2,
                "completion_percentage_above_expectation": -0.02,
            },
        ]
    )

    result = prepare_qb_attempts_dataset(
        weekly,
        schedule,
        ud_lines,
        pbp=pbp,
        ngs=ngs,
    )

    kyler_week1 = result[(result["qb_id"] == "A") & (result["week"] == 1)].iloc[0]
    assert kyler_week1["ud_line"] == 31.5
    assert bool(kyler_week1["home"])
    assert pd.isna(kyler_week1["prev_attempts"])
    assert kyler_week1["plays_per_game"] > 0
    assert kyler_week1["ngs_avg_time_to_throw"] == 2.5

    kyler_week2 = result[(result["qb_id"] == "A") & (result["week"] == 2)].iloc[0]
    assert kyler_week2["ud_line"] == 30.5
    assert kyler_week2["prev_attempts"] == 30
    assert pd.isna(kyler_week2["rolling3_attempts"])
    assert not bool(kyler_week2["short_week"])

    josh = result[result["qb_id"] == "B"].iloc[0]
    assert not bool(josh["home"])
    assert josh["ud_line"] == 38.5
    assert josh["opponent_pass_rate_allowed"] >= 0


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
                "gameday": "2016-10-10",
                "div_game": 1,
                "home_team": "OAK",
                "away_team": "SD",
                "spread_line": -2.0,
                "total_line": 49.5,
            }
        ]
    )

    result = prepare_qb_attempts_dataset(
        weekly,
        schedule,
        ud_lines=None,
        pbp=pd.DataFrame(),
        ngs=pd.DataFrame(),
    )
    row = result.iloc[0]

    assert row["team"] == "LV"
    assert row["opponent"] == "LAC"
    assert bool(row["home"])
    assert pd.isna(row["ud_line"])
    assert bool(row["is_divisional"])
    for feature in [
        "plays_per_game",
        "qb_dropbacks",
        "ngs_avg_time_to_throw",
    ]:
        assert feature in row.index
        assert row[feature] == 0

def test_load_weekly_data_skips_missing_year(monkeypatch):
    class DummyNFL:
        def import_weekly_data(self, years):
            if len(years) > 1:
                raise HTTPError(
                    url="http://example/multi",
                    code=404,
                    msg="Not Found",
                    hdrs=None,
                    fp=None,
                )
            year = years[0]
            if year == 2025:
                raise HTTPError(
                    url=f"http://example/{year}",
                    code=404,
                    msg="Not Found",
                    hdrs=None,
                    fp=None,
                )
            return pd.DataFrame(
                {
                    "position": ["QB"],
                    "season": [year],
                    "week": [1],
                    "game_id": [f"{year}_01"],
                    "player_id": [f"QB-{year}"],
                    "player_display_name": [f"QB {year}"],
                    "recent_team": ["AAA"],
                    "opponent_team": ["BBB"],
                    "attempts": [30],
                }
            )

    monkeypatch.setattr(qb_attempts, "nfl", DummyNFL())
    monkeypatch.setattr(qb_attempts, "_NFL_IMPORT_ERROR", None)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = qb_attempts.load_weekly_data([2024, 2025])

    assert list(result["season"].unique()) == [2024]
    assert any("Skipping weekly data" in str(entry.message) for entry in caught)

