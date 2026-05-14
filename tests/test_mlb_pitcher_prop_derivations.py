from __future__ import annotations

import pandas as pd
from src.mlb.pitcher_props.data import build_batter_game_table, build_pitcher_game_table


def test_build_pitcher_game_table_derives_multi_targets() -> None:
    frame = pd.DataFrame(
        [
            {
                "pitcher": 1,
                "player_name": "Cole, Gerrit",
                "game_date": "2024-04-01",
                "description": "called_strike",
                "events": "strikeout",
                "inning": 1,
                "pitch_type": "FF",
                "home_team": "NYM",
                "away_team": "ATL",
                "pitcher_days_since_prev_game": 5,
                "inning_topbot": "Top",
                "game_pk": 1,
                "at_bat_number": 1,
                "pitch_number": 3,
                "home_score": 0,
                "away_score": 0,
                "post_home_score": 0,
                "post_away_score": 0,
                "batter": 100,
            },
            {
                "pitcher": 1,
                "player_name": "Cole, Gerrit",
                "game_date": "2024-04-01",
                "description": "hit_into_play",
                "events": "grounded_into_double_play",
                "inning": 2,
                "pitch_type": "SL",
                "home_team": "NYM",
                "away_team": "ATL",
                "pitcher_days_since_prev_game": 5,
                "inning_topbot": "Top",
                "game_pk": 1,
                "at_bat_number": 2,
                "pitch_number": 1,
                "home_score": 0,
                "away_score": 0,
                "post_home_score": 0,
                "post_away_score": 0,
                "batter": 101,
            },
            {
                "pitcher": 1,
                "player_name": "Cole, Gerrit",
                "game_date": "2024-04-01",
                "description": "ball",
                "events": "walk",
                "inning": 3,
                "pitch_type": "CH",
                "home_team": "NYM",
                "away_team": "ATL",
                "pitcher_days_since_prev_game": 5,
                "inning_topbot": "Top",
                "game_pk": 1,
                "at_bat_number": 3,
                "pitch_number": 4,
                "home_score": 0,
                "away_score": 0,
                "post_home_score": 0,
                "post_away_score": 0,
                "batter": 102,
            },
            {
                "pitcher": 1,
                "player_name": "Cole, Gerrit",
                "game_date": "2024-04-01",
                "description": "hit_into_play",
                "events": "single",
                "inning": 4,
                "pitch_type": "CH",
                "home_team": "NYM",
                "away_team": "ATL",
                "pitcher_days_since_prev_game": 5,
                "inning_topbot": "Top",
                "game_pk": 1,
                "at_bat_number": 4,
                "pitch_number": 2,
                "home_score": 0,
                "away_score": 0,
                "post_home_score": 0,
                "post_away_score": 1,
                "batter": 103,
            },
        ]
    )

    games = build_pitcher_game_table(frame)
    row = games.iloc[0]

    assert float(row["outs_recorded"]) == 3.0
    assert float(row["hits_allowed"]) == 1.0
    assert float(row["bb_allowed"]) == 1.0
    assert float(row["earned_runs"]) == 1.0
    assert row["pitcher_id"] == 1
    assert row["pitcher_name"] == "Gerrit Cole"


def test_build_batter_game_table_builds_foundation_columns() -> None:
    frame = pd.read_csv("tests/testdata/mlb_multi_stat_pitches.csv")

    batter_games = build_batter_game_table(frame)

    required = {
        "batter",
        "game_date",
        "plate_appearances",
        "hits",
        "total_bases",
        "walks",
        "strikeouts",
        "hard_hit_rate",
        "pa_vs_lhp",
        "pa_vs_rhp",
    }
    assert required.issubset(batter_games.columns)
    assert len(batter_games) > 0
