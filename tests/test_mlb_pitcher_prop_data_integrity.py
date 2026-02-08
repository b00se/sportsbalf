from __future__ import annotations

import pandas as pd
from src.mlb.pitcher_props.data import build_pitcher_game_table
from src.mlb.pitcher_props.pipeline import _add_opponent_tendency


def test_add_opponent_tendency_uses_historical_only() -> None:
    games = pd.DataFrame(
        [
            {
                "opponent_team": "ATL",
                "game_date": "2024-04-03",
                "outs_recorded": 30.0,
            },
            {
                "opponent_team": "ATL",
                "game_date": "2024-04-01",
                "outs_recorded": 10.0,
            },
            {
                "opponent_team": "ATL",
                "game_date": "2024-04-02",
                "outs_recorded": 20.0,
            },
            {
                "opponent_team": "NYM",
                "game_date": "2024-04-01",
                "outs_recorded": 40.0,
            },
        ]
    )
    games["game_date"] = pd.to_datetime(games["game_date"])

    enriched = _add_opponent_tendency(
        games,
        target_col="outs_recorded",
        feature_col="opponent_out_rate",
    )
    atl = enriched[enriched["opponent_team"] == "ATL"].sort_values("game_date")

    assert float(atl.iloc[1]["opponent_out_rate"]) == 10.0
    assert float(atl.iloc[2]["opponent_out_rate"]) == 15.0


def test_build_pitcher_game_table_deduplicates_terminal_plate_appearances() -> None:
    frame = pd.DataFrame(
        [
            {
                "pitcher": 1,
                "game_date": "2024-04-01",
                "game_pk": 1001,
                "at_bat_number": 1,
                "pitch_number": 1,
                "events": "single",
                "description": "hit_into_play",
                "inning": 1,
                "pitch_type": "FF",
                "home_team": "NYM",
                "away_team": "ATL",
                "pitcher_days_since_prev_game": 5,
                "inning_topbot": "Top",
            },
            {
                "pitcher": 1,
                "game_date": "2024-04-01",
                "game_pk": 1001,
                "at_bat_number": 1,
                "pitch_number": 2,
                "events": "single",
                "description": "hit_into_play",
                "inning": 1,
                "pitch_type": "FF",
                "home_team": "NYM",
                "away_team": "ATL",
                "pitcher_days_since_prev_game": 5,
                "inning_topbot": "Top",
            },
            {
                "pitcher": 1,
                "game_date": "2024-04-01",
                "game_pk": 1001,
                "at_bat_number": 2,
                "pitch_number": 1,
                "events": "strikeout",
                "description": "swinging_strike",
                "inning": 2,
                "pitch_type": "SL",
                "home_team": "NYM",
                "away_team": "ATL",
                "pitcher_days_since_prev_game": 5,
                "inning_topbot": "Top",
            },
        ]
    )

    games = build_pitcher_game_table(frame)
    row = games.iloc[0]

    assert float(row["hits_allowed"]) == 1.0
    assert float(row["outs_recorded"]) == 1.0


def test_build_pitcher_game_table_earned_runs_fallback_uses_game_level_value() -> None:
    frame = pd.DataFrame(
        [
            {
                "pitcher": 1,
                "game_date": "2024-04-01",
                "game_pk": 1001,
                "at_bat_number": 1,
                "pitch_number": 1,
                "events": "walk",
                "description": "ball",
                "inning": 1,
                "pitch_type": "FF",
                "home_team": "NYM",
                "away_team": "ATL",
                "pitcher_days_since_prev_game": 5,
                "inning_topbot": "Top",
                "runs_allowed": 2,
            },
            {
                "pitcher": 1,
                "game_date": "2024-04-01",
                "game_pk": 1001,
                "at_bat_number": 2,
                "pitch_number": 1,
                "events": "single",
                "description": "hit_into_play",
                "inning": 2,
                "pitch_type": "SL",
                "home_team": "NYM",
                "away_team": "ATL",
                "pitcher_days_since_prev_game": 5,
                "inning_topbot": "Top",
                "runs_allowed": 2,
            },
        ]
    )

    games = build_pitcher_game_table(frame)

    assert float(games.iloc[0]["earned_runs"]) == 2.0
