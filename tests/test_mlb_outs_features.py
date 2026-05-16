from __future__ import annotations

import pandas as pd
from src.mlb.pitcher_props.data import build_pitcher_game_table
from src.mlb.pitcher_props.descriptors import get_stat_descriptor
from src.mlb.pitcher_props.outs_features import add_outs_workload_features
from src.mlb.pitcher_props.pipeline import _model_features


def test_add_outs_workload_features_uses_prior_games_only() -> None:
    frame = pd.DataFrame(
        [
            {
                "pitcher": 1,
                "game_date": "2024-04-01",
                "game_pk": 1001,
                "at_bat_number": 1,
                "pitch_number": 1,
                "pitch_count": 90,
                "events": "strikeout",
                "description": "called_strike",
                "inning": 1,
                "pitch_type": "FF",
                "home_team": "NYM",
                "away_team": "ATL",
                "pitcher_days_since_prev_game": 5,
                "inning_topbot": "Top",
                "batter": 101,
            },
            {
                "pitcher": 1,
                "game_date": "2024-04-01",
                "game_pk": 1001,
                "at_bat_number": 2,
                "pitch_number": 1,
                "pitch_count": 91,
                "events": "field_out",
                "description": "hit_into_play",
                "inning": 2,
                "pitch_type": "SL",
                "home_team": "NYM",
                "away_team": "ATL",
                "pitcher_days_since_prev_game": 5,
                "inning_topbot": "Top",
                "batter": 102,
            },
            {
                "pitcher": 1,
                "game_date": "2024-04-02",
                "game_pk": 1002,
                "at_bat_number": 1,
                "pitch_number": 1,
                "pitch_count": 88,
                "events": "triple_play",
                "description": "hit_into_play",
                "inning": 1,
                "pitch_type": "FF",
                "home_team": "NYM",
                "away_team": "ATL",
                "pitcher_days_since_prev_game": 4,
                "inning_topbot": "Top",
                "batter": 201,
            },
            {
                "pitcher": 1,
                "game_date": "2024-04-02",
                "game_pk": 1002,
                "at_bat_number": 2,
                "pitch_number": 1,
                "pitch_count": 89,
                "events": "single",
                "description": "hit_into_play",
                "inning": 2,
                "pitch_type": "SL",
                "home_team": "NYM",
                "away_team": "ATL",
                "pitcher_days_since_prev_game": 4,
                "inning_topbot": "Top",
                "batter": 202,
            },
            {
                "pitcher": 1,
                "game_date": "2024-04-02",
                "game_pk": 1002,
                "at_bat_number": 3,
                "pitch_number": 1,
                "pitch_count": 87,
                "events": "walk",
                "description": "ball",
                "inning": 3,
                "pitch_type": "CH",
                "home_team": "NYM",
                "away_team": "ATL",
                "pitcher_days_since_prev_game": 4,
                "inning_topbot": "Top",
                "batter": 203,
            },
            {
                "pitcher": 1,
                "game_date": "2024-04-02",
                "game_pk": 1002,
                "at_bat_number": 4,
                "pitch_number": 1,
                "pitch_count": 86,
                "events": "strikeout",
                "description": "called_strike",
                "inning": 4,
                "pitch_type": "FF",
                "home_team": "NYM",
                "away_team": "ATL",
                "pitcher_days_since_prev_game": 4,
                "inning_topbot": "Top",
                "batter": 204,
            },
            {
                "pitcher": 1,
                "game_date": "2024-04-03",
                "game_pk": 1003,
                "at_bat_number": 1,
                "pitch_number": 1,
                "pitch_count": 92,
                "events": "field_out",
                "description": "hit_into_play",
                "inning": 1,
                "pitch_type": "FF",
                "home_team": "NYM",
                "away_team": "ATL",
                "pitcher_days_since_prev_game": 3,
                "inning_topbot": "Top",
                "batter": 301,
            },
            {
                "pitcher": 1,
                "game_date": "2024-04-03",
                "game_pk": 1003,
                "at_bat_number": 2,
                "pitch_number": 1,
                "pitch_count": 93,
                "events": "walk",
                "description": "ball",
                "inning": 2,
                "pitch_type": "SL",
                "home_team": "NYM",
                "away_team": "ATL",
                "pitcher_days_since_prev_game": 3,
                "inning_topbot": "Top",
                "batter": 302,
            },
        ]
    )

    games = build_pitcher_game_table(frame)
    enriched = add_outs_workload_features(games)
    game2 = enriched.sort_values("game_date").iloc[1]

    assert float(game2["batters_faced"]) == 2.0
    assert float(game2["rolling_outs_recorded_5"]) == 2.0
    assert float(game2["rolling_batters_faced_5"]) == 2.0
    assert float(game2["rolling_outs_per_batter_faced_5"]) == 1.0
    assert float(game2["prev_outs_recorded"]) == 2.0
    assert float(game2["prev_batters_faced"]) == 2.0
    assert float(game2["prev_pitch_count"]) == 2.0
    assert float(game2["rolling_pitch_count_10"]) == 2.0
    assert float(game2["season_avg_pitch_count_to_date"]) == 2.0
    assert float(game2["career_avg_pitch_count"]) == 2.0


def test_model_features_adds_outs_only_columns_for_outs_stat() -> None:
    outs_descriptor = get_stat_descriptor("outs_recorded")
    strikeouts_descriptor = get_stat_descriptor("strikeouts")

    outs_features = _model_features(outs_descriptor)
    strikeouts_features = _model_features(strikeouts_descriptor)

    assert "batters_faced" in outs_features
    assert "rolling_outs_recorded_5" in outs_features
    assert "prev_outs_recorded" in outs_features
    assert "prev_pitch_count" in outs_features
    assert "rolling_pitch_count_10" in outs_features
    assert "outs_per_batter_faced" not in outs_features
    assert "prev_outs_per_batter_faced" not in outs_features
    assert "rolling_outs_recorded_5" not in strikeouts_features
    assert "batters_faced" not in strikeouts_features
