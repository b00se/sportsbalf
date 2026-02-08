from __future__ import annotations

import pandas as pd
from src.mlb.pitcher_props.park_factors import add_rolling_park_factor


def test_add_rolling_park_factor_uses_neutral_fallback_until_min_samples() -> None:
    games = pd.DataFrame(
        [
            {"game_date": "2024-04-01", "home_team": "NYM", "outs_recorded": 10},
            {"game_date": "2024-04-02", "home_team": "NYM", "outs_recorded": 30},
            {"game_date": "2024-04-03", "home_team": "NYM", "outs_recorded": 30},
            {"game_date": "2024-04-04", "home_team": "BOS", "outs_recorded": 15},
            {"game_date": "2024-04-05", "home_team": "BOS", "outs_recorded": 12},
            {"game_date": "2024-04-06", "home_team": "BOS", "outs_recorded": 30},
        ]
    )
    games["game_date"] = pd.to_datetime(games["game_date"])

    enriched = add_rolling_park_factor(
        games,
        target_col="outs_recorded",
        park_col="park_factor_outs",
        min_samples=2,
        half_life_games=3,
    )

    assert float(enriched.loc[0, "park_factor_outs"]) == 1.0
    assert float(enriched.loc[1, "park_factor_outs"]) == 1.0
    assert 0.5 <= float(enriched.loc[2, "park_factor_outs"]) <= 1.5


def test_add_rolling_park_factor_same_date_rows_use_prior_dates_only() -> None:
    games = pd.DataFrame(
        [
            {
                "game_date": "2024-04-01",
                "game_pk": 1001,
                "home_team": "NYM",
                "outs_recorded": 12.0,
            },
            {
                "game_date": "2024-04-02",
                "game_pk": 1002,
                "home_team": "NYM",
                "outs_recorded": 30.0,
            },
            {
                "game_date": "2024-04-02",
                "game_pk": 1003,
                "home_team": "NYM",
                "outs_recorded": 6.0,
            },
        ]
    )
    games["game_date"] = pd.to_datetime(games["game_date"])

    enriched = add_rolling_park_factor(
        games,
        target_col="outs_recorded",
        park_col="park_factor_outs",
        min_samples=1,
        half_life_games=3,
    )
    same_day = enriched[enriched["game_date"] == pd.Timestamp("2024-04-02")]

    assert len(same_day) == 2
    assert same_day["park_factor_outs"].nunique() == 1
