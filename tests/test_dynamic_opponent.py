from __future__ import annotations

import pandas as pd
from src.mlb.features.dynamic_opponent import compute_opponent_k_pct_dynamic


def _source_frame() -> pd.DataFrame:
    frame = pd.DataFrame(
        [
            {
                "game_date": "2024-04-01",
                "events": "strikeout",
                "home_team": "ATL",
                "away_team": "NYM",
                "inning_topbot": "Bot",
            },
            {
                "game_date": "2024-04-01",
                "events": "single",
                "home_team": "ATL",
                "away_team": "NYM",
                "inning_topbot": "Bot",
            },
            {
                "game_date": "2024-04-02",
                "events": "strikeout",
                "home_team": "ATL",
                "away_team": "PHI",
                "inning_topbot": "Bot",
            },
            {
                "game_date": "2024-04-02",
                "events": "walk",
                "home_team": "ATL",
                "away_team": "PHI",
                "inning_topbot": "Bot",
            },
            {
                "game_date": "2024-04-03",
                "events": "strikeout",
                "home_team": "ATL",
                "away_team": "MIA",
                "inning_topbot": "Bot",
            },
            {
                "game_date": "2024-04-03",
                "events": "strikeout",
                "home_team": "ATL",
                "away_team": "MIA",
                "inning_topbot": "Bot",
            },
        ]
    )
    frame["game_date"] = pd.to_datetime(frame["game_date"])
    return frame


def test_dynamic_opponent_k_pct_does_not_use_future_games() -> None:
    base = _source_frame()
    mutated = base.copy()
    mutated.loc[
        mutated["game_date"] == pd.Timestamp("2024-04-03"),
        "events",
    ] = "strikeout"

    base_daily = compute_opponent_k_pct_dynamic(
        "2024-04-01",
        "2024-04-03",
        source_df=base,
    ).sort_values(["Team", "game_date"])
    mutated_daily = compute_opponent_k_pct_dynamic(
        "2024-04-01",
        "2024-04-03",
        source_df=mutated,
    ).sort_values(["Team", "game_date"])

    # Earlier dates must not change when only future outcomes change.
    earlier_base = base_daily[base_daily["game_date"] < pd.Timestamp("2024-04-03")]
    earlier_mutated = mutated_daily[
        mutated_daily["game_date"] < pd.Timestamp("2024-04-03")
    ]

    assert earlier_base["K_pct_so_far"].equals(earlier_mutated["K_pct_so_far"])
