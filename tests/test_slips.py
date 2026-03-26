"""Unit tests for slip generation logic."""

from __future__ import annotations

import pandas as pd
import pytest
from src.mlb.slips import generate_slips, prepare_long_df


def test_generate_slips_uses_leg_multipliers() -> None:
    data = [
        {
            "pitcher_name": "Player A",
            "pitcher_team": "AAA",
            "upcoming_opponent": "BBB",
            "upcoming_game_date": pd.Timestamp("2025-04-01"),
            "upcoming_rest_days": 3,
            "upcoming_park_factor_K": 1.0,
            "play": "over",
            "prob": 0.6,
            "ev": 0.2,
            "payout": 1.9,
            "k_line": 5.5,
        },
        {
            "pitcher_name": "Player B",
            "pitcher_team": "CCC",
            "upcoming_opponent": "DDD",
            "upcoming_game_date": pd.Timestamp("2025-04-01"),
            "upcoming_rest_days": 4,
            "upcoming_park_factor_K": 0.95,
            "play": "under",
            "prob": 0.55,
            "ev": 0.15,
            "payout": 2.1,
            "k_line": 4.5,
        },
    ]

    df = pd.DataFrame(data)

    slips = generate_slips(df, slip_size=2, payout_table={2: 2.0})

    assert slips, "Expected a slip to be generated"
    slip = slips[0]

    expected_payout = 2.0
    expected_p_win = pytest.approx(0.6 * 0.55)
    expected_total_ev = pytest.approx(expected_payout * (0.6 * 0.55) - 1)

    assert slip["payout"] == pytest.approx(expected_payout)
    assert slip["p_win"] == expected_p_win
    assert slip["total_ev"] == expected_total_ev
    assert slip["legs"][0]["opponent"] == "BBB"
    assert slip["legs"][0]["game_date"] == "2025-04-01T00:00:00"
    assert slip["legs"][0]["rest_days"] == 3
    assert slip["legs"][0]["park_factor"] == 1.0


def test_generate_slips_rejects_slips_without_two_teams() -> None:
    data = [
        {
            "player": "Player A",
            "player_id": "a",
            "team": "AAA",
            "opponent": "BBB",
            "game_date": pd.Timestamp("2025-04-01"),
            "rest_days": 3,
            "park_factor": 1.0,
            "stat_id": "strikeouts",
            "line": 5.5,
            "play": "over",
            "prob": 0.6,
            "ev": 0.2,
            "payout": 1.9,
            "payout_multiplier": 0.9,
        },
        {
            "player": "Player B",
            "player_id": "b",
            "team": "AAA",
            "opponent": "CCC",
            "game_date": pd.Timestamp("2025-04-01"),
            "rest_days": 4,
            "park_factor": 0.95,
            "stat_id": "outs_recorded",
            "line": 15.5,
            "play": "under",
            "prob": 0.55,
            "ev": 0.15,
            "payout": 2.1,
            "payout_multiplier": 0.91,
        },
        {
            "player": "Player C",
            "player_id": "c",
            "team": "AAA",
            "opponent": "DDD",
            "game_date": pd.Timestamp("2025-04-01"),
            "rest_days": 5,
            "park_factor": 1.05,
            "stat_id": "hits_allowed",
            "line": 6.5,
            "play": "over",
            "prob": 0.5,
            "ev": 0.1,
            "payout": 1.95,
            "payout_multiplier": 0.92,
        },
    ]

    df = pd.DataFrame(data)

    slips = generate_slips(df, slip_size=3, payout_table={3: 6.0})

    assert slips == []


def test_prepare_long_df_preserves_legacy_context_fields() -> None:
    results = pd.DataFrame(
        [
            {
                "player": "Player A",
                "pitcher_id": "a",
                "pitcher_team": "AAA",
                "upcoming_opponent": "BBB",
                "upcoming_game_date": pd.Timestamp("2025-04-01"),
                "upcoming_rest_days": 3,
                "upcoming_park_factor_K": 1.1,
                "k_line": 5.5,
                "prob_over": 0.6,
                "prob_under": 0.4,
                "ev_over": 0.2,
                "ev_under": -0.1,
                "over_decimal_price": 1.9,
                "under_decimal_price": 1.8,
            }
        ]
    )

    long_df = prepare_long_df(results, min_ev=-1.0)

    assert list(long_df["opponent"]) == ["BBB", "BBB"]
    assert list(long_df["game_date"]) == [pd.Timestamp("2025-04-01")] * 2
    assert list(long_df["rest_days"]) == [3, 3]
    assert list(long_df["park_factor"]) == [1.1, 1.1]
