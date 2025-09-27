"""Unit tests for slip generation logic."""

from __future__ import annotations

import pandas as pd
import pytest

from src.mlb.slips import generate_slips


def test_generate_slips_uses_leg_multipliers() -> None:
    data = [
        {
            "player": "Player A",
            "team": "AAA",
            "opponent": "BBB",
            "game_date": pd.Timestamp("2025-04-01"),
            "rest_days": 3,
            "park_factor": 1.0,
            "play": "over",
            "prob": 0.6,
            "ev": 0.2,
            "payout": 1.9,
            "payout_multiplier": 1.05,
            "line": 5.5,
            "sport": "MLB",
            "market": "strikeouts",
        },
        {
            "player": "Player B",
            "team": "CCC",
            "opponent": "DDD",
            "game_date": pd.Timestamp("2025-04-01"),
            "rest_days": 4,
            "park_factor": 0.95,
            "play": "under",
            "prob": 0.55,
            "ev": 0.15,
            "payout": 2.1,
            "payout_multiplier": 0.95,
            "line": 4.5,
            "sport": "MLB",
            "market": "strikeouts",
        },
    ]

    df = pd.DataFrame(data)

    slips = generate_slips(df, slip_size=2, base_multipliers={2: 2.0})

    assert slips, "Expected a slip to be generated"
    slip = slips[0]

    expected_payout = 2.0 * 1.05 * 0.95
    expected_p_win = pytest.approx(0.6 * 0.55)
    expected_total_ev = pytest.approx(expected_payout * (0.6 * 0.55) - 1)

    assert slip["payout"] == pytest.approx(expected_payout)
    assert slip["p_win"] == expected_p_win
    assert slip["total_ev"] == expected_total_ev

    leg_multipliers = [leg["payout_multiplier"] for leg in slip["legs"]]
    assert pytest.approx(leg_multipliers[0]) == 1.05
    assert pytest.approx(leg_multipliers[1]) == 0.95
