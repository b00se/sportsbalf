"""Mixed MLB prop slip tests."""

from __future__ import annotations

import pandas as pd
from src.mlb.slips import SlipBuilderConfig, build_slip_sets


def _mixed_candidate_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "player": "Gerrit Cole",
                "player_id": "cole-1",
                "team": "NYY",
                "opponent": "BOS",
                "game_date": pd.Timestamp("2026-03-25"),
                "rest_days": 4,
                "park_factor": 1.02,
                "stat_id": "strikeouts",
                "line": 8.5,
                "play": "over",
                "prob": 0.62,
                "ev": 0.16,
                "payout": 1.92,
                "payout_multiplier": 0.92,
                "run_mode": "prediction",
                "lines_status": "present",
                "model_name": "xgboost",
                "model_strategy": "baseline",
                "sport": "MLB",
                "market": "strikeouts",
            },
            {
                "player": "Gerrit Cole",
                "player_id": "cole-1",
                "team": "NYY",
                "opponent": "BOS",
                "game_date": pd.Timestamp("2026-03-25"),
                "rest_days": 4,
                "park_factor": 1.02,
                "stat_id": "outs_recorded",
                "line": 18.5,
                "play": "over",
                "prob": 0.58,
                "ev": 0.11,
                "payout": 1.9,
                "payout_multiplier": 0.9,
                "run_mode": "prediction",
                "lines_status": "present",
                "model_name": "xgboost",
                "model_strategy": "baseline",
                "sport": "MLB",
                "market": "outs_recorded",
            },
            {
                "player": "Aaron Nola",
                "player_id": "nola-1",
                "team": "PHI",
                "opponent": "ATL",
                "game_date": pd.Timestamp("2026-03-25"),
                "rest_days": 5,
                "park_factor": 0.97,
                "stat_id": "hits_allowed",
                "line": 6.5,
                "play": "under",
                "prob": 0.57,
                "ev": 0.13,
                "payout": 2.05,
                "payout_multiplier": 1.05,
                "run_mode": "prediction",
                "lines_status": "present",
                "model_name": "xgboost",
                "model_strategy": "baseline",
                "sport": "MLB",
                "market": "hits_allowed",
            },
        ]
    )


def test_build_slip_sets_supports_mixed_stat_same_pitcher_stack() -> None:
    results = _mixed_candidate_rows()
    config = SlipBuilderConfig(
        top_n=10,
        conservative_count=0,
        fullsend_count=1,
        fullsend_min_size=3,
        fullsend_max_size=3,
        payout_table={3: 6.0},
    )

    slip_sets = build_slip_sets(results, config=config)

    assert slip_sets["conservative"] == []
    assert len(slip_sets["fullsend"]) == 1

    slip = slip_sets["fullsend"][0]
    assert sum(leg["player"] == "Gerrit Cole" for leg in slip["legs"]) == 2
    assert {leg["team"] for leg in slip["legs"]} == {"NYY", "PHI"}
    assert {leg["stat_id"] for leg in slip["legs"]} == {
        "strikeouts",
        "outs_recorded",
        "hits_allowed",
    }
    assert {leg["opponent"] for leg in slip["legs"]} == {"BOS", "ATL"}
    assert {leg["rest_days"] for leg in slip["legs"]} == {4, 5}
    assert {leg["park_factor"] for leg in slip["legs"]} == {1.02, 0.97}
    assert {leg["run_mode"] for leg in slip["legs"]} == {"prediction"}
    assert {leg["lines_status"] for leg in slip["legs"]} == {"present"}
    assert {leg["model_name"] for leg in slip["legs"]} == {"xgboost"}
    assert {leg["model_strategy"] for leg in slip["legs"]} == {"baseline"}
