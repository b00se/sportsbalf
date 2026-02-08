from __future__ import annotations

import pandas as pd
from src.mlb.features.feature_store import build_historical_live_features


def _games_frame() -> pd.DataFrame:
    frame = pd.DataFrame(
        [
            {
                "pitcher": 1,
                "pitcher_id": 1,
                "game_date": "2024-04-01",
                "strikeouts": 6,
                "pitch_count": 88,
                "opponent_team": "NYY",
                "umpire": "U1",
                "p_throws": "R",
            },
            {
                "pitcher": 2,
                "pitcher_id": 2,
                "game_date": "2024-04-02",
                "strikeouts": 4,
                "pitch_count": 92,
                "opponent_team": "NYY",
                "umpire": "U1",
                "p_throws": "L",
            },
            {
                "pitcher": 1,
                "pitcher_id": 1,
                "game_date": "2024-04-03",
                "strikeouts": 8,
                "pitch_count": 94,
                "opponent_team": "BOS",
                "umpire": "U2",
                "p_throws": "R",
            },
            {
                "pitcher": 2,
                "pitcher_id": 2,
                "game_date": "2024-04-04",
                "strikeouts": 5,
                "pitch_count": 90,
                "opponent_team": "BOS",
                "umpire": "U2",
                "p_throws": "L",
            },
        ]
    )
    frame["game_date"] = pd.to_datetime(frame["game_date"])
    return frame


def test_global_historical_live_feature_build_matches_pipeline_style() -> None:
    frame = _games_frame()

    # Script-style accumulation: concatenate pitcher slices, then build globally.
    script_like = pd.concat(
        [group.copy() for _, group in frame.groupby("pitcher_id", sort=False)],
        ignore_index=True,
    )
    script_like = build_historical_live_features(script_like)

    # Pipeline-style: build globally on one frame.
    pipeline_like = build_historical_live_features(frame.copy())

    key_cols = [
        "pitcher_id",
        "game_date",
        "umpire_sample_size",
        "umpire_k_boost_expanding",
        "pitcher_throws_encoded",
        "same_hand_matchup_rate",
    ]
    script_norm = (
        script_like[key_cols]
        .sort_values(["pitcher_id", "game_date"])
        .reset_index(drop=True)
    )
    pipeline_norm = (
        pipeline_like[key_cols]
        .sort_values(["pitcher_id", "game_date"])
        .reset_index(drop=True)
    )

    assert script_norm.equals(pipeline_norm)
