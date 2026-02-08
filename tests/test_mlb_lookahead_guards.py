from __future__ import annotations

import pandas as pd
from src.mlb.features.feature_store import build_historical_live_features
from src.mlb.features.opponent_k import add_opponent_k_rate
from src.mlb.features.rolling import add_rolling_features


def _synthetic_games() -> pd.DataFrame:
    frame = pd.DataFrame(
        [
            {
                "pitcher": 101,
                "pitcher_id": 101,
                "game_date": "2024-04-01",
                "strikeouts": 5,
                "pitch_count": 90,
                "opponent_team": "NYY",
                "umpire": "U1",
                "p_throws": "R",
            },
            {
                "pitcher": 102,
                "pitcher_id": 102,
                "game_date": "2024-04-02",
                "strikeouts": 7,
                "pitch_count": 88,
                "opponent_team": "NYY",
                "umpire": "U1",
                "p_throws": "L",
            },
            {
                "pitcher": 101,
                "pitcher_id": 101,
                "game_date": "2024-04-03",
                "strikeouts": 3,
                "pitch_count": 92,
                "opponent_team": "NYY",
                "umpire": "U1",
                "p_throws": "R",
            },
            {
                "pitcher": 102,
                "pitcher_id": 102,
                "game_date": "2024-04-04",
                "strikeouts": 10,
                "pitch_count": 95,
                "opponent_team": "NYY",
                "umpire": "U1",
                "p_throws": "L",
            },
        ]
    )
    frame["game_date"] = pd.to_datetime(frame["game_date"])
    return frame


def _keyed(frame: pd.DataFrame) -> pd.DataFrame:
    keyed = frame.copy()
    keyed["game_date"] = pd.to_datetime(keyed["game_date"])
    return keyed.sort_values(["game_date", "pitcher_id"]).reset_index(drop=True)


def test_umpire_features_do_not_use_future_games() -> None:
    base = _synthetic_games()
    mutated = base.copy()
    mutated.loc[mutated["game_date"].idxmax(), "strikeouts"] = 40

    base_features = _keyed(build_historical_live_features(base))
    mutated_features = _keyed(build_historical_live_features(mutated))

    # Prior rows must not change when only a future outcome changes.
    base_prior = base_features.iloc[:-1]
    mutated_prior = mutated_features.iloc[:-1]

    assert base_prior["umpire_sample_size"].equals(mutated_prior["umpire_sample_size"])
    assert base_prior["umpire_k_boost_expanding"].equals(
        mutated_prior["umpire_k_boost_expanding"]
    )


def test_rolling_and_opponent_features_do_not_use_future_games() -> None:
    base = _synthetic_games()
    mutated = base.copy()
    mutated.loc[mutated["game_date"].idxmax(), "strikeouts"] = 40
    mutated.loc[mutated["game_date"].idxmax(), "pitch_count"] = 120

    base_features = _keyed(add_opponent_k_rate(add_rolling_features(base)))
    mutated_features = _keyed(add_opponent_k_rate(add_rolling_features(mutated)))

    # Skip first row (fallback/default row) and final mutated future row.
    base_slice = base_features.iloc[1:-1]
    mutated_slice = mutated_features.iloc[1:-1]

    columns = [
        "rolling_K_avg_3",
        "rolling_K_avg_5",
        "rolling_pitch_count_5",
        "rolling_K_rate",
        "opponent_k_rate",
    ]
    for column in columns:
        assert base_slice[column].equals(mutated_slice[column])


def test_feature_build_is_order_independent() -> None:
    base = _synthetic_games()
    shuffled = base.sample(frac=1.0, random_state=7).reset_index(drop=True)

    base_features = _keyed(
        build_historical_live_features(add_opponent_k_rate(add_rolling_features(base)))
    )
    shuffled_features = _keyed(
        build_historical_live_features(
            add_opponent_k_rate(add_rolling_features(shuffled))
        )
    )

    compare_cols = [
        "rolling_K_avg_3",
        "rolling_K_avg_5",
        "rolling_pitch_count_5",
        "rolling_K_rate",
        "opponent_k_rate",
        "umpire_sample_size",
        "umpire_k_boost_expanding",
    ]
    for column in compare_cols:
        assert base_features[column].equals(shuffled_features[column])


def test_global_umpire_history_differs_from_per_pitcher_history() -> None:
    base = _synthetic_games()

    global_features = _keyed(build_historical_live_features(base))
    per_pitcher_features = _keyed(
        pd.concat(
            [
                build_historical_live_features(group.copy())
                for _, group in base.groupby("pitcher_id", sort=False)
            ],
            ignore_index=True,
        )
    )

    # Per-pitcher expansion undercounts umpire history; global is expected behavior.
    assert not global_features["umpire_sample_size"].equals(
        per_pitcher_features["umpire_sample_size"]
    )
    assert float(global_features.loc[1, "umpire_sample_size"]) == 1.0
    assert float(per_pitcher_features.loc[1, "umpire_sample_size"]) == 0.0
