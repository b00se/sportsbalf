"""Phase 1.5 feature engineering and snapshot dataset tests."""

from __future__ import annotations

import pandas as pd


def test_shifted_rolling_features_are_leakage_safe() -> None:
    from src.fantasy.adapters.mlb.feature_engineering import (
        add_phase15_rolling_features,
    )

    frame = pd.DataFrame(
        {
            "batter": ["42", "42", "42"],
            "game_date": ["2025-04-01", "2025-04-02", "2025-04-03"],
            "hits": [1.0, 3.0, 100.0],
            "plate_appearances": [4.0, 4.0, 4.0],
            "total_bases": [2.0, 4.0, 12.0],
            "walks": [0.0, 1.0, 0.0],
            "strikeouts": [1.0, 0.0, 0.0],
            "hard_hit_events": [1.0, 1.0, 4.0],
        }
    )

    engineered = add_phase15_rolling_features(
        frame,
        entity_id_col="batter",
        date_col="game_date",
    )

    third_row = engineered.iloc[2]
    # Shifted rolling mean should only use rows before 2025-04-03.
    assert float(third_row["roll_7_hits"]) == 2.0


def test_snapshot_builder_generates_rest_of_season_target() -> None:
    from src.fantasy.adapters.mlb.datasets import build_player_season_snapshots

    frame = pd.DataFrame(
        {
            "batter": ["10", "10", "10", "10"],
            "game_date": ["2025-04-01", "2025-04-08", "2025-05-01", "2025-06-01"],
            "season": [2025, 2025, 2025, 2025],
            "plate_appearances": [4.0, 5.0, 4.0, 4.0],
            "hits": [1.0, 2.0, 1.0, 3.0],
            "total_bases": [2.0, 3.0, 2.0, 5.0],
            "walks": [0.0, 1.0, 0.0, 1.0],
            "strikeouts": [1.0, 1.0, 0.0, 1.0],
            "hard_hit_events": [1.0, 1.0, 1.0, 2.0],
            "pa_vs_lhp": [1.0, 2.0, 1.0, 2.0],
            "pa_vs_rhp": [3.0, 3.0, 3.0, 2.0],
            "hard_hit_rate": [0.25, 0.2, 0.25, 0.5],
        }
    )

    snapshots = build_player_season_snapshots(
        frame,
        entity_id_col="batter",
        date_col="game_date",
        target_col="hits",
        snapshot_min_games=1,
        snapshot_anchor_frequency="weekly",
    )

    assert not snapshots.empty
    first = snapshots.iloc[0]
    assert "anchor_date" in snapshots.columns
    assert "season_to_date_hits" in snapshots.columns
    assert "target_rest_of_season_hits" in snapshots.columns
    assert float(first["target_rest_of_season_hits"]) >= 0.0
