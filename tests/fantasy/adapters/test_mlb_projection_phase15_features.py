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


def test_hits_pa_training_view_applies_regular_season_dedup_and_constraints() -> None:
    from src.fantasy.adapters.mlb.datasets import build_hits_pa_training_view

    frame = pd.DataFrame(
        {
            "game_pk": [1, 1, 1, 2],
            "batter": ["42", "42", "42", "42"],
            "game_date": ["2025-04-01", "2025-04-01", "2025-04-01", "2025-04-02"],
            "at_bat_number": [10, 10, 11, 12],
            "pitch_number": [1, 2, 1, 1],
            "game_type": ["R", "R", "R", "S"],
            "events": ["none", "single", "walk", "single"],
            "pitcher_throws": ["R", "R", "L", "R"],
            "launch_speed": [70.0, 101.0, 82.0, 99.0],
        }
    )

    cleaned = build_hits_pa_training_view(
        frame,
        entity_id_col="batter",
        date_col="game_date",
        regular_season_only=True,
        require_batter_pa_dedup=True,
    )

    assert set(cleaned.columns).issuperset(
        {"is_regular_season", "pa_terminal_dedup_applied", "qa_invalid_row_flag"}
    )
    assert cleaned["is_regular_season"].all()
    # Two regular-season terminal PA rows survive for 2025-04-01:
    # at_bat_number=10 -> single, at_bat_number=11 -> walk.
    row = cleaned.iloc[0]
    assert float(row["plate_appearances"]) == 2.0
    assert float(row["hits"]) == 1.0
    assert float(row["hits"]) <= float(row["plate_appearances"])


def test_snapshot_builder_includes_anchor_day_in_future_target() -> None:
    from src.fantasy.adapters.mlb.datasets import build_player_season_snapshots

    frame = pd.DataFrame(
        {
            "batter": ["10", "10", "10"],
            "game_date": ["2025-04-01", "2025-04-02", "2025-04-03"],
            "season": [2025, 2025, 2025],
            "plate_appearances": [4.0, 4.0, 4.0],
            "hits": [1.0, 2.0, 3.0],
            "total_bases": [1.0, 2.0, 3.0],
            "walks": [0.0, 0.0, 0.0],
            "strikeouts": [1.0, 1.0, 1.0],
            "hard_hit_events": [1.0, 1.0, 1.0],
            "pa_vs_lhp": [1.0, 1.0, 1.0],
            "pa_vs_rhp": [3.0, 3.0, 3.0],
        }
    )

    snapshots = build_player_season_snapshots(
        frame,
        entity_id_col="batter",
        date_col="game_date",
        target_col="hits",
        snapshot_min_games=1,
        snapshot_anchor_frequency="daily",
    )

    row = snapshots[snapshots["anchor_date"] == "2025-04-02"].iloc[0]
    assert float(row["season_to_date_hits"]) == 1.0
    # Anchor-day hit total (2.0) should be included in future label.
    assert float(row["target_rest_of_season_hits"]) == 5.0


def test_hits_pa_training_view_uses_hard_hit_rate_fallback_without_statcast() -> None:
    from src.fantasy.adapters.mlb.datasets import build_hits_pa_training_view

    frame = pd.DataFrame(
        {
            "batter": ["42"],
            "game_date": ["2025-04-01"],
            "plate_appearances": [4.0],
            "hits": [1.0],
            "hard_hit_rate": [0.5],
        }
    )

    cleaned = build_hits_pa_training_view(
        frame,
        entity_id_col="batter",
        date_col="game_date",
        regular_season_only=True,
        require_batter_pa_dedup=True,
    )

    row = cleaned.iloc[0]
    assert float(row["hard_hit_events"]) == 2.0
