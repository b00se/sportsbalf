"""Tests for MLB live line normalization and snapshot writing."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from src.mlb.data.load_props import load_pitcher_prop_lines
from src.mlb.pitcher_props.descriptors import STAT_DESCRIPTORS
from src.mlb.pitcher_props.live_lines import (
    normalize_live_pitcher_prop_lines,
    write_live_pitcher_prop_snapshot,
)

SUPPORTED_STATS = tuple(STAT_DESCRIPTORS.keys())


def _unified_rows(
    target_stat: str,
    *,
    player_name_override: str | None = None,
) -> pd.DataFrame:
    other_stat = next(stat for stat in SUPPORTED_STATS if stat != target_stat)
    target_player_name = player_name_override or "Zack Wheeler"
    return pd.DataFrame(
        [
            {
                "appearance_id": "appearance-z",
                "player_ud_id": "player-z",
                "player": "Zack Wheeler",
                "player_name": target_player_name,
                "game_id": "game-2",
                "team_id": "team-home",
                "line": 8.5,
                "book": "Underdog",
                "scheduled_at": "2026-03-25T19:05:00Z",
                "season_type": "regular",
                "stat_id": target_stat,
                "over_decimal_price": 1.92,
                "over_payout_multiplier": 0.92,
                "over_american_price": -110,
                "under_decimal_price": 1.88,
                "under_payout_multiplier": 0.88,
                "under_american_price": -115,
            },
            {
                "appearance_id": "appearance-d",
                "player_ud_id": "player-d",
                "player": "Dylan Cease",
                "player_name": "Dylan Cease",
                "game_id": "game-3",
                "team_id": "team-away",
                "line": 6.5,
                "book": "Underdog",
                "scheduled_at": "2026-03-25T20:15:00Z",
                "season_type": "regular",
                "stat_id": other_stat,
                "over_decimal_price": 1.95,
                "over_payout_multiplier": 0.95,
                "over_american_price": -105,
                "under_decimal_price": 1.85,
                "under_payout_multiplier": 0.85,
                "under_american_price": -120,
            },
            {
                "appearance_id": "appearance-a",
                "player_ud_id": "player-a",
                "player": "Aaron Nola",
                "player_name": "Aaron Nola",
                "game_id": "game-1",
                "team_id": "team-away",
                "line": 7.5,
                "book": "Underdog",
                "scheduled_at": "2026-03-25T18:35:00Z",
                "season_type": "regular",
                "stat_id": target_stat,
                "over_decimal_price": 1.9,
                "over_payout_multiplier": 0.9,
                "over_american_price": -111,
                "under_decimal_price": 1.9,
                "under_payout_multiplier": 0.9,
                "under_american_price": -111,
            },
        ]
    )


@pytest.mark.parametrize("stat", SUPPORTED_STATS)
def test_normalize_live_pitcher_prop_lines_maps_supported_stat_columns(
    stat: str,
) -> None:
    frame = normalize_live_pitcher_prop_lines(_unified_rows(stat), stat)
    descriptor = STAT_DESCRIPTORS[stat]

    assert list(frame["player"]) == ["Aaron Nola", "Zack Wheeler"]
    assert descriptor.line_col in frame.columns
    assert frame[descriptor.line_col].tolist() == [7.5, 8.5]
    assert frame["stat_id"].tolist() == [stat, stat]
    assert frame["over_decimal_price"].tolist() == [1.9, 1.92]
    assert frame["under_decimal_price"].tolist() == [1.9, 1.88]


@pytest.mark.parametrize("stat", SUPPORTED_STATS)
def test_normalize_live_pitcher_prop_lines_falls_back_to_player_when_name_blank(
    stat: str,
) -> None:
    frame = normalize_live_pitcher_prop_lines(
        _unified_rows(stat, player_name_override="   "),
        stat,
    )

    assert list(frame["player"]) == ["Aaron Nola", "Zack Wheeler"]
    assert list(frame["player_name"]) == ["Aaron Nola", "Zack Wheeler"]


def test_normalize_live_pitcher_prop_lines_keeps_uuid_backed_provider_stat_ids() -> (
    None
):
    frame = _unified_rows("strikeouts").copy()
    frame["stat_id"] = "311b6775-4d03-4466-8ab9-776442468b27"

    normalized = normalize_live_pitcher_prop_lines(frame, "strikeouts")

    assert list(normalized["player"]) == ["Aaron Nola", "Dylan Cease", "Zack Wheeler"]
    assert normalized["stat_id"].tolist() == [
        "311b6775-4d03-4466-8ab9-776442468b27",
        "311b6775-4d03-4466-8ab9-776442468b27",
        "311b6775-4d03-4466-8ab9-776442468b27",
    ]


@pytest.mark.parametrize("stat", SUPPORTED_STATS)
def test_write_live_pitcher_prop_snapshot_writes_deterministic_csv(
    tmp_path: Path,
    stat: str,
) -> None:
    descriptor = STAT_DESCRIPTORS[stat]
    output_path = write_live_pitcher_prop_snapshot(
        _unified_rows(stat),
        stat,
        output_dir=tmp_path,
        snapshot_date=pd.Timestamp("2026-03-25T12:34:56Z"),
    )

    assert output_path == tmp_path / f"{stat}_2026-03-25.csv"
    assert output_path.exists()

    loaded = load_pitcher_prop_lines(str(output_path), descriptor.line_col)
    assert list(loaded["player"]) == ["Aaron Nola", "Zack Wheeler"]
    assert loaded[descriptor.line_col].tolist() == [7.5, 8.5]
    assert loaded["stat_id"].tolist() == [stat, stat]
    assert loaded["book"].tolist() == ["Underdog", "Underdog"]


def test_normalize_live_pitcher_prop_lines_returns_empty_frame_for_missing_stat() -> (
    None
):
    frame = normalize_live_pitcher_prop_lines(_unified_rows("strikeouts"), "bb_allowed")

    assert frame.empty
    assert "bb_line" in frame.columns
    assert "player" in frame.columns


@pytest.mark.parametrize("missing_column", ["game_id", "appearance_id", "scheduled_at"])
def test_normalize_live_pitcher_prop_lines_rejects_missing_sort_columns(
    missing_column: str,
) -> None:
    frame = _unified_rows("strikeouts").drop(columns=[missing_column])

    with pytest.raises(ValueError, match=missing_column):
        normalize_live_pitcher_prop_lines(frame, "strikeouts")
