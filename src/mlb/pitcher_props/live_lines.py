"""Helpers for normalizing live MLB pitcher prop lines and writing snapshots."""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from src.mlb.data.load_props import normalize_pitcher_prop_lines
from src.mlb.pitcher_props.descriptors import (
    STAT_DESCRIPTORS,
    StatDescriptor,
    get_stat_descriptor,
)

logger = logging.getLogger(__name__)

_SOURCE_COLUMNS: tuple[str, ...] = (
    "appearance_id",
    "player_ud_id",
    "player_name",
    "game_id",
    "team_id",
    "scheduled_at",
    "season_type",
    "stat_id",
    "book",
    "line",
    "over_decimal_price",
    "over_payout_multiplier",
    "over_american_price",
    "under_decimal_price",
    "under_payout_multiplier",
    "under_american_price",
)

_SORT_COLUMNS: tuple[str, ...] = ("game_id", "appearance_id", "scheduled_at")


def _snapshot_date_string(snapshot_date: date | datetime | pd.Timestamp | str) -> str:
    """Return a deterministic ISO date string for a snapshot filename."""

    timestamp = pd.Timestamp(snapshot_date)
    if pd.isna(timestamp):
        raise ValueError("snapshot_date must be a valid date or timestamp.")
    return timestamp.date().isoformat()


def _empty_live_line_frame(descriptor: StatDescriptor) -> pd.DataFrame:
    """Return an empty canonical live-line frame for a stat descriptor."""

    columns = [
        "appearance_id",
        "player_ud_id",
        "player",
        "player_name",
        "game_id",
        "team_id",
        "scheduled_at",
        "season_type",
        "stat_id",
        "book",
        descriptor.line_col,
        "over_decimal_price",
        "over_payout_multiplier",
        "over_american_price",
        "under_decimal_price",
        "under_payout_multiplier",
        "under_american_price",
    ]
    return pd.DataFrame(columns=columns)


def _clean_text_series(frame: pd.Series) -> pd.Series:
    """Return a string series with blank values normalized to missing."""

    cleaned = frame.astype("string").str.strip()
    return cleaned.mask(cleaned.eq(""))


def _require_columns(frame: pd.DataFrame, columns: tuple[str, ...]) -> None:
    """Raise if a frame is missing required columns."""

    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(
            f"Live lines missing required columns for normalization: {missing}"
        )


def normalize_live_pitcher_prop_lines(
    live_lines: pd.DataFrame,
    stat: str,
) -> pd.DataFrame:
    """Normalize unified Underdog live rows into a stat-specific line frame.

    Args:
        live_lines: Unified live line rows from the Underdog ingestion helper.
        stat: MLB pitcher-prop stat identifier.

    Returns:
        Canonical stat-specific line frame suitable for existing loaders.
    """

    descriptor = get_stat_descriptor(stat)
    work = live_lines.copy()

    if "stat_id" in work.columns:
        stat_ids = work["stat_id"].astype(str)
        canonical_stats = set(STAT_DESCRIPTORS)
        if set(stat_ids) & canonical_stats:
            work = work.loc[stat_ids == descriptor.stat].copy()

    if work.empty:
        return _empty_live_line_frame(descriptor)

    _require_columns(work, _SORT_COLUMNS)

    if "player" not in work.columns:
        if "player_name" not in work.columns:
            raise ValueError("Live lines require a player or player_name column.")
        work["player"] = pd.NA
    if "player_name" not in work.columns:
        work["player_name"] = pd.NA

    work["player"] = _clean_text_series(work["player"])
    work["player_name"] = _clean_text_series(work["player_name"])
    resolved_player = work["player_name"].combine_first(work["player"])
    if resolved_player.isna().any():
        raise ValueError("Live lines require a non-empty player_name or player value.")
    work["player"] = resolved_player
    work["player_name"] = resolved_player

    if descriptor.line_col not in work.columns:
        if "line" not in work.columns:
            raise ValueError("Live lines require a line column.")
        work[descriptor.line_col] = work["line"]

    work = work.drop(columns=["line"], errors="ignore")

    work = work.sort_values(
        ["player", "game_id", "appearance_id", "scheduled_at"],
        kind="stable",
    ).reset_index(drop=True)

    work = normalize_pitcher_prop_lines(work, descriptor.line_col)

    ordered_columns = [
        "appearance_id",
        "player_ud_id",
        "player",
        "player_name",
        "game_id",
        "team_id",
        "scheduled_at",
        "season_type",
        "stat_id",
        "book",
        descriptor.line_col,
        "over_decimal_price",
        "over_payout_multiplier",
        "over_american_price",
        "under_decimal_price",
        "under_payout_multiplier",
        "under_american_price",
    ]
    for column in ordered_columns:
        if column not in work.columns:
            work[column] = pd.NA

    return work.loc[:, ordered_columns].copy()


def write_live_pitcher_prop_snapshot(
    live_lines: pd.DataFrame,
    stat: str,
    *,
    output_dir: str | Path,
    snapshot_date: date | datetime | pd.Timestamp | str,
) -> Path:
    """Write a stat-specific live line snapshot to a dated CSV file.

    Args:
        live_lines: Unified live line rows from the Underdog ingestion helper.
        stat: MLB pitcher-prop stat identifier.
        output_dir: Directory where the dated snapshot should be written.
        snapshot_date: Date used in the filename.

    Returns:
        Path to the written snapshot file.
    """

    descriptor = get_stat_descriptor(stat)
    normalized = normalize_live_pitcher_prop_lines(live_lines, stat)
    snapshot_dir = Path(output_dir)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = snapshot_dir / (
        f"{descriptor.stat}_{_snapshot_date_string(snapshot_date)}.csv"
    )
    normalized.to_csv(snapshot_path, index=False)
    logger.info(
        "Wrote MLB live line snapshot for '%s' to %s",
        descriptor.stat,
        snapshot_path,
    )
    return snapshot_path
