"""Helpers for aggregating Statcast pitch-level data to game summaries."""

from __future__ import annotations

import logging

import pandas as pd

from .team_abbr_map import team_fix_map

REQUIRED_PITCH_COLUMNS = {"description", "events", "inning", "pitch_type"}


WHIFF_EVENTS = {"swinging_strike", "swinging_strike_blocked"}
SWING_EVENTS = WHIFF_EVENTS | {"foul", "foul_tip", "hit_into_play"}
logger = logging.getLogger(__name__)


def _dedupe_pitch_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate pitch-level rows using stable canonical keys.

    Args:
        df: Pitch-level Statcast-like frame.

    Returns:
        DataFrame with duplicate pitch rows removed.
    """

    key_candidates = [
        ["game_pk", "pitcher", "at_bat_number", "pitch_number"],
        ["pitcher", "game_date", "inning", "at_bat_number", "pitch_number"],
        ["pitcher", "game_date", "inning", "pitch_number"],
    ]
    for columns in key_candidates:
        if set(columns).issubset(df.columns):
            deduped = df.drop_duplicates(subset=columns, keep="last").copy()
            removed = len(df) - len(deduped)
            if removed > 0:
                logger.info(
                    "Removed %d duplicate pitch rows using key=%s.",
                    removed,
                    columns,
                )
            return deduped

    deduped = df.drop_duplicates().copy()
    removed = len(df) - len(deduped)
    if removed > 0:
        logger.info("Removed %d duplicate pitch rows using full-row dedupe.", removed)
    return deduped


def _first_pitch_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Return the first pitch metadata for each pitcher/date pairing."""

    meta_cols = [
        "home_team",
        "away_team",
        "pitcher_days_since_prev_game",
        "inning_topbot",
    ]
    sort_cols = ["pitcher", "game_date"]
    if "pitch_number" in df.columns:
        sort_cols.append("pitch_number")

    meta = (
        df.sort_values(sort_cols)
        .groupby(["pitcher", "game_date"], as_index=False)[meta_cols]
        .first()
    )

    meta["rest_days"] = (
        pd.to_numeric(meta["pitcher_days_since_prev_game"], errors="coerce")
        .fillna(5)
        .astype(int)
    )
    meta.drop(columns=["pitcher_days_since_prev_game"], inplace=True)

    meta["pitcher_team"] = meta.apply(
        lambda row: (
            row["home_team"] if row["inning_topbot"] == "Top" else row["away_team"]
        ),
        axis=1,
    )
    meta["opponent_team"] = meta.apply(
        lambda row: (
            row["away_team"] if row["inning_topbot"] == "Top" else row["home_team"]
        ),
        axis=1,
    )
    meta.drop(columns=["inning_topbot"], inplace=True)

    for col in ["pitcher_team", "opponent_team", "home_team", "away_team"]:
        meta[col] = meta[col].replace(team_fix_map)

    return meta


def _count_events(df: pd.DataFrame) -> pd.DataFrame:
    grouped = df.groupby(["pitcher", "game_date"])

    return (
        grouped["description"]
        .agg(
            whiff_count=lambda x: x.isin(WHIFF_EVENTS).sum(),
            swing_count=lambda x: x.isin(SWING_EVENTS).sum(),
            called_count=lambda x: (x == "called_strike").sum(),
        )
        .reset_index()
    )


def aggregate_pitcher_games(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate Statcast pitch-level records into pitcher game summaries."""

    if not REQUIRED_PITCH_COLUMNS.issubset(df.columns):
        # Dataset already appears to be aggregated to the game level.
        cleanup_cols = [
            "rolling_K_avg_3",
            "rolling_K_avg_5",
            "rolling_pitch_count_5",
            "rolling_K_rate",
            "park_factor_K",
            "opponent_k_rate",
            "Unnamed: 0",
        ]
        existing = [col for col in cleanup_cols if col in df.columns]
        if existing:
            df = df.drop(columns=existing)
        return df.reset_index(drop=True)

    deduped = _dedupe_pitch_rows(df)
    grouped = deduped.groupby(["pitcher", "game_date"])
    games = grouped.agg(
        pitch_count=("description", "count"),
        strikeouts=("events", lambda x: (x == "strikeout").sum()),
        max_inning=("inning", "max"),
        num_pitch_types=("pitch_type", pd.Series.nunique),
    ).reset_index()

    meta = _first_pitch_metadata(deduped)
    games = games.merge(meta, on=["pitcher", "game_date"], how="left")

    counts = _count_events(deduped)
    games = games.merge(counts, on=["pitcher", "game_date"], how="left")

    games.sort_values(["pitcher", "game_date"], inplace=True)
    games.reset_index(drop=True, inplace=True)

    games["whiff_rate"] = (
        (games["whiff_count"] / games["swing_count"])
        .replace([float("inf"), -float("inf")], 0)
        .fillna(0)
    )
    games["csw_pct"] = (
        ((games["whiff_count"] + games["called_count"]) / games["pitch_count"])
        .replace([float("inf"), -float("inf")], 0)
        .fillna(0)
    )

    cumulative = (
        games.groupby("pitcher")[["whiff_count", "called_count", "pitch_count"]]
        .cumsum()
        .fillna(0)
    )
    games["whiff_rate_expanding"] = (
        (cumulative["whiff_count"] / cumulative["pitch_count"])
        .replace([float("inf"), -float("inf")], 0)
        .fillna(0)
    )
    games["csw_pct_expanding"] = (
        (
            (cumulative["whiff_count"] + cumulative["called_count"])
            / cumulative["pitch_count"]
        )
        .replace([float("inf"), -float("inf")], 0)
        .fillna(0)
    )

    games.drop(columns=["whiff_count", "swing_count", "called_count"], inplace=True)

    return games
