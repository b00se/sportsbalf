"""Data engineering helpers for MLB multi-stat pitcher props."""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from src.mlb.features import aggregate_pitcher_games

logger = logging.getLogger(__name__)

HIT_EVENTS = {"single", "double", "triple", "home_run"}
WALK_EVENTS = {"walk", "intent_walk"}
ON_BASE_EVENTS = HIT_EVENTS | WALK_EVENTS | {"hit_by_pitch", "field_error"}

_OUT_EVENT_MAP = {
    "field_out": 1,
    "force_out": 1,
    "grounded_into_double_play": 2,
    "double_play": 2,
    "triple_play": 3,
    "sac_fly": 1,
    "sac_bunt": 1,
    "strikeout": 1,
    "strikeout_double_play": 2,
    "fielders_choice_out": 1,
    "other_out": 1,
    "caught_stealing_2b": 1,
    "caught_stealing_3b": 1,
    "caught_stealing_home": 1,
    "pickoff_caught_stealing_2b": 1,
    "pickoff_caught_stealing_3b": 1,
    "pickoff_caught_stealing_home": 1,
}


def _terminal_plate_appearances(frame: pd.DataFrame) -> pd.DataFrame:
    """Return one terminal row per pitcher/game/plate appearance.

    Args:
        frame: Pitch-level Statcast-like frame.

    Returns:
        Deduplicated plate-appearance terminal rows.
    """

    terminal = frame.copy()
    terminal = terminal[terminal["events"].notna()].copy()
    terminal = terminal[
        ~terminal["events"].astype(str).str.lower().isin({"none", "nan"})
    ]

    if "pitch_number" in terminal.columns:
        terminal = terminal.sort_values("pitch_number")

    if {"game_pk", "pitcher", "at_bat_number"}.issubset(terminal.columns):
        keys = ["game_pk", "pitcher", "game_date", "at_bat_number"]
        return terminal.drop_duplicates(subset=keys, keep="last")

    return terminal


def _outs_from_events(events: pd.Series) -> pd.Series:
    """Convert terminal events into outs recorded.

    Args:
        events: Event name series.

    Returns:
        Numeric outs per event.
    """

    normalized = events.fillna("").astype(str).str.lower()
    mapped = normalized.map(_OUT_EVENT_MAP).fillna(0).astype(int)

    # Catch residual custom event labels that contain double/triple play text.
    mapped = np.where(normalized.str.contains("triple_play"), 3, mapped)
    mapped = np.where(
        normalized.str.contains("double_play")
        & (~normalized.str.contains("triple_play")),
        np.maximum(mapped, 2),
        mapped,
    )
    return pd.Series(mapped, index=events.index, dtype="int64")


def _runs_allowed_from_score_delta(terminal: pd.DataFrame) -> pd.Series:
    """Compute runs allowed from pre/post scoreboard state when available.

    Args:
        terminal: Terminal plate-appearance frame.

    Returns:
        Runs allowed per row as float, with NaN when score columns are absent.
    """

    required = {
        "inning_topbot",
        "home_score",
        "away_score",
        "post_home_score",
        "post_away_score",
    }
    if not required.issubset(terminal.columns):
        return pd.Series(np.nan, index=terminal.index, dtype="float64")

    home_delta = (
        pd.to_numeric(terminal["post_home_score"], errors="coerce")
        - pd.to_numeric(terminal["home_score"], errors="coerce")
    ).clip(lower=0)
    away_delta = (
        pd.to_numeric(terminal["post_away_score"], errors="coerce")
        - pd.to_numeric(terminal["away_score"], errors="coerce")
    ).clip(lower=0)

    is_top = terminal["inning_topbot"].astype(str).str.lower().eq("top")
    # Top inning: away bats, so home pitcher allows away runs.
    runs_allowed = np.where(is_top, away_delta, home_delta)
    return pd.Series(runs_allowed, index=terminal.index, dtype="float64")


def build_pitcher_game_table(pitch_df: pd.DataFrame) -> pd.DataFrame:
    """Build a multi-target pitcher-game table from Statcast-level data.

    Args:
        pitch_df: Pitch-level or already-aggregated frame.

    Returns:
        Pitcher-game table with shared and stat-specific target columns.
    """

    source = pitch_df.copy()
    if "game_date" in source.columns:
        source["game_date"] = pd.to_datetime(source["game_date"], errors="coerce")

    games = aggregate_pitcher_games(source)
    games = games.copy()

    if "game_date" in games.columns:
        games["game_date"] = pd.to_datetime(games["game_date"], errors="coerce")

    required_pitch_cols = {"events", "pitcher", "game_date"}
    if not required_pitch_cols.issubset(source.columns):
        for target in ["outs_recorded", "earned_runs", "hits_allowed", "bb_allowed"]:
            if target not in games.columns:
                games[target] = 0.0
        return games

    terminal = _terminal_plate_appearances(source)
    if terminal.empty:
        for target in ["outs_recorded", "earned_runs", "hits_allowed", "bb_allowed"]:
            if target not in games.columns:
                games[target] = 0.0
        return games

    terminal = terminal.copy()
    terminal["game_date"] = pd.to_datetime(terminal["game_date"], errors="coerce")
    terminal["outs_recorded"] = _outs_from_events(terminal["events"])
    terminal["hits_allowed"] = (
        terminal["events"].astype(str).isin(HIT_EVENTS).astype(int)
    )
    terminal["bb_allowed"] = (
        terminal["events"].astype(str).isin(WALK_EVENTS).astype(int)
    )
    terminal["on_base_events_allowed"] = (
        terminal["events"].astype(str).isin(ON_BASE_EVENTS).astype(int)
    )
    terminal["runs_allowed_play"] = _runs_allowed_from_score_delta(terminal)

    grouped = (
        terminal.groupby(["pitcher", "game_date"], as_index=False)[
            [
                "outs_recorded",
                "hits_allowed",
                "bb_allowed",
                "on_base_events_allowed",
                "runs_allowed_play",
            ]
        ]
        .sum(min_count=1)
        .rename(columns={"runs_allowed_play": "earned_runs"})
    )

    if "launch_speed" in source.columns:
        contact = source[source["launch_speed"].notna()].copy()
        if not contact.empty:
            contact["hard_contact_allowed"] = (
                pd.to_numeric(contact["launch_speed"], errors="coerce") >= 95
            ).astype(int)
            hard_contact = (
                contact.groupby(["pitcher", "game_date"], as_index=False)[
                    "hard_contact_allowed"
                ]
                .mean()
                .rename(columns={"hard_contact_allowed": "hard_contact_rate_allowed"})
            )
            grouped = grouped.merge(
                hard_contact, on=["pitcher", "game_date"], how="left"
            )

    games = games.merge(grouped, on=["pitcher", "game_date"], how="left")

    fallback_earned = None
    for candidate in ["earned_runs", "er", "runs_allowed"]:
        if candidate in source.columns:
            fallback_earned = candidate
            break

    if fallback_earned is not None:
        fallback = (
            source.groupby(["pitcher", "game_date"], as_index=False)[fallback_earned]
            .max()
            .rename(columns={fallback_earned: "earned_runs_fallback"})
        )
        games = games.merge(fallback, on=["pitcher", "game_date"], how="left")
        games["earned_runs"] = pd.to_numeric(games["earned_runs"], errors="coerce")
        games["earned_runs_fallback"] = pd.to_numeric(
            games["earned_runs_fallback"], errors="coerce"
        )
        missing_before = int(games["earned_runs"].isna().sum())
        games["earned_runs"] = games["earned_runs"].fillna(
            games["earned_runs_fallback"]
        )
        filled = missing_before - int(games["earned_runs"].isna().sum())
        if filled > 0:
            logger.info(
                "Filled %d earned-runs rows from fallback column '%s'.",
                filled,
                fallback_earned,
            )
        games.drop(columns=["earned_runs_fallback"], inplace=True)

    fill_zero = [
        "outs_recorded",
        "hits_allowed",
        "bb_allowed",
        "earned_runs",
        "on_base_events_allowed",
        "hard_contact_rate_allowed",
    ]
    for col in fill_zero:
        if col in games.columns:
            games[col] = pd.to_numeric(games[col], errors="coerce").fillna(0.0)

    return games


def build_batter_game_table(pitch_df: pd.DataFrame) -> pd.DataFrame:
    """Build reusable batter-game aggregates for future batter prop models.

    Args:
        pitch_df: Pitch-level Statcast-like frame.

    Returns:
        Batter-game table with PA, hits, total bases, walk/strikeout,
        and contact metrics.
    """

    required = {"batter", "events", "game_date"}
    if not required.issubset(pitch_df.columns):
        return pd.DataFrame(
            columns=[
                "batter",
                "game_date",
                "plate_appearances",
                "hits",
                "total_bases",
                "walks",
                "strikeouts",
                "hard_hit_rate",
                "pa_vs_lhp",
                "pa_vs_rhp",
            ]
        )

    terminal = _terminal_plate_appearances(pitch_df)
    if "batter" not in terminal.columns or terminal.empty:
        return pd.DataFrame()

    terminal = terminal.copy()
    terminal["game_date"] = pd.to_datetime(terminal["game_date"], errors="coerce")

    event_series = terminal["events"].astype(str)
    terminal["hits"] = event_series.isin(HIT_EVENTS).astype(int)
    terminal["walks"] = event_series.isin(WALK_EVENTS).astype(int)
    terminal["strikeouts"] = event_series.eq("strikeout").astype(int)

    tb_map = {"single": 1, "double": 2, "triple": 3, "home_run": 4}
    terminal["total_bases"] = event_series.map(tb_map).fillna(0).astype(int)
    terminal["plate_appearances"] = 1

    if "p_throws" in terminal.columns:
        throws = terminal["p_throws"].astype(str).str.upper()
        terminal["pa_vs_lhp"] = throws.eq("L").astype(int)
        terminal["pa_vs_rhp"] = throws.eq("R").astype(int)
    else:
        terminal["pa_vs_lhp"] = 0
        terminal["pa_vs_rhp"] = 0

    batter_games = terminal.groupby(["batter", "game_date"], as_index=False)[
        [
            "plate_appearances",
            "hits",
            "total_bases",
            "walks",
            "strikeouts",
            "pa_vs_lhp",
            "pa_vs_rhp",
        ]
    ].sum()

    if "launch_speed" in pitch_df.columns:
        contact = pitch_df[pitch_df["launch_speed"].notna()].copy()
        if not contact.empty and "batter" in contact.columns:
            contact["game_date"] = pd.to_datetime(contact["game_date"], errors="coerce")
            contact["hard_hit"] = (
                pd.to_numeric(contact["launch_speed"], errors="coerce") >= 95
            ).astype(int)
            hard = (
                contact.groupby(["batter", "game_date"], as_index=False)["hard_hit"]
                .mean()
                .rename(columns={"hard_hit": "hard_hit_rate"})
            )
            batter_games = batter_games.merge(
                hard, on=["batter", "game_date"], how="left"
            )

    if "hard_hit_rate" not in batter_games.columns:
        batter_games["hard_hit_rate"] = 0.0
    batter_games["hard_hit_rate"] = pd.to_numeric(
        batter_games["hard_hit_rate"], errors="coerce"
    ).fillna(0.0)

    return batter_games


def persist_reusable_tables(
    pitch_df: pd.DataFrame,
    *,
    pitcher_output_path: str | None,
    batter_output_path: str | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build and optionally persist pitcher/batter reusable game tables.

    Args:
        pitch_df: Pitch-level Statcast frame.
        pitcher_output_path: Optional path for persisted pitcher-game table.
        batter_output_path: Optional path for persisted batter-game table.

    Returns:
        Tuple of (pitcher_game_table, batter_game_table).
    """

    pitcher_games = build_pitcher_game_table(pitch_df)
    batter_games = build_batter_game_table(pitch_df)

    if pitcher_output_path:
        output = Path(pitcher_output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        if output.suffix.lower() == ".parquet":
            pitcher_games.to_parquet(output, index=False)
        else:
            pitcher_games.to_csv(output, index=False)

    if batter_output_path:
        output = Path(batter_output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        if output.suffix.lower() == ".parquet":
            batter_games.to_parquet(output, index=False)
        else:
            batter_games.to_csv(output, index=False)

    return pitcher_games, batter_games
