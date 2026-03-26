"""Dataset builders for Phase 1.5 MLB season-horizon projection snapshots."""

from __future__ import annotations

import logging
from collections.abc import Iterable

import pandas as pd

from src.fantasy.adapters.mlb.feature_engineering import add_phase15_rolling_features

logger = logging.getLogger(__name__)

SNAPSHOT_FEATURE_PREFIXES: tuple[str, ...] = (
    "roll_",
    "games_played_",
    "pa_per_game_",
)
SNAPSHOT_EXTRA_FEATURE_COLUMNS: tuple[str, ...] = (
    "days_since_last_game",
    "hard_hit_rate",
    "team_games_seen_last_30",
    "player_game_share_last_30",
    "recent_consecutive_games_played",
    "smoothed_hit_rate_rolling_30",
    "smoothed_hit_rate_season_to_date",
)
_HIT_EVENTS: frozenset[str] = frozenset({"single", "double", "triple", "home_run"})
_WALK_EVENTS: frozenset[str] = frozenset({"walk", "intent_walk"})
_STRIKEOUT_EVENTS: frozenset[str] = frozenset({"strikeout", "strikeout_double_play"})
_TOTAL_BASES_BY_EVENT: dict[str, int] = {
    "single": 1,
    "double": 2,
    "triple": 3,
    "home_run": 4,
}


def _normalize_anchor_frequency(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in {"weekly", "daily"}:
        return "weekly"
    return normalized


def _build_anchor_dates(
    season_dates: pd.Series,
    *,
    frequency: str,
) -> list[pd.Timestamp]:
    if season_dates.empty:
        return []
    minimum = pd.Timestamp(season_dates.min()).normalize()
    maximum = pd.Timestamp(season_dates.max()).normalize()
    if frequency == "daily":
        return list(pd.date_range(minimum, maximum, freq="D"))
    return list(pd.date_range(minimum, maximum, freq="7D"))


def _snapshot_feature_columns(frame: pd.DataFrame) -> list[str]:
    return [
        column
        for column in frame.columns
        if column.startswith(SNAPSHOT_FEATURE_PREFIXES)
        or column in SNAPSHOT_EXTRA_FEATURE_COLUMNS
    ]


def _event_series(frame: pd.DataFrame) -> pd.Series:
    return (
        frame.get("events", pd.Series("", index=frame.index))
        .astype(str)
        .str.strip()
        .str.lower()
    )


def _coerce_numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame.get(column, 0.0), errors="coerce").fillna(0.0)


def build_hits_pa_training_view(
    frame: pd.DataFrame,
    *,
    entity_id_col: str,
    date_col: str,
    regular_season_only: bool = True,
    require_batter_pa_dedup: bool = True,
) -> pd.DataFrame:
    """Build cleaned batter-game rows for `hits` and `plate_appearances`.

    Args:
        frame: Raw/normalized MLB frame.
        entity_id_col: Batter id column.
        date_col: Game date column.
        regular_season_only: Keep only `game_type == "R"` when available.
        require_batter_pa_dedup: Deduplicate to PA terminal pitch rows when available.

    Returns:
        Cleaned daily batter rows with constraint-safe count columns and QA flags.
    """

    if frame.empty:
        return pd.DataFrame()

    working = frame.copy()
    if entity_id_col not in working.columns:
        working[entity_id_col] = ""
    if date_col not in working.columns:
        working[date_col] = pd.NaT
    working[entity_id_col] = working[entity_id_col].astype(str).str.strip()
    working[date_col] = pd.to_datetime(working[date_col], errors="coerce")
    working = working.dropna(subset=[date_col]).copy()
    working = working[working[entity_id_col].ne("")].copy()
    if working.empty:
        return pd.DataFrame()

    working["is_regular_season"] = True
    if "game_type" in working.columns:
        working["is_regular_season"] = (
            working["game_type"].astype(str).str.strip().str.upper().eq("R")
        )
    if regular_season_only:
        working = working[working["is_regular_season"]].copy()
    if working.empty:
        return pd.DataFrame()

    dedup_applied = False
    dedup_keys = ["game_pk", entity_id_col, date_col, "at_bat_number"]
    can_dedup = require_batter_pa_dedup and set(dedup_keys).issubset(working.columns)
    if can_dedup:
        events = _event_series(working)
        valid_events = events.ne("") & ~events.isin({"none", "nan"})
        working = working[valid_events].copy()
        if not working.empty:
            if "pitch_number" in working.columns:
                working["pitch_number"] = pd.to_numeric(
                    working["pitch_number"], errors="coerce"
                ).fillna(0.0)
                working = working.sort_values(
                    dedup_keys + ["pitch_number"], kind="stable"
                )
            else:
                working = working.sort_values(dedup_keys, kind="stable")
            working = working.drop_duplicates(subset=dedup_keys, keep="last")
            dedup_applied = True
    working["pa_terminal_dedup_applied"] = bool(dedup_applied)

    if working.empty:
        return pd.DataFrame()

    if "game_pk" in working.columns and "events" in working.columns:
        events = _event_series(working)
        hit_flag = events.isin(_HIT_EVENTS).astype("float64")
        total_bases = events.map(_TOTAL_BASES_BY_EVENT).fillna(0).astype("float64")
        walk_flag = events.isin(_WALK_EVENTS).astype("float64")
        strikeout_flag = events.isin(_STRIKEOUT_EVENTS).astype("float64")

        hard_hit_events = pd.Series(0.0, index=working.index, dtype="float64")
        if "launch_speed" in working.columns:
            launch_speed = pd.to_numeric(
                working["launch_speed"], errors="coerce"
            ).fillna(0.0)
            hard_hit_events = launch_speed.ge(95.0).astype("float64")
        elif "hard_hit_events" in working.columns:
            hard_hit_events = _coerce_numeric(working, "hard_hit_events")
        elif "hard_hit_rate" in working.columns:
            hard_hit_events = _coerce_numeric(
                working, "hard_hit_rate"
            ) * _coerce_numeric(working, "plate_appearances").clip(lower=0.0)

        pa_vs_lhp = pd.Series(0.0, index=working.index, dtype="float64")
        pa_vs_rhp = pd.Series(0.0, index=working.index, dtype="float64")
        if "pitcher_throws" in working.columns:
            throws = working["pitcher_throws"].astype(str).str.strip().str.upper()
            pa_vs_lhp = throws.eq("L").astype("float64")
            pa_vs_rhp = throws.eq("R").astype("float64")

        terminal = pd.DataFrame(
            {
                entity_id_col: working[entity_id_col].astype(str),
                date_col: pd.to_datetime(working[date_col], errors="coerce"),
                "game_pk": working["game_pk"],
                "plate_appearances": 1.0,
                "hits": hit_flag,
                "total_bases": total_bases,
                "walks": walk_flag,
                "strikeouts": strikeout_flag,
                "hard_hit_events": hard_hit_events,
                "pa_vs_lhp": pa_vs_lhp,
                "pa_vs_rhp": pa_vs_rhp,
                "is_regular_season": working["is_regular_season"].astype(bool),
                "pa_terminal_dedup_applied": working[
                    "pa_terminal_dedup_applied"
                ].astype(bool),
            }
        )
        batter_game = (
            terminal.groupby([entity_id_col, date_col, "game_pk"], dropna=False)
            .agg(
                {
                    "plate_appearances": "sum",
                    "hits": "sum",
                    "total_bases": "sum",
                    "walks": "sum",
                    "strikeouts": "sum",
                    "hard_hit_events": "sum",
                    "pa_vs_lhp": "sum",
                    "pa_vs_rhp": "sum",
                    "is_regular_season": "max",
                    "pa_terminal_dedup_applied": "max",
                }
            )
            .reset_index()
        )
        working = (
            batter_game.groupby([entity_id_col, date_col], dropna=False)
            .agg(
                {
                    "plate_appearances": "sum",
                    "hits": "sum",
                    "total_bases": "sum",
                    "walks": "sum",
                    "strikeouts": "sum",
                    "hard_hit_events": "sum",
                    "pa_vs_lhp": "sum",
                    "pa_vs_rhp": "sum",
                    "is_regular_season": "max",
                    "pa_terminal_dedup_applied": "max",
                }
            )
            .reset_index()
        )
    else:
        for column in (
            "plate_appearances",
            "hits",
            "total_bases",
            "walks",
            "strikeouts",
            "pa_vs_lhp",
            "pa_vs_rhp",
        ):
            if column not in working.columns:
                working[column] = 0.0
            working[column] = pd.to_numeric(working[column], errors="coerce").fillna(
                0.0
            )
        if "hard_hit_events" in working.columns:
            working["hard_hit_events"] = _coerce_numeric(working, "hard_hit_events")
        elif "hard_hit_rate" in working.columns:
            working["hard_hit_events"] = _coerce_numeric(
                working, "hard_hit_rate"
            ) * _coerce_numeric(working, "plate_appearances").clip(lower=0.0)
        else:
            working["hard_hit_events"] = 0.0
        working = (
            working.groupby([entity_id_col, date_col], dropna=False)
            .agg(
                {
                    "plate_appearances": "sum",
                    "hits": "sum",
                    "total_bases": "sum",
                    "walks": "sum",
                    "strikeouts": "sum",
                    "hard_hit_events": "sum",
                    "pa_vs_lhp": "sum",
                    "pa_vs_rhp": "sum",
                    "is_regular_season": "max",
                    "pa_terminal_dedup_applied": "max",
                }
            )
            .reset_index()
        )

    for column in (
        "plate_appearances",
        "hits",
        "total_bases",
        "walks",
        "strikeouts",
        "hard_hit_events",
        "pa_vs_lhp",
        "pa_vs_rhp",
    ):
        working[column] = pd.to_numeric(working[column], errors="coerce").fillna(0.0)

    invalid = (
        working["plate_appearances"].lt(0.0)
        | working["hits"].lt(0.0)
        | working["hits"].gt(working["plate_appearances"])
    )
    working["qa_invalid_row_flag"] = invalid.astype(bool)
    working["plate_appearances"] = working["plate_appearances"].clip(lower=0.0)
    working["hits"] = working["hits"].clip(lower=0.0)
    working["hits"] = working[["hits", "plate_appearances"]].min(axis=1)
    working["hard_hit_events"] = working["hard_hit_events"].clip(lower=0.0)
    working["hard_hit_rate"] = (
        working["hard_hit_events"] / working["plate_appearances"].replace(0.0, pd.NA)
    ).fillna(0.0)
    working["season"] = pd.to_datetime(working[date_col], errors="coerce").dt.year
    return working.sort_values([entity_id_col, date_col], kind="stable").reset_index(
        drop=True
    )


def build_player_season_snapshots(
    frame: pd.DataFrame,
    *,
    entity_id_col: str,
    date_col: str,
    target_col: str,
    snapshot_min_games: int = 5,
    snapshot_anchor_frequency: str = "weekly",
) -> pd.DataFrame:
    """Build `(player, season, anchor)` snapshots with rest-of-season labels.

    Args:
        frame: Batter-game frame with base metric columns.
        entity_id_col: Entity id column.
        date_col: Date column.
        target_col: Count metric target name.
        snapshot_min_games: Minimum prior games required at anchor.
        snapshot_anchor_frequency: One of `weekly` or `daily`.

    Returns:
        Snapshot DataFrame with leakage-safe features and rest-of-season target.
    """

    if frame.empty:
        return pd.DataFrame()

    working = frame.copy()
    if entity_id_col not in working.columns or date_col not in working.columns:
        return pd.DataFrame()

    working[entity_id_col] = working[entity_id_col].astype(str).str.strip()
    working = working[working[entity_id_col].ne("")].copy()
    working[date_col] = pd.to_datetime(working[date_col], errors="coerce")
    working = working.dropna(subset=[date_col]).copy()
    if working.empty:
        return pd.DataFrame()

    if "season" not in working.columns:
        working["season"] = working[date_col].dt.year
    working["season"] = pd.to_numeric(working["season"], errors="coerce").astype(
        "Int64"
    )

    if target_col not in working.columns:
        working[target_col] = 0.0
    working[target_col] = pd.to_numeric(working[target_col], errors="coerce").fillna(
        0.0
    )
    working["plate_appearances"] = pd.to_numeric(
        working.get("plate_appearances", 0.0), errors="coerce"
    ).fillna(0.0)

    engineered = add_phase15_rolling_features(
        working,
        entity_id_col=entity_id_col,
        date_col=date_col,
    )
    league_hit_rate_prior = 0.0
    pa_total = (
        pd.to_numeric(engineered.get("plate_appearances", 0.0), errors="coerce")
        .fillna(0.0)
        .sum()
    )
    if pa_total > 0.0:
        hit_total = (
            pd.to_numeric(engineered.get("hits", 0.0), errors="coerce")
            .fillna(0.0)
            .sum()
        )
        league_hit_rate_prior = float(hit_total / pa_total)
    feature_columns = _snapshot_feature_columns(engineered)
    frequency = _normalize_anchor_frequency(snapshot_anchor_frequency)

    rows: list[dict[str, object]] = []
    for season, season_frame in engineered.groupby("season", dropna=True):
        if pd.isna(season):
            continue
        season_dates = pd.to_datetime(season_frame[date_col], errors="coerce").dropna()
        for anchor_date in _build_anchor_dates(season_dates, frequency=frequency):
            history = season_frame[season_frame[date_col] < anchor_date].copy()
            if history.empty:
                continue
            grouped_history = history.groupby(entity_id_col, dropna=False)
            game_counts = grouped_history[date_col].size()
            eligible_entities: Iterable[object] = game_counts[
                game_counts >= int(max(snapshot_min_games, 1))
            ].index
            for entity in eligible_entities:
                entity_history = history[history[entity_id_col] == entity].copy()
                if entity_history.empty:
                    continue
                latest = (
                    entity_history.sort_values(date_col, kind="stable").tail(1).iloc[0]
                )
                future = season_frame[
                    (season_frame[entity_id_col] == entity)
                    & (season_frame[date_col] >= anchor_date)
                ].copy()

                target_future = pd.to_numeric(
                    future.get(target_col, 0.0), errors="coerce"
                ).fillna(0.0)
                target_history = pd.to_numeric(
                    entity_history.get(target_col, 0.0), errors="coerce"
                ).fillna(0.0)
                pa_history = pd.to_numeric(
                    entity_history.get("plate_appearances", 0.0), errors="coerce"
                ).fillna(0.0)

                row: dict[str, object] = {
                    "entity_id": str(entity),
                    "season": int(season),
                    "anchor_date": pd.Timestamp(anchor_date).date().isoformat(),
                    f"season_to_date_{target_col}": float(target_history.sum()),
                    f"target_rest_of_season_{target_col}": float(target_future.sum()),
                    "season_to_date_pa": float(pa_history.sum()),
                    "season_to_date_plate_appearances": float(pa_history.sum()),
                    "snapshot_games_played": int(len(entity_history)),
                }
                alpha = 25.0
                roll_pa_30 = float(
                    pd.to_numeric(
                        latest.get("roll_30_plate_appearances", 0.0), errors="coerce"
                    )
                )
                roll_hits_30 = float(
                    pd.to_numeric(latest.get("roll_30_hits", 0.0), errors="coerce")
                )
                std_hits = float(target_history.sum())
                std_pa = float(pa_history.sum())
                row["smoothed_hit_rate_rolling_30"] = (
                    roll_hits_30 + (alpha * league_hit_rate_prior)
                ) / max(roll_pa_30 + alpha, 1.0)
                row["smoothed_hit_rate_season_to_date"] = (
                    std_hits + (alpha * league_hit_rate_prior)
                ) / max(std_pa + alpha, 1.0)
                for column in feature_columns:
                    row[column] = float(
                        pd.to_numeric(latest.get(column, 0.0), errors="coerce")
                    )
                rows.append(row)

    if not rows:
        return pd.DataFrame()

    snapshot = pd.DataFrame(rows)
    snapshot = snapshot.sort_values(
        ["season", "anchor_date", "entity_id"], kind="stable"
    )
    snapshot = snapshot.reset_index(drop=True)

    numeric_columns = [
        col for col in snapshot.columns if col not in {"entity_id", "anchor_date"}
    ]
    for column in numeric_columns:
        snapshot[column] = pd.to_numeric(snapshot[column], errors="coerce").fillna(0.0)
    return snapshot
