"""Dataset builders for Phase 1.5 MLB season-horizon projection snapshots."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from src.fantasy.adapters.mlb.feature_engineering import add_phase15_rolling_features

SNAPSHOT_FEATURE_PREFIXES: tuple[str, ...] = (
    "roll_",
    "games_played_",
    "pa_per_game_",
)


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
        or column in {"days_since_last_game", "hard_hit_rate"}
    ]


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
                    & (season_frame[date_col] > anchor_date)
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
                    "snapshot_games_played": int(len(entity_history)),
                }
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
