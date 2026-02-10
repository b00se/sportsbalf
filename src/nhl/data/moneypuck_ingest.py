"""MoneyPuck single-snapshot ingestion helpers for NHL skater game data."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.utils.io import read_csv

CANONICAL_SKATER_GAME_COLUMNS: tuple[str, ...] = (
    "season",
    "game_id",
    "game_date",
    "player_id",
    "player_name",
    "team",
    "opponent",
    "shots_on_goal",
    "time_on_ice_minutes",
)

_REQUIRED_CANONICAL_COLUMNS: tuple[str, ...] = (
    "season",
    "game_id",
    "game_date",
    "player_id",
    "player_name",
    "team",
    "opponent",
    "shots_on_goal",
)

_SOURCE_ALIASES: dict[str, tuple[str, ...]] = {
    "season": ("season",),
    "game_id": ("game_id", "gameId", "gamePk"),
    "game_date": ("game_date", "gameDate", "date"),
    "player_id": ("player_id", "playerId", "playerID", "name"),
    "player_name": ("player_name", "playerName", "name"),
    "team": ("team", "teamAbbrev", "team_abbrev", "playerTeam"),
    "opponent": ("opponent", "opponentTeam", "opponent_abbrev", "opposingTeam"),
    "shots_on_goal": (
        "shots_on_goal",
        "shotsOnGoal",
        "shotsOnGoalFor",
        "I_F_shotsOnGoal",
        "i_f_shotsOnGoal",
    ),
    "time_on_ice_minutes": (
        "time_on_ice_minutes",
        "timeOnIce",
        "iceTime",
        "timeOnIceMinutes",
    ),
}


def _resolve_source_column(frame: pd.DataFrame, aliases: tuple[str, ...]) -> str | None:
    """Resolve a canonical column using a list of source aliases."""

    lower_map = {column.lower(): column for column in frame.columns}
    for alias in aliases:
        match = lower_map.get(alias.lower())
        if match is not None:
            return match
    return None


def normalize_skater_games_snapshot(raw_snapshot: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw MoneyPuck skater game rows into canonical PR#9 schema.

    Args:
        raw_snapshot: Raw all-seasons skater-game frame.

    Returns:
        Canonical, typed skater-game frame.

    Raises:
        RuntimeError: If required source columns are missing or unparseable.
    """

    canonical = pd.DataFrame(index=raw_snapshot.index)
    for target in CANONICAL_SKATER_GAME_COLUMNS:
        source = _resolve_source_column(raw_snapshot, _SOURCE_ALIASES[target])
        if source is None:
            if target == "time_on_ice_minutes":
                canonical[target] = pd.NA
                continue
            raise RuntimeError(
                f"Missing required MoneyPuck snapshot column for '{target}'."
            )
        canonical[target] = raw_snapshot[source]

    canonical["season"] = pd.to_numeric(canonical["season"], errors="coerce")
    canonical["game_date"] = pd.to_datetime(canonical["game_date"], errors="coerce")
    canonical["shots_on_goal"] = pd.to_numeric(
        canonical["shots_on_goal"], errors="coerce"
    )
    canonical["time_on_ice_minutes"] = pd.to_numeric(
        canonical["time_on_ice_minutes"], errors="coerce"
    )

    for text_column in ("game_id", "player_id", "player_name", "team", "opponent"):
        canonical[text_column] = canonical[text_column].astype("string")

    invalid_required_rows = canonical[list(_REQUIRED_CANONICAL_COLUMNS)].isna().any(axis=1)
    if invalid_required_rows.any():
        raise RuntimeError(
            "MoneyPuck snapshot contains rows with null required canonical fields."
        )

    canonical["season"] = canonical["season"].astype(int)
    return canonical.loc[:, list(CANONICAL_SKATER_GAME_COLUMNS)].copy()


def build_skater_games_curated_cache(
    snapshot_path: str,
    curated_cache_path: str,
    seasons: list[int],
) -> pd.DataFrame:
    """Build a curated skater-game parquet cache from the raw snapshot.

    Args:
        snapshot_path: Local path to raw all-seasons CSV/Parquet snapshot.
        curated_cache_path: Local output parquet path for curated runtime cache.
        seasons: Seasons to keep in the runtime cache.

    Returns:
        Curated, season-filtered DataFrame written to parquet.

    Raises:
        RuntimeError: If the normalized snapshot has no rows for requested seasons.
    """

    raw_snapshot = read_csv(snapshot_path)
    normalized = normalize_skater_games_snapshot(raw_snapshot)

    curated = normalized.loc[normalized["season"].isin(seasons)].copy()
    if curated.empty:
        raise RuntimeError(
            "MoneyPuck curated cache build produced no rows for requested seasons."
        )

    destination = Path(curated_cache_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    curated.to_parquet(destination, index=False)
    return curated


def refresh_skater_games_snapshot(
    snapshot_path: str,
    curated_cache_path: str,
    seasons: list[int],
) -> pd.DataFrame:
    """Refresh the PR#9 curated cache from the configured raw snapshot.

    Args:
        snapshot_path: Local path to raw all-seasons CSV/Parquet snapshot.
        curated_cache_path: Local output parquet path for curated runtime cache.
        seasons: Seasons to keep in the runtime cache.

    Returns:
        Refreshed curated cache DataFrame.
    """

    return build_skater_games_curated_cache(
        snapshot_path=snapshot_path,
        curated_cache_path=curated_cache_path,
        seasons=seasons,
    )


__all__ = [
    "CANONICAL_SKATER_GAME_COLUMNS",
    "build_skater_games_curated_cache",
    "normalize_skater_games_snapshot",
    "refresh_skater_games_snapshot",
]
