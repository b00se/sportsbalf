"""Build NHL skater-game snapshots from shot-level event data."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

REQUIRED_SHOT_COLUMNS: tuple[str, ...] = (
    "season",
    "game_id",
    "isHomeTeam",
    "homeTeamCode",
    "awayTeamCode",
    "teamCode",
    "shooterPlayerId",
    "shooterName",
    "shotWasOnGoal",
    "shooterTimeOnIce",
)

SNAPSHOT_COLUMNS: tuple[str, ...] = (
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


def _require_columns(frame: pd.DataFrame) -> None:
    missing = [
        column for column in REQUIRED_SHOT_COLUMNS if column not in frame.columns
    ]
    if missing:
        raise ValueError(f"Shot-level frame missing required columns: {missing}")


def aggregate_shot_events_to_skater_games(shot_events: pd.DataFrame) -> pd.DataFrame:
    """Aggregate shot-level events into skater game rows.

    Args:
        shot_events: Shot-level event frame from MoneyPuck shots export.

    Returns:
        Canonical skater game frame with required snapshot columns.
    """

    _require_columns(shot_events)

    work = shot_events.copy()
    work["season"] = pd.to_numeric(work["season"], errors="coerce").astype("Int64")
    work["game_id"] = pd.to_numeric(work["game_id"], errors="coerce").astype("Int64")
    work["player_id"] = pd.to_numeric(
        work["shooterPlayerId"], errors="coerce"
    ).astype("Int64")
    work["player_name"] = work["shooterName"].astype("string")
    work["team"] = work["teamCode"].astype("string")

    is_home = pd.to_numeric(work["isHomeTeam"], errors="coerce").fillna(0).astype(int)
    work["opponent"] = np.where(
        is_home.eq(1),
        work["awayTeamCode"].astype("string"),
        work["homeTeamCode"].astype("string"),
    )

    work["shots_on_goal"] = pd.to_numeric(
        work["shotWasOnGoal"], errors="coerce"
    ).fillna(0.0)
    work["toi_seconds"] = pd.to_numeric(
        work["shooterTimeOnIce"], errors="coerce"
    ).fillna(0.0)

    grouped = (
        work.groupby(
            ["season", "game_id", "player_id", "player_name", "team", "opponent"],
            dropna=False,
            as_index=False,
        )
        .agg(
            shots_on_goal=("shots_on_goal", "sum"),
            toi_seconds=("toi_seconds", "max"),
        )
        .dropna(subset=["season", "game_id", "player_id", "shots_on_goal"])
    )

    grouped["season"] = grouped["season"].astype(int)
    grouped["game_id"] = grouped["game_id"].astype(int).astype(str)
    grouped["player_id"] = grouped["player_id"].astype(int).astype(str)
    grouped["player_name"] = grouped["player_name"].astype("string")
    grouped["team"] = grouped["team"].astype("string")
    grouped["opponent"] = grouped["opponent"].astype("string")

    for column in ("player_id", "player_name", "team", "opponent", "game_id"):
        grouped[column] = grouped[column].str.strip()
    grouped = grouped.loc[
        grouped["player_id"].notna()
        & grouped["player_name"].notna()
        & grouped["team"].notna()
        & grouped["opponent"].notna()
        & grouped["game_id"].notna()
        & (grouped["player_id"] != "")
        & (grouped["player_name"] != "")
        & (grouped["team"] != "")
        & (grouped["opponent"] != "")
        & (grouped["game_id"] != "")
    ].copy()

    grouped["time_on_ice_minutes"] = (
        pd.to_numeric(grouped["toi_seconds"], errors="coerce").fillna(0.0) / 60.0
    )

    season_game = (
        grouped[["season", "game_id"]]
        .drop_duplicates()
        .sort_values(["season", "game_id"])
        .reset_index(drop=True)
    )
    season_game["_ordinal"] = season_game.groupby("season").cumcount()
    season_game["game_date"] = (
        pd.to_datetime(season_game["season"].astype(int).astype(str) + "-10-01")
        + pd.to_timedelta(season_game["_ordinal"], unit="D")
    )

    snapshot = grouped.merge(
        season_game[["season", "game_id", "game_date"]],
        on=["season", "game_id"],
        how="left",
    )

    snapshot = snapshot.loc[:, list(SNAPSHOT_COLUMNS)].copy()
    return snapshot.sort_values(
        ["season", "game_date", "game_id", "player_id"]
    ).reset_index(drop=True)


def build_skater_snapshot_from_shots_csv(
    input_path: str | Path,
    output_path: str | Path,
    *,
    chunk_size: int = 500_000,
) -> pd.DataFrame:
    """Build canonical skater-game snapshot CSV from a shot-level CSV.

    Args:
        input_path: Path to shot-level CSV.
        output_path: Destination canonical snapshot CSV path.
        chunk_size: CSV chunksize used during aggregation.

    Returns:
        Snapshot DataFrame that was written to ``output_path``.
    """

    partial_frames: list[pd.DataFrame] = []
    for chunk in pd.read_csv(input_path, chunksize=chunk_size):
        partial_frames.append(aggregate_shot_events_to_skater_games(chunk))

    if not partial_frames:
        empty = pd.DataFrame(columns=list(SNAPSHOT_COLUMNS))
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        empty.to_csv(output_path, index=False)
        return empty

    combined = pd.concat(partial_frames, ignore_index=True)
    collapsed = (
        combined.groupby(
            ["season", "game_id", "player_id", "player_name", "team", "opponent"],
            as_index=False,
            dropna=False,
        )
        .agg(
            shots_on_goal=("shots_on_goal", "sum"),
            time_on_ice_minutes=("time_on_ice_minutes", "max"),
        )
    )

    season_game = (
        collapsed[["season", "game_id"]]
        .drop_duplicates()
        .sort_values(["season", "game_id"])
        .reset_index(drop=True)
    )
    season_game["_ordinal"] = season_game.groupby("season").cumcount()
    season_game["game_date"] = (
        pd.to_datetime(season_game["season"].astype(int).astype(str) + "-10-01")
        + pd.to_timedelta(season_game["_ordinal"], unit="D")
    )

    snapshot = collapsed.merge(
        season_game[["season", "game_id", "game_date"]],
        on=["season", "game_id"],
        how="left",
    )
    snapshot = snapshot.loc[:, list(SNAPSHOT_COLUMNS)].copy()
    snapshot = snapshot.sort_values(["season", "game_date", "game_id", "player_id"])

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    snapshot.to_csv(path, index=False)
    return snapshot.reset_index(drop=True)


__all__ = [
    "REQUIRED_SHOT_COLUMNS",
    "SNAPSHOT_COLUMNS",
    "aggregate_shot_events_to_skater_games",
    "build_skater_snapshot_from_shots_csv",
]
