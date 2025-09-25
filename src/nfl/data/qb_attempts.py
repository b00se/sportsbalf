"""Dataset builder for NFL QB pass attempts."""
from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import pandas as pd

try:  # pragma: no cover - optional dependency
    import nfl_data_py as nfl  # type: ignore
except ImportError as exc:  # pragma: no cover - optional dependency missing
    nfl = None  # type: ignore
    _NFL_IMPORT_ERROR: Exception | None = exc
else:  # pragma: no cover
    _NFL_IMPORT_ERROR = None

from .underdog import import_ud_pass_attempt_lines

TEAM_ABBREVIATION_MAP: dict[str, str] = {
    "OAK": "LV",
    "SD": "LAC",
    "STL": "LA",
    "WSH": "WAS",
    "JAX": "JAC",
}

QB_COLUMNS = [
    "season",
    "week",
    "game_id",
    "player_id",
    "player_display_name",
    "recent_team",
    "opponent_team",
    "attempts",
]

SCHEDULE_COLUMNS = [
    "game_id",
    "season",
    "week",
    "home_team",
    "away_team",
    "spread_line",
    "total_line",
]


def _require_nfl_data_py() -> Any:
    """Return the nfl_data_py module or raise a helpful error."""
    if nfl is None:
        raise ImportError(
            "nfl_data_py is required to build the QB attempts dataset. "
            "Install the package in your environment before running this loader."
        ) from _NFL_IMPORT_ERROR
    return nfl


def load_weekly_data(years: Sequence[int]) -> pd.DataFrame:
    """Fetch weekly NFL data for the given seasons."""
    nfl_module = _require_nfl_data_py()
    frame = nfl_module.import_weekly_data(list(years))
    return pd.DataFrame(frame)


def load_schedule(years: Sequence[int]) -> pd.DataFrame:
    """Fetch season schedules for the given seasons."""
    nfl_module = _require_nfl_data_py()
    frame = nfl_module.import_schedules(list(years))
    return pd.DataFrame(frame)


def _normalize_name(value: str | None) -> str:
    """Lowercase and strip punctuation for fuzzy name matches."""
    if not isinstance(value, str):
        return ""
    normalized = re.sub(r"[^a-zA-Z\s]", " ", value).lower()
    return re.sub(r"\s+", " ", normalized).strip()


def _select_qbs(weekly: pd.DataFrame) -> pd.DataFrame:
    """Filter to quarterback rows and select relevant columns."""
    if "position" not in weekly.columns:
        raise KeyError("weekly data must include a 'position' column")
    qbs = weekly.loc[weekly["position"] == "QB", QB_COLUMNS].copy()
    qbs["attempts"] = pd.to_numeric(qbs["attempts"], errors="coerce")
    qbs["season"] = pd.to_numeric(qbs["season"], errors="coerce").astype("Int64")
    qbs["week"] = pd.to_numeric(qbs["week"], errors="coerce").astype("Int64")
    return qbs


def _prepare_schedule(schedule: pd.DataFrame) -> pd.DataFrame:
    """Trim schedule columns to the fields needed for merging."""
    missing = [column for column in SCHEDULE_COLUMNS if column not in schedule.columns]
    if missing:
        raise KeyError(f"schedule data missing required columns: {missing}")
    frame = schedule[SCHEDULE_COLUMNS].copy()
    frame["season"] = pd.to_numeric(frame["season"], errors="coerce").astype("Int64")
    frame["week"] = pd.to_numeric(frame["week"], errors="coerce").astype("Int64")
    return frame


def _apply_team_map(frame: pd.DataFrame, columns: Sequence[str], team_map: Mapping[str, str]) -> None:
    """Normalize legacy team abbreviations in place."""
    if not team_map:
        return
    for column in columns:
        if column in frame.columns:
            frame[column] = frame[column].replace(team_map)


def _attach_ud_lines(
    qbs: pd.DataFrame,
    schedule: pd.DataFrame,
    ud_lines: pd.DataFrame | None,
) -> pd.DataFrame:
    """Merge Underdog pass attempt lines onto quarterback stats."""
    result = qbs.copy()
    if ud_lines is None or ud_lines.empty:
        result["ud_line"] = pd.NA
        return result

    required = {"player_name", "game_id"}
    missing = required - set(ud_lines.columns)
    if missing:
        raise KeyError(f"UD lines missing required columns: {sorted(missing)}")

    lines = ud_lines.copy()
    lines["game_id"] = lines["game_id"].astype(str)

    schedule_keys = schedule[["game_id", "season", "week"]].drop_duplicates().copy()
    schedule_keys["game_id"] = schedule_keys["game_id"].astype(str)
    lines = lines.merge(schedule_keys, on="game_id", how="left")

    name_lookup = (
        result[["player_id", "player_display_name"]]
        .drop_duplicates()
        .assign(name_key=lambda df: df["player_display_name"].map(_normalize_name))
    )
    lines["name_key"] = lines["player_name"].map(_normalize_name)
    lines = lines.merge(name_lookup[["name_key", "player_id"]], on="name_key", how="left")
    lines.dropna(subset=["player_id"], inplace=True)

    if "line" in lines.columns and "ud_line" not in lines.columns:
        lines.rename(columns={"line": "ud_line"}, inplace=True)
    lines["ud_line"] = pd.to_numeric(lines["ud_line"], errors="coerce")

    lines.sort_values(["season", "week", "player_id", "ud_line"], inplace=True, na_position="last")
    lines = lines.drop_duplicates(subset=["season", "week", "player_id"], keep="last")

    return result.merge(
        lines[["season", "week", "player_id", "ud_line"]],
        on=["season", "week", "player_id"],
        how="left",
    )


def prepare_qb_attempts_dataset(
    weekly: pd.DataFrame,
    schedule: pd.DataFrame,
    ud_lines: pd.DataFrame | None,
    team_map: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """Transform raw weekly stats into the canonical QB attempts dataset."""
    team_map = dict(team_map) if team_map is not None else TEAM_ABBREVIATION_MAP

    qbs = _select_qbs(weekly)
    sched = _prepare_schedule(schedule)

    qbs["game_id"] = qbs["game_id"].astype(str)
    sched["game_id"] = sched["game_id"].astype(str)

    merged = qbs.merge(sched, on=["season", "week", "game_id"], how="left")
    _apply_team_map(merged, ["recent_team", "opponent_team", "home_team", "away_team"], team_map)

    merged.sort_values(["player_id", "season", "week"], inplace=True)
    merged["prev_attempts"] = merged.groupby("player_id")["attempts"].shift(1)
    rolling = merged.groupby("player_id")["attempts"].rolling(window=3).mean()
    merged["rolling3_attempts"] = rolling.reset_index(level=0, drop=True)

    merged = _attach_ud_lines(merged, sched, ud_lines)

    for column in ("spread_line", "total_line"):
        if column in merged.columns:
            merged[column] = pd.to_numeric(merged[column], errors="coerce")

    merged = merged.rename(
        columns={
            "player_display_name": "qb_name",
            "player_id": "qb_id",
            "recent_team": "team",
            "opponent_team": "opponent",
            "attempts": "pass_attempts",
            "spread_line": "spread",
            "total_line": "total",
        }
    )
    merged["home"] = merged["team"].eq(merged["home_team"])
    merged.drop(columns=["home_team", "away_team"], inplace=True)

    column_order = [
        "season",
        "week",
        "game_id",
        "qb_name",
        "qb_id",
        "team",
        "opponent",
        "home",
        "spread",
        "total",
        "pass_attempts",
        "ud_line",
        "prev_attempts",
        "rolling3_attempts",
    ]
    remaining = [col for col in merged.columns if col not in column_order]
    return merged[column_order + remaining]


def build_qb_attempts_dataset(
    years: Sequence[int],
    output_path: str | Path = "data/qb_attempts_dataset.parquet",
    ud_loader: Callable[[Sequence[int]], pd.DataFrame] | None = None,
    team_map: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """High-level helper to fetch data, assemble the dataset, and persist to disk."""
    years_list = list(years)
    weekly = load_weekly_data(years_list)
    schedule = load_schedule(years_list)

    loader = ud_loader or import_ud_pass_attempt_lines
    ud_lines = loader(years=years_list)

    dataset = prepare_qb_attempts_dataset(weekly, schedule, ud_lines=ud_lines, team_map=team_map)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_parquet(output, index=False)
    return dataset
