"""Dataset builder for NFL QB pass attempts."""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

import numpy as np
import pandas as pd

try:  # pragma: no cover - optional dependency
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - optional dependency missing
    pa = None  # type: ignore
    pq = None  # type: ignore

from src.nfl.data.providers import DEFAULT_PROVIDER_NAME, NFLDataProvider, get_provider
from src.nfl.features import (
    compute_game_context_features,
    compute_ngs_passing_features,
    compute_player_passing_features,
    compute_qb_pbp_metrics,
    compute_team_and_opponent_features,
)

from .underdog import import_ud_pass_attempt_lines

SCHEMA_VERSION = "1.1"
RAW_DATA_DIR = Path("data/raw")
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


def _cache_path(prefix: str, years: Sequence[int]) -> Path:
    start, end = min(years), max(years)
    return RAW_DATA_DIR / f"{prefix}_{start}_{end}.parquet"


def _get_provider(provider: NFLDataProvider | None) -> NFLDataProvider:
    """Return a provider instance, defaulting to configured provider."""

    return provider or get_provider(DEFAULT_PROVIDER_NAME)


def load_weekly_data(
    years: Sequence[int],
    provider: NFLDataProvider | None = None,
) -> pd.DataFrame:
    """Fetch weekly NFL data for the given seasons."""

    resolved = _get_provider(provider)
    result = resolved.load_weekly(list(years))
    return result.data


def load_schedule(
    years: Sequence[int],
    provider: NFLDataProvider | None = None,
) -> pd.DataFrame:
    """Fetch season schedules for the given seasons."""

    resolved = _get_provider(provider)
    result = resolved.load_schedules(list(years))
    return result.data


def load_pbp_data(
    years: Sequence[int],
    provider: NFLDataProvider | None = None,
) -> pd.DataFrame:
    """Fetch play-by-play data with parquet caching."""

    years_list = list(years)
    if not years_list:
        return pd.DataFrame()

    resolved = _get_provider(provider)
    cache_file = _cache_path("pbp", years_list)
    if cache_file.exists():
        return pd.read_parquet(cache_file)

    result = resolved.load_pbp(years_list)
    frame = result.data

    if not frame.empty and not result.skipped_years:
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(cache_file, index=False)

    return frame


def load_ngs_passing_data(
    years: Sequence[int],
    provider: NFLDataProvider | None = None,
) -> pd.DataFrame:
    """Return Next Gen Stats passing data when available."""

    years_list = list(years)
    if not years_list:
        return pd.DataFrame()

    resolved = _get_provider(provider)
    cache_file = _cache_path("ngs_passing", years_list)
    if cache_file.exists():
        return pd.read_parquet(cache_file)

    result = resolved.load_ngs_passing(years_list)
    frame = result.data

    if not frame.empty and not result.skipped_years:
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(cache_file, index=False)

    return frame

def _normalize_name(value: str | None) -> str:
    """Lowercase and strip punctuation for fuzzy name matches."""
    if not isinstance(value, str):
        return ""
    normalized = (
        value.lower().replace(".", " ").replace("-", " ")
    )
    normalized = " ".join(normalized.split())
    return normalized


def _select_qbs(weekly: pd.DataFrame) -> pd.DataFrame:
    """Filter to quarterback rows and select relevant columns."""
    if "position" not in weekly.columns:
        raise KeyError("weekly data must include a 'position' column")
    available = [col for col in QB_COLUMNS if col in weekly.columns]
    qbs = weekly.loc[weekly["position"] == "QB", available].copy()
    qbs["attempts"] = pd.to_numeric(qbs["attempts"], errors="coerce")
    qbs["season"] = pd.to_numeric(qbs["season"], errors="coerce").astype("Int64")
    qbs["week"] = pd.to_numeric(qbs["week"], errors="coerce").astype("Int64")
    if "game_id" not in qbs.columns:
        qbs["game_id"] = pd.NA
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


def _attach_game_ids_from_schedule(qbs: pd.DataFrame, schedule: pd.DataFrame) -> pd.DataFrame:
    """Fill missing game_id values by matching team/opponent to the schedule."""
    if schedule is None or schedule.empty:
        return qbs

    lookup = schedule[["season", "week", "game_id", "home_team", "away_team"]].copy()
    home = lookup.rename(columns={"home_team": "team", "away_team": "opponent"})
    away = lookup.rename(columns={"away_team": "team", "home_team": "opponent"})
    mapping = pd.concat([home, away], ignore_index=True)
    mapping.drop_duplicates(subset=["season", "week", "team"], inplace=True)

    qbs = qbs.merge(
        mapping[["season", "week", "team", "opponent", "game_id"]],
        left_on=["season", "week", "recent_team", "opponent_team"],
        right_on=["season", "week", "team", "opponent"],
        how="left",
        suffixes=("", "_schedule"),
    )
    if "game_id_schedule" in qbs.columns:
        qbs["game_id"] = qbs["game_id"].where(qbs["game_id"].notna(), qbs["game_id_schedule"])
        qbs.drop(columns=["game_id_schedule"], inplace=True)
    qbs.drop(columns=["team", "opponent"], inplace=True, errors="ignore")
    return qbs


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
    extra_cols = [
        "ud_line",
        "over_decimal_price",
        "over_payout_multiplier",
        "over_american_price",
        "under_decimal_price",
        "under_payout_multiplier",
        "under_american_price",
    ]
    if ud_lines is None or ud_lines.empty:
        for col in extra_cols:
            result[col] = pd.NA
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

    numeric_cols = [
        "ud_line",
        "over_decimal_price",
        "over_payout_multiplier",
        "under_decimal_price",
        "under_payout_multiplier",
    ]
    for col in numeric_cols:
        if col in lines.columns:
            lines[col] = pd.to_numeric(lines[col], errors="coerce")

    lines.sort_values(["season", "week", "player_id", "ud_line"], inplace=True, na_position="last")
    lines = lines.drop_duplicates(subset=["season", "week", "player_id"], keep="last")

    merge_cols = [
        "season",
        "week",
        "player_id",
        "ud_line",
        "over_decimal_price",
        "over_payout_multiplier",
        "over_american_price",
        "under_decimal_price",
        "under_payout_multiplier",
        "under_american_price",
    ]
    available_cols = [col for col in merge_cols if col in lines.columns]

    return result.merge(
        lines[available_cols],
        on=["season", "week", "player_id"],
        how="left",
    )


def _fill_missing_values(merged: pd.DataFrame) -> pd.DataFrame:
    filled = merged.copy()

    # Player-centric backfills.
    # Keep these leakage-safe by avoiding fills derived from full-sample target values.
    for column in [
        "season_avg_attempts",
        "career_avg_attempts",
        "season_avg_attempts_to_date",
    ]:
        if column in filled.columns:
            column_values = pd.to_numeric(filled[column], errors="coerce")
            column_values = column_values.fillna(0.0)
            filled[column] = column_values

    numeric_team_cols = [
        "plays_per_game",
        "pass_rate",
        "neutral_pass_rate",
        "pass_rate_over_expected",
        "plays_faced",
        "opponent_pass_rate_allowed",
        "opponent_neutral_pass_rate",
    ]
    for column in numeric_team_cols:
        if column in filled.columns:
            column_values = pd.to_numeric(filled[column], errors="coerce")
            column_values = column_values.fillna(0.0)
            filled[column] = column_values

    qb_numeric_cols = [
        "qb_dropbacks",
        "avg_cpoe",
        "epa_per_dropback",
        "air_yards_per_attempt",
        "qb_rush_attempts",
        "ngs_avg_time_to_throw",
        "ngs_avg_air_yards",
        "ngs_cpoe",
    ]
    for column in qb_numeric_cols:
        if column in filled.columns:
            column_values = pd.to_numeric(filled[column], errors="coerce")
            column_values = column_values.fillna(0.0)
            filled[column] = column_values

    if "rest_days" in filled.columns:
        rest_days = pd.to_numeric(filled["rest_days"], errors="coerce")
        rest_days = rest_days.where(rest_days.notna(), 7)
        filled["rest_days"] = rest_days
    for boolean_col in ["short_week", "is_divisional"]:
        if boolean_col in filled.columns:
            boolean_values = filled[boolean_col].where(filled[boolean_col].notna(), False)
            filled[boolean_col] = boolean_values.astype(bool)

    return filled


def _write_dataset(dataset: pd.DataFrame, path: Path) -> None:
    if pa is None or pq is None:  # pragma: no cover - pyarrow optional
        dataset.to_parquet(path, index=False)
        return

    table = pa.Table.from_pandas(dataset, preserve_index=False)
    metadata = dict(table.schema.metadata or {})
    metadata[b"schema_version"] = SCHEMA_VERSION.encode()
    table = table.replace_schema_metadata(metadata)
    pq.write_table(table, path)


def prepare_qb_attempts_dataset(
    weekly: pd.DataFrame,
    schedule: pd.DataFrame,
    ud_lines: pd.DataFrame | None,
    team_map: Mapping[str, str] | None = None,
    pbp: pd.DataFrame | None = None,
    ngs: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Transform raw weekly stats into the canonical QB attempts dataset."""
    team_map = dict(team_map) if team_map is not None else TEAM_ABBREVIATION_MAP

    qbs = _select_qbs(weekly)
    schedule_trimmed = _prepare_schedule(schedule)

    qbs = _attach_game_ids_from_schedule(qbs, schedule_trimmed)
    qbs["game_id"] = qbs["game_id"].astype(str)
    schedule_trimmed["game_id"] = schedule_trimmed["game_id"].astype(str)

    merged = qbs.merge(schedule_trimmed, on=["season", "week", "game_id"], how="left")
    _apply_team_map(merged, ["recent_team", "opponent_team", "home_team", "away_team"], team_map)

    merged.sort_values(["player_id", "season", "week"], inplace=True)
    attempts_by_qb = merged.groupby("player_id")["attempts"]
    merged["prev_attempts"] = attempts_by_qb.shift(1)
    merged["rolling3_attempts"] = attempts_by_qb.transform(
        lambda s: s.shift(1).rolling(window=3).mean()
    )

    merged = _attach_ud_lines(merged, schedule_trimmed, ud_lines)

    # Player-level aggregates
    player_features = compute_player_passing_features(weekly)
    join_keys = ["season", "week", "player_id"]
    if "game_id" in player_features.columns:
        join_keys.insert(2, "game_id")
    merged = merged.merge(
        player_features,
        on=join_keys,
        how="left",
    )

    # Play-by-play features
    if pbp is not None and not pbp.empty:
        team_features, opponent_features = compute_team_and_opponent_features(pbp)
        _apply_team_map(team_features, ["team"], team_map)
        _apply_team_map(opponent_features, ["opponent"], team_map)
        merged = merged.merge(
            team_features,
            left_on=["season", "week", "game_id", "recent_team"],
            right_on=["season", "week", "game_id", "team"],
            how="left",
        )
        merged.drop(columns=["team"], inplace=True, errors="ignore")
        merged = merged.merge(
            opponent_features,
            left_on=["season", "week", "game_id", "opponent_team"],
            right_on=["season", "week", "game_id", "opponent"],
            how="left",
        )
        merged.drop(columns=["opponent"], inplace=True, errors="ignore")

        qb_metrics = compute_qb_pbp_metrics(pbp)
        qb_metrics.rename(columns={"qb_id": "player_id"}, inplace=True)
        merged = merged.merge(
            qb_metrics,
            on=["season", "week", "game_id", "player_id"],
            how="left",
        )

    # NGS metrics (optional)
    if ngs is not None and not ngs.empty:
        ngs_features = compute_ngs_passing_features(ngs)
        merged = merged.merge(
            ngs_features,
            left_on=["season", "week", "player_id"],
            right_on=["season", "week", "qb_id"],
            how="left",
        )
        merged.drop(columns=["qb_id"], inplace=True, errors="ignore")
        merged.sort_values(["player_id", "season", "week"], inplace=True)
        for column in ("ngs_avg_time_to_throw", "ngs_avg_air_yards", "ngs_cpoe"):
            if column in merged.columns:
                merged[column] = merged.groupby("player_id", dropna=False)[
                    column
                ].shift(1)

    # Game context features
    context_features = compute_game_context_features(schedule)
    _apply_team_map(context_features, ["team"], team_map)
    context_lookup = context_features[["season", "week", "game_id", "team", "is_divisional"]].drop_duplicates()
    merged = merged.merge(
        context_features,
        left_on=["season", "week", "game_id", "recent_team"],
        right_on=["season", "week", "game_id", "team"],
        how="left",
    )
    merged.drop(columns=["team"], inplace=True, errors="ignore")

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

    if not context_lookup.empty:
        lookup_series = context_lookup.set_index(["season", "week", "game_id", "team"])['is_divisional']
        key_index = merged.set_index(["season", "week", "game_id", "team"]).index
        context_values = lookup_series.reindex(key_index).to_numpy()
        mask = merged["is_divisional"].isna()
        merged.loc[mask, "is_divisional"] = context_values[mask]

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
        "season_avg_attempts",
        "career_avg_attempts",
        "season_attempts_to_date",
        "season_games_played",
        "season_avg_attempts_to_date",
        "plays_per_game",
        "pass_rate",
        "neutral_pass_rate",
        "pass_rate_over_expected",
        "plays_faced",
        "opponent_pass_rate_allowed",
        "opponent_neutral_pass_rate",
        "qb_dropbacks",
        "avg_cpoe",
        "epa_per_dropback",
        "air_yards_per_attempt",
        "qb_rush_attempts",
        "ngs_avg_time_to_throw",
        "ngs_avg_air_yards",
        "ngs_cpoe",
        "rest_days",
        "short_week",
        "is_divisional",
    ]
    for column in column_order:
        if column not in merged.columns:
            merged[column] = pd.NA

    remaining = [col for col in merged.columns if col not in column_order]


    merged = _fill_missing_values(merged)

    return merged[column_order + remaining]


def build_qb_attempts_dataset(
    years: Sequence[int],
    output_path: str | Path = "data/qb_attempts_dataset.parquet",
    ud_loader: Callable[[Sequence[int]], pd.DataFrame] | None = None,
    team_map: Mapping[str, str] | None = None,
    pbp_loader: Callable[[Sequence[int]], pd.DataFrame] | None = None,
    ngs_loader: Callable[[Sequence[int]], pd.DataFrame] | None = None,
    provider: NFLDataProvider | None = None,
) -> pd.DataFrame:
    """High-level helper to fetch data, assemble the dataset, and persist to disk.

    Args:
        years: Season years to include.
        output_path: Destination parquet path for the assembled dataset.
        ud_loader: Optional override for Underdog lines loader.
        team_map: Optional mapping to normalise team abbreviations.
        pbp_loader: Optional override for play-by-play loader.
        ngs_loader: Optional override for Next Gen Stats loader.
        provider: Optional provider override (defaults to nflreadpy).

    Returns:
        The assembled dataset.
    """

    years_list = list(years)
    resolved_provider = _get_provider(provider)

    weekly = load_weekly_data(years_list, provider=resolved_provider)
    schedule = load_schedule(years_list, provider=resolved_provider)

    loader = ud_loader or import_ud_pass_attempt_lines
    ud_lines = loader(years_list)

    if pbp_loader is None:
        pbp = load_pbp_data(years_list, provider=resolved_provider)
    else:
        pbp = pbp_loader(years_list)

    if ngs_loader is None:
        ngs = load_ngs_passing_data(years_list, provider=resolved_provider)
    else:
        ngs = ngs_loader(years_list)

    dataset = prepare_qb_attempts_dataset(
        weekly,
        schedule,
        ud_lines=ud_lines,
        team_map=team_map,
        pbp=pbp,
        ngs=ngs,
    )

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    _write_dataset(dataset, output)
    return dataset
