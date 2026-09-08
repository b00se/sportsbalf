"""nflreadpy-backed provider."""

from __future__ import annotations

import warnings
from collections.abc import Callable, Sequence
from typing import Any
from urllib.error import HTTPError

import pandas as pd

from .base import (
    CapabilityRecord,
    FailureMetadata,
    LoadResult,
    NFLDataProvider,
    ProviderCapabilities,
    reconcile_seasons,
)

# These are the datasets for which this adapter has a concrete loader and
# normalization contract.  Do not advertise provider surfaces we cannot load.
SUPPORTED_DATASETS = ("schedules", "player_stats", "pbp", "ngs")
DATASET_LOADERS = {
    "schedules": "load_schedules",
    "player_stats": "load_weekly",
    "pbp": "load_pbp",
    "ngs": "load_ngs_passing",
}

try:  # pragma: no cover - optional dependency
    import nflreadpy as nfl  # type: ignore
except ImportError as exc:  # pragma: no cover - optional dependency missing
    nfl = None  # type: ignore
    _NFL_IMPORT_ERROR: Exception | None = exc
else:  # pragma: no cover
    _NFL_IMPORT_ERROR = None


_WEEKLY_ALIASES: dict[str, tuple[str, ...]] = {
    "position": ("position", "player_position"),
    "season": ("season",),
    "week": ("week", "game_week"),
    "game_id": ("game_id", "gsis_game_id"),
    "player_id": (
        "player_id",
        "player_gsis_id",
        "player_id_gsis",
        "gsis_id",
        "gsis_player_id",
    ),
    "player_display_name": (
        "player_display_name",
        "player_name",
        "display_name",
        "full_name",
    ),
    "recent_team": ("recent_team", "team", "team_abbr"),
    "opponent_team": ("opponent_team", "opponent", "opp_team", "opponent_abbr"),
    "attempts": ("attempts", "pass_attempts", "passing_attempts"),
}

_SCHEDULE_ALIASES: dict[str, tuple[str, ...]] = {
    "game_id": ("game_id", "gsis_game_id"),
    "season": ("season",),
    "week": ("week", "game_week"),
    "gameday": ("gameday", "game_time", "game_date", "start_time"),
    "home_team": ("home_team", "home", "home_team_abbr"),
    "away_team": ("away_team", "away", "away_team_abbr"),
    "spread_line": ("spread_line", "spread", "home_spread"),
    "total_line": ("total_line", "total", "over_under", "ou_line"),
    "div_game": ("div_game", "is_division_game", "divisional_game"),
}

_PBP_ALIASES: dict[str, tuple[str, ...]] = {
    "season": ("season",),
    "week": ("week",),
    "game_id": ("game_id", "gsis_game_id"),
    "posteam": ("posteam", "pos_team", "offense_team"),
    "defteam": ("defteam", "def_team", "defense_team"),
    "pass_attempt": ("pass_attempt", "is_pass_attempt"),
    "rush_attempt": ("rush_attempt", "is_rush_attempt"),
    "score_differential": ("score_differential", "score_diff"),
    "qtr": ("qtr", "quarter"),
    "passer_player_id": (
        "passer_player_id",
        "passer_id",
        "passer_player_gsis_id",
    ),
    "rusher_player_id": (
        "rusher_player_id",
        "rusher_id",
        "rusher_player_gsis_id",
    ),
    "qb_dropback": ("qb_dropback", "is_qb_dropback"),
    "epa": ("epa",),
    "cpoe": ("cpoe",),
    "air_yards": ("air_yards", "air_yards_intended"),
}

_NGS_ALIASES: dict[str, tuple[str, ...]] = {
    "season": ("season",),
    "week": ("week",),
    "player_gsis_id": (
        "player_gsis_id",
        "player_id",
        "gsis_id",
        "player_id_gsis",
    ),
    "avg_time_to_throw": (
        "avg_time_to_throw",
        "average_time_to_throw",
        "avg_time_throw",
    ),
    "avg_intended_air_yards": (
        "avg_intended_air_yards",
        "intended_air_yards_avg",
    ),
    "completion_percentage_above_expectation": (
        "completion_percentage_above_expectation",
        "cpoe",
    ),
}


def _require_nflreadpy() -> Any:
    """Return the nflreadpy module or raise a helpful error."""
    if nfl is None:
        raise ImportError(
            "nflreadpy is required for this provider. "
            "Install the package before running."
        ) from _NFL_IMPORT_ERROR
    return nfl


def _to_pandas(value: Any) -> pd.DataFrame:
    """Convert provider output to a pandas DataFrame."""
    if isinstance(value, pd.DataFrame):
        return value.copy()
    if hasattr(value, "to_pandas"):
        try:
            return value.to_pandas()
        except TypeError:  # pragma: no cover - defensive
            return pd.DataFrame(value)
    return pd.DataFrame(value)


def _coalesce_columns(frame: pd.DataFrame, aliases: dict[str, tuple[str, ...]]) -> None:
    """Ensure each target column exists using a set of aliases."""
    for target, candidates in aliases.items():
        had_target = target in frame.columns
        if not had_target:
            frame[target] = pd.NA
        for column in candidates:
            if column == target and not had_target:
                continue
            if column not in frame.columns:
                continue
            mask = frame[target].isna()
            if not mask.any():
                break
            frame.loc[mask, target] = frame.loc[mask, column]
            if not frame[target].isna().any():
                break


def _failure_for_exception(exc: Exception, year: int) -> FailureMetadata:
    """Classify one provider exception and attach it to a season."""
    message = str(exc)
    unavailable = isinstance(exc, HTTPError) and exc.code == 404
    unavailable = unavailable or "404" in message or "not found" in message.lower()
    return FailureMetadata(
        "unavailable" if unavailable else "error",
        message,
        type(exc).__name__,
        int(year),
    )


def _retain_requested_seasons(
    frame: pd.DataFrame, years: Sequence[int]
) -> pd.DataFrame:
    """Drop provider rows whose season is outside the requested set."""
    if "season" not in frame.columns:
        # A response without season provenance cannot safely be attributed to
        # any requested year.  Fail closed; reconciliation will emit one typed
        # MissingSeason record per requested year.
        return pd.DataFrame(columns=frame.columns)
    seasons = pd.to_numeric(frame["season"], errors="coerce")
    return frame.loc[seasons.isin({int(year) for year in years})].copy().reset_index(
        drop=True
    )


def _fetch_with_fallback(
    label: str,
    fetch_fn: Callable[[list[int]], Any],
    years: Sequence[int],
) -> LoadResult:
    """Fetch data while gracefully skipping unavailable seasons."""
    years_list = [int(year) for year in years]
    if not years_list:
        return LoadResult.empty()

    failures: list[FailureMetadata] = []
    try:
        frame = _to_pandas(fetch_fn(list(years_list)))
    except Exception:  # pragma: no cover - network exception path
        # Bulk failures have no season scope. Retry each season and let the
        # shared reconciliation contract create the final typed records.
        frame = None
    else:
        frame = _retain_requested_seasons(frame, years_list)
        available = set()
        if "season" in frame:
            available = set(
                pd.to_numeric(frame["season"], errors="coerce").dropna().astype(int)
            )
        if frame is not None and all(year in available for year in years_list):
            return reconcile_seasons(frame, years_list)

    frames: list[pd.DataFrame] = []
    bulk_frame = frame if frame is not None else pd.DataFrame()
    bulk_available = set()
    if "season" in bulk_frame:
        bulk_available = set(
            pd.to_numeric(bulk_frame["season"], errors="coerce").dropna().astype(int)
        )
    retry_years = [year for year in years_list if year not in bulk_available]
    for year in retry_years:
        try:
            raw_frame = _to_pandas(fetch_fn([int(year)]))
        except HTTPError as exc:  # pragma: no cover - network exception path
            failures.append(_failure_for_exception(exc, year))
            continue
        except Exception as exc:  # pragma: no cover - unexpected network error
            failures.append(_failure_for_exception(exc, year))
            continue
        if "season" not in raw_frame.columns:
            failures.append(
                FailureMetadata(
                    "unavailable",
                    "season absent from response",
                    "MissingSeason",
                    year,
                )
            )
            continue
        frame = _retain_requested_seasons(raw_frame, [year])
        if frame.empty:
            failures.append(
                FailureMetadata("unavailable", "empty response", "EmptyResponse", year)
            )
            continue
        frames.append(frame)

    frames.insert(0, bulk_frame)
    frames = [item for item in frames if not item.empty]
    data = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    result = reconcile_seasons(data, years_list, failures)
    if result.skipped_years:
        warnings.warn(
            f"Skipping {label} data for unavailable seasons: "
            f"{result.skipped_years}",
            RuntimeWarning,
            stacklevel=3,
        )
    return result


def _normalize_weekly(frame: pd.DataFrame) -> pd.DataFrame:
    _coalesce_columns(frame, _WEEKLY_ALIASES)
    for column in ("season", "week"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
    frame["attempts"] = pd.to_numeric(frame.get("attempts"), errors="coerce")
    return frame


def _normalize_schedule(frame: pd.DataFrame) -> pd.DataFrame:
    _coalesce_columns(frame, _SCHEDULE_ALIASES)
    div_game = pd.to_numeric(frame.get("div_game"), errors="coerce")
    div_game = div_game.where(div_game.notna(), 0).astype(int)
    frame["div_game"] = div_game
    for column in ("season", "week"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
    if "gameday" in frame.columns:
        frame["gameday"] = frame["gameday"].astype(str)
    return frame


def _normalize_pbp(frame: pd.DataFrame) -> pd.DataFrame:
    _coalesce_columns(frame, _PBP_ALIASES)
    for column in ("season", "week"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
    return frame


def _normalize_ngs(frame: pd.DataFrame) -> pd.DataFrame:
    _coalesce_columns(frame, _NGS_ALIASES)
    for column in ("season", "week"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
    return frame


class NFLReadPyProvider(NFLDataProvider):
    """Implementation that delegates to nflreadpy."""

    @property
    def name(self) -> str:
        """Return the provider identifier."""
        return "nflreadpy"

    @property
    def capabilities(self) -> ProviderCapabilities:
        """Return the audited nflreadpy dataset capabilities."""
        return ProviderCapabilities(
            provider=self.name,
            datasets=SUPPORTED_DATASETS,
            supports_current_season=True,
            source_license="nflverse data license",
            audit=tuple(
                CapabilityRecord(
                    dataset,
                    "1999-present",
                    "weekly or per-season",
                    "nflverse data license",
                    "skip unavailable season",
                )
                for dataset in SUPPORTED_DATASETS
            ),
        )

    @staticmethod
    def _failure(exc: Exception, years: Sequence[int]) -> LoadResult:
        """Return typed metadata when nflreadpy cannot be imported."""
        failures = tuple(
            FailureMetadata("error", str(exc), type(exc).__name__, int(year))
            for year in years
        )
        return reconcile_seasons(pd.DataFrame(), years, failures)

    def load_weekly(self, years: Sequence[int]) -> LoadResult:
        try:
            module = _require_nflreadpy()
        except Exception as exc:
            return self._failure(exc, years)
        result = _fetch_with_fallback(
            "weekly",
            lambda season_list: module.load_player_stats(
                list(season_list), summary_level="week"
            ),
            years,
        )
        if result.data.empty:
            return result
        return result.with_data(_normalize_weekly(result.data))

    def load_schedules(self, years: Sequence[int]) -> LoadResult:
        try:
            module = _require_nflreadpy()
        except Exception as exc:
            return self._failure(exc, years)
        result = _fetch_with_fallback(
            "schedule",
            lambda season_list: module.load_schedules(list(season_list)),
            years,
        )
        if result.data.empty:
            return result
        return result.with_data(_normalize_schedule(result.data))

    def load_pbp(self, years: Sequence[int]) -> LoadResult:
        try:
            module = _require_nflreadpy()
        except Exception as exc:
            return self._failure(exc, years)
        result = _fetch_with_fallback(
            "pbp",
            lambda season_list: module.load_pbp(list(season_list)),
            years,
        )
        if result.data.empty:
            return result
        return result.with_data(_normalize_pbp(result.data))

    def load_ngs_passing(self, years: Sequence[int]) -> LoadResult:
        try:
            module = _require_nflreadpy()
        except Exception as exc:
            return self._failure(exc, years)
        result = _fetch_with_fallback(
            "ngs passing",
            lambda season_list: module.load_nextgen_stats(
                list(season_list), stat_type="passing"
            ),
            years,
        )
        if result.data.empty:
            return result
        return result.with_data(_normalize_ngs(result.data))
