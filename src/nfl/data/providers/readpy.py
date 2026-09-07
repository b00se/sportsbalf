"""nflreadpy-backed provider."""
from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any
import warnings
from urllib.error import HTTPError

import pandas as pd

from .base import LoadResult, NFLDataProvider

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
            "nflreadpy is required for this provider. Install the package before running."
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


def _fetch_with_fallback(
    label: str,
    fetch_fn: Callable[[list[int]], Any],
    years: Sequence[int],
) -> LoadResult:
    """Fetch data while gracefully skipping unavailable seasons."""
    years_list = [int(year) for year in years]
    if not years_list:
        return LoadResult.empty()

    try:
        frame = _to_pandas(fetch_fn(list(years_list)))
    except HTTPError as exc:  # pragma: no cover - network exception path
        if exc.code != 404:
            raise
    except Exception as exc:  # pragma: no cover - unexpected network error
        message = str(exc)
        if "404" not in message and "Not Found" not in message:
            raise
    else:
        if not frame.empty:
            return LoadResult(frame, [])

    frames: list[pd.DataFrame] = []
    skipped: list[int] = []
    for year in years_list:
        try:
            frame = _to_pandas(fetch_fn([int(year)]))
        except HTTPError as exc:  # pragma: no cover - network exception path
            if exc.code == 404:
                skipped.append(year)
                continue
            raise
        except Exception as exc:  # pragma: no cover - unexpected network error
            message = str(exc)
            if "404" in message or "Not Found" in message:
                skipped.append(year)
                continue
            raise
        if frame.empty:
            skipped.append(year)
            continue
        frames.append(frame)

    if not frames:
        raise ValueError(f"No {label} data available for seasons {sorted(set(years_list))}")

    if skipped:
        warnings.warn(
            f"Skipping {label} data for unavailable seasons: {sorted(set(skipped))}",
            RuntimeWarning,
            stacklevel=3,
        )

    return LoadResult(pd.concat(frames, ignore_index=True), skipped)


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

    def load_weekly(self, years: Sequence[int]) -> LoadResult:
        module = _require_nflreadpy()
        result = _fetch_with_fallback(
            "weekly",
            lambda season_list: module.load_player_stats(list(season_list), summary_level="week"),
            years,
        )
        if result.data.empty:
            return result
        return LoadResult(_normalize_weekly(result.data), result.skipped_years)

    def load_schedules(self, years: Sequence[int]) -> LoadResult:
        module = _require_nflreadpy()
        result = _fetch_with_fallback(
            "schedule",
            lambda season_list: module.load_schedules(list(season_list)),
            years,
        )
        if result.data.empty:
            return result
        return LoadResult(_normalize_schedule(result.data), result.skipped_years)

    def load_pbp(self, years: Sequence[int]) -> LoadResult:
        module = _require_nflreadpy()
        result = _fetch_with_fallback(
            "pbp",
            lambda season_list: module.load_pbp(list(season_list)),
            years,
        )
        if result.data.empty:
            return result
        return LoadResult(_normalize_pbp(result.data), result.skipped_years)

    def load_ngs_passing(self, years: Sequence[int]) -> LoadResult:
        module = _require_nflreadpy()
        result = _fetch_with_fallback(
            "ngs passing",
            lambda season_list: module.load_nextgen_stats(list(season_list), stat_type="passing"),
            years,
        )
        if result.data.empty:
            return result
        return LoadResult(_normalize_ngs(result.data), result.skipped_years)
