"""Archive-backed orchestration for NFL fantasy baseline backtests."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import platform
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from src.nfl.data.archive import ArchiveAdapter, ArchiveWriteResult
from src.nfl.data.providers.base import NFLDataProvider
from src.nfl.models.baseline_evaluation import (
    BaselineEvaluationResult,
    evaluate_season_baselines,
    evaluate_weekly_baselines,
)
from src.nfl.models.fantasy_scoring import derive_fantasy_points


@dataclass(frozen=True, slots=True)
class ArchivedBaselineBacktestResult:
    """Immutable inputs and both rolling-origin evaluation results."""

    outcomes: pd.DataFrame
    weekly: BaselineEvaluationResult
    season: BaselineEvaluationResult
    archive: ArchiveWriteResult
    output_dir: Path | None = None


_ALIASES: dict[str, tuple[str, ...]] = {
    "player_id": ("player_id", "player_gsis_id", "gsis_id"),
    "position": ("position", "player_position"),
    "season": ("season",),
    "week": ("week", "game_week"),
    "pass_attempts": ("pass_attempts", "attempts", "passing_attempts"),
    "completions": ("completions", "complete_passes"),
    "passing_yards": ("passing_yards", "pass_yards"),
    "passing_tds": ("passing_tds", "pass_tds", "passing_touchdowns"),
    "interceptions": ("interceptions", "passing_interceptions"),
    "rushing_yards": ("rushing_yards", "rush_yards"),
    "rushing_attempts": ("rushing_attempts", "rush_attempts", "carries"),
    "rushing_tds": ("rushing_tds", "rush_tds", "rushing_touchdowns"),
    "receptions": ("receptions", "rec"),
    "targets": ("targets", "receiving_targets"),
    "receiving_yards": ("receiving_yards", "rec_yards"),
    "receiving_tds": ("receiving_tds", "receiving_touchdowns", "rec_tds"),
    "fumbles_lost": (
        "fumbles_lost",
        "rushing_fumbles_lost",
        "receiving_fumbles_lost",
        "sack_fumbles_lost",
    ),
    "two_point_conversions": (
        "two_point_conversions",
        "passing_2pt_conversions",
        "rushing_2pt_conversions",
        "receiving_2pt_conversions",
    ),
}
_STAT_COLUMNS = tuple(
    name
    for name in _ALIASES
    if name not in {"player_id", "position", "season", "week"}
)


_ADDITIVE = {"fumbles_lost", "two_point_conversions"}


def _coalesce(frame: pd.DataFrame, target: str, names: tuple[str, ...]) -> None:
    """Populate a canonical column from the first available provider alias."""
    present = [name for name in names if name in frame.columns]
    if not present:
        frame[target] = 0.0 if target in _STAT_COLUMNS else pd.NA
        return
    if target not in _STAT_COLUMNS:
        values = [frame[name] for name in present]
        for value in values[1:]:
            mismatch = ~(value.eq(values[0]) | (value.isna() & values[0].isna()))
            if mismatch.any():
                raise ValueError(f"conflicting alternate values for {target}")
        frame[target] = values[0]
        return
    parsed = {name: _strict_numeric(frame[name], name) for name in present}
    if target in _ADDITIVE:
        components = [name for name in present if name != target]
        if target in parsed and components:
            total = sum(parsed[name] for name in components)
            if not parsed[target].equals(total):
                raise ValueError(f"conflicting alternate values for {target}")
            frame[target] = parsed[target]
        else:
            frame[target] = sum(parsed[name] for name in present)
        return
    values = list(parsed.values())
    for value in values[1:]:
        if not value.equals(values[0]):
            mismatch = ~(value.eq(values[0]) | (value.isna() & values[0].isna()))
            if mismatch.any():
                raise ValueError(f"conflicting alternate values for {target}")
    frame[target] = values[0]


def _strict_numeric(series: pd.Series, name: str) -> pd.Series:
    """Parse a provider statistic without converting malformed values to zero."""
    if series.map(lambda value: isinstance(value, (bool, np.bool_))).any():
        raise ValueError(f"{name} must not contain booleans")
    values = pd.to_numeric(series, errors="coerce")
    malformed = series.notna() & values.isna()
    if malformed.any() or (~np.isfinite(values.dropna())).any():
        raise ValueError(f"{name} must contain finite numeric values")
    return values.fillna(0.0).astype(float)


def _validate_calendar(
    series: pd.Series, name: str, lower: int, upper: int
) -> pd.Series:
    if series.map(lambda value: isinstance(value, (bool, np.bool_))).any():
        raise ValueError(f"{name} must not contain booleans")
    values = pd.to_numeric(series, errors="coerce")
    if values.isna().any() or (~np.isfinite(values.dropna())).any():
        raise ValueError(f"{name} must contain finite integer values")
    if (values % 1 != 0).any() or (values < lower).any() or (values > upper).any():
        raise ValueError(f"{name} must contain integer values in range")
    return values.astype(int)


def _canonical_outcomes(raw: pd.DataFrame, seasons: Sequence[int]) -> pd.DataFrame:
    """Normalize nflverse weekly rows and derive half-PPR fantasy points."""
    if not isinstance(raw, pd.DataFrame):
        raise TypeError("provider weekly output must be a pandas DataFrame")
    frame = raw.copy(deep=True)
    for target, names in _ALIASES.items():
        _coalesce(frame, target, names)
    required = {"season", "week", "player_id", "position"}
    if frame[sorted(required)].isna().any().any():
        raise ValueError("weekly data has missing identity or calendar fields")
    frame["season"] = _validate_calendar(frame["season"], "season", 1900, 9999)
    frame["week"] = _validate_calendar(frame["week"], "week", 1, 22)
    frame = frame[frame["season"].isin({int(year) for year in seasons})].copy()
    frame["position"] = frame["position"].astype(str).str.upper().str.strip()
    frame = frame[frame["position"].isin({"QB", "RB", "WR", "TE"})].copy()
    if frame.duplicated(["season", "week", "player_id"]).any():
        raise ValueError("duplicate player-week rows in weekly data")
    for column in _STAT_COLUMNS:
        frame[column] = _strict_numeric(frame[column], column)
    return derive_fantasy_points(
        frame[["season", "week", "player_id", "position", *_STAT_COLUMNS]],
        "half_ppr",
    )


def run_archived_baseline_backtest(
    provider: NFLDataProvider | None,
    seasons: Sequence[int],
    *,
    cache_dir: str | Path,
    fetch: bool = False,
    archive_path: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> ArchivedBaselineBacktestResult:
    """Load an immutable weekly archive and evaluate weekly and season baselines.

    Args:
        provider: Provider used only when ``fetch`` is true.  Tests can inject
            a fixture provider implementing ``load_weekly``.
        seasons: NFL seasons to request and evaluate.
        cache_dir: External cache directory; it must not be the repository's
            protected ``data/`` directory.
        fetch: Fetch and create a new immutable archive when true.
        archive_path: Existing payload path when reading an archive.
    """
    requested_values = list(seasons)
    if any(isinstance(year, (bool,)) for year in requested_values):
        raise ValueError("requested seasons must not contain booleans")
    requested_numeric = pd.to_numeric(pd.Series(requested_values), errors="coerce")
    if requested_numeric.isna().any() or (requested_numeric % 1 != 0).any():
        raise ValueError("requested seasons must be finite integers")
    if (requested_numeric < 1900).any() or (requested_numeric > 9999).any():
        raise ValueError("requested seasons must be positive NFL calendar years")
    requested = tuple(dict.fromkeys(int(year) for year in requested_numeric))
    if not requested:
        raise ValueError("at least one season is required")
    repo_root = Path(__file__).resolve().parents[3]
    protected = {
        repo_root / name for name in ("data", "models", "notebooks", "betslips")
    }
    cache = Path(cache_dir).resolve()
    if cache == repo_root or any(
        cache == path or path in cache.parents for path in protected
    ):
        raise ValueError("archive cache must be external to repository protected paths")
    adapter = ArchiveAdapter(cache)
    if fetch:
        if provider is None:
            raise ValueError("provider is required when fetch=True")
        loaded = provider.load_weekly(requested)
        if loaded.data.empty:
            raise ValueError("provider returned no weekly data")
        raw_loader = getattr(provider, "load_weekly_raw", None)
        raw_data = raw_loader(requested) if callable(raw_loader) else loaded.data
        if not isinstance(raw_data, pd.DataFrame):
            raise TypeError("provider raw weekly output must be a pandas DataFrame")
        identity = {"season", "week", "player_id"}
        if identity <= set(raw_data.columns) and raw_data.duplicated(
            sorted(identity)
        ).any():
            raise ValueError("duplicate player-week rows in weekly data")
        freshness = loaded.freshness
        if (
            loaded.skipped_years
            or loaded.failures
            or set(freshness.available_years) != set(requested)
        ):
            raise ValueError("provider did not return every requested season")
        archive = adapter.write(
            "nflverse_weekly",
            loaded.data,
            requested_seasons=requested,
            source_version=getattr(provider, "name", "unknown"),
            cutoff_semantics=(
                "completed weekly observations available at retrieval time"
            ),
        )
    else:
        if archive_path is None:
            raise ValueError("archive_path is required when fetch=False")
        payload = Path(archive_path)
        _cached, manifest = adapter.load_cache_hit(payload)
        if not set(requested) <= set(manifest["available_seasons"]):
            raise ValueError("archive does not cover every requested season")
        archive = ArchiveWriteResult(
            payload_path=payload,
            manifest_path=payload.with_suffix(payload.suffix + ".manifest.json"),
        )
    raw, _manifest = adapter.load(archive.payload_path)
    outcomes = _canonical_outcomes(raw, requested)
    if outcomes.empty:
        raise ValueError("archive contains no supported QB/RB/WR/TE outcomes")
    weekly = evaluate_weekly_baselines(outcomes)
    season = evaluate_season_baselines(outcomes)
    destination = None
    if output_dir is not None:
        destination = Path(output_dir).resolve()
        if destination == repo_root or any(
            destination == path or path in destination.parents for path in protected
        ):
            raise ValueError(
                "output directory must be external to repository protected paths"
            )
        destination.mkdir(parents=True, exist_ok=True)
        outcomes.to_csv(destination / "outcomes.csv", index=False)
        weekly.metrics.to_csv(destination / "weekly_metrics.csv", index=False)
        season.metrics.to_csv(destination / "season_metrics.csv", index=False)
        (destination / "run_manifest.json").write_text(
            json.dumps(
                {
                    "invocation": (
                        "run_archived_baseline_backtest(provider='nflreadpy', "
                        f"seasons={list(requested)!r}, cache_dir={str(cache)!r}, "
                        f"fetch={fetch!r}, output_dir={str(destination)!r})"
                    ),
                    "config": {"scoring": "half_ppr", "target": "fantasy_points"},
                    "seasons": list(requested),
                    "fetch": fetch,
                    "archive_payload": str(archive.payload_path),
                    "archive_manifest": str(archive.manifest_path),
                    "archive_sha256": hashlib.sha256(
                        archive.payload_path.read_bytes()
                    ).hexdigest(),
                    "archive_manifest_sha256": hashlib.sha256(
                        archive.manifest_path.read_bytes()
                    ).hexdigest(),
                    "outputs": {
                        name: hashlib.sha256(
                            (destination / name).read_bytes()
                        ).hexdigest()
                        for name in (
                            "outcomes.csv",
                            "weekly_metrics.csv",
                            "season_metrics.csv",
                        )
                    },
                    "weekly_status": weekly.status,
                    "season_status": season.status,
                    "environment": {
                        "python": platform.python_version(),
                        "platform": platform.platform(),
                        "pandas": pd.__version__,
                        "runner_package": importlib.metadata.version("pip"),
                    },
                },
                sort_keys=True, indent=2,
            ), encoding="utf-8",
        )
    return ArchivedBaselineBacktestResult(
        outcomes=outcomes,
        weekly=weekly,
        season=season,
        archive=archive,
        output_dir=destination,
    )


__all__ = ["ArchivedBaselineBacktestResult", "run_archived_baseline_backtest"]
