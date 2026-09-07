"""Provider abstractions for NFL data ingestion."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Literal, Protocol, runtime_checkable

import pandas as pd

ProviderName = Literal["nfl_data_py", "nflreadpy"]
DEFAULT_PROVIDER_NAME: ProviderName = "nfl_data_py"
FreshnessStatus = Literal["complete", "partial", "empty"]
CANONICAL_DATASETS = (
    "schedules",
    "player_stats",
    "pbp",
    "rosters",
    "depth_charts",
    "ngs",
    "participation",
    "betting_lines",
)


@dataclass(frozen=True, slots=True)
class ProviderCapabilities:
    """Audited capabilities of an NFL data provider."""

    provider: str
    datasets: tuple[str, ...]
    supports_current_season: bool
    source_license: str
    audit: tuple[CapabilityRecord, ...] = ()


@dataclass(frozen=True, slots=True)
class CapabilityRecord:
    """Capability and fallback facts for one provider dataset."""

    dataset: str
    seasons: str
    cadence: str
    license: str
    fallback: str | None


@dataclass(frozen=True, slots=True)
class FreshnessMetadata:
    """Typed provenance describing the currency of one provider response."""

    requested_years: tuple[int, ...] = ()
    available_years: tuple[int, ...] = ()
    retrieved_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    status: FreshnessStatus = "empty"


@dataclass(frozen=True, slots=True)
class FailureMetadata:
    """Typed, non-sensitive description of a provider load failure."""

    kind: Literal["unavailable", "error"]
    message: str
    exception_type: str
    year: int | None = None


@dataclass(slots=True)
class LoadResult:
    """Container for provider responses."""

    data: pd.DataFrame
    skipped_years: list[int] = field(default_factory=list)
    freshness: FreshnessMetadata = field(default_factory=FreshnessMetadata)
    failures: tuple[FailureMetadata, ...] = ()

    def with_data(self, data: pd.DataFrame) -> LoadResult:
        """Return this result with normalized data while preserving metadata."""
        return replace(self, data=data)

    @classmethod
    def empty(cls) -> LoadResult:
        """Return an empty provider result."""
        return cls(pd.DataFrame())


def reconcile_seasons(
    data: pd.DataFrame,
    years: Sequence[int],
    failures: Sequence[FailureMetadata] = (),
) -> LoadResult:
    """Reconcile requested seasons and emit one typed failure per missing year."""
    requested = tuple(dict.fromkeys(int(year) for year in years))
    available = (
        tuple(sorted({int(value) for value in data["season"].dropna()}))
        if "season" in data
        else ()
    )
    existing = {failure.year for failure in failures}
    missing = [
        year for year in requested if year not in available and year not in existing
    ]
    all_failures = tuple(failures) + tuple(
        FailureMetadata(
            "unavailable", "season absent from response", "MissingSeason", year
        )
        for year in missing
    )
    skipped = [year for year in requested if year not in available]
    status: FreshnessStatus = (
        "empty" if not available else ("partial" if skipped else "complete")
    )
    return LoadResult(
        data,
        skipped,
        FreshnessMetadata(requested, available, datetime.now(UTC), status),
        all_failures,
    )


@runtime_checkable
class NFLDataProvider(Protocol):
    """Protocol describing data provider capabilities."""

    @property
    def name(self) -> str:
        """Return the provider identifier."""
        ...

    @property
    def capabilities(self) -> ProviderCapabilities:
        """Return the provider's audited dataset capabilities."""
        ...

    def load_weekly(self, years: Sequence[int]) -> LoadResult:
        """Return weekly player statistics for the requested seasons."""
        ...

    def load_schedules(self, years: Sequence[int]) -> LoadResult:
        """Return season schedules for the requested seasons."""
        ...

    def load_pbp(self, years: Sequence[int]) -> LoadResult:
        """Return play-by-play data for the requested seasons."""
        ...

    def load_ngs_passing(self, years: Sequence[int]) -> LoadResult:
        """Return Next Gen Stats passing data for the requested seasons."""
        ...


def get_provider(name: str | ProviderName | None = None) -> NFLDataProvider:
    """Instantiate a provider by name.

    Args:
        name: Requested provider identifier. Defaults to ``DEFAULT_PROVIDER_NAME``
            when not provided.

    Returns:
        Concrete provider implementation matching ``name``.

    Raises:
        ValueError: If the ``name`` does not correspond to a supported provider.
    """

    normalized = str(name or DEFAULT_PROVIDER_NAME).lower()
    if normalized == "nfl_data_py":
        from .nfl_data_py_provider import NflDataPyProvider

        return NflDataPyProvider()
    if normalized == "nflreadpy":
        from .readpy import NFLReadPyProvider

        return NFLReadPyProvider()

    raise ValueError(f"Unsupported provider '{name}'")
