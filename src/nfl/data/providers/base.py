"""Provider abstractions for NFL data ingestion."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Protocol, Sequence, runtime_checkable

import pandas as pd

ProviderName = Literal["nfl_data_py", "nflreadpy"]
DEFAULT_PROVIDER_NAME: ProviderName = "nfl_data_py"


@dataclass(slots=True)
class LoadResult:
    """Container for provider responses."""

    data: pd.DataFrame
    skipped_years: list[int] = field(default_factory=list)

    @classmethod
    def empty(cls) -> "LoadResult":
        """Return an empty provider result."""
        return cls(pd.DataFrame())


@runtime_checkable
class NFLDataProvider(Protocol):
    """Protocol describing data provider capabilities."""

    @property
    def name(self) -> str:
        """Return the provider identifier."""
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
