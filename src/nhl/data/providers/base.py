"""Provider abstractions for NHL PR#9 data access."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Protocol, Sequence, runtime_checkable

import pandas as pd

ProviderName = Literal["moneypuck_snapshot"]
DEFAULT_PROVIDER_NAME: ProviderName = "moneypuck_snapshot"


@dataclass(slots=True)
class LoadResult:
    """Container for provider load responses."""

    data: pd.DataFrame
    metadata: dict[str, Any]


@runtime_checkable
class NhlDataProvider(Protocol):
    """Protocol describing NHL provider capabilities for PR#9."""

    @property
    def name(self) -> str:
        """Return provider identifier."""
        ...

    def load_skater_games(self, seasons: Sequence[int]) -> LoadResult:
        """Return curated skater game rows for requested seasons."""
        ...


def get_provider(
    name: str | ProviderName | None = None,
    *,
    curated_cache_path: str | None = None,
) -> NhlDataProvider:
    """Instantiate a concrete NHL provider.

    Args:
        name: Provider identifier. Defaults to ``DEFAULT_PROVIDER_NAME``.
        curated_cache_path: Runtime path to curated skater-games parquet cache.

    Returns:
        Concrete provider instance.

    Raises:
        ValueError: If provider is unsupported or required args are missing.
    """

    normalized = str(name or DEFAULT_PROVIDER_NAME).lower()
    if normalized == "moneypuck_snapshot":
        if not isinstance(curated_cache_path, str) or not curated_cache_path.strip():
            raise ValueError(
                "Provider 'moneypuck_snapshot' requires curated_cache_path."
            )
        from src.nhl.data.providers.moneypuck_snapshot import MoneyPuckSnapshotProvider

        return MoneyPuckSnapshotProvider(curated_cache_path=curated_cache_path)

    raise ValueError(f"Unsupported provider '{name}'")


__all__ = [
    "DEFAULT_PROVIDER_NAME",
    "LoadResult",
    "NhlDataProvider",
    "ProviderName",
    "get_provider",
]
