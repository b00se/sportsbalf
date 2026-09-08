"""Provider exports for NFL data ingestion."""

from __future__ import annotations

from .base import (
    DEFAULT_PROVIDER_NAME,
    CapabilityRecord,
    FailureMetadata,
    FreshnessMetadata,
    LoadResult,
    NFLDataProvider,
    ProviderCapabilities,
    ProviderName,
    get_provider,
)
from .nfl_data_py_provider import NflDataPyProvider
from .readpy import NFLReadPyProvider

__all__ = [
    "DEFAULT_PROVIDER_NAME",
    "CapabilityRecord",
    "FailureMetadata",
    "FreshnessMetadata",
    "LoadResult",
    "NFLDataProvider",
    "NflDataPyProvider",
    "NFLReadPyProvider",
    "ProviderName",
    "ProviderCapabilities",
    "get_provider",
]
