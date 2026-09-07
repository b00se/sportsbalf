"""nfl_data_py-backed provider."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd

from .base import (
    CANONICAL_DATASETS,
    CapabilityRecord,
    FailureMetadata,
    LoadResult,
    NFLDataProvider,
    ProviderCapabilities,
    reconcile_seasons,
)

try:  # pragma: no cover - optional dependency
    import nfl_data_py as nfl
except Exception as exc:  # pragma: no cover - optional dependency missing
    nfl = None  # type: ignore
    _NFL_DATA_PY_IMPORT_ERROR: Exception | None = exc
else:  # pragma: no cover
    _NFL_DATA_PY_IMPORT_ERROR = None


def _require_module() -> Any:
    """Return imported nfl_data_py module or raise a helpful error."""

    if nfl is None:
        raise ImportError(
            "nfl_data_py is required for this provider. "
            "Install the package before running."
        ) from _NFL_DATA_PY_IMPORT_ERROR
    return nfl


def _to_frame(value: Any) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value.copy()
    return pd.DataFrame(value)


def _result(frame: pd.DataFrame, years: Sequence[int]) -> LoadResult:
    """Wrap a legacy response in the shared freshness contract."""
    return reconcile_seasons(frame, years)


def _safe_load(loader: Any, years: Sequence[int]) -> LoadResult:
    """Run a legacy loader and convert failures to the shared result contract."""
    requested = [int(year) for year in years]
    try:
        return _result(_to_frame(loader(requested)), requested)
    except Exception as exc:
        failures = tuple(
            FailureMetadata("error", str(exc), type(exc).__name__, year)
            for year in requested
        )
        return reconcile_seasons(pd.DataFrame(), requested, failures)


def _error_result(exc: Exception, years: Sequence[int]) -> LoadResult:
    """Return a typed result when the legacy module itself is unavailable."""
    requested = [int(year) for year in years]
    return reconcile_seasons(
        pd.DataFrame(),
        requested,
        tuple(
            FailureMetadata("error", str(exc), type(exc).__name__, year)
            for year in requested
        ),
    )


class NflDataPyProvider(NFLDataProvider):
    """Implementation that delegates to nfl_data_py."""

    @property
    def name(self) -> str:
        """Return the provider identifier."""
        return "nfl_data_py"

    @property
    def capabilities(self) -> ProviderCapabilities:
        """Return the audited legacy provider capabilities."""
        return ProviderCapabilities(
            self.name,
            CANONICAL_DATASETS,
            False,
            "nflverse data license",
            tuple(
                CapabilityRecord(
                    dataset,
                    "1999-2024",
                    "per-season",
                    "nflverse data license",
                    "nflreadpy",
                )
                for dataset in CANONICAL_DATASETS
            ),
        )

    def load_weekly(self, years: Sequence[int]) -> LoadResult:
        try:
            module = _require_module()
        except Exception as exc:
            return _error_result(exc, years)
        return _safe_load(lambda values: module.import_weekly_data(values), years)

    def load_schedules(self, years: Sequence[int]) -> LoadResult:
        try:
            module = _require_module()
        except Exception as exc:
            return _error_result(exc, years)
        return _safe_load(lambda values: module.import_schedules(values), years)

    def load_pbp(self, years: Sequence[int]) -> LoadResult:
        try:
            module = _require_module()
        except Exception as exc:
            return _error_result(exc, years)
        return _safe_load(
            lambda values: module.import_pbp_data(values, downcast=True), years
        )

    def load_ngs_passing(self, years: Sequence[int]) -> LoadResult:
        try:
            module = _require_module()
        except Exception as exc:
            return _error_result(exc, years)

        # nfl_data_py naming has changed across versions.
        if hasattr(module, "import_ngs_data"):
            return _safe_load(
                lambda values: module.import_ngs_data("passing", years=values), years
            )
        if hasattr(module, "import_ngs_passing"):
            return _safe_load(
                lambda values: module.import_ngs_passing(years=values), years
            )

        requested = [int(year) for year in years]
        failures = tuple(
            FailureMetadata(
                "error", "unsupported NGS capability", "UnsupportedCapability", year
            )
            for year in requested
        )
        return reconcile_seasons(pd.DataFrame(), requested, failures)
