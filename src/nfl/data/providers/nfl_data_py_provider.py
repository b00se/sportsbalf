"""nfl_data_py-backed provider."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from .base import (
    CapabilityRecord,
    FreshnessMetadata,
    LoadResult,
    NFLDataProvider,
    ProviderCapabilities,
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
    requested = tuple(int(year) for year in years)
    available = (
        tuple(sorted({int(value) for value in frame["season"].dropna()}))
        if "season" in frame
        else requested
    )
    freshness = FreshnessMetadata(
        requested,
        available,
        datetime.now(UTC),
        "complete" if set(requested) <= set(available) else "partial",
    )
    return LoadResult(frame, [], freshness)


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
            ("weekly", "schedules", "pbp", "ngs_passing"),
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
                for dataset in (
                    "schedules",
                    "player_stats",
                    "pbp",
                    "rosters",
                    "depth_charts",
                    "ngs",
                    "participation",
                    "betting_lines",
                )
            ),
        )

    def load_weekly(self, years: Sequence[int]) -> LoadResult:
        module = _require_module()
        frame = _to_frame(module.import_weekly_data(list(years)))
        return _result(frame, years)

    def load_schedules(self, years: Sequence[int]) -> LoadResult:
        module = _require_module()
        frame = _to_frame(module.import_schedules(list(years)))
        return _result(frame, years)

    def load_pbp(self, years: Sequence[int]) -> LoadResult:
        module = _require_module()
        frame = _to_frame(module.import_pbp_data(list(years), downcast=True))
        return _result(frame, years)

    def load_ngs_passing(self, years: Sequence[int]) -> LoadResult:
        module = _require_module()

        # nfl_data_py naming has changed across versions.
        if hasattr(module, "import_ngs_data"):
            frame = _to_frame(module.import_ngs_data("passing", years=list(years)))
            return _result(frame, years)
        if hasattr(module, "import_ngs_passing"):
            frame = _to_frame(module.import_ngs_passing(years=list(years)))
            return _result(frame, years)

        return LoadResult.empty()
