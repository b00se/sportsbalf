"""nfl_data_py-backed provider."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd

from .base import LoadResult, NFLDataProvider

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
            "nfl_data_py is required for this provider. Install the package before running."
        ) from _NFL_DATA_PY_IMPORT_ERROR
    return nfl


def _to_frame(value: Any) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value.copy()
    return pd.DataFrame(value)


class NflDataPyProvider(NFLDataProvider):
    """Implementation that delegates to nfl_data_py."""

    @property
    def name(self) -> str:
        """Return the provider identifier."""
        return "nfl_data_py"

    def load_weekly(self, years: Sequence[int]) -> LoadResult:
        module = _require_module()
        frame = _to_frame(module.import_weekly_data(list(years)))
        return LoadResult(frame)

    def load_schedules(self, years: Sequence[int]) -> LoadResult:
        module = _require_module()
        frame = _to_frame(module.import_schedules(list(years)))
        return LoadResult(frame)

    def load_pbp(self, years: Sequence[int]) -> LoadResult:
        module = _require_module()
        frame = _to_frame(module.import_pbp_data(list(years), downcast=True))
        return LoadResult(frame)

    def load_ngs_passing(self, years: Sequence[int]) -> LoadResult:
        module = _require_module()

        # nfl_data_py naming has changed across versions.
        if hasattr(module, "import_ngs_data"):
            frame = _to_frame(module.import_ngs_data("passing", years=list(years)))
            return LoadResult(frame)
        if hasattr(module, "import_ngs_passing"):
            frame = _to_frame(module.import_ngs_passing(years=list(years)))
            return LoadResult(frame)

        return LoadResult.empty()
