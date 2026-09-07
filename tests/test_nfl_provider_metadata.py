from datetime import UTC

import pandas as pd
from src.nfl.data.providers.base import (
    FailureMetadata,
    FreshnessMetadata,
    ProviderCapabilities,
)
from src.nfl.data.providers.readpy import NFLReadPyProvider


def test_nflreadpy_declares_capabilities_and_typed_freshness(monkeypatch):
    class FakePolars:
        def to_pandas(self):
            return pd.DataFrame({"season": [2024], "week": [1]})

    class Stub:
        def load_player_stats(self, years, summary_level="week"):
            return FakePolars()

    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "nfl", Stub())
    monkeypatch.setattr(module, "_NFL_IMPORT_ERROR", None)
    result = NFLReadPyProvider().load_weekly([2024])

    assert isinstance(NFLReadPyProvider().capabilities, ProviderCapabilities)
    assert "weekly" in NFLReadPyProvider().capabilities.datasets
    assert isinstance(result.freshness, FreshnessMetadata)
    assert result.freshness.requested_years == (2024,)
    assert result.freshness.available_years == (2024,)
    assert result.freshness.status == "complete"
    assert result.freshness.retrieved_at.tzinfo == UTC
    assert result.failures == ()


def test_nflreadpy_records_typed_failure_for_skipped_year(monkeypatch):
    class Stub:
        def load_player_stats(self, years, summary_level="week"):
            if years == [2025]:
                raise RuntimeError("404 Not Found")
            return pd.DataFrame({"season": [2024], "week": [1]})

    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "nfl", Stub())
    monkeypatch.setattr(module, "_NFL_IMPORT_ERROR", None)
    result = NFLReadPyProvider().load_weekly([2024, 2025])

    assert result.freshness.status == "partial"
    assert len(result.failures) == 1
    assert isinstance(result.failures[0], FailureMetadata)
    assert result.failures[0].year == 2025
    assert result.failures[0].kind == "unavailable"
