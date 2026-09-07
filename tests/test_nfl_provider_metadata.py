import hashlib
import json
from datetime import UTC
from pathlib import Path

import pandas as pd
from src.nfl.data.providers.base import (
    FailureMetadata,
    FreshnessMetadata,
    ProviderCapabilities,
)
from src.nfl.data.providers.nfl_data_py_provider import NflDataPyProvider
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
    assert "player_stats" in NFLReadPyProvider().capabilities.datasets
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


def test_capability_audit_covers_all_required_sources():
    capability = NFLReadPyProvider().capabilities
    assert {record.dataset for record in capability.audit} == {
        "schedules",
        "player_stats",
        "pbp",
        "rosters",
        "depth_charts",
        "ngs",
        "participation",
        "betting_lines",
    }
    assert all(
        record.seasons and record.cadence and record.license
        for record in capability.audit
    )
    assert all(record.fallback is not None for record in capability.audit)


def test_legacy_provider_exposes_capabilities_and_freshness(monkeypatch):
    class Stub:
        def import_weekly_data(self, years):
            return pd.DataFrame({"season": years, "week": [1] * len(years)})

    import src.nfl.data.providers.nfl_data_py_provider as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NflDataPyProvider().load_weekly([2024])
    assert result.freshness.status == "complete"
    assert NflDataPyProvider().capabilities.datasets


def test_all_unavailable_returns_typed_failure_result(monkeypatch):
    class Stub:
        def load_player_stats(self, years, summary_level="week"):
            raise RuntimeError("404 Not Found")

    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NFLReadPyProvider().load_weekly([2025])
    assert result.data.empty
    assert result.skipped_years == [2025]
    assert result.freshness.status == "empty"
    assert result.failures[0].year == 2025


def test_all_unavailable_attributes_each_requested_season(monkeypatch):
    class Stub:
        def load_player_stats(self, years, summary_level="week"):
            raise RuntimeError("404 Not Found")

    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NFLReadPyProvider().load_weekly([2024, 2025])
    assert [failure.year for failure in result.failures] == [2024, 2025]


def test_non404_is_typed_and_legacy_missing_module_is_typed(monkeypatch):
    class Stub:
        def load_player_stats(self, years, summary_level="week"):
            raise RuntimeError("500 Service Unavailable")

    import src.nfl.data.providers.readpy as readpy

    monkeypatch.setattr(readpy, "nfl", Stub())
    result = NFLReadPyProvider().load_weekly([2025])
    assert result.failures[0].kind == "error"
    assert result.failures[0].year == 2025

    import src.nfl.data.providers.nfl_data_py_provider as datapy

    monkeypatch.setattr(datapy, "nfl", None)
    result = NflDataPyProvider().load_weekly([2025])
    assert result.failures[0].kind == "error"
    assert result.failures[0].year == 2025


def test_unsupported_legacy_ngs_is_typed(monkeypatch):
    class Stub:
        pass

    import src.nfl.data.providers.nfl_data_py_provider as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NflDataPyProvider().load_ngs_passing([2025])
    assert result.failures[0].kind == "error"
    assert result.failures[0].exception_type == "UnsupportedCapability"


def test_fixture_bytes_are_loaded_and_hash_verified():
    path = Path(__file__).parent / "testdata" / "nflreadpy_weekly.csv"
    manifest = json.loads(
        (
            Path(__file__).parent / "testdata" / "nflreadpy_provider_manifest.json"
        ).read_text()
    )
    payload = path.read_bytes()
    assert hashlib.sha256(payload).hexdigest() == manifest["sha256"]
    frame = pd.read_csv(path)
    assert list(frame.columns) == manifest["columns"]
    assert frame.loc[0, "season"] == 2024
