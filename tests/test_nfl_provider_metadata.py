import hashlib
import json
from datetime import UTC
from pathlib import Path

import pandas as pd
import pytest
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
        "ngs",
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


def test_fixture_is_exercised_through_weekly_normalization(monkeypatch):
    path = Path(__file__).parent / "testdata" / "nflreadpy_weekly.csv"

    class Stub:
        def load_player_stats(self, years, summary_level="week"):
            return pd.read_csv(path)

    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NFLReadPyProvider().load_weekly([2024])
    assert result.freshness.status == "complete"
    assert result.data.loc[0, "attempts"] == 30


def test_bulk_failure_is_not_unscoped_or_duplicated(monkeypatch):
    class Stub:
        def load_player_stats(self, years, summary_level="week"):
            if len(years) > 1:
                raise RuntimeError("500 Service Unavailable")
            if years == [2024]:
                raise RuntimeError("404 Not Found")
            return pd.DataFrame({"season": [2025], "week": [1]})

    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NFLReadPyProvider().load_weekly([2024, 2025])
    assert result.freshness.status == "partial"
    assert [(failure.year, failure.kind) for failure in result.failures] == [
        (2024, "unavailable")
    ]
    assert all(failure.year is not None for failure in result.failures)


def test_missing_season_column_uses_shared_reconciliation(monkeypatch):
    class Stub:
        def load_player_stats(self, years, summary_level="week"):
            return pd.DataFrame({"week": [1], "pass_attempts": [20]})

    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NFLReadPyProvider().load_weekly([2024, 2025])
    assert result.data.empty
    assert result.skipped_years == [2024, 2025]
    assert [failure.year for failure in result.failures] == [2024, 2025]


def test_no_season_response_is_discarded_and_reports_unique_requested_years(
    monkeypatch,
):
    class Stub:
        def load_player_stats(self, years, summary_level="week"):
            return pd.DataFrame({"week": [1], "pass_attempts": [20]})

    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NFLReadPyProvider().load_weekly([2024, 2024, 2025])

    assert result.data.empty
    assert result.skipped_years == [2024, 2025]
    assert [failure.year for failure in result.failures] == [2024, 2025]


def test_advertised_nflreadpy_datasets_have_fixture_contracts():
    fixture_dir = Path(__file__).parent / "testdata"
    manifest = json.loads(
        (fixture_dir / "nflreadpy_provider_manifest.json").read_text()
    )
    capability = NFLReadPyProvider().capabilities
    assert set(manifest["datasets"]) == set(capability.datasets)
    for dataset in capability.datasets:
        entry = manifest["fixtures"][dataset]
        payload = (fixture_dir / entry["fixture"]).read_bytes()
        assert hashlib.sha256(payload).hexdigest() == entry["sha256"]
        assert list(pd.read_csv(fixture_dir / entry["fixture"]).columns) == entry[
            "columns"
        ]


@pytest.mark.parametrize(
    ("dataset", "expected_columns"),
    [
        ("player_stats", {"season", "week", "attempts"}),
        ("schedules", {"season", "week", "game_id", "div_game"}),
        ("pbp", {"season", "week", "game_id", "pass_attempt"}),
        ("ngs", {"season", "week", "player_gsis_id", "avg_time_to_throw"}),
    ],
)
def test_each_advertised_fixture_exercises_real_loader_normalization(
    monkeypatch, dataset, expected_columns
):
    import src.nfl.data.providers.readpy as module

    fixture_dir = Path(__file__).parent / "testdata"
    manifest = json.loads(
        (fixture_dir / "nflreadpy_provider_manifest.json").read_text()
    )
    fixture = pd.read_csv(fixture_dir / manifest["fixtures"][dataset]["fixture"])

    class Stub:
        def load_player_stats(self, years, summary_level="week"):
            return fixture

        def load_schedules(self, years):
            return fixture

        def load_pbp(self, years):
            return fixture

        def load_nextgen_stats(self, years, stat_type="passing"):
            return fixture

    monkeypatch.setattr(module, "nfl", Stub())
    loader_name = module.DATASET_LOADERS[dataset]
    loader = getattr(NFLReadPyProvider(), loader_name)
    result = loader([2024])

    assert result.freshness.available_years == (2024,)
    assert expected_columns <= set(result.data.columns)


def test_advertised_datasets_map_to_callable_loaders():
    from src.nfl.data.providers.nfl_data_py_provider import DATASET_LOADERS as legacy
    from src.nfl.data.providers.readpy import DATASET_LOADERS as modern

    for provider, mapping in (
        (NFLReadPyProvider(), modern),
        (NflDataPyProvider(), legacy),
    ):
        assert set(mapping) == set(provider.capabilities.datasets)
        assert all(callable(getattr(provider, name)) for name in mapping.values())


def test_legacy_empty_and_partial_responses_reconcile_seasons(monkeypatch):
    class Stub:
        def import_weekly_data(self, years):
            return pd.DataFrame({"season": [years[0]], "week": [1]})

    import src.nfl.data.providers.nfl_data_py_provider as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NflDataPyProvider().load_weekly([2024, 2025])
    assert result.skipped_years == [2025]
    assert [failure.year for failure in result.failures] == [2025]


def test_legacy_no_season_response_is_discarded(monkeypatch):
    class Stub:
        def import_weekly_data(self, years):
            return pd.DataFrame({"week": [1], "pass_attempts": [20]})

    import src.nfl.data.providers.nfl_data_py_provider as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NflDataPyProvider().load_weekly([2024])
    assert result.data.empty
    assert result.failures[0].exception_type == "MissingSeason"


def test_legacy_foreign_only_response_is_unavailable(monkeypatch):
    class Stub:
        def import_weekly_data(self, years):
            return pd.DataFrame({"season": [2023], "week": [1]})

    import src.nfl.data.providers.nfl_data_py_provider as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NflDataPyProvider().load_weekly([2024])
    assert result.data.empty
    assert result.freshness.available_years == ()
    assert result.skipped_years == [2024]
    assert [failure.year for failure in result.failures] == [2024]


def test_legacy_mixed_response_discards_foreign_rows(monkeypatch):
    class Stub:
        def import_weekly_data(self, years):
            return pd.DataFrame({"season": [2023, 2024], "week": [1, 2]})

    import src.nfl.data.providers.nfl_data_py_provider as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NflDataPyProvider().load_weekly([2024])
    assert result.data["season"].tolist() == [2024]
    assert result.freshness.available_years == (2024,)
    assert result.failures == ()


def test_bulk_partial_and_foreign_seasons_retry_only_missing(monkeypatch):
    calls = []

    class Stub:
        def load_player_stats(self, years, summary_level="week"):
            calls.append(years)
            if len(years) > 1:
                return pd.DataFrame({"season": [2023, 2024], "week": [1, 1]})
            return pd.DataFrame({"season": years, "week": [2]})

    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NFLReadPyProvider().load_weekly([2024, 2025])
    assert calls == [[2024, 2025], [2025]]
    assert set(result.data["season"].dropna().astype(int)) == {2024, 2025}
    assert result.freshness.available_years == (2024, 2025)


def test_per_season_foreign_rows_are_unavailable(monkeypatch):
    class Stub:
        def load_player_stats(self, years, summary_level="week"):
            if len(years) > 1:
                raise RuntimeError("bulk unavailable")
            return pd.DataFrame({"season": [2023], "week": [1]})

    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NFLReadPyProvider().load_weekly([2024])
    assert result.data.empty
    assert result.skipped_years == [2024]
    assert result.failures[0].year == 2024


def test_partial_fallback_emits_one_warning(monkeypatch):
    class Stub:
        def load_player_stats(self, years, summary_level="week"):
            if len(years) > 1:
                raise RuntimeError("bulk unavailable")
            if years == [2025]:
                raise RuntimeError("404 Not Found")
            return pd.DataFrame({"season": years, "week": [1]})

    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "nfl", Stub())
    import pytest

    with pytest.warns(RuntimeWarning) as records:
        NFLReadPyProvider().load_weekly([2024, 2025])
    assert len(records) == 1
