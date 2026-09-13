"""Offline contract tests for strict nflverse participation ingestion."""

import pandas as pd
import pytest
from src.nfl.data.providers.base import (
    RawSourceUnavailableError,
    UnsupportedCapabilityError,
)
from src.nfl.data.providers.nfl_data_py_provider import NflDataPyProvider
from src.nfl.data.providers.readpy import NFLReadPyProvider


def _source_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "nflverse_game_id": ["2024_01_DEN_OAK", "2024_01_DEN_OAK"],
            "play_id": [1, 1],
            "players_on_play": ["00-1;00-2", "00-1;00-2"],
            "offense_players": ["00-1;00-2", "00-1;00-2"],
            "route": ["GO", "GO"],
        }
    )


def test_nflreadpy_participation_normalizes_play_identity_and_preserves_route(
    monkeypatch,
):
    frame = _source_frame()

    class Stub:
        def load_participation(self, years):
            return frame.copy()

    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NFLReadPyProvider().load_participation([2024])
    assert result.freshness.status == "complete"
    assert result.data["week"].dtype.name == "Int64"
    assert result.data["season"].iloc[0] == 2024
    assert result.data["week"].iloc[0] == 1
    assert result.data["game_id"].iloc[0] == "2024_01_DEN_OAK"
    assert result.data["route"].iloc[0] == "GO"
    assert "routes" not in result.data


def test_nflreadpy_participation_raw_preserves_duplicates_and_requires_all_seasons(
    monkeypatch,
):
    frame = _source_frame()

    class Stub:
        def load_participation(self, years):
            return frame.copy()

    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "nfl", Stub())
    provider = NFLReadPyProvider()
    raw = provider.load_participation_raw([2024])
    assert len(raw) == 2
    assert raw.duplicated(["nflverse_game_id", "play_id"]).any()
    with pytest.raises(RawSourceUnavailableError, match="season 2025"):
        provider.load_participation_raw([2024, 2025])


def test_nflreadpy_participation_fails_closed_without_play_fields(monkeypatch):
    class Stub:
        def load_participation(self, years):
            return pd.DataFrame({"nflverse_game_id": ["2024_01_DEN_OAK"]})

    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NFLReadPyProvider().load_participation([2024])
    assert result.data.empty
    assert result.failures[0].exception_type == "RawSourceUnavailableError"


def test_nflreadpy_participation_rejects_malformed_game_identity(monkeypatch):
    class Stub:
        def load_participation(self, years):
            return pd.DataFrame(
                {
                    "nflverse_game_id": ["bad"],
                    "play_id": [1],
                    "players_on_play": ["00-1"],
                }
            )

    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NFLReadPyProvider().load_participation([2024])
    assert result.data.empty
    assert result.failures[0].exception_type == "RawSourceUnavailableError"


@pytest.mark.parametrize(
    "game_id",
    ["2015_01_DEN_OAK", "2024_00_DEN_OAK", "2024_99_DEN_OAK", "2099_01_DEN_OAK"],
)
def test_nflreadpy_participation_rejects_out_of_range_calendar(monkeypatch, game_id):
    class Stub:
        def load_participation(self, years):
            return _source_frame().assign(nflverse_game_id=game_id)

    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NFLReadPyProvider().load_participation([2024])
    assert result.data.empty
    assert result.failures[0].exception_type == "RawSourceUnavailableError"


def test_nflreadpy_participation_caps_at_current_year_with_injected_boundary(
    monkeypatch,
):
    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "_participation_max_season", lambda: 2026)

    class Stub:
        def load_participation(self, years):
            return _source_frame().assign(nflverse_game_id="2026_01_DEN_OAK")

    monkeypatch.setattr(module, "nfl", Stub())
    accepted = module.NFLReadPyProvider().load_participation([2026])
    assert accepted.freshness.status == "complete"
    assert accepted.data["season"].iloc[0] == 2026

    class FutureStub:
        def load_participation(self, years):
            return _source_frame().assign(nflverse_game_id="2027_01_DEN_OAK")

    monkeypatch.setattr(module, "nfl", FutureStub())
    rejected = module.NFLReadPyProvider().load_participation([2027])
    assert rejected.data.empty
    assert rejected.failures[0].exception_type == "RawSourceUnavailableError"
    assert "2016-2026" in rejected.failures[0].message


@pytest.mark.parametrize("play_id", [None, float("inf"), 1.5, True])
def test_nflreadpy_participation_rejects_invalid_play_id(monkeypatch, play_id):
    class Stub:
        def load_participation(self, years):
            return _source_frame().assign(play_id=play_id)

    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NFLReadPyProvider().load_participation([2024])
    assert result.data.empty
    assert result.failures[0].exception_type == "RawSourceUnavailableError"


def test_nflreadpy_participation_requires_categorical_route_column(monkeypatch):
    class Stub:
        def load_participation(self, years):
            return _source_frame().drop(columns="route")

    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NFLReadPyProvider().load_participation([2024])
    assert result.data.empty
    assert result.failures[0].exception_type == "RawSourceUnavailableError"


def test_nflreadpy_participation_rejects_numeric_route_column(monkeypatch):
    class Stub:
        def load_participation(self, years):
            return _source_frame().assign(route=[1, 2])

    import src.nfl.data.providers.readpy as module

    monkeypatch.setattr(module, "nfl", Stub())
    result = NFLReadPyProvider().load_participation([2024])
    assert result.data.empty
    assert result.failures[0].exception_type == "RawSourceUnavailableError"


def test_legacy_participation_is_typed_unsupported(monkeypatch):
    import src.nfl.data.providers.nfl_data_py_provider as module

    monkeypatch.setattr(module, "nfl", object())
    provider = NflDataPyProvider()
    result = provider.load_participation([2024])
    assert result.data.empty
    assert result.failures[0].exception_type == "UnsupportedCapability"
    with pytest.raises(UnsupportedCapabilityError):
        provider.load_participation_raw([2024])


def test_nflreadpy_capability_advertises_participation(monkeypatch):
    capability = NFLReadPyProvider().capabilities
    assert "participation" in capability.datasets
    record = next(item for item in capability.audit if item.dataset == "participation")
    assert record.fallback == "skip unavailable season"
