from __future__ import annotations

from pathlib import Path

import pytest
from src.nhl.data.moneypuck_ingest import build_skater_games_curated_cache
from src.nhl.data.providers import get_provider

SNAPSHOT_FIXTURE = "tests/testdata/nhl/moneypuck/skater_games_full_snapshot_sample.csv"


def _build_curated(tmp_path: Path) -> Path:
    curated_path = tmp_path / "provider_curated.parquet"
    build_skater_games_curated_cache(
        snapshot_path=SNAPSHOT_FIXTURE,
        curated_cache_path=str(curated_path),
        seasons=[2023, 2024],
    )
    return curated_path


def test_provider_loads_and_filters_requested_seasons(tmp_path: Path) -> None:
    curated_path = _build_curated(tmp_path)
    provider = get_provider(
        "moneypuck_snapshot",
        curated_cache_path=str(curated_path),
    )

    result = provider.load_skater_games([2024])

    assert not result.data.empty
    assert set(result.data["season"].astype(int).unique()) == {2024}
    assert result.metadata["provider"] == "moneypuck_snapshot"


def test_provider_raises_for_missing_requested_season_rows(tmp_path: Path) -> None:
    curated_path = _build_curated(tmp_path)
    provider = get_provider(
        "moneypuck_snapshot",
        curated_cache_path=str(curated_path),
    )

    with pytest.raises(RuntimeError, match="requested seasons"):
        provider.load_skater_games([2022])


def test_get_provider_rejects_unsupported_provider_name() -> None:
    with pytest.raises(ValueError, match="Unsupported provider"):
        get_provider("not-a-provider")
