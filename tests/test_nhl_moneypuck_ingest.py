from __future__ import annotations

from pathlib import Path

import pandas as pd
from src.nhl.data.moneypuck_ingest import (
    build_skater_games_curated_cache,
    refresh_skater_games_snapshot,
)

SNAPSHOT_FIXTURE = "tests/testdata/nhl/moneypuck/skater_games_full_snapshot_sample.csv"
LIVE_HEADER_FIXTURE = "tests/testdata/nhl/moneypuck/skater_games_live_header_sample.csv"

EXPECTED_COLUMNS = [
    "season",
    "game_id",
    "game_date",
    "player_id",
    "player_name",
    "team",
    "opponent",
    "shots_on_goal",
    "time_on_ice_minutes",
]


def test_build_skater_games_curated_cache_normalizes_and_filters(
    tmp_path: Path,
) -> None:
    curated_path = tmp_path / "skater_games_curated.parquet"

    curated = build_skater_games_curated_cache(
        snapshot_path=SNAPSHOT_FIXTURE,
        curated_cache_path=str(curated_path),
        seasons=[2024],
    )

    assert curated_path.exists()
    assert curated.columns.tolist() == EXPECTED_COLUMNS
    assert set(curated["season"].astype(int).unique()) == {2024}
    assert pd.api.types.is_datetime64_any_dtype(curated["game_date"])
    assert pd.api.types.is_numeric_dtype(curated["shots_on_goal"])
    assert pd.api.types.is_numeric_dtype(curated["time_on_ice_minutes"])


def test_refresh_skater_games_snapshot_materializes_curated_cache(
    tmp_path: Path,
) -> None:
    curated_path = tmp_path / "refresh_curated.parquet"

    result = refresh_skater_games_snapshot(
        snapshot_path=SNAPSHOT_FIXTURE,
        curated_cache_path=str(curated_path),
        seasons=[2023, 2024],
    )

    on_disk = pd.read_parquet(curated_path)
    assert not result.empty
    assert result.columns.tolist() == EXPECTED_COLUMNS
    assert on_disk.columns.tolist() == EXPECTED_COLUMNS
    assert set(on_disk["season"].astype(int).unique()) == {2023, 2024}


def test_build_skater_games_curated_cache_supports_live_moneypuck_headers(
    tmp_path: Path,
) -> None:
    curated_path = tmp_path / "live_header_curated.parquet"

    curated = build_skater_games_curated_cache(
        snapshot_path=LIVE_HEADER_FIXTURE,
        curated_cache_path=str(curated_path),
        seasons=[2024],
    )

    assert curated_path.exists()
    assert curated.columns.tolist() == EXPECTED_COLUMNS
    assert set(curated["player_name"].astype(str).unique()) == {"Player One", "Player Two"}
    assert curated["player_id"].astype(str).str.len().gt(0).all()
