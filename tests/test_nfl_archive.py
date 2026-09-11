"""Offline contract tests for the immutable NFLverse archive adapter."""

import json
from pathlib import Path

import pandas as pd
import pytest
from src.nfl.data.archive import ArchiveAdapter, ArchiveIntegrityError


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {"season": [2023, 2024], "week": [1, 2], "player_id": ["a", "b"]}
    )


def test_round_trip_writes_and_verifies_manifest(tmp_path):
    adapter = ArchiveAdapter(tmp_path)
    result = adapter.write(
        "weekly", _frame(), requested_seasons=[2023, 2024],
        source_url="https://github.com/nflverse/nflverse-data/releases",
        source_version="v1", cutoff_semantics="strictly before 2025-01-01T00:00:00Z",
    )
    loaded, manifest = adapter.load(result.payload_path)
    pd.testing.assert_frame_equal(loaded, _frame().loc[:, sorted(_frame().columns)])
    assert manifest["requested_seasons"] == [2023, 2024]
    assert manifest["available_seasons"] == [2023, 2024]
    assert manifest["row_count"] == 2
    assert manifest["byte_size"] == result.payload_path.stat().st_size
    assert manifest["sha256"]
    assert manifest["schema_fingerprint"]


def test_tampering_or_schema_change_fails_closed(tmp_path):
    adapter = ArchiveAdapter(tmp_path)
    result = adapter.write("weekly", _frame(), requested_seasons=[2023])
    result.payload_path.write_bytes(result.payload_path.read_bytes() + b"\n")
    with pytest.raises(ArchiveIntegrityError, match="(SHA-256|byte size)"):
        adapter.load(result.payload_path)

    result = adapter.write("other", _frame(), requested_seasons=[2023])
    manifest = json.loads(result.manifest_path.read_text())
    manifest["schema_fingerprint"] = "wrong"
    result.manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ArchiveIntegrityError, match="schema"):
        adapter.load(result.payload_path)


def test_collision_never_overwrites_and_malformed_manifest_rejected(tmp_path):
    adapter = ArchiveAdapter(tmp_path)
    result = adapter.write("weekly", _frame(), requested_seasons=[2023])
    with pytest.raises(FileExistsError):
        adapter.write("weekly", _frame(), requested_seasons=[2023])
    result.manifest_path.write_text("not json")
    with pytest.raises(ArchiveIntegrityError, match="manifest"):
        adapter.load(result.payload_path)


def test_schema_fingerprint_is_deterministic_for_column_order():
    first = ArchiveAdapter.schema_fingerprint(_frame())
    second = ArchiveAdapter.schema_fingerprint(
        _frame()[["player_id", "week", "season"]]
    )
    assert first == second


def test_schema_fingerprint_survives_normalized_nullable_fixture(tmp_path):
    frame = pd.read_csv(Path(__file__).parent / "testdata" / "nflreadpy_weekly.csv")
    frame["season"] = frame["season"].astype("Int64")
    frame["week"] = frame["week"].astype("Int64")
    frame["optional_note"] = pd.Series([None] * len(frame), dtype="object")
    adapter = ArchiveAdapter(tmp_path)
    result = adapter.write("normalized", frame, requested_seasons=[2024])
    loaded, _ = adapter.load_cache_hit(result.payload_path)
    assert adapter.schema_fingerprint(loaded) == adapter.schema_fingerprint(frame)


def test_missing_payload_and_invalid_manifest_fields_fail_closed(tmp_path):
    adapter = ArchiveAdapter(tmp_path)
    result = adapter.write("weekly", _frame(), requested_seasons=[2023])
    result.payload_path.unlink()
    with pytest.raises(ArchiveIntegrityError, match="payload"):
        adapter.load(result.payload_path)

    result = adapter.write("invalid", _frame(), requested_seasons=[2023])
    manifest = json.loads(result.manifest_path.read_text())
    manifest["dataset"] = " "
    result.manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ArchiveIntegrityError, match="dataset"):
        adapter.load(result.payload_path)


def test_tampered_one_row_manifest_rejects_boolean_row_count(tmp_path):
    adapter = ArchiveAdapter(tmp_path)
    result = adapter.write("weekly", _frame(), requested_seasons=[2023])
    manifest = json.loads(result.manifest_path.read_text())
    manifest["row_count"] = True
    result.manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ArchiveIntegrityError, match="row count"):
        adapter.load(result.payload_path)
