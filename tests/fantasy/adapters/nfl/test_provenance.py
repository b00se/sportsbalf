"""Tests for deterministic NFL daily rankings provenance manifests."""

from __future__ import annotations

import pytest
from src.fantasy.adapters.nfl.provenance import (
    SnapshotInput,
    build_snapshot_manifest,
)


def test_manifest_is_byte_stable_for_identical_inputs() -> None:
    inputs = (
        SnapshotInput(
            name="rankings.csv",
            content=b"id,playerId\n1,p1\n",
            source_timestamp_utc="2026-09-06T12:00:00Z",
        ),
    )

    first = build_snapshot_manifest(
        inputs=inputs,
        configuration={"scoring": "half_ppr"},
        outputs={"rankings.csv": b"id,playerId\n1,p1\n"},
        run_metadata={"seed": 2026, "code_commit": "abc123"},
    )
    second = build_snapshot_manifest(
        inputs=inputs,
        configuration={"scoring": "half_ppr"},
        outputs={"rankings.csv": b"id,playerId\n1,p1\n"},
        run_metadata={"seed": 2026, "code_commit": "abc123"},
    )

    assert first.to_json_bytes() == second.to_json_bytes()
    assert first.snapshot_id == second.snapshot_id


def test_manifest_id_changes_when_an_input_changes() -> None:
    common_kwargs = {
        "configuration": {"scoring": "half_ppr"},
        "outputs": {},
        "run_metadata": {"seed": 2026},
    }
    first = build_snapshot_manifest(
        inputs=(SnapshotInput("rankings.csv", b"one", "2026-09-06T12:00:00Z"),),
        **common_kwargs,
    )
    second = build_snapshot_manifest(
        inputs=(SnapshotInput("rankings.csv", b"two", "2026-09-06T12:00:00Z"),),
        **common_kwargs,
    )

    assert first.snapshot_id != second.snapshot_id


def test_manifest_rejects_sensitive_metadata_keys() -> None:
    try:
        build_snapshot_manifest(
            inputs=(),
            configuration={},
            outputs={},
            run_metadata={"api_token": "secret"},
        )
    except ValueError as error:
        assert "sensitive" in str(error)
    else:
        raise AssertionError("Sensitive metadata must be rejected.")


@pytest.mark.parametrize(
    "metadata",
    [
        {"items": [{"api_token": "secret"}]},
        {"credentials": "secret"},
        {"account_id": "private"},
    ],
)
def test_manifest_rejects_nested_sensitive_or_account_metadata(
    metadata: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="sensitive"):
        build_snapshot_manifest(
            inputs=(), configuration={}, outputs={}, run_metadata=metadata
        )


def test_manifest_payload_is_immutable() -> None:
    manifest = build_snapshot_manifest(
        inputs=(), configuration={"scoring": "half_ppr"}, outputs={}, run_metadata={}
    )

    with pytest.raises(TypeError):
        manifest.payload["configuration"] = {}


@pytest.mark.parametrize("timestamp", ["UTC", "2026-09-06T12:00:00+01:00"])
def test_manifest_rejects_malformed_or_non_utc_source_timestamps(
    timestamp: str,
) -> None:
    with pytest.raises(ValueError, match="UTC timestamp"):
        build_snapshot_manifest(
            inputs=(SnapshotInput("rankings.csv", b"rows", timestamp),),
            configuration={},
            outputs={},
            run_metadata={},
        )
