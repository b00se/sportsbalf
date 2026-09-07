from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from src.nfl.data.availability import (
    AvailabilityGateError,
    AvailabilityRecord,
    AvailabilityStatus,
    check_unattended_availability,
    fixture_sha256,
    load_fixture,
    reconcile_availability,
)

NOW = datetime.now(UTC)


def record(
    player: str,
    source: str,
    status: AvailabilityStatus,
    confidence: float = 1.0,
) -> AvailabilityRecord:
    return AvailabilityRecord(
        player, source, status, confidence, NOW, f"fixture:{source}", 2026
    )


def test_weighted_tournament_prefers_ud_and_records_provenance() -> None:
    result = reconcile_availability(
        [
            record("p1", "ud", AvailabilityStatus.INACTIVE),
            record("p1", "espn", AvailabilityStatus.ACTIVE),
        ]
    )["p1"]
    assert result.status is AvailabilityStatus.INACTIVE
    assert result.sources == ("espn", "ud")
    assert result.conflict


def test_high_impact_conflict_blocks_but_warning_is_reportable() -> None:
    result = reconcile_availability(
        [
            record("p1", "ud", AvailabilityStatus.INACTIVE),
            record("p1", "sleeper", AvailabilityStatus.ACTIVE),
        ]
    )["p1"]
    assert result.conflict
    assert result.warnings == ("sources disagree",)
    with pytest.raises(AvailabilityGateError, match="p1"):
        check_unattended_availability(["p1"], {"p1": result}, high_impact=["p1"])


def test_stale_observations_are_not_used_and_missing_blocks() -> None:
    stale = AvailabilityRecord(
        "p1",
        "ud",
        AvailabilityStatus.ACTIVE,
        1.0,
        NOW - timedelta(days=3),
        "fixture",
        2026,
    )
    assert reconcile_availability([stale]) == {}
    with pytest.raises(AvailabilityGateError, match="no fresh"):
        check_unattended_availability(["p1"], {})


def test_non_impact_conflict_is_warning_without_gate() -> None:
    result = reconcile_availability(
        [
            record("p1", "ud", AvailabilityStatus.INACTIVE),
            record("p1", "sleeper", AvailabilityStatus.ACTIVE),
        ]
    )["p1"]
    check_unattended_availability(["p1"], {"p1": result}, high_impact=[])


def test_invalid_record_metadata_fails_closed() -> None:
    with pytest.raises(ValueError):
        AvailabilityRecord(
            "p1", "ud", AvailabilityStatus.ACTIVE, 2, NOW, "fixture", 2026
        )


def test_fixture_loader_is_offline_and_auditable() -> None:
    path = Path("tests/testdata/nfl_availability_fixture.json")
    rows = load_fixture(path)
    assert len(rows) == 4
    assert {row.source for row in rows} == {"ud", "sleeper", "espn", "roster_depth"}
    assert len(fixture_sha256(path)) == 64
