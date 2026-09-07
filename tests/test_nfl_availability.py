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
    record_from_source,
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
        ], season=2026, as_of_utc=NOW
    )["p1"]
    assert result.status is AvailabilityStatus.INACTIVE
    assert result.sources == ("espn", "ud")
    assert result.conflict


def test_high_impact_conflict_blocks_but_warning_is_reportable() -> None:
    result = reconcile_availability(
        [
            record("p1", "ud", AvailabilityStatus.INACTIVE),
            record("p1", "sleeper", AvailabilityStatus.ACTIVE),
        ], season=2026, as_of_utc=NOW
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
    assert reconcile_availability([stale], season=2026, as_of_utc=NOW) == {}
    with pytest.raises(AvailabilityGateError, match="no fresh"):
        check_unattended_availability(["p1"], {})


def test_non_impact_conflict_is_warning_without_gate() -> None:
    result = reconcile_availability(
        [
            record("p1", "ud", AvailabilityStatus.INACTIVE),
            record("p1", "sleeper", AvailabilityStatus.ACTIVE),
        ], season=2026, as_of_utc=NOW
    )["p1"]
    check_unattended_availability(["p1"], {"p1": result}, high_impact=[])


def test_invalid_record_metadata_fails_closed() -> None:
    with pytest.raises(ValueError):
        AvailabilityRecord(
            "p1", "ud", AvailabilityStatus.ACTIVE, 2, NOW, "fixture", 2026
        )


def test_unknown_source_and_naive_freshness_fail_closed() -> None:
    with pytest.raises(ValueError, match="unknown availability source"):
        AvailabilityRecord("p1", "news", AvailabilityStatus.ACTIVE, 1, NOW, "x", 2026)
    with pytest.raises(ValueError, match="fresh_until"):
        AvailabilityRecord(
            "p1", "ud", AvailabilityStatus.ACTIVE, 1, NOW, "x", 2026,
            fresh_until_utc=datetime(2026, 9, 8),
        )
    with pytest.raises(ValueError, match="observed_at"):
        AvailabilityRecord(
            "p1", "ud", AvailabilityStatus.ACTIVE, 1, datetime(2026, 9, 7), "x", 2026
        )


def test_reconciliation_requires_timezone_aware_as_of() -> None:
    with pytest.raises(ValueError, match="as_of_utc"):
        reconcile_availability([], season=2026, as_of_utc=datetime(2026, 9, 7))


def test_fixture_manifest_records_exact_digest_and_schema() -> None:
    import json

    root = Path("tests/testdata")
    manifest = json.loads((root / "nfl_availability_manifest.json").read_text())
    assert fixture_sha256(root / manifest["fixture"]) == manifest["sha256"]
    assert manifest["schema"][:4] == [
        "nflverse_id", "source", "status", "confidence"
    ]


def test_source_id_is_canonicalized_through_identity_graph() -> None:
    from src.nfl.data.identity import build_identity_graph

    graph = build_identity_graph(
        players=[{
            "nflverse_id": "nfl-p1", "ud_player_id": "ud-p1",
            "gsis_id": "g1", "season": 2026, "slate_id": "s1",
        }], teams=[], games=[]
    )
    result = record_from_source(
        "ud", "ud-p1", graph, status=AvailabilityStatus.ACTIVE,
        confidence=1, observed_at_utc=NOW, provenance="fixture", season=2026,
        slate_id="s1",
    )
    assert result.nflverse_id == "nfl-p1"


def test_fixture_loader_is_offline_and_auditable() -> None:
    path = Path("tests/testdata/nfl_availability_fixture.json")
    rows = load_fixture(path)
    assert len(rows) == 4
    assert {row.source for row in rows} == {"ud", "sleeper", "espn", "roster_depth"}
    assert len(fixture_sha256(path)) == 64
