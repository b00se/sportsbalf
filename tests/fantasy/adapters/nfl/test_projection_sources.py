"""Offline tests for the NFL projection-source audit contract."""

from datetime import UTC, datetime, timedelta

import pytest
from src.fantasy.adapters.nfl.projection_sources import (
    ProjectionSource,
    SourceAuditError,
    audit_projection_source,
    load_projection_sources,
    run_projection_source_tournament,
)


def _source(**overrides: object) -> ProjectionSource:
    values: dict[str, object] = {
        "source_id": "fixture-consensus",
        "publisher": "Example Research",
        "access_url": "https://example.test/consensus.csv",
        "license_name": "CC BY 4.0",
        "license_url": "https://creativecommons.org/licenses/by/4.0/",
        "cost_usd": 0.0,
        "commercial_use": True,
        "redistribution_allowed": True,
        "snapshot_format": "csv",
        "version": "2026.09.01",
        "retrieval_method": "manual_download",
        "source_timestamp_utc": "2026-09-01T12:00:00Z",
        "content_sha256": "a" * 64,
        "coverage_score": 0.9,
    }
    values.update(overrides)
    return ProjectionSource(**values)


def test_audit_accepts_licensed_reproducible_free_source() -> None:
    result = audit_projection_source(
        _source(), as_of_utc=datetime(2026, 9, 2, 12, tzinfo=UTC)
    )

    assert result.eligible is True
    assert result.failures == ()
    assert result.freshness_hours == 24.0


def test_audit_rejects_missing_license_and_unreproducible_snapshot() -> None:
    source = _source(
        license_name="",
        license_url="",
        snapshot_format="",
        content_sha256="",
    )

    result = audit_projection_source(
        source, as_of_utc=datetime(2026, 9, 2, 12, tzinfo=UTC)
    )

    assert result.eligible is False
    assert "license_missing" in result.failures
    assert "snapshot_format_missing" in result.failures
    assert "content_hash_missing" in result.failures


def test_tournament_is_deterministic_and_excludes_stale_or_paid_sources() -> None:
    as_of = datetime(2026, 9, 2, 12, tzinfo=UTC)
    winner = _source(source_id="winner", coverage_score=0.8)
    stale = _source(
        source_id="stale",
        source_timestamp_utc=(as_of - timedelta(days=10)).isoformat().replace(
            "+00:00", "Z"
        ),
    )
    paid = _source(source_id="paid", cost_usd=1.0)

    tournament = run_projection_source_tournament(
        (paid, stale, winner), as_of_utc=as_of, max_age_hours=72
    )

    assert [item.source.source_id for item in tournament] == ["winner"]
    assert tournament[0].rank == 1


def test_invalid_timestamp_fails_closed() -> None:
    with pytest.raises(SourceAuditError, match="UTC"):
        audit_projection_source(
            _source(source_timestamp_utc="2026-09-01"),
            as_of_utc=datetime(2026, 9, 2, 12, tzinfo=UTC),
        )


def test_version_digest_and_boolean_types_fail_closed() -> None:
    as_of = datetime(2026, 9, 2, 12, tzinfo=UTC)
    assert "version_missing" in audit_projection_source(
        _source(version=""), as_of_utc=as_of
    ).failures
    assert "content_hash_missing" in audit_projection_source(
        _source(content_sha256="A" * 64), as_of_utc=as_of
    ).failures
    assert "commercial_use_invalid" in audit_projection_source(
        _source(commercial_use="true"), as_of_utc=as_of
    ).failures


def test_config_loader_rejects_string_booleans(tmp_path) -> None:
    path = tmp_path / "sources.yaml"
    path.write_text(
        "projection_sources:\n"
        "  - source_id: x\n"
        "    publisher: p\n"
        "    access_url: https://example.test\n"
        "    license_name: CC0\n"
        "    license_url: https://creativecommons.org/publicdomain/zero/1.0/\n"
        "    cost_usd: 0\n"
        "    commercial_use: 'true'\n"
        "    redistribution_allowed: true\n"
        "    snapshot_format: csv\n"
        "    version: v1\n"
        "    retrieval_method: local_fixture\n"
        "    source_timestamp_utc: 2026-09-01T00:00:00Z\n"
        f"    content_sha256: {'a' * 64}\n"
        "    coverage_score: 0.5\n",
        encoding="utf-8",
    )
    with pytest.raises(SourceAuditError, match="boolean"):
        load_projection_sources(path)
