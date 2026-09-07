"""Offline licensing and reproducibility audit for NFL projections.

This module records source declarations; it deliberately performs no network
access.  A caller supplies a source snapshot's metadata and can then use the
same declaration to reproduce a source tournament from archived inputs.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REPRODUCIBLE_METHODS = frozenset(
    {"manual_download", "package_snapshot", "local_fixture"}
)


class SourceAuditError(ValueError):
    """Raised when source metadata cannot be interpreted safely."""


@dataclass(frozen=True, slots=True)
class ProjectionSource:
    """Declared metadata for one free projection or consensus source."""

    source_id: str
    publisher: str
    access_url: str
    license_name: str
    license_url: str
    cost_usd: float
    commercial_use: bool
    redistribution_allowed: bool
    snapshot_format: str
    version: str
    retrieval_method: str
    source_timestamp_utc: str
    content_sha256: str
    coverage_score: float


@dataclass(frozen=True, slots=True)
class SourceAuditResult:
    """Fail-closed result for one source declaration."""

    source: ProjectionSource
    eligible: bool
    failures: tuple[str, ...]
    freshness_hours: float
    reproducibility_score: float


@dataclass(frozen=True, slots=True)
class TournamentEntry:
    """An eligible source and its deterministic tournament rank."""

    rank: int
    source: ProjectionSource
    audit: SourceAuditResult
    score: float


def _parse_timestamp(value: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise SourceAuditError(
            "source_timestamp_utc must be an ISO-8601 UTC timestamp ending in Z"
        )
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise SourceAuditError(
            "source_timestamp_utc must be an ISO-8601 UTC timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise SourceAuditError("source_timestamp_utc must be UTC")
    return parsed


def audit_projection_source(
    source: ProjectionSource,
    *,
    as_of_utc: datetime,
    max_age_hours: float = 168.0,
) -> SourceAuditResult:
    """Audit licensing, freshness, and reproducibility without fetching data.

    Args:
        source: Metadata declaration for an archived source snapshot.
        as_of_utc: UTC cutoff against which freshness is measured.
        max_age_hours: Maximum permitted snapshot age.
    """

    if as_of_utc.tzinfo is None or as_of_utc.utcoffset() != UTC.utcoffset(as_of_utc):
        raise SourceAuditError("as_of_utc must be timezone-aware UTC")
    if max_age_hours < 0:
        raise SourceAuditError("max_age_hours must be non-negative")
    timestamp = _parse_timestamp(source.source_timestamp_utc)
    age_hours = (as_of_utc - timestamp).total_seconds() / 3600
    failures: list[str] = []
    if source.cost_usd != 0:
        failures.append("not_free")
    if not source.license_name.strip() or not source.license_url.strip():
        failures.append("license_missing")
    if not source.commercial_use:
        failures.append("commercial_use_not_permitted")
    if not source.redistribution_allowed:
        failures.append("redistribution_not_permitted")
    if not source.snapshot_format.strip():
        failures.append("snapshot_format_missing")
    if source.retrieval_method not in _REPRODUCIBLE_METHODS:
        failures.append("retrieval_method_not_reproducible")
    if not _SHA256_RE.fullmatch(source.content_sha256.lower()):
        failures.append("content_hash_missing")
    if age_hours < 0:
        failures.append("timestamp_in_future")
    elif age_hours > max_age_hours:
        failures.append("stale")
    reproducibility_score = float(
        bool(source.snapshot_format.strip())
        and source.retrieval_method in _REPRODUCIBLE_METHODS
        and bool(_SHA256_RE.fullmatch(source.content_sha256.lower()))
    )
    return SourceAuditResult(
        source=source,
        eligible=not failures,
        failures=tuple(failures),
        freshness_hours=age_hours,
        reproducibility_score=reproducibility_score,
    )


def run_projection_source_tournament(
    sources: Iterable[ProjectionSource],
    *,
    as_of_utc: datetime,
    max_age_hours: float = 168.0,
) -> tuple[TournamentEntry, ...]:
    """Return eligible sources in a deterministic coverage/freshness order."""

    audits = [
        audit_projection_source(
            source, as_of_utc=as_of_utc, max_age_hours=max_age_hours
        )
        for source in sources
    ]
    eligible = [audit for audit in audits if audit.eligible]
    ranked = sorted(
        eligible,
        key=lambda audit: (
            -float(audit.source.coverage_score),
            audit.freshness_hours,
            audit.source.source_id,
        ),
    )
    return tuple(
        TournamentEntry(
            rank=index,
            source=audit.source,
            audit=audit,
            score=audit.source.coverage_score,
        )
        for index, audit in enumerate(ranked, start=1)
    )
