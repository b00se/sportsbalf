"""Offline licensing and reproducibility audit for NFL projections.

This module records source declarations; it deliberately performs no network
access.  A caller supplies a source snapshot's metadata and can then use the
same declaration to reproduce a source tournament from archived inputs.
"""

from __future__ import annotations

import csv
import hashlib
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

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
    max_age_hours: float = 168.0
    baseline_role: str = "candidate"
    snapshot_path: str = ""
    snapshot_schema: tuple[str, ...] = ()
    policy: str = "noncommercial"

    @property
    def lineage(self) -> str:
        """Return normalized publisher/domain lineage for independence checks."""

        host = self.access_url.split("//", 1)[-1].split("/", 1)[0].lower()
        domain = re.sub(r"^www\.", "", host)
        registrable = ".".join(domain.split(".")[-2:])
        publisher = re.sub(r"[^a-z0-9]+", "", self.publisher.lower())
        return f"{publisher}:{registrable}"


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


@dataclass(frozen=True, slots=True)
class EvaluationResult:
    """Rolling-origin error summary for an archived projection snapshot."""

    rows: int
    mean_absolute_error: float
    coverage: float
    historical_mean_mae: float = 0.0
    public_baseline_mae: float = 0.0
    consensus_baseline_mae: float = 0.0


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
    commercial_mode: bool = False,
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
    if source.policy != "noncommercial":
        failures.append("policy_not_approved")
    if commercial_mode:
        failures.append("commercial_mode_not_approved")
    if not source.snapshot_path:
        failures.append("snapshot_missing")
    else:
        snapshot = Path(source.snapshot_path)
        if not snapshot.is_file():
            failures.append("snapshot_missing")
        else:
            digest = hashlib.sha256(snapshot.read_bytes()).hexdigest()
            if digest != source.content_sha256:
                failures.append("snapshot_hash_mismatch")
            if source.snapshot_schema:
                header = snapshot.read_text(encoding="utf-8").splitlines()[0].split(",")
                if tuple(header) != source.snapshot_schema:
                    failures.append("snapshot_schema_mismatch")
    if not source.version.strip():
        failures.append("version_missing")
    if not isinstance(source.cost_usd, (int, float)) or isinstance(
        source.cost_usd, bool
    ):
        failures.append("cost_invalid")
    elif source.cost_usd != 0:
        failures.append("not_free")
    if not source.license_name.strip() or not source.license_url.strip():
        failures.append("license_missing")
    if not isinstance(source.commercial_use, bool):
        failures.append("commercial_use_invalid")
    elif commercial_mode and not source.commercial_use:
        failures.append("commercial_use_not_permitted")
    if not isinstance(source.redistribution_allowed, bool):
        failures.append("redistribution_allowed_invalid")
    elif commercial_mode and not source.redistribution_allowed:
        failures.append("redistribution_not_permitted")
    if not source.snapshot_format.strip():
        failures.append("snapshot_format_missing")
    if source.retrieval_method not in _REPRODUCIBLE_METHODS:
        failures.append("retrieval_method_not_reproducible")
    if not _SHA256_RE.fullmatch(source.content_sha256):
        failures.append("content_hash_missing")
    if age_hours < 0:
        failures.append("timestamp_in_future")
    elif age_hours > max_age_hours:
        failures.append("stale")
    reproducibility_score = float(
        bool(source.snapshot_format.strip())
        and source.retrieval_method in _REPRODUCIBLE_METHODS
        and bool(_SHA256_RE.fullmatch(source.content_sha256))
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
    commercial_mode: bool = False,
) -> tuple[TournamentEntry, ...]:
    """Return eligible sources in a deterministic coverage/freshness order."""

    if commercial_mode:
        raise SourceAuditError("commercial mode is not approved for R2.4")
    audits = [
        audit_projection_source(
            source,
            as_of_utc=as_of_utc,
            max_age_hours=min(max_age_hours, source.max_age_hours),
            commercial_mode=commercial_mode,
        )
        for source in sources
    ]
    eligible = [
        audit
        for audit in audits
        if audit.eligible
        and audit.source.baseline_role in {"public_projection", "consensus_reference"}
    ]
    roles = {audit.source.baseline_role for audit in eligible}
    lineages = {audit.source.lineage for audit in eligible}
    if roles != {"public_projection", "consensus_reference"} or len(lineages) < 2:
        raise SourceAuditError(
            "at least two independent eligible baseline roles are required"
        )
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


def _required_text(raw: dict[str, Any], key: str, index: int) -> str:
    value = raw.get(key)
    if isinstance(value, datetime):
        value = value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if not isinstance(value, str) or not value.strip():
        raise SourceAuditError(
            f"projection_sources[{index}].{key} must be non-empty text"
        )
    return value.strip()


def _required_bool(raw: dict[str, Any], key: str, index: int) -> bool:
    value = raw.get(key)
    if not isinstance(value, bool):
        raise SourceAuditError(f"projection_sources[{index}].{key} must be a boolean")
    return value


def load_projection_sources(path: str | Path) -> tuple[ProjectionSource, ...]:
    """Load and type-check a projection-source registry from YAML."""

    try:
        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise SourceAuditError(
            f"Unable to load projection source config: {exc}"
        ) from exc
    registry_path = Path(path).resolve()
    rows = payload.get("projection_sources") if isinstance(payload, dict) else None
    if not isinstance(rows, list) or not rows:
        raise SourceAuditError("projection_sources must be a non-empty list")
    parsed: list[ProjectionSource] = []
    for index, raw in enumerate(rows):
        if not isinstance(raw, dict):
            raise SourceAuditError(f"projection_sources[{index}] must be a mapping")
        numeric: dict[str, float] = {}
        for key in ("cost_usd", "coverage_score", "max_age_hours"):
            value = raw.get(key)
            if key == "max_age_hours" and value is None:
                numeric[key] = 168.0
                continue
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise SourceAuditError(
                    f"projection_sources[{index}].{key} must be numeric"
                )
            numeric[key] = float(value)
        if not 0 <= numeric["coverage_score"] <= 1:
            raise SourceAuditError(
                f"projection_sources[{index}].coverage_score must be 0..1"
            )
        digest = _required_text(raw, "content_sha256", index)
        if not _SHA256_RE.fullmatch(digest):
            raise SourceAuditError(
                f"projection_sources[{index}].content_sha256 must be lowercase SHA-256"
            )
        parsed.append(
            ProjectionSource(
                source_id=_required_text(raw, "source_id", index),
                publisher=_required_text(raw, "publisher", index),
                access_url=_required_text(raw, "access_url", index),
                license_name=_required_text(raw, "license_name", index),
                license_url=_required_text(raw, "license_url", index),
                cost_usd=numeric["cost_usd"],
                commercial_use=_required_bool(raw, "commercial_use", index),
                redistribution_allowed=_required_bool(
                    raw, "redistribution_allowed", index
                ),
                snapshot_format=_required_text(raw, "snapshot_format", index),
                version=_required_text(raw, "version", index),
                retrieval_method=_required_text(raw, "retrieval_method", index),
                source_timestamp_utc=_required_text(raw, "source_timestamp_utc", index),
                content_sha256=digest,
                coverage_score=numeric["coverage_score"],
                max_age_hours=numeric["max_age_hours"],
                baseline_role=str(raw.get("baseline_role", "candidate")),
                snapshot_path=str(
                    (
                        registry_path.parent
                        / _required_text(raw, "snapshot_path", index)
                    ).resolve()
                ),
                snapshot_schema=tuple(
                    _required_text(raw, "snapshot_schema", index).split(",")
                ),
                policy=_required_text(raw, "policy", index),
            )
        )
    return tuple(parsed)


def score_rolling_origin_snapshot(
    path: str | Path,
    *,
    public_path: str | Path | None = None,
    consensus_path: str | Path | None = None,
    cutoff_utc: datetime | None = None,
    material_player_ids: Iterable[str] | None = None,
) -> EvaluationResult:
    """Score archived rows with projection, actual, and as-of columns."""

    def read_rows(source_path: str | Path) -> list[dict[str, str]]:
        try:
            with Path(source_path).open(newline="", encoding="utf-8") as handle:
                return list(csv.DictReader(handle))
        except (OSError, UnicodeError, csv.Error) as exc:
            raise SourceAuditError(
                f"unable to read evaluation snapshot: {exc}"
            ) from exc

    rows = read_rows(path)
    public_rows = read_rows(public_path or path)
    consensus_rows = read_rows(consensus_path or path)
    required = {
        "player_id",
        "game_id",
        "season",
        "week",
        "projection",
        "actual",
        "as_of_utc",
        "target_cutoff_utc",
    }
    required |= {"historical_player_mean", "public_projection", "consensus_projection"}
    if not rows or not required.issubset(rows[0]):
        raise SourceAuditError("evaluation snapshot is missing required columns")
    if cutoff_utc is not None:
        if cutoff_utc.tzinfo is None or cutoff_utc.utcoffset() != UTC.utcoffset(
            cutoff_utc
        ):
            raise SourceAuditError("cutoff_utc must be timezone-aware UTC")
        rows = [row for row in rows if _parse_timestamp(row["as_of_utc"]) <= cutoff_utc]
        if not rows:
            raise SourceAuditError(
                "evaluation snapshot has no rows at or before cutoff"
            )

    def numeric(row: dict[str, str], key: str) -> float:
        try:
            return float(row[key])
        except (KeyError, TypeError, ValueError) as exc:
            raise SourceAuditError(f"evaluation field '{key}' must be numeric") from exc

    def cutoff_rows(source_rows: list[dict[str, str]]) -> list[dict[str, str]]:
        if cutoff_utc is None:
            return source_rows
        return [
            row
            for row in source_rows
            if _parse_timestamp(row["as_of_utc"]) <= cutoff_utc
        ]

    public_by_key = {
        (r["player_id"], r["game_id"], r["season"], r["week"]): r
        for r in cutoff_rows(public_rows)
    }
    consensus_by_key = {
        (r["player_id"], r["game_id"], r["season"], r["week"]): r
        for r in cutoff_rows(consensus_rows)
    }
    material = set(material_player_ids or {row["player_id"] for row in rows})
    for row in rows:
        key = (row["player_id"], row["game_id"], row["season"], row["week"])
        if key not in public_by_key or key not in consensus_by_key:
            raise SourceAuditError("baseline snapshot is missing an evaluation key")
        target_cutoff = _parse_timestamp(row["target_cutoff_utc"])
        for baseline in (public_by_key[key], consensus_by_key[key]):
            if _parse_timestamp(baseline["as_of_utc"]) > target_cutoff:
                raise SourceAuditError("baseline timestamp is after target cutoff")
            numeric(baseline, "projection")
    covered_ids = {
        row["player_id"]
        for row in rows
        if row["player_id"].strip()
        and row["player_id"] in material
        and row["projection"] != ""
        and 0 <= numeric(row, "projection")
    }
    errors = [abs(numeric(row, "projection") - numeric(row, "actual")) for row in rows]
    historical, public, consensus = [], [], []
    for row in rows:
        earlier = [
            numeric(previous, "actual")
            for previous in rows
            if previous["player_id"] == row["player_id"]
            and (int(previous["season"]), int(previous["week"]))
            < (int(row["season"]), int(row["week"]))
        ]
        if not earlier:
            continue
        historical.append(abs(sum(earlier) / len(earlier) - numeric(row, "actual")))
        key = (row["player_id"], row["game_id"], row["season"], row["week"])
        public.append(
            abs(numeric(public_by_key[key], "projection") - numeric(row, "actual"))
        )
        consensus.append(
            abs(numeric(consensus_by_key[key], "projection") - numeric(row, "actual"))
        )
    if not historical:
        raise SourceAuditError("historical mean requires an earlier observation")
    return EvaluationResult(
        len(rows),
        sum(errors) / len(errors),
        min(len(covered_ids) / max(len(material), 1), 1.0),
        sum(historical) / len(rows),
        sum(public) / len(rows),
        sum(consensus) / len(rows),
    )
