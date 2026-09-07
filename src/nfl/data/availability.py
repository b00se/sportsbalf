"""Deterministic NFL availability source tournament and export gate."""

from __future__ import annotations

import csv
import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path


class AvailabilityStatus(StrEnum):
    """Normalized availability state."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    QUESTIONABLE = "questionable"
    UNKNOWN = "unknown"


class AvailabilityGateError(ValueError):
    """Raised when unattended export is unsafe."""


@dataclass(frozen=True, slots=True)
class AvailabilityRecord:
    """One source observation with audit metadata."""

    nflverse_id: str
    source: str
    status: AvailabilityStatus
    confidence: float
    observed_at_utc: datetime
    provenance: str
    season: int
    game_id: str | None = None
    slate_id: str | None = None
    fresh_until_utc: datetime | None = None

    def __post_init__(self) -> None:
        if not self.nflverse_id or not self.source or not self.provenance:
            raise ValueError(
                "availability identity, source, and provenance are required"
            )
        if not 0 <= self.confidence <= 1:
            raise ValueError("confidence must be between 0 and 1")
        if self.observed_at_utc.tzinfo is None:
            raise ValueError("observed_at_utc must be timezone-aware")

    @property
    def is_fresh(self) -> bool:
        """Return whether the observation is within its declared freshness window."""
        return self.fresh_until_utc is None or datetime.now(UTC) <= self.fresh_until_utc


@dataclass(frozen=True, slots=True)
class AvailabilityDecision:
    """Reconciled state for one canonical player."""

    nflverse_id: str
    status: AvailabilityStatus
    confidence: float
    sources: tuple[str, ...]
    warnings: tuple[str, ...]
    conflict: bool


SOURCE_WEIGHTS: Mapping[str, float] = {
    "ud": 1.0,
    "sleeper": 0.9,
    "espn": 0.8,
    "roster_depth": 0.7,
}


def load_fixture(path: str | Path) -> list[AvailabilityRecord]:
    """Load a CSV or JSON fixture without network access."""
    path = Path(path)
    if path.suffix.lower() == ".json":
        rows = json.loads(path.read_text(encoding="utf-8"))
    else:
        with path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
    result: list[AvailabilityRecord] = []
    for row in rows:
        observed = datetime.fromisoformat(
            str(row["observed_at_utc"]).replace("Z", "+00:00")
        )
        fresh = row.get("fresh_until_utc")
        result.append(
            AvailabilityRecord(
                nflverse_id=str(row["nflverse_id"]),
                source=str(row["source"]),
                status=AvailabilityStatus(str(row["status"]).lower()),
                confidence=float(row["confidence"]),
                observed_at_utc=observed,
                provenance=str(row["provenance"]),
                season=int(row["season"]),
                game_id=row.get("game_id") or None,
                slate_id=row.get("slate_id") or None,
                fresh_until_utc=(
                    datetime.fromisoformat(str(fresh).replace("Z", "+00:00"))
                    if fresh
                    else None
                ),
            )
        )
    return result


def fixture_sha256(path: str | Path) -> str:
    """Return a stable hash for fixture provenance."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def reconcile_availability(
    records: Iterable[AvailabilityRecord], *, max_age_hours: float = 36
) -> dict[str, AvailabilityDecision]:
    """Tournament observations by weighted, confidence-adjusted vote."""
    now = datetime.now(UTC)
    grouped: dict[str, list[AvailabilityRecord]] = {}
    for record in records:
        age = (now - record.observed_at_utc).total_seconds() / 3600
        if age > max_age_hours or age < -1 / 60 or not record.is_fresh:
            continue
        grouped.setdefault(record.nflverse_id, []).append(record)
    decisions: dict[str, AvailabilityDecision] = {}
    for player, observations in grouped.items():
        scores: dict[AvailabilityStatus, float] = {}
        for obs in observations:
            scores[obs.status] = scores.get(obs.status, 0) + (
                SOURCE_WEIGHTS.get(obs.source, 0.5) * obs.confidence
            )
        ordered = sorted(scores.items(), key=lambda item: (-item[1], item[0].value))
        status, winning = ordered[0]
        total = sum(scores.values())
        conflict = len(scores) > 1 and (winning / total < 0.7 or len(scores) >= 3)
        warnings = ("sources disagree",) if conflict else ()
        decisions[player] = AvailabilityDecision(
            player,
            status,
            round(winning / total, 6),
            tuple(sorted({o.source for o in observations})),
            warnings,
            conflict,
        )
    return decisions


def check_unattended_availability(
    players: Iterable[str],
    decisions: Mapping[str, AvailabilityDecision],
    *,
    high_impact: Iterable[str] = (),
) -> None:
    """Fail closed for missing or conflicting high-impact availability."""
    impact = set(high_impact)
    blockers = []
    for player in players:
        decision = decisions.get(player)
        if decision is None:
            blockers.append(f"{player}: no fresh observations")
        elif player in impact and (
            decision.conflict or decision.status is AvailabilityStatus.UNKNOWN
        ):
            blockers.append(f"{player}: {decision.status} conflict={decision.conflict}")
    if blockers:
        raise AvailabilityGateError("unattended export blocked: " + "; ".join(blockers))
