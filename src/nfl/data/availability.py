"""Deterministic NFL availability source tournament and export gate."""

from __future__ import annotations

import csv
import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.nfl.data.identity import IdentityGraph


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
        if self.source not in ALLOWED_SOURCES:
            raise ValueError(f"unknown availability source: {self.source!r}")
        if not 0 <= self.confidence <= 1:
            raise ValueError("confidence must be between 0 and 1")
        if self.observed_at_utc.tzinfo is None:
            raise ValueError("observed_at_utc must be timezone-aware")
        if self.fresh_until_utc is not None and self.fresh_until_utc.tzinfo is None:
            raise ValueError("fresh_until_utc must be timezone-aware")

    def is_fresh(self, as_of_utc: datetime) -> bool:
        """Return whether the observation is within its declared freshness window."""
        return self.fresh_until_utc is None or as_of_utc <= self.fresh_until_utc


@dataclass(frozen=True, slots=True)
class AvailabilityDecision:
    """Reconciled state for one canonical player."""

    nflverse_id: str
    status: AvailabilityStatus
    confidence: float
    sources: tuple[str, ...]
    warnings: tuple[str, ...]
    conflict: bool
    season: int | None = None
    as_of_utc: datetime | None = None


SOURCE_WEIGHTS: Mapping[str, float] = {
    "ud": 1.0,
    "sleeper": 0.9,
    "espn": 0.8,
    "roster_depth": 0.7,
}
ALLOWED_SOURCES = frozenset(SOURCE_WEIGHTS)


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


def record_from_source(
    source: str,
    source_id: str,
    identity_graph: IdentityGraph,
    *,
    status: AvailabilityStatus,
    confidence: float,
    observed_at_utc: datetime,
    provenance: str,
    season: int,
    game_id: str | None = None,
    slate_id: str | None = None,
    fresh_until_utc: datetime | None = None,
) -> AvailabilityRecord:
    """Canonicalize a source ID through the accepted identity graph."""
    source_key = "ud_player_id" if source == "ud" else f"{source}_id"
    outcome = identity_graph.resolve_player(
        source_id,
        season,
        source=source_key,
        game_id=game_id,
        slate_id=slate_id,
    )
    if outcome.status.value != "resolved" or not outcome.nflverse_id:
        raise AvailabilityGateError(
            f"availability identity unresolved for {source}:{source_id}"
        )
    return AvailabilityRecord(
        outcome.nflverse_id,
        source,
        status,
        confidence,
        observed_at_utc,
        provenance,
        season,
        game_id,
        slate_id,
        fresh_until_utc,
    )


def reconcile_availability(
    records: Iterable[AvailabilityRecord],
    *,
    season: int,
    as_of_utc: datetime,
    max_age_hours: float = 36,
) -> dict[str, AvailabilityDecision]:
    """Tournament observations by weighted, confidence-adjusted vote."""
    if as_of_utc.tzinfo is None:
        raise ValueError("as_of_utc must be timezone-aware")
    grouped: dict[str, list[AvailabilityRecord]] = {}
    for record in records:
        if record.season != season:
            continue
        age = (as_of_utc - record.observed_at_utc).total_seconds() / 3600
        if age > max_age_hours or age < -1 / 60 or not record.is_fresh(as_of_utc):
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
            season,
            as_of_utc,
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
