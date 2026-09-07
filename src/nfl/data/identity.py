"""Offline, season-scoped identity graph for Underdog and football sources."""

# Public signatures intentionally keep their descriptive keyword arguments.
# ruff: noqa: E501

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from difflib import SequenceMatcher
from enum import StrEnum
from typing import Any


class IdentityStatus(StrEnum):
    """Outcome of an identity lookup."""

    RESOLVED = "resolved"
    UNRESOLVED = "unresolved"
    AMBIGUOUS = "ambiguous"


class IdentityIngestionError(ValueError):
    """Raised when a mapping row cannot be safely ingested."""


@dataclass(frozen=True, slots=True)
class IdentityResolution:
    """Auditable result of an identity lookup."""

    entity_type: str
    status: IdentityStatus
    ud_id: str | None = None
    gsis_id: str | None = None
    season: int | None = None
    reason: str = ""
    method: str = "identifier"
    confidence: float = 1.0
    source_ids: Mapping[str, str] | None = None


@dataclass(frozen=True, slots=True)
class MaterialPlayerReport:
    """Resolution summary used by downstream export gates."""

    total: int
    resolved: int
    unresolved: tuple[str, ...]
    ambiguous: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return not self.unresolved and not self.ambiguous


def _clean(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _season(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def normalize_name(value: Any) -> str:
    """Normalize punctuation and case for auditable name review."""
    return re.sub(r"[^a-z0-9]", "", str(value or "").casefold())


def _valid_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError as exc:
        raise IdentityIngestionError(f"invalid effective date: {value!r}") from exc


class IdentityGraph:
    """Mappings with explicit IDs, aliases, validity windows, and scopes."""

    def __init__(
        self,
        records: Mapping[str, Iterable[Mapping[str, Any]]],
        *,
        name_threshold: float = 0.95,
    ) -> None:
        if not 0.0 < name_threshold <= 1.0:
            raise IdentityIngestionError("name_threshold must be in (0, 1]")
        self.name_threshold = name_threshold
        self._rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for entity_type, rows in records.items():
            for row in rows:
                self._ingest(entity_type, row)

    def _ingest(self, entity_type: str, raw: Mapping[str, Any]) -> None:
        season = _season(raw.get("season"))
        if season is None:
            raise IdentityIngestionError("each identity row requires a positive season")
        gsis = _clean(raw.get("gsis_id"))
        provided = raw.get("source_ids")
        if provided is not None and not isinstance(provided, Mapping):
            raise IdentityIngestionError("source_ids must be a mapping")
        source_ids = dict(provided or {})
        for key in ("nflverse_id", "consensus_id", "appearance_id"):
            value = _clean(raw.get(key))
            if value:
                source_ids[key] = value
        ud_id = _clean(raw.get("ud_id") or raw.get("ud_player_id") or raw.get("ud_team_id") or raw.get("ud_game_id"))
        if ud_id:
            source_ids["ud_id"] = ud_id
        if not gsis or not source_ids:
            raise IdentityIngestionError("each identity row requires GSIS and source IDs")
        self._rows[entity_type].append(
            {
                "season": season,
                "gsis_id": gsis,
                "source_ids": {k: str(v).strip() for k, v in source_ids.items() if _clean(v)},
                "name": _clean(raw.get("name")),
                "team_gsis_id": _clean(raw.get("team_gsis_id")),
                "game_id": _clean(raw.get("game_id")),
                "week": raw.get("week"),
                "from": _valid_date(raw.get("effective_from")),
                "to": _valid_date(raw.get("effective_to")),
            }
        )

    @staticmethod
    def _in_scope(row: Mapping[str, Any], season: int, as_of: Any = None, week: Any = None) -> bool:
        if row["season"] != season:
            return False
        if week is not None and row.get("week") not in (None, week, str(week)):
            return False
        moment = _valid_date(as_of)
        return (not moment or not row["from"] or row["from"] <= moment) and (
            not moment or not row["to"] or moment <= row["to"]
        )

    def _resolve(
        self, entity_type: str, ud_id: Any, season: Any, *, name: Any = None,
        as_of: Any = None, week: Any = None, game_id: Any = None,
        allow_name_review: bool = False,
    ) -> IdentityResolution:
        clean_ud, clean_season = _clean(ud_id), _season(season)
        if clean_season is None:
            return IdentityResolution(entity_type, IdentityStatus.UNRESOLVED, clean_ud, season=clean_season, reason="valid season is required")
        rows = [r for r in self._rows.get(entity_type, []) if self._in_scope(r, clean_season, as_of, week)]
        if game_id is not None:
            rows = [r for r in rows if r.get("game_id") == _clean(game_id)]
        matches = [r for r in rows if clean_ud and clean_ud in r["source_ids"].values()]
        method, confidence = "identifier", 1.0
        if not clean_ud:
            if not _clean(name):
                return IdentityResolution(entity_type, IdentityStatus.UNRESOLVED, season=clean_season, reason="missing identifier")
            if not allow_name_review:
                return IdentityResolution(entity_type, IdentityStatus.UNRESOLVED, season=clean_season, reason="name-only lookup requires explicit review", method="name", confidence=0.0)
            target = normalize_name(name)
            scored = sorted(
                ((SequenceMatcher(None, target, normalize_name(r["name"])).ratio(), r) for r in rows if r["name"]),
                key=lambda x: x[0], reverse=True,
            )
            if not scored or scored[0][0] < self.name_threshold:
                confidence = scored[0][0] if scored else 0.0
                return IdentityResolution(entity_type, IdentityStatus.UNRESOLVED, season=clean_season, reason="name match below threshold", method="name", confidence=confidence)
            matches = [r for score, r in scored if score == scored[0][0]]
            method, confidence = "name-reviewed", scored[0][0]
        if method == "name-reviewed":
            return IdentityResolution(
                entity_type,
                IdentityStatus.AMBIGUOUS,
                season=clean_season,
                reason="name match requires human review",
                method=method,
                confidence=confidence,
            )
        if len({r["gsis_id"] for r in matches}) != 1:
            status = IdentityStatus.AMBIGUOUS if matches else IdentityStatus.UNRESOLVED
            reason = "multiple valid mappings" if matches else "no season-scoped mapping"
            return IdentityResolution(entity_type, status, clean_ud, season=clean_season, reason=reason, method=method, confidence=confidence)
        row = matches[0]
        return IdentityResolution(entity_type, IdentityStatus.RESOLVED, clean_ud, row["gsis_id"], clean_season, method=method, confidence=confidence, source_ids=row["source_ids"])

    def resolve_player(self, ud_id: Any = None, season: Any = None, *, name: Any = None, as_of: Any = None, week: Any = None, game_id: Any = None, allow_name_review: bool = False) -> IdentityResolution:
        """Resolve a player by explicit source ID or auditable name review."""
        return self._resolve("player", ud_id, season, name=name, as_of=as_of, week=week, game_id=game_id, allow_name_review=allow_name_review)

    def resolve_appearance(self, appearance_id: Any, season: Any) -> IdentityResolution:
        """Resolve an Underdog appearance ID to its player GSIS identity."""
        return self._resolve("player", appearance_id, season)

    def resolve_team(self, ud_id: Any = None, season: Any = None, *, as_of: Any = None) -> IdentityResolution:
        """Resolve a team by identifier and optional date."""
        return self._resolve("team", ud_id, season, as_of=as_of)

    def resolve_game(self, ud_id: Any = None, season: Any = None) -> IdentityResolution:
        """Resolve a game by identifier and season."""
        return self._resolve("game", ud_id, season)

    def check_material_players(self, players: Iterable[Any], season: Any, *, unattended: bool = False) -> MaterialPlayerReport:
        """Check material players and optionally block unattended export."""
        unresolved, ambiguous = [], []
        total = resolved = 0
        for item in players:
            if isinstance(item, Mapping):
                ud_id = next(
                    (item.get(key) for key in (
                        "ud_id", "ud_player_id", "appearance_id", "nflverse_id", "consensus_id", "id", "playerId"
                    ) if _clean(item.get(key))), None
                )
            else:
                ud_id = item
            key = _clean(ud_id) or "<missing>"
            total += 1
            outcome = self.resolve_player(ud_id, season)
            if outcome.status is IdentityStatus.RESOLVED:
                resolved += 1
            elif outcome.status is IdentityStatus.AMBIGUOUS:
                ambiguous.append(key)
            else:
                unresolved.append(key)
        report = MaterialPlayerReport(total, resolved, tuple(unresolved), tuple(ambiguous))
        if unattended and not report.ready:
            raise IdentityIngestionError(f"unattended export blocked: {report}")
        return report


def build_identity_graph(
    players: Iterable[Mapping[str, Any]], teams: Iterable[Mapping[str, Any]],
    games: Iterable[Mapping[str, Any]], *, appearances: Iterable[Mapping[str, Any]] = (),
    name_threshold: float = 0.95,
) -> IdentityGraph:
    """Build a graph from explicit player, team, game, and appearance rows."""
    return IdentityGraph(
        {"player": list(players) + list(appearances), "team": teams, "game": games},
        name_threshold=name_threshold,
    )
