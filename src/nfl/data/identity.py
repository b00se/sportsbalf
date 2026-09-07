"""Deterministic Underdog-to-GSIS identity mappings.

The graph deliberately resolves by provider identifiers only.  A display name
is useful for diagnostics, but is never sufficient evidence for an automatic
identity match because names can be shared, changed, or misspelled.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class IdentityStatus(StrEnum):
    """Outcome of an identity lookup."""

    RESOLVED = "resolved"
    UNRESOLVED = "unresolved"
    AMBIGUOUS = "ambiguous"


@dataclass(frozen=True, slots=True)
class IdentityResolution:
    """Typed result of a player, team, or game lookup."""

    entity_type: str
    status: IdentityStatus
    ud_id: str | None = None
    gsis_id: str | None = None
    season: int | None = None
    reason: str = ""


@dataclass(frozen=True, slots=True)
class MaterialPlayerReport:
    """Resolution summary used by downstream export gates."""

    total: int
    resolved: int
    unresolved: tuple[str, ...]
    ambiguous: tuple[str, ...]

    @property
    def ready(self) -> bool:
        """Whether every material player has one unambiguous identity."""
        return not self.unresolved and not self.ambiguous


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _season(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


class IdentityGraph:
    """Season-scoped, fail-closed mappings between UD and GSIS identifiers."""

    def __init__(self, records: Mapping[str, Iterable[Mapping[str, Any]]]) -> None:
        self._ids: dict[str, dict[tuple[str, int], frozenset[str]]] = {}
        for entity_type, rows in records.items():
            candidates: defaultdict[tuple[str, int], set[str]] = defaultdict(set)
            for row in rows:
                ud_id = _clean(row.get("ud_id"))
                gsis_id = _clean(row.get("gsis_id"))
                season = _season(row.get("season"))
                if ud_id and gsis_id and season is not None:
                    candidates[(ud_id, season)].add(gsis_id)
            self._ids[entity_type] = {
                key: frozenset(values) for key, values in candidates.items()
            }

    def _resolve(
        self, entity_type: str, ud_id: Any, season: Any, *, name: Any = None
    ) -> IdentityResolution:
        clean_ud = _clean(ud_id)
        clean_season = _season(season)
        if clean_ud is None:
            reason = "name-only lookup is not safe" if _clean(name) else (
                "missing UD identifier"
            )
            return IdentityResolution(
                entity_type,
                IdentityStatus.UNRESOLVED,
                season=clean_season,
                reason=reason,
            )
        if clean_season is None:
            return IdentityResolution(
                entity_type,
                IdentityStatus.UNRESOLVED,
                ud_id=clean_ud,
                reason="valid season is required",
            )
        matches = self._ids.get(entity_type, {}).get(
            (clean_ud, clean_season), frozenset()
        )
        if len(matches) == 1:
            return IdentityResolution(
                entity_type,
                IdentityStatus.RESOLVED,
                clean_ud,
                next(iter(matches)),
                clean_season,
            )
        if len(matches) > 1:
            return IdentityResolution(
                entity_type,
                IdentityStatus.AMBIGUOUS,
                clean_ud,
                season=clean_season,
                reason="UD identifier maps to multiple GSIS identifiers",
            )
        return IdentityResolution(
            entity_type,
            IdentityStatus.UNRESOLVED,
            clean_ud,
            season=clean_season,
            reason="no season-scoped mapping",
        )

    def resolve_player(
        self, ud_id: Any = None, season: Any = None, *, name: Any = None
    ) -> IdentityResolution:
        """Resolve a player by UD identifier and season; never by name alone."""
        return self._resolve("player", ud_id, season, name=name)

    def resolve_team(self, ud_id: Any = None, season: Any = None) -> IdentityResolution:
        """Resolve a team by UD identifier and season."""
        return self._resolve("team", ud_id, season)

    def resolve_game(self, ud_id: Any = None, season: Any = None) -> IdentityResolution:
        """Resolve a game by UD match identifier and season."""
        return self._resolve("game", ud_id, season)

    def check_material_players(
        self, players: Iterable[Any], season: Any
    ) -> MaterialPlayerReport:
        """Summarize whether a material-player set passes the identity gate."""
        unresolved: list[str] = []
        ambiguous: list[str] = []
        total = resolved = 0
        for item in players:
            ud_id = item.get("ud_id") if isinstance(item, Mapping) else item
            key = _clean(ud_id) or "<missing>"
            total += 1
            outcome = self.resolve_player(ud_id=ud_id, season=season)
            if outcome.status is IdentityStatus.RESOLVED:
                resolved += 1
            elif outcome.status is IdentityStatus.AMBIGUOUS:
                ambiguous.append(key)
            else:
                unresolved.append(key)
        return MaterialPlayerReport(
            total, resolved, tuple(unresolved), tuple(ambiguous)
        )


def build_identity_graph(
    players: Iterable[Mapping[str, Any]],
    teams: Iterable[Mapping[str, Any]],
    games: Iterable[Mapping[str, Any]],
) -> IdentityGraph:
    """Build an offline graph from explicit, season-scoped mapping rows."""
    return IdentityGraph({"player": players, "team": teams, "game": games})
