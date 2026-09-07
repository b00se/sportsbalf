"""Explicit NFL identity graph with scoped external references."""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from numbers import Integral
from typing import Any


class IdentityStatus(StrEnum):
    """Identity lookup status."""

    RESOLVED = "resolved"
    UNRESOLVED = "unresolved"
    AMBIGUOUS = "ambiguous"


class IdentityIngestionError(ValueError):
    """Unsafe identity input."""


@dataclass(frozen=True, slots=True)
class IdentityResolution:
    """Auditable identity lookup result."""

    entity_type: str
    status: IdentityStatus
    ud_id: str | None = None
    gsis_id: str | None = None
    season: int | None = None
    reason: str = ""
    method: str = "identifier"
    confidence: float = 1.0
    source_ids: Mapping[str, str] | None = None
    nflverse_id: str | None = None
    team_gsis_id: str | None = None


@dataclass(frozen=True, slots=True)
class MaterialPlayerReport:
    """Summary used by an export gate."""

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


def _valid_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError as exc:
        raise IdentityIngestionError(f"invalid effective date: {value!r}") from exc


def _intervals_overlap(
    start_a: date | None, end_a: date | None,
    start_b: date | None, end_b: date | None,
) -> bool:
    """Return whether two inclusive validity intervals overlap."""
    return (end_a is None or start_b is None or end_a >= start_b) and (
        end_b is None or start_a is None or end_b >= start_a
    )


def _week(value: Any) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool) or isinstance(value, float):
        raise IdentityIngestionError(f"invalid week: {value!r}")
    if isinstance(value, Integral):
        parsed = int(value)
    elif isinstance(value, str) and re.fullmatch(r"[+-]?\d+(?:\.0+)?", value.strip()):
        parsed = int(float(value.strip()))
    else:
        raise IdentityIngestionError(f"invalid week: {value!r}")
    if not 1 <= parsed <= 18:
        raise IdentityIngestionError(f"invalid week: {value!r}")
    return parsed


def normalize_name(value: Any) -> str:
    """Normalize a name for review candidates, never automatic matching."""
    return re.sub(r"[^a-z0-9]", "", str(value or "").casefold())


class IdentityGraph:
    """Canonical players and scoped references for teams, games, and slates."""

    def __init__(
        self,
        records: Mapping[str, Iterable[Mapping[str, Any]]],
        *,
        name_threshold: float = 0.95,
    ) -> None:
        del name_threshold
        self._rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for entity, rows in records.items():
            for row in rows:
                self._ingest(entity, row)

    def _ingest(self, entity: str, raw: Mapping[str, Any]) -> None:
        season = _season(raw.get("season"))
        if season is None:
            raise IdentityIngestionError("each identity row requires a positive season")
        canonical = _clean(raw.get("nflverse_id"))
        if entity != "player":
            canonical = canonical or _clean(raw.get("gsis_id"))
        if not canonical:
            raise IdentityIngestionError(
                "player identity rows require nflverse_id"
                if entity == "player"
                else "identity rows require a canonical identifier"
            )
        source_ids = {
            str(k): str(v).strip()
            for k, v in dict(raw.get("source_ids") or {}).items()
            if _clean(v)
        }
        for key in (
            "ud_id",
            "ud_player_id",
            "ud_team_id",
            "ud_game_id",
            "appearance_id",
            "consensus_id",
            "nflverse_id",
            "gsis_id",
        ):
            value = _clean(raw.get(key))
            if value:
                source_ids[key] = value
        record = {
                "season": season,
                "canonical": canonical,
                "gsis_id": _clean(raw.get("gsis_id")),
                "source_ids": source_ids,
                "name": _clean(raw.get("name")),
                "game_id": _clean(raw.get("game_id")),
                "slate_id": _clean(raw.get("slate_id")),
                "week": _week(raw.get("week")),
                "from": _valid_date(raw.get("effective_from")),
                "to": _valid_date(raw.get("effective_to")),
                "team_gsis_id": _clean(raw.get("team_gsis_id")),
            }
        for prior in self._rows[entity]:
            if prior["season"] != record["season"]:
                continue
            if not set(source_ids.items()).intersection(prior["source_ids"].items()):
                continue
            if any(
                record.get(key) != prior.get(key)
                for key in ("game_id", "slate_id", "week")
            ):
                continue
            if _intervals_overlap(
                prior["from"], prior["to"], record["from"], record["to"]
            ):
                raise IdentityIngestionError("overlapping identity validity intervals")
        self._rows[entity].append(record)

    @staticmethod
    def _in_scope(
        row: Mapping[str, Any],
        season: int,
        *,
        as_of: Any = None,
        week: Any = None,
        game_id: Any = None,
        slate_id: Any = None,
    ) -> bool:
        if row["season"] != season or (
            week is not None and row.get("week") not in (None, week, str(week))
        ):
            return False
        if game_id is not None and row.get("game_id") != _clean(game_id):
            return False
        if slate_id is not None and row.get("slate_id") != _clean(slate_id):
            return False
        moment = _valid_date(as_of)
        return (not moment or not row["from"] or row["from"] <= moment) and (
            not moment or not row["to"] or moment <= row["to"]
        )

    def _resolve(
        self,
        entity: str,
        reference: Any,
        season: Any,
        *,
        source: str | None = None,
        name: Any = None,
        as_of: Any = None,
        week: Any = None,
        game_id: Any = None,
        slate_id: Any = None,
        allow_name_review: bool = False,
    ) -> IdentityResolution:
        ref, year = _clean(reference), _season(season)
        if year is None:
            return IdentityResolution(
                entity,
                IdentityStatus.UNRESOLVED,
                ref,
                season=year,
                reason="valid season is required",
            )
        canonical_lookup = source == "nflverse_id"
        scope_game = None if canonical_lookup else game_id
        scope_slate = None if canonical_lookup else slate_id
        scope_week = None if canonical_lookup else week
        scope_as_of = None if canonical_lookup else as_of
        rows = [
            r
            for r in self._rows.get(entity, [])
            if self._in_scope(
                r,
                year,
                as_of=scope_as_of,
                week=_week(scope_week),
                game_id=scope_game,
                slate_id=scope_slate,
            )
        ]
        scoped_source = source in {"appearance_id", "ud_player_id", "consensus_id"}
        if scoped_source and game_id is None and slate_id is None:
            return IdentityResolution(
                entity,
                IdentityStatus.UNRESOLVED,
                ref,
                season=year,
                reason="scoped reference requires game_id or slate_id",
            )
        if not ref and name and allow_name_review:
            return IdentityResolution(
                entity,
                IdentityStatus.AMBIGUOUS,
                season=year,
                reason="name match requires human review",
                method="name-reviewed",
                confidence=1.0,
            )
        if not ref:
            if name:
                return IdentityResolution(
                    entity,
                    IdentityStatus.UNRESOLVED,
                    season=year,
                    reason="name-only lookup requires explicit review",
                    method="name",
                    confidence=0.0,
                )
            return IdentityResolution(
                entity,
                IdentityStatus.UNRESOLVED,
                season=year,
                reason="identifier is required",
            )
        matches = [
            r
            for r in rows
            if (source and r["source_ids"].get(source) == ref)
            or (not source and ref in r["source_ids"].values())
        ]
        if not source and ref:
            scoped_matches = [
                r
                for r in matches
                if any(
                    r["source_ids"].get(k) == ref
                    for k in ("appearance_id", "ud_player_id", "consensus_id")
                )
            ]
            if scoped_matches and game_id is None and slate_id is None:
                return IdentityResolution(
                    entity,
                    IdentityStatus.UNRESOLVED,
                    ref,
                    season=year,
                    reason="scoped reference requires game_id or slate_id",
                )
        canonical = {r["canonical"] for r in matches}
        if not matches:
            return IdentityResolution(
                entity,
                IdentityStatus.UNRESOLVED,
                ref,
                season=year,
                reason="no season/scoped mapping",
            )
        if len(canonical) != 1:
            return IdentityResolution(
                entity,
                IdentityStatus.AMBIGUOUS,
                ref,
                season=year,
                reason="multiple valid scoped mappings",
            )
        if len({r.get("team_gsis_id") for r in matches}) > 1:
            return IdentityResolution(
                entity,
                IdentityStatus.AMBIGUOUS,
                ref,
                season=year,
                reason="overlapping team context mappings",
            )
        row = matches[0]
        return IdentityResolution(
            entity,
            IdentityStatus.RESOLVED,
            ref,
            row["gsis_id"],
            year,
            source_ids=row["source_ids"],
            nflverse_id=row["canonical"],
            team_gsis_id=row["team_gsis_id"],
        )

    def resolve_player(
        self,
        ud_id: Any = None,
        season: Any = None,
        *,
        source: str | None = None,
        name: Any = None,
        as_of: Any = None,
        week: Any = None,
        game_id: Any = None,
        slate_id: Any = None,
        allow_name_review: bool = False,
    ) -> IdentityResolution:
        """Resolve a player reference in explicit game/slate/time scope."""
        return self._resolve(
            "player",
            ud_id,
            season,
            source=source,
            name=name,
            as_of=as_of,
            week=week,
            game_id=game_id,
            slate_id=slate_id,
            allow_name_review=allow_name_review,
        )

    def resolve_appearance(
        self,
        appearance_id: Any,
        season: Any,
        *,
        game_id: Any = None,
        slate_id: Any = None,
    ) -> IdentityResolution:
        """Resolve an appearance ID, which is not a permanent player ID."""
        return self.resolve_player(
            appearance_id,
            season,
            source="appearance_id",
            game_id=game_id,
            slate_id=slate_id,
        )

    def resolve_team(
        self, ud_id: Any = None, season: Any = None, *, as_of: Any = None
    ) -> IdentityResolution:
        """Resolve a separately modeled team reference."""
        return self._resolve("team", ud_id, season, as_of=as_of)

    def resolve_game(self, ud_id: Any = None, season: Any = None) -> IdentityResolution:
        """Resolve a separately modeled game reference."""
        return self._resolve("game", ud_id, season)

    def check_material_players(
        self, players: Iterable[Any], season: Any, *, unattended: bool = False
    ) -> MaterialPlayerReport:
        """Resolve material rows and optionally fail closed."""
        unresolved: list[str] = []
        ambiguous: list[str] = []
        total = resolved = 0
        for item in players:
            context = item if isinstance(item, Mapping) else {}
            aliases = (
                ("nflverse_id", "nflverse_id"),
                ("ud_player_id", "ud_player_id"),
                ("appearance_id", "appearance_id"),
                ("consensus_id", "consensus_id"),
                ("id", None),
                ("playerId", None),
                ("ud_id", "ud_id"),
            )
            supplied = [
                (key, _clean(context.get(key)), source)
                for key, source in aliases
                if _clean(context.get(key))
            ]
            reference = supplied[0][1] if supplied else (item if not context else None)
            key = _clean(reference) or "<missing>"
            outcomes = []
            for alias, value, source in supplied:
                outcomes.append(
                    self.resolve_player(
                        value,
                        season,
                        source=source,
                        game_id=context.get("game_id"),
                        slate_id=context.get("slate_id"),
                        as_of=context.get("as_of"),
                        week=context.get("week"),
                    )
                )
            result = outcomes[0] if outcomes else self.resolve_player(reference, season)
            canonical = {
                out.nflverse_id
                for out in outcomes
                if out.status is IdentityStatus.RESOLVED
            }
            if len(canonical) > 1:
                result = IdentityResolution(
                    "player",
                    IdentityStatus.AMBIGUOUS,
                    reference,
                    season=_season(season),
                    reason="conflicting player aliases",
                )
            elif any(
                out.status is not IdentityStatus.RESOLVED
                and supplied[index][2] in {"appearance_id", "ud_player_id"}
                for index, out in enumerate(outcomes)
            ):
                result = next(
                    out
                    for index, out in enumerate(outcomes)
                    if out.status is not IdentityStatus.RESOLVED
                    and supplied[index][2] in {"appearance_id", "ud_player_id"}
                )
            total += 1
            if result.status is IdentityStatus.RESOLVED:
                resolved += 1
            elif result.status is IdentityStatus.AMBIGUOUS:
                ambiguous.append(key)
            else:
                unresolved.append(key)
        report = MaterialPlayerReport(
            total, resolved, tuple(unresolved), tuple(ambiguous)
        )
        if unattended and not report.ready:
            raise IdentityIngestionError(f"unattended export blocked: {report}")
        return report


def build_identity_graph(
    players: Iterable[Mapping[str, Any]],
    teams: Iterable[Mapping[str, Any]],
    games: Iterable[Mapping[str, Any]],
    *,
    appearances: Iterable[Mapping[str, Any]] = (),
    name_threshold: float = 0.95,
) -> IdentityGraph:
    """Build a graph from canonical players and scoped references."""
    return IdentityGraph(
        {"player": list(players) + list(appearances), "team": teams, "game": games},
        name_threshold=name_threshold,
    )
