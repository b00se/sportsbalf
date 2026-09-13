"""Offline, deterministic simulator for exact NFL Autopilot drafts."""

from __future__ import annotations

import math
import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from src.fantasy.adapters.nfl.contest import NflContestConfig


class DraftValidationError(ValueError):
    """Raised when a draft contract or eligible pool cannot be satisfied."""


@dataclass(frozen=True, slots=True)
class DraftPlayer:
    """A ranked, eligible player supplied to the offline simulator."""

    player_id: str
    position: str
    rank: float
    projection: float = 0.0


@dataclass(frozen=True, slots=True)
class DraftSelection:
    """One auditable pick in draft order."""

    round: int
    pick: int
    entrant: int
    player: DraftPlayer


@dataclass(frozen=True, slots=True)
class DraftRoster:
    """Completed roster, including its deterministic FLEX assignment."""

    entrant: int
    players: tuple[DraftPlayer, ...]
    by_slot: dict[str, tuple[DraftPlayer, ...]]


@dataclass(frozen=True, slots=True)
class AutopilotDraftResult:
    """Draft output and safe metadata; no account or upload information."""

    selections: tuple[DraftSelection, ...]
    rosters: tuple[DraftRoster, ...]
    audit: dict[str, Any]


def _player(value: DraftPlayer | Mapping[str, Any]) -> DraftPlayer:
    if isinstance(value, DraftPlayer):
        item = value
    elif isinstance(value, Mapping):
        ident = value.get("player_id", value.get("id"))
        item = DraftPlayer(
            ident,
            value.get("position"),
            value.get("rank"),
            value.get("projection", 0.0),
        )
    else:
        raise DraftValidationError("each pool item must be DraftPlayer or a mapping")
    if not isinstance(item.player_id, str) or not item.player_id.strip():
        raise DraftValidationError("player_id must be a non-empty string")
    if item.position not in {"QB", "RB", "WR", "TE"}:
        raise DraftValidationError(f"unsupported player position: {item.position!r}")
    for name, number in (("rank", item.rank), ("projection", item.projection)):
        if (
            isinstance(number, bool)
            or not isinstance(number, (int, float))
            or not math.isfinite(number)
        ):
            raise DraftValidationError(f"player {name} must be finite")
    return DraftPlayer(
        item.player_id.strip(),
        item.position.upper(),
        float(item.rank),
        float(item.projection),
    )


def _legal(players: Sequence[DraftPlayer], config: NflContestConfig) -> bool:
    counts = {
        pos: sum(p.position == pos for p in players) for pos in ("QB", "RB", "WR", "TE")
    }
    roster = config.roster
    caps = config.position_caps
    if len(players) > 6 or any(counts[p] > getattr(caps, p.lower()) for p in counts):
        return False
    if (
        counts["QB"] < roster.qb
        or counts["RB"] < roster.rb
        or counts["WR"] < roster.wr
        or counts["TE"] < roster.te
    ):
        return False
    extras = sum(
        max(0, counts[p] - getattr(roster, p.lower())) for p in ("RB", "WR", "TE")
    )
    return extras == roster.flex and all(
        p in config.roster.flex_positions or counts[p] == getattr(roster, p.lower())
        for p in ("RB", "WR", "TE")
    )


def _partial_possible(players: Sequence[DraftPlayer], config: NflContestConfig) -> bool:
    """Return whether this partial roster has enough slots for a legal finish."""
    if len(players) > 6:
        return False
    roster = config.roster
    counts = {
        pos: sum(p.position == pos for p in players) for pos in ("QB", "RB", "WR", "TE")
    }
    if any(counts[p] > getattr(config.position_caps, p.lower()) for p in counts):
        return False
    if any(
        counts[p]
        > getattr(roster, p.lower()) + (1 if p in roster.flex_positions else 0)
        for p in ("RB", "WR", "TE")
    ):
        return False
    deficits = sum(
        max(0, getattr(roster, p.lower()) - counts[p]) for p in ("QB", "RB", "WR", "TE")
    )
    extras = sum(
        max(0, counts[p] - getattr(roster, p.lower())) for p in ("RB", "WR", "TE")
    )
    return deficits + max(0, roster.flex - extras) <= 6 - len(players)


def _roster(
    entrant: int, players: tuple[DraftPlayer, ...], config: NflContestConfig
) -> DraftRoster:
    counts = {
        pos: [p for p in players if p.position == pos]
        for pos in ("QB", "RB", "WR", "TE")
    }
    extras = [
        p
        for pos in config.roster.flex_positions
        for p in counts[pos][getattr(config.roster, pos.lower()) :]
    ]
    if not _legal(players, config) or len(extras) != 1:
        raise DraftValidationError(f"entrant {entrant} roster is incomplete or illegal")
    flex = extras[0]
    by_slot = {
        "QB": tuple(counts["QB"][: config.roster.qb]),
        "RB": tuple(counts["RB"][: config.roster.rb]),
        "WR": tuple(counts["WR"][: config.roster.wr]),
        "TE": tuple(counts["TE"][: config.roster.te]),
        "FLEX": (flex,),
    }
    return DraftRoster(entrant, players, by_slot)


def _globally_possible(
    rosters: Sequence[Sequence[DraftPlayer]],
    players: Sequence[DraftPlayer],
    drafted: set[str],
    config: NflContestConfig,
) -> bool:
    """Check aggregate position supply can finish every partial roster."""
    positions = ("QB", "RB", "WR", "TE")
    required = {pos: 0 for pos in positions}
    flex_needed = 0
    for roster in rosters:
        counts = {pos: sum(p.position == pos for p in roster) for pos in positions}
        for pos in positions:
            required[pos] += max(0, getattr(config.roster, pos.lower()) - counts[pos])
        extras = sum(
            max(0, counts[pos] - getattr(config.roster, pos.lower()))
            for pos in config.roster.flex_positions
        )
        flex_needed += max(0, config.roster.flex - extras)
    available = {
        pos: sum(p.position == pos and p.player_id not in drafted for p in players)
        for pos in positions
    }
    if any(available[pos] < required[pos] for pos in positions):
        return False
    flex_supply = sum(
        max(0, available[pos] - required[pos]) for pos in config.roster.flex_positions
    )
    return flex_supply >= flex_needed


def simulate_autopilot_draft(
    config: NflContestConfig,
    eligible_players: Sequence[DraftPlayer | Mapping[str, Any]],
    *,
    seed: int = 0,
) -> AutopilotDraftResult:
    """Simulate six rounds in explicit serpentine order (entrants four or six)."""
    if config.draft.entrants not in (4, 6) or config.draft.rounds != 6:
        raise DraftValidationError(
            "Autopilot supports exactly 4 or 6 entrants and 6 rounds"
        )
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise DraftValidationError("seed must be an integer")
    players = tuple(_player(p) for p in eligible_players)
    if len({p.player_id for p in players}) != len(players):
        raise DraftValidationError("eligible player IDs must be unique")
    if len(players) < config.draft.entrants * 6:
        raise DraftValidationError("eligible pool is too small for the draft")
    rng = random.Random(seed)
    tie_order = list(players)
    rng.shuffle(tie_order)
    tie_key = {p.player_id: i for i, p in enumerate(tie_order)}
    rosters: list[list[DraftPlayer]] = [[] for _ in range(config.draft.entrants)]
    selections: list[DraftSelection] = []
    drafted: set[str] = set()
    for round_number in range(1, 7):
        order = (
            range(config.draft.entrants)
            if round_number % 2
            else range(config.draft.entrants - 1, -1, -1)
        )
        for entrant in order:
            current = rosters[entrant]
            candidates = []
            for candidate in players:
                if candidate.player_id in drafted:
                    continue
                proposed = current + [candidate]
                if _partial_possible(proposed, config):
                    counts = {
                        p: sum(x.position == p for x in current)
                        for p in ("QB", "RB", "WR", "TE")
                    }
                    roster_limit = getattr(
                        config.roster, candidate.position.lower()
                    ) + (1 if candidate.position in config.roster.flex_positions else 0)
                    if counts[candidate.position] < min(
                        getattr(config.position_caps, candidate.position.lower()),
                        roster_limit,
                    ) and _globally_possible(
                        [
                            proposed if i == entrant else roster
                            for i, roster in enumerate(rosters)
                        ],
                        players,
                        drafted | {candidate.player_id},
                        config,
                    ):
                        candidates.append(candidate)
            if not candidates:
                raise DraftValidationError(
                    f"no eligible player for entrant {entrant} pick"
                )
            choice = min(
                candidates, key=lambda p: (p.rank, -p.projection, tie_key[p.player_id])
            )
            current.append(choice)
            drafted.add(choice.player_id)
            selections.append(
                DraftSelection(round_number, len(selections) + 1, entrant, choice)
            )
    completed = tuple(
        _roster(i, tuple(players_), config) for i, players_ in enumerate(rosters)
    )
    return AutopilotDraftResult(
        tuple(selections),
        completed,
        {
            "engine": "offline_autopilot",
            "seed": seed,
            "entrants": config.draft.entrants,
            "rounds": 6,
            "draft_order": tuple(s.entrant for s in selections),
            "automated_actions": False,
        },
    )


__all__ = [
    "AutopilotDraftResult",
    "DraftPlayer",
    "DraftRoster",
    "DraftSelection",
    "DraftValidationError",
    "simulate_autopilot_draft",
]
