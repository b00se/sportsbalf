"""Typed, fail-closed contracts for standard NFL daily contests."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal

from src.utils.io import load_config

ScoringRuleset = Literal["half_ppr", "ppr"]
Objective = Literal["expected_net_payout"]


class UnknownScoringError(ValueError):
    """Raised when a contest declares a scoring system we do not support."""


@dataclass(frozen=True, slots=True)
class ScoringConfig:
    """Fantasy scoring rules for a contest."""

    ruleset: ScoringRuleset = "half_ppr"
    points_per_reception: float = 0.5


@dataclass(frozen=True, slots=True)
class RosterConfig:
    """Number of roster slots by position."""

    qb: int = 1
    rb: int = 1
    wr: int = 2
    flex: int = 1
    te: int = 1
    flex_positions: tuple[str, ...] = ("RB", "WR", "TE")


DEFAULT_ROSTER = RosterConfig()


@dataclass(frozen=True, slots=True)
class DraftConfig:
    """Draft room size and selections per entrant."""

    entrants: int
    rounds: int


@dataclass(frozen=True, slots=True)
class FieldConfig:
    """Tournament field and entry economics."""

    size: int
    entry_fee: Decimal


@dataclass(frozen=True, slots=True)
class PositionCaps:
    """Maximum drafted players by position."""

    qb: int
    rb: int
    wr: int
    te: int


@dataclass(frozen=True, slots=True)
class Payout:
    """One rank (or rank range) payout amount."""

    rank: int
    amount: Decimal


@dataclass(frozen=True, slots=True)
class PayoutConfig:
    """Prize ladder and reconciled prize pool."""

    ladder: tuple[Payout, ...]
    prize_pool: Decimal


@dataclass(frozen=True, slots=True)
class NflContestConfig:
    """Complete validated standard-slate NFL contest contract."""

    contest_id: str
    scoring: ScoringConfig
    roster: RosterConfig
    draft: DraftConfig
    field: FieldConfig
    position_caps: PositionCaps
    payouts: PayoutConfig
    objective: Objective


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a mapping")
    return value


def _int(value: Any, name: str, *, minimum: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _money(value: Any, name: str) -> Decimal:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"{name} must be a finite amount") from None
    if (
        not amount.is_finite()
        or amount < 0
        or amount != amount.quantize(Decimal("0.01"))
    ):
        raise ValueError(f"{name} must be a non-negative amount with cents precision")
    return amount


def _check_keys(section: dict[str, Any], allowed: set[str], name: str) -> None:
    unknown = sorted(set(section) - allowed)
    if unknown:
        raise ValueError(f"Unknown {name} field(s): {', '.join(unknown)}")


def load_nfl_contest_config(path: str | Path) -> NflContestConfig:
    """Load and strictly validate an NFL contest YAML configuration."""
    raw = _mapping(load_config(str(path)), "config")
    _check_keys(
        raw,
        {
            "contest_id",
            "scoring",
            "roster",
            "draft",
            "field",
            "position_caps",
            "payouts",
            "objective",
        },
        "config",
    )
    contest_id = raw.get("contest_id", Path(path).stem)
    if not isinstance(contest_id, str) or not contest_id.strip():
        raise ValueError("contest_id must be a non-empty string")

    scoring_raw = _mapping(raw.get("scoring", {}), "scoring")
    _check_keys(scoring_raw, {"ruleset"}, "scoring")
    ruleset = scoring_raw.get("ruleset", "half_ppr")
    if ruleset not in ("half_ppr", "ppr"):
        raise UnknownScoringError(f"Unsupported scoring ruleset: {ruleset!r}")
    scoring = ScoringConfig(
        ruleset=ruleset, points_per_reception=0.5 if ruleset == "half_ppr" else 1.0
    )

    roster_raw = _mapping(raw.get("roster", {}), "roster")
    _check_keys(
        roster_raw, {"qb", "rb", "wr", "flex", "te", "flex_positions"}, "roster"
    )
    roster = RosterConfig(
        qb=_int(roster_raw.get("qb", 1), "roster.qb", minimum=0),
        rb=_int(roster_raw.get("rb", 1), "roster.rb", minimum=0),
        wr=_int(roster_raw.get("wr", 2), "roster.wr", minimum=0),
        flex=_int(roster_raw.get("flex", 1), "roster.flex", minimum=0),
        te=_int(roster_raw.get("te", 1), "roster.te", minimum=0),
        flex_positions=tuple(roster_raw.get("flex_positions", ("RB", "WR", "TE"))),
    )
    if set(roster.flex_positions) - {"RB", "WR", "TE"}:
        raise ValueError("roster.flex_positions may only contain RB, WR, TE")
    if roster.flex and not roster.flex_positions:
        raise ValueError("roster.flex requires non-empty flex positions")

    draft_raw = _mapping(raw.get("draft", {}), "draft")
    _check_keys(draft_raw, {"entrants", "rounds"}, "draft")
    draft = DraftConfig(
        _int(draft_raw.get("entrants"), "draft.entrants"),
        _int(draft_raw.get("rounds"), "draft.rounds"),
    )
    if draft.rounds != roster.qb + roster.rb + roster.wr + roster.flex + roster.te:
        raise ValueError("draft.rounds must equal the roster slot count")

    field_raw = _mapping(raw.get("field", {}), "field")
    _check_keys(field_raw, {"size", "entry_fee"}, "field")
    field = FieldConfig(
        _int(field_raw.get("size"), "field.size"),
        _money(field_raw.get("entry_fee"), "field.entry_fee"),
    )

    caps_raw = _mapping(raw.get("position_caps", {}), "position_caps")
    _check_keys(caps_raw, {"qb", "rb", "wr", "te"}, "position_caps")
    caps = PositionCaps(
        *(_int(caps_raw.get(k), f"position_caps.{k}") for k in ("qb", "rb", "wr", "te"))
    )
    for pos, starters, cap in (
        ("qb", roster.qb, caps.qb),
        ("rb", roster.rb, caps.rb),
        ("wr", roster.wr, caps.wr),
        ("te", roster.te, caps.te),
    ):
        if cap < starters:
            raise ValueError(f"position_caps.{pos} cannot be below roster.{pos}")
    if roster.flex:
        available_flex_capacity = sum(
            max(0, cap - starters)
            for pos, starters, cap in (
                ("RB", roster.rb, caps.rb),
                ("WR", roster.wr, caps.wr),
                ("TE", roster.te, caps.te),
            )
            if pos in roster.flex_positions
        )
        if available_flex_capacity < roster.flex:
            raise ValueError(
                "position caps cannot provide enough capacity for roster.flex"
            )

    payouts_raw = raw.get("payouts")
    if not isinstance(payouts_raw, list) or not payouts_raw:
        raise ValueError("payouts must be a non-empty list")
    ladder: list[Payout] = []
    for index, item in enumerate(payouts_raw):
        item_map = _mapping(item, f"payouts[{index}]")
        _check_keys(item_map, {"rank", "amount"}, f"payouts[{index}]")
        rank = _int(item_map.get("rank"), f"payouts[{index}].rank")
        if rank > field.size:
            raise ValueError(f"payouts[{index}].rank cannot exceed field.size")
        ladder.append(
            Payout(rank, _money(item_map.get("amount"), f"payouts[{index}].amount"))
        )
    if [x.rank for x in ladder] != sorted({x.rank for x in ladder}):
        raise ValueError("payout ranks must be unique and ascending")
    prize_pool = sum((x.amount for x in ladder), Decimal("0.00"))
    expected_pool = field.entry_fee * field.size
    if abs(prize_pool - expected_pool) > Decimal("0.01"):
        raise ValueError(
            f"payout total {prize_pool} does not reconcile to prize pool "
            f"{expected_pool}"
        )

    objective = raw.get("objective", "expected_net_payout")
    if objective != "expected_net_payout":
        raise ValueError("objective must be 'expected_net_payout'")
    return NflContestConfig(
        contest_id.strip(),
        scoring,
        roster,
        draft,
        field,
        caps,
        PayoutConfig(tuple(ladder), prize_pool),
        objective,
    )


__all__ = [
    "DEFAULT_ROSTER",
    "NflContestConfig",
    "UnknownScoringError",
    "load_nfl_contest_config",
]
