"""Typed, fail-closed contracts for standard NFL daily contests."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
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
    reception: float = 0.5
    receiving_touchdown: float = 6.0
    receiving_yard: float = 0.1
    rushing_touchdown: float = 6.0
    rushing_yard: float = 0.1
    passing_yard: float = 0.04
    passing_touchdown: float = 4.0
    interception: float = -1.0
    two_point_conversion: float = 2.0
    fumble_lost: float = -2.0

    @property
    def points_per_reception(self) -> float:
        """Return the reception scoring coefficient for compatibility."""

        return self.reception


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

    field_size: int
    entry_fee: Decimal
    prize_pool: Decimal
    rake: Decimal
    max_entries: int | None

    @property
    def size(self) -> int:
        """Return the final field size for compatibility."""

        return self.field_size


@dataclass(frozen=True, slots=True)
class ContestProvenance:
    """Source-specific context retained alongside contest terms."""

    label: str
    reference_date: date
    contest_type: str
    slate: str
    source_live_field_size: int | None
    final_field_size: int
    source: str


@dataclass(frozen=True, slots=True)
class PositionCaps:
    """Maximum drafted players by position."""

    qb: int
    rb: int
    wr: int
    te: int


@dataclass(frozen=True, slots=True)
class Payout:
    """One expanded rank-to-payout value."""

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
    provenance: ContestProvenance | None
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


def _score(value: Any, name: str, *, default: float) -> float:
    raw = default if value is None else value
    if isinstance(raw, bool):
        raise ValueError(f"{name} must be a finite number")
    try:
        parsed = float(raw)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be a finite number") from None
    if not math.isfinite(parsed):
        raise ValueError(f"{name} must be a finite number")
    return parsed


def _parse_provenance(raw: Any, *, field_size: int) -> ContestProvenance | None:
    if raw is None:
        return None
    provenance = _mapping(raw, "provenance")
    _check_keys(
        provenance,
        {
            "label",
            "reference_date",
            "contest_type",
            "slate",
            "source_live_field_size",
            "final_field_size",
            "source",
        },
        "provenance",
    )
    required = ("label", "contest_type", "slate", "source")
    for key in required:
        if not isinstance(provenance.get(key), str) or not provenance[key].strip():
            raise ValueError(f"provenance.{key} must be a non-empty string")
    raw_reference_date = provenance.get("reference_date")
    if isinstance(raw_reference_date, date):
        reference_date = raw_reference_date
    elif isinstance(raw_reference_date, str):
        try:
            reference_date = date.fromisoformat(raw_reference_date)
        except ValueError as exc:
            raise ValueError("provenance.reference_date must be an ISO date") from exc
    else:
        raise ValueError("provenance.reference_date must be an ISO date")
    final_field_size = _int(
        provenance.get("final_field_size"), "provenance.final_field_size"
    )
    if final_field_size != field_size:
        raise ValueError("provenance.final_field_size must equal field.field_size")
    live_size_raw = provenance.get("source_live_field_size")
    live_size = (
        None
        if live_size_raw is None
        else _int(live_size_raw, "provenance.source_live_field_size")
    )
    return ContestProvenance(
        label=provenance["label"].strip(),
        reference_date=reference_date,
        contest_type=provenance["contest_type"].strip(),
        slate=provenance["slate"].strip(),
        source_live_field_size=live_size,
        final_field_size=final_field_size,
        source=provenance["source"].strip(),
    )


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
            "provenance",
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
    _check_keys(
        scoring_raw,
        {
            "ruleset",
            "reception",
            "receiving_touchdown",
            "receiving_yard",
            "rushing_touchdown",
            "rushing_yard",
            "passing_yard",
            "passing_touchdown",
            "interception",
            "two_point_conversion",
            "fumble_lost",
        },
        "scoring",
    )
    ruleset = scoring_raw.get("ruleset", "half_ppr")
    if ruleset not in ("half_ppr", "ppr"):
        raise UnknownScoringError(f"Unsupported scoring ruleset: {ruleset!r}")
    scoring = ScoringConfig(
        ruleset=ruleset,
        reception=_score(
            scoring_raw.get("reception"),
            "scoring.reception",
            default=0.5 if ruleset == "half_ppr" else 1.0,
        ),
        receiving_touchdown=_score(
            scoring_raw.get("receiving_touchdown"),
            "scoring.receiving_touchdown",
            default=6.0,
        ),
        receiving_yard=_score(
            scoring_raw.get("receiving_yard"), "scoring.receiving_yard", default=0.1
        ),
        rushing_touchdown=_score(
            scoring_raw.get("rushing_touchdown"),
            "scoring.rushing_touchdown",
            default=6.0,
        ),
        rushing_yard=_score(
            scoring_raw.get("rushing_yard"), "scoring.rushing_yard", default=0.1
        ),
        passing_yard=_score(
            scoring_raw.get("passing_yard"), "scoring.passing_yard", default=0.04
        ),
        passing_touchdown=_score(
            scoring_raw.get("passing_touchdown"),
            "scoring.passing_touchdown",
            default=4.0,
        ),
        interception=_score(
            scoring_raw.get("interception"), "scoring.interception", default=-1.0
        ),
        two_point_conversion=_score(
            scoring_raw.get("two_point_conversion"),
            "scoring.two_point_conversion",
            default=2.0,
        ),
        fumble_lost=_score(
            scoring_raw.get("fumble_lost"), "scoring.fumble_lost", default=-2.0
        ),
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
    _check_keys(
        field_raw,
        {"field_size", "entry_fee", "prize_pool", "rake", "max_entries"},
        "field",
    )
    field = FieldConfig(
        field_size=_int(field_raw.get("field_size"), "field.field_size"),
        entry_fee=_money(field_raw.get("entry_fee"), "field.entry_fee"),
        prize_pool=_money(field_raw.get("prize_pool"), "field.prize_pool"),
        rake=_money(field_raw.get("rake"), "field.rake"),
        max_entries=(
            None
            if field_raw.get("max_entries") is None
            else _int(field_raw.get("max_entries"), "field.max_entries")
        ),
    )
    if field.rake > Decimal("1.00"):
        raise ValueError("field.rake must be a decimal fraction in [0, 1]")
    provenance = _parse_provenance(raw.get("provenance"), field_size=field.field_size)

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
        is_single_rank = "rank" in item_map
        _check_keys(
            item_map,
            {"rank", "rank_start", "rank_end", "amount"},
            f"payouts[{index}]",
        )
        if is_single_rank == ("rank_start" in item_map or "rank_end" in item_map):
            raise ValueError(
                f"payouts[{index}] must use rank or rank_start and rank_end"
            )
        if is_single_rank:
            rank_start = _int(item_map.get("rank"), f"payouts[{index}].rank")
            rank_end = rank_start
        else:
            rank_start = _int(
                item_map.get("rank_start"), f"payouts[{index}].rank_start"
            )
            rank_end = _int(
                item_map.get("rank_end"), f"payouts[{index}].rank_end"
            )
        if rank_end < rank_start or rank_end > field.field_size:
            raise ValueError(f"payouts[{index}] has a rank outside field.field_size")
        amount = _money(item_map.get("amount"), f"payouts[{index}].amount")
        ladder.extend(Payout(rank, amount) for rank in range(rank_start, rank_end + 1))
    if [x.rank for x in ladder] != sorted({x.rank for x in ladder}):
        raise ValueError("payout ranks must be unique and ascending")
    prize_pool = sum((x.amount for x in ladder), Decimal("0.00"))
    if prize_pool != field.prize_pool:
        raise ValueError("payout total does not match field.prize_pool")
    expected_pool = field.entry_fee * field.field_size * (Decimal("1.00") - field.rake)
    if field.prize_pool != expected_pool:
        raise ValueError(
            f"field.prize_pool {field.prize_pool} does not reconcile to entry fees "
            f"{expected_pool}"
        )

    objective = raw.get("objective", "expected_net_payout")
    if objective != "expected_net_payout":
        raise ValueError("objective must be 'expected_net_payout'")
    return NflContestConfig(
        contest_id.strip(),
        provenance,
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
