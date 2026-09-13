"""Offline NFL tournament-field and exact payout simulation.

This module deliberately composes the three offline R4 simulation contracts:
archived-ADP rank scenarios, exact Autopilot draft rooms, and joint direct-FP
outcome paths.  It has no account, entry, upload, or live-service behavior.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal
from typing import Any

import numpy as np
import pandas as pd

from src.fantasy.adapters.nfl.contest import NflContestConfig
from src.nfl.models.autopilot_draft import (
    DraftPlayer,
    DraftValidationError,
    simulate_autopilot_draft,
)
from src.nfl.models.joint_simulation import (
    JointSimulationConfig,
    simulate_joint_player_outcomes,
)
from src.nfl.models.opponent_adp import (
    OpponentAdpConfig,
    simulate_opponent_adp,
)


class FieldSimulationError(ValueError):
    """Raised when field inputs or dependency provenance are not usable."""


@dataclass(frozen=True, slots=True)
class FieldSimulationConfig:
    """Deterministic controls for an offline field simulation.

    ``target_field_size`` defaults to the configured contest field size.  If
    it is not divisible by the draft-room entrant count, the simulator uses
    the largest compatible field not exceeding the target and reports that
    approximation in the result audit.  It refuses an approximation that
    would leave a configured payout rank outside the simulated field.
    """

    target_field_size: int | None = None
    simulations: int = 100
    seed: int = 0
    adp_dispersion: float = 1.0

    def __post_init__(self) -> None:
        """Validate controls without touching global random state."""
        if self.target_field_size is not None and (
            isinstance(self.target_field_size, bool)
            or not isinstance(self.target_field_size, int)
            or self.target_field_size < 1
        ):
            raise ValueError("target_field_size must be a positive integer or None")
        if (
            isinstance(self.simulations, bool)
            or not isinstance(self.simulations, int)
            or self.simulations < 1
        ):
            raise ValueError("simulations must be a positive integer")
        if isinstance(self.seed, bool) or not isinstance(self.seed, int):
            raise ValueError("seed must be an integer")
        if (
            isinstance(self.adp_dispersion, bool)
            or not np.isscalar(self.adp_dispersion)
            or not np.isfinite(float(self.adp_dispersion))
            or float(self.adp_dispersion) < 0
        ):
            raise ValueError("adp_dispersion must be finite and nonnegative")


@dataclass(frozen=True, slots=True)
class FieldSimulationResult:
    """Auditable per-entry simulation output and non-operational metadata."""

    entries: pd.DataFrame
    audit: dict[str, Any]

    @property
    def results(self) -> pd.DataFrame:
        """Compatibility alias for the per-entry result table."""

        return self.entries


def _require_config(config: NflContestConfig) -> None:
    if not isinstance(config, NflContestConfig):
        raise TypeError("config must be an NflContestConfig")
    if config.draft.entrants not in (4, 6) or config.draft.rounds != 6:
        raise FieldSimulationError(
            "field simulation requires a six-round room with 4 or 6 entrants"
        )
    if config.field.field_size < config.draft.entrants:
        raise FieldSimulationError("configured field is smaller than one draft room")
    if not config.payouts.ladder:
        raise FieldSimulationError("payout ladder must be non-empty")
    amounts = [Decimal(str(p.amount)) for p in config.payouts.ladder]
    if any(amount != amount.quantize(Decimal("0.01")) for amount in amounts):
        raise FieldSimulationError("payout amounts must be exact cents")
    amounts = [amount.quantize(Decimal("0.01")) for amount in amounts]
    if any(amount < 0 for amount in amounts):
        raise FieldSimulationError("payout amounts must be nonnegative")
    prize_pool = Decimal(str(config.payouts.prize_pool))
    if prize_pool != prize_pool.quantize(Decimal("0.01")):
        raise FieldSimulationError("prize pool must be exact cents")
    if sum(amounts, Decimal("0.00")) != prize_pool:
        raise FieldSimulationError("payout ladder does not reconcile to prize pool")
    ranks = [p.rank for p in config.payouts.ladder]
    if ranks != sorted(set(ranks)):
        raise FieldSimulationError("payout ranks must be unique and ascending")


def _validate_projection_input(projections: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(projections, pd.DataFrame):
        raise TypeError("projections must be a pandas DataFrame")
    required = {"player_id", "position", "adp", "projection", "team", "provenance"}
    missing = sorted(required - set(projections.columns))
    if "game_id" not in projections.columns and "game" not in projections.columns:
        missing.append("game_id")
    if missing:
        raise FieldSimulationError(f"projections missing required columns: {missing}")
    if projections.empty:
        raise FieldSimulationError("projections must contain at least one player")
    work = projections.copy(deep=True)
    ids = work["player_id"]
    if ids.isna().any() or ids.astype(str).str.strip().eq("").any():
        raise FieldSimulationError("player_id must be non-empty")
    work["player_id"] = ids.astype(str).str.strip()
    if work["player_id"].duplicated().any():
        raise FieldSimulationError("player_id must be unique for field simulation")
    if work["position"].isna().any():
        raise FieldSimulationError("position must be non-empty")
    positions = work["position"].astype(str).str.strip().str.upper()
    if (~positions.isin(("QB", "RB", "WR", "TE"))).any():
        raise FieldSimulationError("position must be one of QB, RB, WR, TE")
    work["position"] = positions
    for column in ("adp", "projection"):
        raw = work[column]
        if raw.map(lambda value: isinstance(value, (bool, np.bool_))).any():
            raise FieldSimulationError(f"{column} must contain finite numeric values")
        values = pd.to_numeric(raw, errors="coerce")
        if values.isna().any() or (~np.isfinite(values)).any():
            raise FieldSimulationError(f"{column} must contain finite numeric values")
        if column == "adp" and (values <= 0).any():
            raise FieldSimulationError("adp must be finite and positive")
        work[column] = values.astype(float)
    source = work["provenance"].astype(str).str.strip().str.lower()
    if source.eq("").any() or ~source.str.contains("direct").any():
        raise FieldSimulationError("projections must identify direct-FP provenance")
    promoted = source.str.contains("promot") & ~source.str.contains(
        "unpromot|non_promot|not_promot"
    )
    if promoted.any():
        raise FieldSimulationError(
            "promoted direct-FP projections are not accepted by the simulator"
        )
    # The joint simulator performs the complete, stricter non-promotion check.
    if not source.map(
        lambda value: "direct" in value and ("fp" in value or "fantasy" in value)
    ).all():
        raise FieldSimulationError("projections must identify direct-FP provenance")
    return work


def _resolve_controls(
    config: NflContestConfig,
    simulation_config: FieldSimulationConfig | None,
    *,
    seed: int | None,
    simulations: int | None,
    target_field_size: int | None,
) -> FieldSimulationConfig:
    base = simulation_config or FieldSimulationConfig()
    if not isinstance(base, FieldSimulationConfig):
        raise TypeError("simulation_config must be a FieldSimulationConfig")
    for name, override in (
        ("seed", seed),
        ("simulations", simulations),
        ("target_field_size", target_field_size),
    ):
        current = getattr(base, name)
        if override is not None and name != "target_field_size" and override != current:
            raise FieldSimulationError(
                f"{name} conflicts with simulation_config.{name}"
            )
        if (
            override is not None
            and name == "target_field_size"
            and current is not None
            and override != current
        ):
            raise FieldSimulationError(
                "target_field_size conflicts with simulation_config"
            )
    resolved_target = (
        base.target_field_size if target_field_size is None else target_field_size
    )
    return FieldSimulationConfig(
        target_field_size=(
            config.field.field_size if resolved_target is None else resolved_target
        ),
        simulations=base.simulations if simulations is None else simulations,
        seed=base.seed if seed is None else seed,
        adp_dispersion=base.adp_dispersion,
    )


def _field_size(config: NflContestConfig, target: int) -> tuple[int, bool]:
    entrants = config.draft.entrants
    if target > config.field.field_size:
        raise FieldSimulationError("target_field_size cannot exceed configured field")
    actual = target - target % entrants
    if actual < entrants:
        raise FieldSimulationError("target_field_size cannot form a complete room")
    ladder_ranks = [p.rank for p in config.payouts.ladder]
    if ladder_ranks and max(ladder_ranks) > actual:
        raise FieldSimulationError(
            "field approximation would exclude a configured payout rank"
        )
    return actual, actual != target


def _validate_adp_output(
    adp: pd.DataFrame, rooms: int, player_count: int
) -> pd.DataFrame:
    if not isinstance(adp, pd.DataFrame):
        raise FieldSimulationError("opponent ADP dependency did not return a DataFrame")
    required = {"player_id", "scenario_id", "simulated_rank", "provenance"}
    if not required.issubset(adp.columns):
        raise FieldSimulationError("opponent ADP output has incompatible schema")
    if adp.empty or adp["scenario_id"].nunique() != rooms:
        raise FieldSimulationError(
            "opponent ADP output has incompatible room scenarios"
        )
    if (
        adp["player_id"].isna().any()
        or adp.duplicated(["scenario_id", "player_id"]).any()
    ):
        raise FieldSimulationError("opponent ADP output has duplicate or empty players")
    if (
        not adp["simulated_rank"]
        .map(
            lambda value: (
                isinstance(value, (int, np.integer)) and not isinstance(value, bool)
            )
        )
        .all()
    ):
        raise FieldSimulationError("opponent ADP ranks must be integers")
    if (
        not adp["scenario_id"]
        .map(
            lambda value: (
                isinstance(value, (int, np.integer)) and not isinstance(value, bool)
            )
        )
        .all()
    ):
        raise FieldSimulationError("opponent ADP scenario IDs must be integers")
    expected_ranks = list(range(1, player_count + 1))
    for scenario_id, scenario in adp.groupby("scenario_id", sort=False):
        if (
            len(scenario) != player_count
            or sorted(scenario["simulated_rank"].tolist()) != expected_ranks
        ):
            raise FieldSimulationError(
                f"opponent ADP scenario {scenario_id} must rank every player "
                "exactly once"
            )
    provenance = adp["provenance"].astype(str).str.lower()
    if (
        not provenance.str.contains("archived").all()
        or not provenance.str.contains("no_live").all()
    ):
        raise FieldSimulationError(
            "opponent ADP provenance must remain archived and non-live"
        )
    return adp.copy(deep=True)


def _validate_joint_output(
    outcomes: pd.DataFrame, players: pd.DataFrame, simulations: int
) -> pd.DataFrame:
    if not isinstance(outcomes, pd.DataFrame):
        raise FieldSimulationError(
            "joint simulation dependency did not return a DataFrame"
        )
    required = {"player_id", "simulation_id", "simulated_fantasy_points"}
    if not required.issubset(outcomes.columns):
        raise FieldSimulationError("joint simulation output has incompatible schema")
    if outcomes.empty or outcomes["simulation_id"].nunique() != simulations:
        raise FieldSimulationError(
            "joint simulation output has incompatible simulation count"
        )
    if outcomes.duplicated(["simulation_id", "player_id"]).any():
        raise FieldSimulationError("joint simulation output has duplicate player paths")
    if (
        not outcomes["simulation_id"]
        .map(
            lambda value: (
                isinstance(value, (int, np.integer)) and not isinstance(value, bool)
            )
        )
        .all()
    ):
        raise FieldSimulationError("joint simulation IDs must be integers")
    expected_ids = set(players["player_id"])
    for simulation_id, simulation in outcomes.groupby("simulation_id", sort=False):
        if set(simulation["player_id"]) != expected_ids:
            raise FieldSimulationError(
                f"joint simulation {simulation_id} does not cover every player"
            )
    marker = outcomes.attrs.get("provenance_status")
    if marker != "direct_fantasy_points_unpromoted":
        raise FieldSimulationError(
            "joint outcomes must retain non-promoted direct-FP provenance"
        )
    values = pd.to_numeric(outcomes["simulated_fantasy_points"], errors="coerce")
    if values.isna().any() or (~np.isfinite(values)).any():
        raise FieldSimulationError("joint outcomes must contain finite fantasy points")
    return outcomes.copy(deep=True)


def _payout_for_scores(
    scores: list[tuple[str, float]],
    ladder: Mapping[int, Decimal],
    simulation_id: int,
) -> list[dict[str, Any]]:
    ordered = sorted(scores, key=lambda item: (-item[1], item[0]))
    rows: list[dict[str, Any]] = []
    cursor = 0
    tie_number = 0
    while cursor < len(ordered):
        end = cursor + 1
        while end < len(ordered) and ordered[end][1] == ordered[cursor][1]:
            end += 1
        rank = cursor + 1
        tie_number += 1
        group = ordered[cursor:end]
        aggregate = sum(
            (
                ladder.get(slot, Decimal("0.00"))
                for slot in range(rank, rank + len(group))
            ),
            Decimal("0.00"),
        )
        cents = int(
            (aggregate / len(group)).quantize(Decimal("0.01"), rounding=ROUND_FLOOR)
            * 100
        )
        remainder = int(aggregate * 100) - cents * len(group)
        tie_group = f"simulation-{simulation_id:04d}-tie-{tie_number:03d}"
        for offset, (entry_id, score) in enumerate(group):
            amount = Decimal(cents + (1 if offset < remainder else 0)) / Decimal(100)
            rows.append(
                {
                    "entry_id": entry_id,
                    "score": score,
                    "rank": rank,
                    "tie_group_id": tie_group,
                    "tie_size": len(group),
                    "payout": amount.quantize(Decimal("0.01")),
                }
            )
        cursor = end
    return rows


def simulate_nfl_field(
    config: NflContestConfig,
    projections: pd.DataFrame,
    *,
    simulation_config: FieldSimulationConfig | None = None,
    seed: int | None = None,
    simulations: int | None = None,
    target_field_size: int | None = None,
    opponent_adp_scenarios: pd.DataFrame | None = None,
    joint_outcomes: pd.DataFrame | None = None,
) -> FieldSimulationResult:
    """Simulate an offline draft field, scores, ranks, and exact payouts.

    Args:
        config: Validated typed NFL contest terms and payout ladder.
        projections: One row per player with direct-FP projection, team/game,
            position, and archived positive ``adp``.
        opponent_adp_scenarios: Optional already-generated R4.3 output.
        joint_outcomes: Optional already-generated R4.1 output.  Supplied
            dependency outputs are validated just as internally generated
            outputs are.

    Returns:
        A deterministic result table with one row per entry per outcome path.
    """
    _require_config(config)
    players = _validate_projection_input(projections)
    controls = _resolve_controls(
        config,
        simulation_config,
        seed=seed,
        simulations=simulations,
        target_field_size=target_field_size,
    )
    actual_size, approximated = _field_size(
        config, controls.target_field_size or config.field.field_size
    )
    rooms = actual_size // config.draft.entrants
    adp = opponent_adp_scenarios
    if adp is None:
        adp_input = players[["player_id", "adp"]].copy()
        adp = simulate_opponent_adp(
            adp_input,
            config=OpponentAdpConfig(
                scenarios=rooms,
                seed=controls.seed,
                dispersion=controls.adp_dispersion,
            ),
        )
    adp = _validate_adp_output(adp, rooms, len(players))
    outcomes = joint_outcomes
    if outcomes is None:
        outcomes = simulate_joint_player_outcomes(
            players,
            config=JointSimulationConfig(
                simulations=controls.simulations, seed=controls.seed + 1
            ),
        )
    outcomes = _validate_joint_output(outcomes, players, controls.simulations)

    player_by_id = players.set_index("player_id")
    roster_ids: dict[str, tuple[str, ...]] = {}
    entries_meta: list[dict[str, Any]] = []
    scenario_ids = sorted(adp["scenario_id"].unique().tolist())
    for room_index, scenario_id in enumerate(scenario_ids):
        scenario = adp[adp["scenario_id"] == scenario_id]
        ranks = scenario.set_index("player_id")["simulated_rank"]
        pool = [
            DraftPlayer(
                player_id=player_id,
                position=str(player_by_id.loc[player_id, "position"]),
                rank=float(ranks[player_id]),
                projection=float(player_by_id.loc[player_id, "projection"]),
            )
            for player_id in players["player_id"]
            if player_id in ranks.index
        ]
        if len(pool) != len(players):
            raise FieldSimulationError(
                "opponent ADP scenario does not cover every player"
            )
        try:
            draft = simulate_autopilot_draft(
                config, pool, seed=controls.seed + room_index
            )
        except (DraftValidationError, ValueError) as exc:
            raise FieldSimulationError(
                f"offline draft room {scenario_id} failed"
            ) from exc
        if draft.audit.get("automated_actions") is not False:
            raise FieldSimulationError(
                "draft dependency must declare automated_actions=False"
            )
        for roster in draft.rosters:
            entry_id = f"room-{room_index:04d}-entrant-{roster.entrant:02d}"
            ids = tuple(player.player_id for player in roster.players)
            if len(ids) != 6 or len(set(ids)) != 6:
                raise FieldSimulationError(
                    "draft dependency produced an illegal roster"
                )
            roster_ids[entry_id] = ids
            entries_meta.append(
                {
                    "entry_id": entry_id,
                    "room_id": f"room-{room_index:04d}",
                    "entrant": roster.entrant,
                    "adp_scenario_id": scenario_id,
                    "adp_provenance": str(scenario["provenance"].iloc[0]),
                    "draft_seed": controls.seed + room_index,
                    "automated_actions": False,
                }
            )
    if len(entries_meta) != actual_size:
        raise FieldSimulationError("generated field size does not match room sizing")

    outcome_matrix = outcomes.pivot(
        index="simulation_id", columns="player_id", values="simulated_fantasy_points"
    )
    outcome_matrix = outcome_matrix.sort_index()
    if len(outcome_matrix) != controls.simulations or set(
        outcome_matrix.columns
    ) != set(players["player_id"]):
        raise FieldSimulationError("joint outcome matrix is incomplete")
    ladder = {
        p.rank: Decimal(p.amount).quantize(Decimal("0.01"))
        for p in config.payouts.ladder
    }
    all_rows: list[dict[str, Any]] = []
    for simulation_id, row in outcome_matrix.iterrows():
        score_pairs = [
            (
                entry["entry_id"],
                float(
                    sum(row[player_id] for player_id in roster_ids[entry["entry_id"]])
                ),
            )
            for entry in entries_meta
        ]
        payouts = _payout_for_scores(score_pairs, ladder, int(simulation_id))
        payout_by_entry = {item["entry_id"]: item for item in payouts}
        for entry in entries_meta:
            item = dict(entry)
            item.update(payout_by_entry[entry["entry_id"]])
            item["simulation_id"] = int(simulation_id)
            item["joint_provenance"] = "direct_fantasy_points_unpromoted"
            all_rows.append(item)
        total = sum((item["payout"] for item in payouts), Decimal("0.00"))
        if total != Decimal(config.payouts.prize_pool).quantize(Decimal("0.01")):
            raise FieldSimulationError(
                "payout allocation did not conserve configured prize pool"
            )
    result = pd.DataFrame(all_rows)
    result["payout"] = result["payout"].map(
        lambda value: Decimal(value).quantize(Decimal("0.01"))
    )
    audit = {
        "engine": "offline_nfl_field_and_payout_simulator",
        "seed": controls.seed,
        "simulations": controls.simulations,
        "target_field_size": controls.target_field_size,
        "actual_field_size": actual_size,
        "approximation_permitted": approximated,
        "entrants_per_room": config.draft.entrants,
        "rooms": rooms,
        "payout_pool": Decimal(config.payouts.prize_pool).quantize(Decimal("0.01")),
        "adp_provenance": "archived_underdog_adp_no_live_quote",
        "joint_provenance": "direct_fantasy_points_unpromoted",
        "automated_actions": False,
        "live_service": False,
        "account_or_entry_operations": False,
    }
    return FieldSimulationResult(result, audit)


simulate_field_payouts = simulate_nfl_field
simulate_nfl_field_and_payouts = simulate_nfl_field


__all__ = [
    "FieldSimulationConfig",
    "FieldSimulationError",
    "FieldSimulationResult",
    "simulate_field_payouts",
    "simulate_nfl_field",
    "simulate_nfl_field_and_payouts",
]
