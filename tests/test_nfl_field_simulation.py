"""Focused offline contract tests for the R4.4 field simulator."""

from decimal import Decimal

import pandas as pd
import pytest
from src.fantasy.adapters.nfl.contest import (
    DEFAULT_ROSTER,
    DraftConfig,
    FieldConfig,
    NflContestConfig,
    Payout,
    PayoutConfig,
    PositionCaps,
    ScoringConfig,
)
from src.nfl.models.field_simulation import (
    FieldSimulationConfig,
    FieldSimulationError,
    simulate_nfl_field,
)
from src.nfl.models.joint_simulation import simulate_joint_player_outcomes
from src.nfl.models.opponent_adp import simulate_opponent_adp


def _config(*, field_size: int = 8, entrants: int = 4) -> NflContestConfig:
    if field_size == 8:
        ladder = (
            Payout(1, Decimal("50.00")),
            Payout(2, Decimal("30.00")),
            Payout(3, Decimal("20.00")),
        )
    else:
        ladder = (Payout(1, Decimal("500.00")),)
    return NflContestConfig(
        contest_id="offline-test",
        provenance=None,
        scoring=ScoringConfig(),
        roster=DEFAULT_ROSTER,
        draft=DraftConfig(entrants, 6),
        field=FieldConfig(
            field_size,
            Decimal("1.00"),
            Decimal("100.00") if field_size == 8 else Decimal("500.00"),
            Decimal("0.00"),
            None,
        ),
        position_caps=PositionCaps(entrants, entrants + 1, entrants + 2, entrants + 1),
        payouts=PayoutConfig(
            ladder, Decimal("100.00") if field_size == 8 else Decimal("500.00")
        ),
        objective="expected_net_payout",
    )


def _players(count: int = 40) -> pd.DataFrame:
    positions = (["QB", "RB", "WR", "WR", "TE"] * ((count // 5) + 1))[:count]
    return pd.DataFrame(
        {
            "player_id": [f"p{i:03d}" for i in range(count)],
            "position": positions,
            "adp": [float(i + 1) for i in range(count)],
            "projection": [20.0 - i / 10 for i in range(count)],
            "team": [f"T{i % 8}" for i in range(count)],
            "game_id": [f"G{i % 4}" for i in range(count)],
            "provenance": ["direct_fantasy_points_unpromoted"] * count,
        }
    )


def test_fixed_seed_reproduces_full_result_and_consumes_provenance() -> None:
    players = _players()
    controls = FieldSimulationConfig(target_field_size=8, simulations=3, seed=17)
    first = simulate_nfl_field(_config(), players, simulation_config=controls)
    second = simulate_nfl_field(_config(), players, simulation_config=controls)
    pd.testing.assert_frame_equal(first.entries, second.entries)
    assert first.audit["adp_provenance"] == "archived_underdog_adp_no_live_quote"
    assert first.audit["joint_provenance"] == "direct_fantasy_points_unpromoted"
    assert first.audit["automated_actions"] is False
    assert set(first.entries["adp_provenance"]) == {first.audit["adp_provenance"]}


def test_realistic_four_entrant_field_is_exactly_500_and_rosters_are_legal() -> None:
    result = simulate_nfl_field(
        _config(field_size=500),
        _players(60),
        simulation_config=FieldSimulationConfig(simulations=1, seed=3),
    )
    entries = result.entries
    assert result.audit["actual_field_size"] == 500
    assert len(entries) == 500
    assert entries["entry_id"].nunique() == 500
    assert entries["room_id"].nunique() == 125
    assert entries.groupby("simulation_id")["payout"].sum().iloc[0] == Decimal("500.00")


def test_explicit_ties_split_occupied_ladder_slots_in_cents() -> None:
    players = _players()
    adp = simulate_opponent_adp(players[["player_id", "adp"]], scenarios=2, seed=1)
    outcomes = simulate_joint_player_outcomes(players, simulations=1, seed=2)
    outcomes["simulated_fantasy_points"] = 0.0
    result = simulate_nfl_field(
        _config(),
        players,
        simulation_config=FieldSimulationConfig(
            target_field_size=8, simulations=1, seed=1
        ),
        opponent_adp_scenarios=adp,
        joint_outcomes=outcomes,
    )
    assert set(result.entries["tie_size"]) == {8}
    assert set(result.entries["payout"]) == {Decimal("12.50")}
    assert result.entries["payout"].sum() == Decimal("100.00")


def test_non_tie_uses_exact_ladder_amount() -> None:
    players = _players()
    adp = simulate_opponent_adp(players[["player_id", "adp"]], scenarios=2, seed=1)
    outcomes = simulate_joint_player_outcomes(players, simulations=1, seed=2)
    # Distinct player values make roster sums deterministic and overwhelmingly unique.
    outcomes["simulated_fantasy_points"] = outcomes["player_id"].str[1:].astype(float)
    result = simulate_nfl_field(
        _config(),
        players,
        simulation_config=FieldSimulationConfig(
            target_field_size=8, simulations=1, seed=1
        ),
        opponent_adp_scenarios=adp,
        joint_outcomes=outcomes,
    )
    top = result.entries[result.entries["rank"] == 1]
    assert len(top) == 1
    assert top.iloc[0]["payout"] == Decimal("50.00")


def test_incompatible_inputs_fail_closed() -> None:
    players = _players()
    with pytest.raises(FieldSimulationError):
        simulate_nfl_field(
            _config(), players.assign(provenance="promoted_model"), target_field_size=8
        )
    adp = simulate_opponent_adp(players[["player_id", "adp"]], scenarios=2, seed=1)
    with pytest.raises(FieldSimulationError):
        simulate_nfl_field(
            _config(),
            players,
            target_field_size=8,
            opponent_adp_scenarios=adp.iloc[:-1],
        )
    with pytest.raises(FieldSimulationError):
        simulate_nfl_field(_config(), players, target_field_size=3)


def test_promoted_direct_fp_provenance_is_rejected_even_with_supplied_outcomes() -> (
    None
):
    players = _players()
    outcomes = simulate_joint_player_outcomes(players, simulations=1, seed=2)
    with pytest.raises(FieldSimulationError, match="promoted"):
        simulate_nfl_field(
            _config(),
            players.assign(provenance="direct_fantasy_points_promoted"),
            target_field_size=8,
            joint_outcomes=outcomes,
        )


def test_each_adp_scenario_must_be_an_exact_rank_permutation() -> None:
    players = _players()
    adp = simulate_opponent_adp(players[["player_id", "adp"]], scenarios=2, seed=1)
    duplicate = adp.copy()
    duplicate.loc[duplicate.index[0], "simulated_rank"] = 2
    with pytest.raises(FieldSimulationError, match="exactly once"):
        simulate_nfl_field(
            _config(), players, target_field_size=8, opponent_adp_scenarios=duplicate
        )
    out_of_range = adp.copy()
    out_of_range.loc[out_of_range.index[0], "simulated_rank"] = len(players) + 1
    with pytest.raises(FieldSimulationError, match="exactly once"):
        simulate_nfl_field(
            _config(), players, target_field_size=8, opponent_adp_scenarios=out_of_range
        )


def test_every_joint_simulation_must_cover_every_player() -> None:
    players = _players()
    outcomes = simulate_joint_player_outcomes(players, simulations=2, seed=2)
    incomplete = outcomes.drop(outcomes.index[-1]).copy()
    with pytest.raises(FieldSimulationError, match="every player"):
        simulate_nfl_field(
            _config(),
            players,
            target_field_size=8,
            simulation_config=FieldSimulationConfig(simulations=2, seed=1),
            joint_outcomes=incomplete,
        )


def test_six_entrant_target_reports_permitted_approximation() -> None:
    config = _config(field_size=500, entrants=6)
    result = simulate_nfl_field(
        config,
        _players(80),
        simulation_config=FieldSimulationConfig(simulations=1, seed=4),
    )
    assert result.audit["target_field_size"] == 500
    assert result.audit["actual_field_size"] == 498
    assert result.audit["approximation_permitted"] is True
