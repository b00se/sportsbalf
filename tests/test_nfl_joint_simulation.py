"""Offline contract tests for the R4.1 NFL joint outcome simulator."""

import numpy as np
import pandas as pd
import pytest
from src.nfl.models.joint_simulation import (
    JointSimulationConfig,
    simulate_joint_player_outcomes,
)


def _projections() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "player_id": "qb-a", "game_id": "g1", "team": "A",
                "position": "QB", "projection": 20.0,
                "baseline": "direct_fantasy_points", "status": "unpromoted",
                "season": 2025, "week": 1,
            },
            {
                "player_id": "wr-a", "game_id": "g1", "team": "A",
                "position": "WR", "projection": 12.0,
                "baseline": "direct_fantasy_points", "status": "unpromoted",
                "season": 2025, "week": 1,
            },
            {
                "player_id": "rb-a", "game_id": "g1", "team": "A",
                "position": "RB", "projection": 10.0,
                "baseline": "direct_fantasy_points", "status": "unpromoted",
                "season": 2025, "week": 1,
            },
            {
                "player_id": "qb-b", "game_id": "g2", "team": "B",
                "position": "QB", "projection": 19.0,
                "baseline": "direct_fantasy_points", "status": "unpromoted",
                "season": 2025, "week": 1,
            },
            {
                "player_id": "wr-b", "game_id": "g2", "team": "B",
                "position": "WR", "projection": 11.0,
                "baseline": "direct_fantasy_points", "status": "unpromoted",
                "season": 2025, "week": 1,
            },
            {
                "player_id": "rb-b", "game_id": "g2", "team": "B",
                "position": "RB", "projection": 9.0,
                "baseline": "direct_fantasy_points", "status": "unpromoted",
                "season": 2025, "week": 1,
            },
        ]
    )


def _samples(frame: pd.DataFrame, seed: int = 123) -> pd.DataFrame:
    return simulate_joint_player_outcomes(
        frame,
        config=JointSimulationConfig(simulations=4000, seed=seed),
    )


def test_fixed_seed_is_bitwise_reproducible() -> None:
    left = _samples(_projections(), seed=123)
    right = _samples(_projections(), seed=123)
    pd.testing.assert_frame_equal(left, right)


def test_marginal_means_retain_projection_means() -> None:
    frame = _projections()
    result = _samples(frame)
    means = result.groupby("player_id", sort=False)["simulated_fantasy_points"].mean()
    expected = frame.set_index("player_id").projection
    pd.testing.assert_series_equal(
        means, expected, check_names=False, atol=0.2, rtol=0.0
    )


def test_common_game_team_factors_create_observable_positive_correlation() -> None:
    result = _samples(_projections())
    wide = result.pivot(
        index="simulation_id",
        columns="player_id",
        values="simulated_fantasy_points",
    )
    within = wide[["qb-a", "wr-a"]].corr().iloc[0, 1]
    cross = wide[["qb-a", "qb-b"]].corr().iloc[0, 1]
    assert within > 0.15
    assert within > cross + 0.10


def test_metadata_and_non_promoted_direct_fp_provenance_are_preserved() -> None:
    frame = _projections()
    result = _samples(frame)
    for column in frame.columns:
        assert column in result
        assert (
            result[column].drop_duplicates().tolist()
            == frame[column].drop_duplicates().tolist()
        )
    assert set(result["baseline"]) == {"direct_fantasy_points"}
    assert set(result["status"]) == {"unpromoted"}
    assert result.attrs["provenance_status"] == "direct_fantasy_points_unpromoted"


@pytest.mark.parametrize(
    "bad_frame",
    [
        _projections().drop(columns=["team"]),
        _projections().assign(projection=np.inf),
        _projections().assign(status="promoted"),
        _projections().iloc[:0],
    ],
)
def test_invalid_schema_values_fail_closed(bad_frame: pd.DataFrame) -> None:
    with pytest.raises((TypeError, ValueError)):
        _samples(bad_frame)


def test_contradictory_status_aliases_fail_closed() -> None:
    frame = _projections().assign(promotion_status="unpromoted", status="promoted")
    with pytest.raises(ValueError, match="conflicting aliases"):
        _samples(frame)


@pytest.mark.parametrize("reserved", ["simulation_id", "simulated_fantasy_points"])
def test_reserved_output_columns_cannot_overwrite_input_metadata(
    reserved: str,
) -> None:
    frame = _projections().assign(**{reserved: "input-metadata"})
    with pytest.raises(ValueError, match="reserved output columns"):
        _samples(frame)


def test_seed_is_required_and_configuration_is_validated() -> None:
    with pytest.raises(ValueError, match="seed"):
        simulate_joint_player_outcomes(_projections())
    with pytest.raises(ValueError, match="simulations"):
        simulate_joint_player_outcomes(
            _projections(), config=JointSimulationConfig(simulations=0, seed=1)
        )
