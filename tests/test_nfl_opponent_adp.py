import numpy as np
import pandas as pd
import pytest
from src.nfl.models.opponent_adp import OpponentAdpConfig, simulate_opponent_adp


@pytest.fixture
def players() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "player_id": ["z", "a", "m", "q"],
            "position": ["WR", "QB", "RB", "TE"],
            "adp": [40, 8, 22, 30],
            "projection": [10.0, 20.0, 15.0, 12.0],
        }
    )


def test_reproducible_order_invariant_and_metadata(players):
    config = OpponentAdpConfig(scenarios=20, seed=7, dispersion=2.0)
    left = simulate_opponent_adp(players, config=config)
    right = simulate_opponent_adp(players.iloc[::-1], config=config)
    pd.testing.assert_frame_equal(left, right)
    assert left["projection"].tolist() == [20.0, 15.0, 12.0, 10.0] * 20
    assert left["provenance"].eq("archived_underdog_adp_no_live_quote").all()
    assert all(
        group["simulated_rank"].is_unique
        for _, group in left.groupby("scenario_id")
    )


@pytest.mark.parametrize("column", ["scenario_id", "simulated_rank", "provenance"])
def test_reserved_output_columns_fail_closed(players, column):
    players[column] = "existing"
    with pytest.raises(ValueError, match="reserved output"):
        simulate_opponent_adp(
            players, config=OpponentAdpConfig(scenarios=2, seed=1, dispersion=1.0)
        )


def test_varying_seed_non_degenerate_and_adp_order(players):
    a = simulate_opponent_adp(players, config=OpponentAdpConfig(4000, 1, 3.0))
    b = simulate_opponent_adp(players, config=OpponentAdpConfig(4000, 2, 3.0))
    assert not a["simulated_rank"].equals(b["simulated_rank"])
    means = a.groupby("player_id")["simulated_rank"].mean()
    assert means["a"] < means["m"] < means["q"] < means["z"]


@pytest.mark.parametrize(
    "bad",
    [
        None,
        pd.DataFrame({"player_id": ["a"]}),
        pd.DataFrame({"player_id": ["a", "a"], "adp": [1, 2]}),
        pd.DataFrame({"player_id": ["a"], "adp": [np.inf]}),
        pd.DataFrame({"player_id": ["a"], "adp": [0]}),
    ],
)
def test_invalid_tables_fail_closed(bad):
    with pytest.raises((TypeError, ValueError)):
        simulate_opponent_adp(bad, config=OpponentAdpConfig(2, 1, 1.0))


@pytest.mark.parametrize("config", [OpponentAdpConfig(1, 1, 0.0),])
def test_zero_dispersion_is_adp_ordered(config, players):
    result = simulate_opponent_adp(players, config=config)
    assert result.sort_values("simulated_rank")["player_id"].tolist() == [
        "a",
        "m",
        "q",
        "z",
    ]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"scenarios": 0},
        {"seed": None},
        {"dispersion": -1},
        {"dispersion": np.inf},
    ],
)
def test_invalid_config_fails_closed(kwargs):
    with pytest.raises(ValueError):
        OpponentAdpConfig(**kwargs)


@pytest.mark.parametrize("seed", [-1, 2**128, 2**200])
def test_out_of_range_seed_fails_closed(seed):
    with pytest.raises(ValueError, match="RNG range"):
        OpponentAdpConfig(scenarios=1, seed=seed, dispersion=1.0)


def test_model_contract_is_explicit(players):
    result = simulate_opponent_adp(
        players, config=OpponentAdpConfig(scenarios=1, seed=1, dispersion=1.0)
    )
    assert result.attrs["model_contract"] == "adp_centered_no_predictive_edge_claim"
