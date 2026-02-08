import numpy as np
import pandas as pd
import src.core.simulation as core_simulation
from src.mlb.models.monte_carlo import MonteCarloConfig, apply_simulations, simulate_row


class DummySampler:
    def __init__(self):
        self.calls = 0

    def sample_counts(
        self, mean: float, entity_id, simulations: int, rng: np.random.Generator
    ):
        self.calls += 1
        return np.full(simulations, mean + 1.0)


def test_simulate_row_skips_sampler_when_not_allowed():
    rng = np.random.default_rng(123)
    config = MonteCarloConfig(simulations=100)

    result = simulate_row(
        mean=10.0,
        std_dev=1.5,
        line=9.5,
        config=config,
        rng=rng,
        sampler=None,
        entity_id="pitcher-a",
    )

    assert set(result.keys()) >= {"prob_over", "prob_under", "prob_push"}


def test_simulate_row_uses_sampler_when_allowed():
    sampler = DummySampler()
    rng = np.random.default_rng(123)
    config = MonteCarloConfig(simulations=100)

    result = simulate_row(
        mean=10.0,
        std_dev=1.5,
        line=9.5,
        config=config,
        rng=rng,
        sampler=sampler,
        entity_id="pitcher-b",
    )

    assert sampler.calls == 1
    # Deterministic because DummySampler always returns a constant array.
    assert result["prob_over"] in (0.0, 1.0)


def test_apply_simulations_accepts_std_dev_column_name():
    lines = pd.DataFrame(
        [
            {
                "prediction": 30.0,
                "ud_line": 29.5,
                "simulation_sigma": 1.2,
                "over_decimal_price": 1.9,
                "under_decimal_price": 1.9,
                "qb_id": "QB1",
            }
        ]
    )
    result = apply_simulations(
        lines=lines,
        mean_col="prediction",
        std_dev="simulation_sigma",
        config=MonteCarloConfig(simulations=200, random_seed=42),
        sampler=None,
        line_col="ud_line",
        id_col="qb_id",
    )

    assert {"prob_over", "prob_under", "prob_push"}.issubset(result.columns)
    assert result["prob_over"].notna().all()


def test_mlb_monte_carlo_exports_core_symbols():
    assert MonteCarloConfig is core_simulation.MonteCarloConfig
    assert simulate_row is core_simulation.simulate_row
    assert apply_simulations is core_simulation.apply_simulations
