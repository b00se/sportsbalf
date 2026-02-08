import numpy as np
import pandas as pd
from src.core.simulation import MonteCarloConfig, apply_simulations, simulate_row


class DummySampler:
    def __init__(self) -> None:
        self.calls = 0

    def sample_counts(
        self,
        mean: float,
        entity_id: str | None,
        simulations: int,
        rng: np.random.Generator,
    ) -> np.ndarray:
        self.calls += 1
        return np.full(simulations, mean + 1.0)


def test_simulate_row_keys_and_nan_mean_behavior() -> None:
    rng = np.random.default_rng(123)
    config = MonteCarloConfig(simulations=100)

    result = simulate_row(
        mean=np.nan,
        std_dev=1.5,
        line=9.5,
        config=config,
        rng=rng,
    )

    expected_keys = {
        "prob_over",
        "prob_under",
        "prob_push",
        "simulated_mean",
        "simulated_std",
        "simulated_median",
    }
    assert set(result.keys()) == expected_keys
    assert all(np.isnan(result[key]) for key in expected_keys)


def test_simulate_row_uses_sampler_when_provided() -> None:
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
        entity_id="p-1",
    )

    assert sampler.calls == 1
    assert result["prob_over"] in (0.0, 1.0)


def test_apply_simulations_accepts_std_dev_column_name() -> None:
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
        line_col="ud_line",
        id_col="qb_id",
    )

    assert {"prob_over", "prob_under", "prob_push"}.issubset(result.columns)
    assert result["prob_over"].notna().all()
