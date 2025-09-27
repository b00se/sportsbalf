import numpy as np

from src.mlb.models.monte_carlo import MonteCarloConfig, simulate_row


class DummySampler:
    def __init__(self, allow: bool):
        self.allow = allow
        self.calls = 0

    def can_bootstrap(self, pitcher_id):
        return self.allow

    def sample_counts(self, mean: float, pitcher_id, simulations: int, rng: np.random.Generator):
        self.calls += 1
        return np.full(simulations, mean + 1.0)


def test_simulate_row_skips_sampler_when_not_allowed():
    sampler = DummySampler(allow=False)
    rng = np.random.default_rng(123)
    config = MonteCarloConfig(simulations=100)

    result = simulate_row(
        mean=10.0,
        std_dev=1.5,
        strikeout_line=9.5,
        config=config,
        rng=rng,
        sampler=sampler,
        pitcher_id="pitcher-a",
    )

    assert sampler.calls == 0
    assert set(result.keys()) >= {"prob_over", "prob_under", "prob_push"}


def test_simulate_row_uses_sampler_when_allowed():
    sampler = DummySampler(allow=True)
    rng = np.random.default_rng(123)
    config = MonteCarloConfig(simulations=100)

    result = simulate_row(
        mean=10.0,
        std_dev=1.5,
        strikeout_line=9.5,
        config=config,
        rng=rng,
        sampler=sampler,
        pitcher_id="pitcher-b",
    )

    assert sampler.calls == 1
    # Bootstrap sampling should produce deterministic fills because we return a constant array.
    assert result["prob_over"] in (0.0, 1.0)
