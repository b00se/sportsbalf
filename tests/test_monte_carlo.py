import src.core.simulation as core_simulation
from src.mlb.models.monte_carlo import MonteCarloConfig, apply_simulations, simulate_row


def test_mlb_monte_carlo_exports_core_symbols():
    assert MonteCarloConfig is core_simulation.MonteCarloConfig
    assert simulate_row is core_simulation.simulate_row
    assert apply_simulations is core_simulation.apply_simulations
