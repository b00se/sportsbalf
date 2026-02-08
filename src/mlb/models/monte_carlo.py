"""MLB compatibility re-exports for shared simulation primitives."""

from src.core.simulation import MonteCarloConfig, apply_simulations, simulate_row

__all__ = ["MonteCarloConfig", "apply_simulations", "simulate_row"]
