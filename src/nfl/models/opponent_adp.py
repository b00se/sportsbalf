"""Offline stochastic opponent ranks centered on archived Underdog ADP.

This is a transparent uncertainty model: archived ``adp`` is the center of
each player's rank distribution and ``dispersion`` controls perturbation size.
It makes no claim of predictive edge and never contacts a live draft service.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True, slots=True)
class OpponentAdpConfig:
    """Controls for deterministic archived-ADP rank scenarios."""

    scenarios: int = 1_000
    seed: int | None = None
    dispersion: float = 1.0

    def __post_init__(self) -> None:
        """Reject controls that cannot define a reproducible simulation."""
        if (
            isinstance(self.scenarios, (bool, np.bool_))
            or not isinstance(self.scenarios, (int, np.integer))
            or self.scenarios < 1
        ):
            raise ValueError("scenarios must be a positive integer")
        if (
            self.seed is None
            or isinstance(self.seed, (bool, np.bool_))
            or not isinstance(self.seed, (int, np.integer))
            or self.seed < 0
            or self.seed >= 2**128
        ):
            raise ValueError("seed must be an integer in NumPy RNG range")
        if (
            isinstance(self.dispersion, (bool, np.bool_))
            or not np.isscalar(self.dispersion)
            or not np.isfinite(float(self.dispersion))
            or float(self.dispersion) < 0
        ):
            raise ValueError("dispersion must be finite and nonnegative")


def _validate_players(players: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(players, pd.DataFrame):
        raise TypeError("players must be a pandas DataFrame")
    required = {"player_id", "adp"}
    missing = sorted(required - set(players.columns))
    if missing:
        raise ValueError(f"players missing required columns: {missing}")
    if players.empty:
        raise ValueError("players must contain at least one row")
    reserved = {"scenario_id", "simulated_rank", "provenance"}.intersection(
        players.columns
    )
    if reserved:
        raise ValueError(
            "input columns collide with reserved output columns: "
            f"{sorted(reserved)}"
        )
    work = players.copy(deep=True)
    ids = work["player_id"]
    if ids.isna().any():
        raise ValueError("player_id must be non-empty")
    work["player_id"] = ids.astype(str).str.strip()
    if work["player_id"].eq("").any() or work["player_id"].duplicated().any():
        raise ValueError("player_id must be unique and non-empty")
    raw = work["adp"]
    if raw.map(lambda value: isinstance(value, (bool, np.bool_))).any():
        raise ValueError("adp must be finite and positive")
    adp = pd.to_numeric(raw, errors="coerce")
    if adp.isna().any() or (~np.isfinite(adp)).any() or (adp <= 0).any():
        raise ValueError("adp must be finite and positive")
    work["adp"] = adp.astype(float)
    # Canonical order makes results independent of input row order.
    return work.sort_values("player_id", kind="mergesort").reset_index(drop=True)


def simulate_opponent_adp(
    players: pd.DataFrame,
    *,
    config: OpponentAdpConfig | None = None,
    seed: int | None = None,
    scenarios: int | None = None,
    dispersion: float | None = None,
) -> pd.DataFrame:
    """Generate unique integer opponent draft ranks from archived ADP.

    All input columns are retained. Output is long-form with one row per
    player/scenario and adds ``scenario_id``, ``simulated_rank``, and explicit
    archived-ADP provenance. ``seed`` and overrides must not conflict with the
    supplied config.
    """
    base = config if config is not None else OpponentAdpConfig(
        scenarios=scenarios if scenarios is not None else 1_000,
        seed=seed,
        dispersion=dispersion if dispersion is not None else 1.0,
    )
    if not isinstance(base, OpponentAdpConfig):
        raise TypeError("config must be an OpponentAdpConfig")
    if seed is not None and seed != base.seed:
        raise ValueError("seed conflicts with config.seed")
    if scenarios is not None and scenarios != base.scenarios:
        raise ValueError("scenarios conflicts with config.scenarios")
    if dispersion is not None and dispersion != base.dispersion:
        raise ValueError("dispersion conflicts with config.dispersion")
    effective = OpponentAdpConfig(
        scenarios=base.scenarios if scenarios is None else scenarios,
        seed=base.seed if seed is None else seed,
        dispersion=base.dispersion if dispersion is None else dispersion,
    )
    work = _validate_players(players)
    ids = work["player_id"].to_numpy()
    adp = work["adp"].to_numpy(dtype=float)
    rng = np.random.default_rng(effective.seed)
    scores = adp + rng.normal(
        0.0, effective.dispersion, size=(effective.scenarios, len(work))
    )
    ranks = np.empty_like(scores, dtype=np.int64)
    for scenario, score in enumerate(scores):
        order = np.lexsort((ids, score))
        ranks[scenario, order] = np.arange(1, len(work) + 1, dtype=np.int64)
    row_index = np.tile(np.arange(len(work)), effective.scenarios)
    output = work.iloc[row_index].reset_index(drop=True)
    output["scenario_id"] = np.repeat(
        np.arange(effective.scenarios, dtype=int), len(work)
    )
    output["simulated_rank"] = ranks.reshape(-1)
    output["provenance"] = "archived_underdog_adp_no_live_quote"
    output.attrs["simulation_config"] = effective
    output.attrs["model_contract"] = "adp_centered_no_predictive_edge_claim"
    return output


simulate_archived_opponent_adp = simulate_opponent_adp

__all__ = [
    "OpponentAdpConfig",
    "simulate_archived_opponent_adp",
    "simulate_opponent_adp",
]
