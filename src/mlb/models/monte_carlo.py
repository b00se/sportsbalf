"""Monte Carlo helpers for strikeout simulations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import numpy as np
import pandas as pd

from .distributions import ResidualBootstrapper

@dataclass
class MonteCarloConfig:
    simulations: int = 10_000
    random_seed: Optional[int] = None


def _ev_from_decimal(prob_win: float, decimal_price: float, prob_push: float = 0.0) -> float:
    """Return expected profit per 1 unit stake using decimal odds."""
    profit_on_win = decimal_price - 1.0
    prob_loss = max(0.0, 1.0 - prob_win - prob_push)
    return prob_win * profit_on_win - prob_loss


def _edge_from_decimal(prob_win: float, decimal_price: float) -> float:
    implied = 1.0 / decimal_price if decimal_price else 0.0
    return prob_win - implied


def simulate_row(
    mean: float,
    std_dev: float,
    strikeout_line: float,
    config: MonteCarloConfig,
    rng: np.random.Generator,
    sampler: Optional[ResidualBootstrapper] = None,
    pitcher_id: Optional[float] = None,
) -> dict[str, float]:
    """Simulate strikeout distribution for a single pitcher line."""
    if np.isnan(mean):
        return {
            "prob_over": np.nan,
            "prob_under": np.nan,
            "prob_push": np.nan,
            "simulated_mean": np.nan,
            "simulated_std": np.nan,
            "simulated_median": np.nan,
        }

    if std_dev is None or np.isnan(std_dev) or std_dev <= 0:
        std_dev = 1.0

    use_sampler = False
    if sampler is not None:
        can_bootstrap = getattr(sampler, "can_bootstrap", None)
        if callable(can_bootstrap):
            try:
                use_sampler = bool(can_bootstrap(pitcher_id))
            except Exception:
                use_sampler = False
        else:
            use_sampler = True
    if use_sampler:
        sims = sampler.sample_counts(mean=mean, pitcher_id=pitcher_id, simulations=config.simulations, rng=rng)
    else:
        scale = max(float(std_dev), 1e-6)
        sims = rng.normal(loc=mean, scale=scale, size=config.simulations)
        sims = np.clip(np.rint(sims), 0.0, None)

    prob_over = float(np.mean(sims > strikeout_line))
    prob_under = float(np.mean(sims < strikeout_line))

    # Account for any pushes (integer lines) when both comparisons are exclusive.
    prob_push = max(0.0, 1.0 - prob_over - prob_under)

    return {
        "prob_over": prob_over,
        "prob_under": prob_under,
        "prob_push": prob_push,
        "simulated_mean": float(np.mean(sims)),
        "simulated_std": float(np.std(sims, ddof=1)),
        "simulated_median": float(np.median(sims)),
    }


def apply_simulations(
    lines: pd.DataFrame,
    mean_col: str,
    std_dev: float,
    config: MonteCarloConfig | None = None,
    sampler: Optional[ResidualBootstrapper] = None,
) -> pd.DataFrame:
    """Run Monte Carlo simulations for each pitcher line."""
    config = config or MonteCarloConfig()
    rng = np.random.default_rng(config.random_seed)

    if isinstance(std_dev, str):
        std_values = pd.to_numeric(lines[std_dev], errors="coerce").to_numpy()
    elif np.isscalar(std_dev):
        std_values = np.full(len(lines), float(std_dev))
    else:
        std_values = pd.to_numeric(np.asarray(std_dev), errors="coerce")
        if std_values.shape[0] != len(lines):
            raise ValueError("std_dev length must match number of rows in lines")

    results = []
    for idx, row in enumerate(lines.itertuples(index=False)):
        sigma = std_values[idx] if idx < len(std_values) else float(std_dev)
        stats = simulate_row(
            mean=getattr(row, mean_col),
            std_dev=sigma,
            strikeout_line=row.k_line,
            config=config,
            rng=rng,
            sampler=sampler,
            pitcher_id=getattr(row, "pitcher_id", np.nan),
        )

        over_decimal = getattr(row, "over_decimal_price", np.nan)
        under_decimal = getattr(row, "under_decimal_price", np.nan)

        prob_over = stats["prob_over"]
        prob_under = stats["prob_under"]
        prob_push = stats["prob_push"]

        stats.update(
            {
                "ev_over": _ev_from_decimal(prob_over, over_decimal, prob_push),
                "ev_under": _ev_from_decimal(prob_under, under_decimal, prob_push),
                "edge_over": _edge_from_decimal(prob_over, over_decimal),
                "edge_under": _edge_from_decimal(prob_under, under_decimal),
            }
        )
        results.append(stats)

    return lines.join(pd.DataFrame(results))
