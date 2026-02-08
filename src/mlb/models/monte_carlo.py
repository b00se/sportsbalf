"""Monte Carlo helpers for strikeout simulations."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .distributions import ResidualBootstrapper


@dataclass
class MonteCarloConfig:
    simulations: int = 10_000
    random_seed: int | None = None


def _ev_from_decimal(
    prob_win: float, decimal_price: float, prob_push: float = 0.0
) -> float:
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
    sampler: ResidualBootstrapper | None = None,
    pitcher_id: float | None = None,
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

    if sampler is not None:
        sims = sampler.sample_counts(
            mean=mean, pitcher_id=pitcher_id, simulations=config.simulations, rng=rng
        )
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
    std_dev: float | str | pd.Series,
    config: MonteCarloConfig | None = None,
    sampler: ResidualBootstrapper | None = None,
    line_col: str = "k_line",
) -> pd.DataFrame:
    """Run Monte Carlo simulations for each pitcher line."""
    config = config or MonteCarloConfig()
    rng = np.random.default_rng(config.random_seed)

    if isinstance(std_dev, str):
        if std_dev not in lines.columns:
            raise KeyError(f"Missing std-dev column '{std_dev}' in simulation frame.")
        std_values = pd.to_numeric(lines[std_dev], errors="coerce").to_numpy()
    elif isinstance(std_dev, pd.Series):
        std_values = pd.to_numeric(
            std_dev.reindex(lines.index), errors="coerce"
        ).to_numpy()
    else:
        std_values = np.full(len(lines), float(std_dev), dtype=float)

    results = []
    for i, row in enumerate(lines.itertuples(index=False)):
        try:
            line_value = getattr(row, line_col)
        except AttributeError as exc:
            raise KeyError(
                f"Missing line column '{line_col}' in simulation frame."
            ) from exc
        stats = simulate_row(
            mean=getattr(row, mean_col),
            std_dev=std_values[i],
            strikeout_line=line_value,
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

    sim_frame = pd.DataFrame(results)
    return pd.concat([lines.reset_index(drop=True), sim_frame], axis=1)
