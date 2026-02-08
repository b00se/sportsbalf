"""Residual-based bootstrap for NFL QB pass attempts."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import numpy as np
import pandas as pd


@dataclass
class QBResidualBootstrapper:
    """Bootstrap residuals at the quarterback level with a global fallback."""

    qb_residuals: Dict[str, np.ndarray]
    global_residuals: np.ndarray
    min_history: int = 5
    mix_global_prob: float = 0.25
    min_sigma: float = 0.5

    @classmethod
    def from_games(
        cls,
        games: pd.DataFrame,
        prediction_col: str = "prediction",
        min_history: int = 5,
        mix_global_prob: float = 0.25,
        min_sigma: float = 0.5,
    ) -> "QBResidualBootstrapper":
        required = {"qb_id", "pass_attempts", prediction_col}
        missing = required - set(games.columns)
        if missing:
            raise ValueError(f"games DataFrame missing required columns: {sorted(missing)}")

        valid = games[list(required)].dropna()
        if valid.empty:
            raise ValueError("No valid rows to build residual bootstrapper")

        residuals = valid["pass_attempts"].to_numpy() - valid[prediction_col].to_numpy()
        global_residuals = residuals[~np.isnan(residuals)]
        if global_residuals.size == 0:
            raise ValueError("Residuals array is empty after removing NaNs")

        grouped = valid.groupby("qb_id")
        qb_residuals: Dict[str, np.ndarray] = {}
        for qb_id, group in grouped:
            diffs = group["pass_attempts"].to_numpy() - group[prediction_col].to_numpy()
            diffs = diffs[~np.isnan(diffs)]
            if diffs.size >= min_history:
                qb_residuals[str(qb_id)] = diffs

        return cls(
            qb_residuals=qb_residuals,
            global_residuals=global_residuals,
            min_history=min_history,
            mix_global_prob=mix_global_prob,
            min_sigma=min_sigma,
        )

    def can_bootstrap(
        self,
        entity_id: Optional[str] = None,
    ) -> bool:
        if entity_id is None:
            return False
        pool = self.qb_residuals.get(str(entity_id))
        return (pool is not None) and (pool.size >= self.min_history)

    def _pool(self, entity_id: Optional[str]) -> np.ndarray:
        if entity_id is not None:
            pool = self.qb_residuals.get(str(entity_id))
            if pool is not None and pool.size >= self.min_history:
                return pool
        return self.global_residuals

    def sample_counts(
        self,
        mean: float,
        entity_id: Optional[str] = None,
        simulations: int = 0,
        rng: np.random.Generator | None = None,
    ) -> np.ndarray:
        residual_pool = self._pool(entity_id)
        if residual_pool.size == 0 and self.global_residuals.size:
            residual_pool = self.global_residuals

        if rng is None:
            rng = np.random.default_rng()
        draws = np.empty(simulations, dtype=float)
        if self.global_residuals.size and self.mix_global_prob > 0:
            mix_mask = rng.random(simulations) < self.mix_global_prob
        else:
            mix_mask = np.zeros(simulations, dtype=bool)

        if mix_mask.any():
            draws[mix_mask] = rng.choice(self.global_residuals, size=mix_mask.sum(), replace=True)
        if (~mix_mask).any():
            draws[~mix_mask] = rng.choice(residual_pool, size=(~mix_mask).sum(), replace=True)

        if self.min_sigma and self.min_sigma > 0:
            draws += rng.normal(0.0, self.min_sigma, size=simulations)

        samples = mean + draws
        samples = np.clip(np.rint(samples), 0.0, None)
        return samples
