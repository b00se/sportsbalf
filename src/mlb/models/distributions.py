"""Residual-based discrete sampling utilities."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class ResidualBootstrapper:
    """Bootstrap residuals at the pitcher level with a global fallback."""

    pitcher_residuals: dict[int, np.ndarray]
    global_residuals: np.ndarray
    min_pitcher_history: int = 5
    mix_global_prob: float = 0.25
    min_sigma: float = 0.5

    @classmethod
    def from_games(
        cls,
        games: pd.DataFrame,
        prediction_col: str = "prediction",
        target_col: str = "strikeouts",
        min_pitcher_history: int = 5,
        mix_global_prob: float = 0.25,
        min_sigma: float = 0.5,
    ) -> ResidualBootstrapper:
        required = {"pitcher_id", target_col, prediction_col}
        missing = required - set(games.columns)
        if missing:
            raise ValueError(
                f"games DataFrame missing required columns: {sorted(missing)}"
            )

        valid = games[["pitcher_id", target_col, prediction_col]].dropna()
        if valid.empty:
            raise ValueError("No valid rows to build residual bootstrapper")

        residuals = valid[target_col].to_numpy() - valid[prediction_col].to_numpy()
        global_residuals = residuals[~np.isnan(residuals)]
        if global_residuals.size == 0:
            raise ValueError("Residuals array is empty after removing NaNs")

        pitcher_groups = valid.groupby("pitcher_id")
        pitcher_residuals: dict[int, np.ndarray] = {}
        for pitcher_id, group in pitcher_groups:
            diffs = group[target_col].to_numpy() - group[prediction_col].to_numpy()
            diffs = diffs[~np.isnan(diffs)]
            if diffs.size >= min_pitcher_history:
                pitcher_residuals[int(pitcher_id)] = diffs

        return cls(
            pitcher_residuals=pitcher_residuals,
            global_residuals=global_residuals,
            min_pitcher_history=min_pitcher_history,
            mix_global_prob=mix_global_prob,
            min_sigma=min_sigma,
        )

    def can_bootstrap(self, entity_id: float | str | None) -> bool:
        return True

    def _residual_pool(self, entity_id: float | str | None) -> np.ndarray:
        if entity_id is not None and not pd.isna(entity_id):
            try:
                key = int(entity_id)
            except (TypeError, ValueError):
                key = None
            if key is not None:
                pool = self.pitcher_residuals.get(key)
                if pool is not None and pool.size >= self.min_pitcher_history:
                    return pool
        return self.global_residuals

    def sample_counts(
        self,
        mean: float,
        entity_id: float | str | None,
        simulations: int,
        rng: np.random.Generator,
    ) -> np.ndarray:
        local_pool = self._residual_pool(entity_id)
        if local_pool.size == 0 and self.global_residuals.size:
            local_pool = self.global_residuals

        draws = np.empty(simulations, dtype=float)
        if self.global_residuals.size and self.mix_global_prob > 0:
            mask = rng.random(simulations) < self.mix_global_prob
        else:
            mask = np.zeros(simulations, dtype=bool)

        if mask.any():
            draws[mask] = rng.choice(
                self.global_residuals, size=mask.sum(), replace=True
            )
        if (~mask).any():
            draws[~mask] = rng.choice(local_pool, size=(~mask).sum(), replace=True)

        if self.min_sigma and self.min_sigma > 0:
            noise = rng.normal(0.0, self.min_sigma, size=simulations)
            draws += noise

        samples = mean + draws
        samples = np.clip(np.rint(samples), 0, None)
        return samples
