"""Residual bootstrap utilities for NHL shots-on-goal simulation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass
class SOGResidualBootstrapper:
    """Bootstrap residuals at the player level with global fallback."""

    player_residuals: dict[str, np.ndarray]
    global_residuals: np.ndarray
    min_history: int = 5
    mix_global_prob: float = 0.25
    min_sigma: float = 0.25

    @classmethod
    def from_games(
        cls,
        games: pd.DataFrame,
        prediction_col: str = "prediction",
        min_history: int = 5,
        mix_global_prob: float = 0.25,
        min_sigma: float = 0.25,
    ) -> "SOGResidualBootstrapper":
        """Create a residual bootstrapper from historical game-level data."""

        required = {"player_id", "shots_on_goal", prediction_col}
        missing = required - set(games.columns)
        if missing:
            raise ValueError(
                "games DataFrame missing required columns: "
                f"{sorted(missing)}"
            )

        valid = games[list(required)].dropna().copy()
        if valid.empty:
            raise ValueError("No valid rows to build NHL residual bootstrapper.")

        residuals = (
            valid["shots_on_goal"].to_numpy(dtype=float)
            - valid[prediction_col].to_numpy(dtype=float)
        )
        global_residuals = residuals[~np.isnan(residuals)]
        if global_residuals.size == 0:
            raise ValueError("Residual pool is empty after filtering NaNs.")

        player_residuals: dict[str, np.ndarray] = {}
        grouped = valid.groupby("player_id", dropna=False)
        for player_id, group in grouped:
            player_diffs = (
                group["shots_on_goal"].to_numpy(dtype=float)
                - group[prediction_col].to_numpy(dtype=float)
            )
            player_diffs = player_diffs[~np.isnan(player_diffs)]
            if player_diffs.size >= min_history:
                player_residuals[str(player_id)] = player_diffs

        return cls(
            player_residuals=player_residuals,
            global_residuals=global_residuals,
            min_history=min_history,
            mix_global_prob=mix_global_prob,
            min_sigma=min_sigma,
        )

    def can_bootstrap(self, entity_id: str | None = None) -> bool:
        """Return whether a player-specific residual pool is available."""

        if entity_id is None:
            return False
        pool = self.player_residuals.get(str(entity_id))
        return pool is not None and pool.size >= self.min_history

    def _pool(self, entity_id: str | None) -> np.ndarray:
        if entity_id is not None:
            pool = self.player_residuals.get(str(entity_id))
            if pool is not None and pool.size >= self.min_history:
                return pool
        return self.global_residuals

    def sample_counts(
        self,
        mean: float,
        entity_id: str | None = None,
        simulations: int = 0,
        rng: np.random.Generator | None = None,
    ) -> np.ndarray:
        """Sample simulated count outcomes from residual pools."""

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
            draws[mix_mask] = rng.choice(
                self.global_residuals,
                size=int(mix_mask.sum()),
                replace=True,
            )
        if (~mix_mask).any():
            draws[~mix_mask] = rng.choice(
                residual_pool,
                size=int((~mix_mask).sum()),
                replace=True,
            )

        if self.min_sigma > 0:
            draws += rng.normal(0.0, self.min_sigma, size=simulations)

        samples = mean + draws
        return np.clip(np.rint(samples), 0.0, None)


__all__ = ["SOGResidualBootstrapper"]
