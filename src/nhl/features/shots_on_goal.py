"""Deterministic NHL shots-on-goal feature engineering for PR#9."""

from __future__ import annotations

import numpy as np
import pandas as pd


def _build_player_averages(
    skater_games: pd.DataFrame,
    *,
    short_window: int,
    long_window: int,
) -> pd.DataFrame:
    """Build rolling and season-average SOG summaries by player."""

    ordered = skater_games.copy()
    ordered["player_id"] = ordered["player_id"].astype("string")
    ordered["game_date"] = pd.to_datetime(ordered["game_date"], errors="coerce")
    ordered["shots_on_goal"] = pd.to_numeric(ordered["shots_on_goal"], errors="coerce")
    ordered = ordered.dropna(subset=["player_id", "game_date", "shots_on_goal"])
    ordered = ordered.sort_values(["player_id", "game_date"])

    grouped = ordered.groupby("player_id", dropna=False)["shots_on_goal"]
    summary = pd.DataFrame(
        {
            "player_id": grouped.mean().index.astype("string"),
            "sog_avg_last_5": grouped.apply(
                lambda values: float(values.tail(short_window).mean())
            ),
            "sog_avg_last_10": grouped.apply(
                lambda values: float(values.tail(long_window).mean())
            ),
            "sog_avg_season": grouped.mean().to_numpy(),
        }
    )
    return summary.reset_index(drop=True)


def build_sog_inference_features(
    inference_rows: pd.DataFrame,
    skater_games: pd.DataFrame,
    rolling_windows: list[int],
    fallback_prediction: float,
) -> pd.DataFrame:
    """Build deterministic PR#9 NHL features and baseline predictions.

    Args:
        inference_rows: Runtime inference rows loaded from configured input.
        skater_games: Canonical skater game history from data provider.
        rolling_windows: Window sizes configured for rolling features.
        fallback_prediction: Baseline prediction when player history is missing.

    Returns:
        Inference rows enriched with deterministic features and prediction baseline.
    """

    if not rolling_windows:
        raise ValueError("feature_rolling_windows must be non-empty.")

    featured = inference_rows.copy()
    featured["player_id"] = featured["player_id"].astype("string")

    short_window = 5 if 5 in rolling_windows else min(rolling_windows)
    long_window = 10 if 10 in rolling_windows else max(rolling_windows)
    averages = _build_player_averages(
        skater_games,
        short_window=short_window,
        long_window=long_window,
    )
    featured = featured.merge(averages, on="player_id", how="left")

    has_history = (
        featured[["sog_avg_last_5", "sog_avg_last_10", "sog_avg_season"]]
        .notna()
        .all(axis=1)
    )
    weighted_prediction = (
        0.5 * featured["sog_avg_last_5"]
        + 0.3 * featured["sog_avg_last_10"]
        + 0.2 * featured["sog_avg_season"]
    )

    featured["predicted_shots_on_goal"] = np.where(
        has_history,
        weighted_prediction,
        float(fallback_prediction),
    )
    featured["predicted_shots_on_goal"] = pd.to_numeric(
        featured["predicted_shots_on_goal"], errors="coerce"
    )
    return featured


__all__ = ["build_sog_inference_features"]
