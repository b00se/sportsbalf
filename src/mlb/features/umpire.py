"""Umpire feature builders with leakage-safe historical calculations."""

from __future__ import annotations

import pandas as pd


def build_umpire_history_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Build shifted expanding umpire strikeout-rate context.

    Args:
        frame: Historical pitcher-game frame.

    Returns:
        Frame with umpire-derived model columns.
    """

    enriched = frame.copy()
    if "umpire" not in enriched.columns:
        enriched["umpire"] = "unknown"

    enriched["umpire"] = enriched["umpire"].fillna("unknown").astype(str)
    enriched = enriched.sort_values(["umpire", "game_date"]).reset_index(drop=True)

    grouped = enriched.groupby("umpire", sort=False)
    prior_k = grouped["strikeouts"].cumsum().groupby(enriched["umpire"]).shift(1)
    prior_games = grouped.cumcount().astype(float)

    global_k_mean = float(pd.to_numeric(enriched["strikeouts"], errors="coerce").mean())
    if pd.isna(global_k_mean):
        global_k_mean = 0.0

    denominator = prior_games.where(prior_games > 0)
    umpire_k_avg = (prior_k / denominator).fillna(global_k_mean)
    enriched["umpire_k_boost_expanding"] = umpire_k_avg - global_k_mean
    enriched["umpire_sample_size"] = prior_games.astype(float)
    enriched["umpire_known_flag"] = (enriched["umpire"] != "unknown").astype(int)

    return enriched.sort_values(["pitcher", "game_date"]).reset_index(drop=True)


def add_live_umpire_defaults(frame: pd.DataFrame) -> pd.DataFrame:
    """Ensure umpire feature columns exist for inference rows."""

    enriched = frame.copy()
    for col, default in {
        "umpire_k_boost_expanding": 0.0,
        "umpire_sample_size": 0.0,
        "umpire_known_flag": 0,
    }.items():
        if col not in enriched.columns:
            enriched[col] = default
        enriched[col] = pd.to_numeric(enriched[col], errors="coerce").fillna(default)

    return enriched
