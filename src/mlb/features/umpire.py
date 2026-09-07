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
    enriched = enriched.reset_index(drop=False).rename(columns={"index": "_row_id"})

    global_order = enriched.sort_values(["game_date", "_row_id"]).copy()
    global_k = pd.to_numeric(global_order["strikeouts"], errors="coerce")
    global_order["global_prior_k_avg"] = (
        global_k.expanding(min_periods=1).mean().shift(1)
    )
    global_prior_lookup = global_order.set_index("_row_id")["global_prior_k_avg"]

    enriched = enriched.sort_values(["umpire", "game_date", "_row_id"]).reset_index(
        drop=True
    )

    grouped = enriched.groupby("umpire", sort=False)
    prior_k = grouped["strikeouts"].cumsum().groupby(enriched["umpire"]).shift(1)
    prior_games = grouped.cumcount().astype(float)

    denominator = prior_games.where(prior_games > 0)
    umpire_k_avg = prior_k / denominator
    global_prior = enriched["_row_id"].map(global_prior_lookup)
    enriched["umpire_k_boost_expanding"] = umpire_k_avg - global_prior
    enriched["umpire_k_boost_expanding"] = pd.to_numeric(
        enriched["umpire_k_boost_expanding"], errors="coerce"
    ).fillna(0.0)
    enriched.loc[prior_games == 0, "umpire_k_boost_expanding"] = 0.0
    enriched["umpire_sample_size"] = prior_games.astype(float)
    enriched["umpire_known_flag"] = (enriched["umpire"] != "unknown").astype(int)

    return (
        enriched.sort_values(["pitcher", "game_date", "_row_id"])
        .drop(columns=["_row_id"], errors="ignore")
        .reset_index(drop=True)
    )


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
