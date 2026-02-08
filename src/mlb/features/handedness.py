"""Handedness and batter-stand matchup feature builders."""

from __future__ import annotations

import pandas as pd


def _encode_pitcher_throws(series: pd.Series) -> pd.Series:
    mapping = {"L": -1.0, "R": 1.0}
    encoded = series.astype(str).str.upper().map(mapping)
    return encoded.fillna(0.0)


def build_historical_handedness_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Build handedness columns from historical stand/throw data.

    Args:
        frame: Game-level or pitch-level aggregated frame.

    Returns:
        Frame containing model-handedness features.
    """

    enriched = frame.copy()
    if "pitcher_throws" not in enriched.columns:
        if "p_throws" in enriched.columns:
            enriched["pitcher_throws"] = enriched["p_throws"]
        else:
            enriched["pitcher_throws"] = "U"

    if "projected_batter_stand_mix_L" not in enriched.columns:
        enriched["projected_batter_stand_mix_L"] = 0.5
    if "projected_batter_stand_mix_R" not in enriched.columns:
        enriched["projected_batter_stand_mix_R"] = 0.5

    mix_l = pd.to_numeric(enriched["projected_batter_stand_mix_L"], errors="coerce")
    mix_r = pd.to_numeric(enriched["projected_batter_stand_mix_R"], errors="coerce")

    total = (mix_l + mix_r).replace(0, pd.NA)
    mix_l = (mix_l / total).fillna(0.5)
    mix_r = (mix_r / total).fillna(0.5)

    throw_upper = enriched["pitcher_throws"].astype(str).str.upper()
    same_hand = pd.Series(0.5, index=enriched.index, dtype=float)
    same_hand = same_hand.where(~throw_upper.eq("L"), mix_l)
    same_hand = same_hand.where(~throw_upper.eq("R"), mix_r)

    enriched["pitcher_throws_encoded"] = _encode_pitcher_throws(throw_upper)
    enriched["projected_batter_stand_mix_L"] = mix_l
    enriched["projected_batter_stand_mix_R"] = mix_r
    enriched["same_hand_matchup_rate"] = same_hand
    return enriched
