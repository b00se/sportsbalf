"""Feature-store utilities for MLB live-context feature parity."""

from __future__ import annotations

import logging
from collections.abc import Mapping

import pandas as pd

from src.mlb.features.handedness import build_historical_handedness_features
from src.mlb.features.umpire import (
    add_live_umpire_defaults,
    build_umpire_history_features,
)
from src.mlb.features.venue import add_roof_interactions, normalize_venue_payload
from src.mlb.features.weather import (
    NEUTRAL_WEATHER,
    add_weather_derived_columns,
    normalize_weather_payload,
)

logger = logging.getLogger(__name__)

LIVE_CONTEXT_FEATURE_COLUMNS: list[str] = [
    "pitcher_throws_encoded",
    "projected_batter_stand_mix_L",
    "projected_batter_stand_mix_R",
    "same_hand_matchup_rate",
    "umpire_k_boost_expanding",
    "umpire_sample_size",
    "umpire_known_flag",
    "game_temp_f",
    "humidity_pct",
    "wind_speed_mph",
    "wind_out_to_cf_flag",
    "weather_run_env_idx",
    "humidity_x_temp",
    "weather_known_flag",
    "roof_state",
    "roof_closed_flag",
    "weather_effective_flag",
    "wind_speed_effective",
    "humidity_effective",
]


LIVE_CONTEXT_DEFAULTS: dict[str, float | int | str] = {
    "pitcher_throws_encoded": 0.0,
    "projected_batter_stand_mix_L": 0.5,
    "projected_batter_stand_mix_R": 0.5,
    "same_hand_matchup_rate": 0.5,
    "umpire_k_boost_expanding": 0.0,
    "umpire_sample_size": 0.0,
    "umpire_known_flag": 0,
    "game_temp_f": float(NEUTRAL_WEATHER["game_temp_f"]),
    "humidity_pct": float(NEUTRAL_WEATHER["humidity_pct"]),
    "wind_speed_mph": float(NEUTRAL_WEATHER["wind_speed_mph"]),
    "wind_out_to_cf_flag": int(NEUTRAL_WEATHER["wind_out_to_cf_flag"]),
    "weather_run_env_idx": 0.0,
    "humidity_x_temp": float(NEUTRAL_WEATHER["humidity_pct"])
    * float(NEUTRAL_WEATHER["game_temp_f"]),
    "weather_known_flag": 0,
    "roof_state": "unknown",
    "roof_closed_flag": 0,
    "weather_effective_flag": 1,
    "wind_speed_effective": float(NEUTRAL_WEATHER["wind_speed_mph"]),
    "humidity_effective": float(NEUTRAL_WEATHER["humidity_pct"]),
}


NUMERIC_LIVE_FEATURES: tuple[str, ...] = (
    "pitcher_throws_encoded",
    "projected_batter_stand_mix_L",
    "projected_batter_stand_mix_R",
    "same_hand_matchup_rate",
    "umpire_k_boost_expanding",
    "umpire_sample_size",
    "umpire_known_flag",
    "game_temp_f",
    "humidity_pct",
    "wind_speed_mph",
    "wind_out_to_cf_flag",
    "weather_run_env_idx",
    "humidity_x_temp",
    "weather_known_flag",
    "roof_closed_flag",
    "weather_effective_flag",
    "wind_speed_effective",
    "humidity_effective",
)


def _coerce_live_numeric_defaults(frame: pd.DataFrame) -> pd.DataFrame:
    enriched = frame.copy()
    for col in NUMERIC_LIVE_FEATURES:
        default = float(LIVE_CONTEXT_DEFAULTS[col])
        enriched[col] = pd.to_numeric(enriched[col], errors="coerce").fillna(default)
    enriched["roof_state"] = (
        enriched["roof_state"].fillna("unknown").astype(str).str.lower()
    )
    return enriched


def _apply_weather_defaults(frame: pd.DataFrame) -> pd.DataFrame:
    enriched = frame.copy()
    normalized = normalize_weather_payload(
        {
            "game_temp_f": enriched.get("game_temp_f"),
            "humidity_pct": enriched.get("humidity_pct"),
            "wind_speed_mph": enriched.get("wind_speed_mph"),
            "wind_out_to_cf_flag": enriched.get("wind_out_to_cf_flag"),
        }
    )
    for key, value in normalized.items():
        if key not in enriched.columns:
            enriched[key] = value
    return enriched


def ensure_live_feature_defaults(frame: pd.DataFrame) -> pd.DataFrame:
    """Guarantee all live-context feature columns exist with neutral defaults.

    Args:
        frame: Input data frame for training or inference.

    Returns:
        Frame with all live feature columns populated.
    """

    enriched = frame.copy()
    for col, default in LIVE_CONTEXT_DEFAULTS.items():
        if col not in enriched.columns:
            enriched[col] = default

    enriched = _apply_weather_defaults(enriched)
    enriched = add_weather_derived_columns(enriched)
    if "roof_state" not in enriched.columns:
        enriched["roof_state"] = "unknown"
    if "weather_effective_flag" not in enriched.columns:
        enriched["weather_effective_flag"] = 1
    if "roof_closed_flag" not in enriched.columns:
        enriched["roof_closed_flag"] = 0
    enriched = add_roof_interactions(enriched)
    enriched = add_live_umpire_defaults(enriched)
    enriched = _coerce_live_numeric_defaults(enriched)
    return enriched


def build_historical_live_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Build leakage-safe live-context features for historical training rows.

    Args:
        frame: Historical game frame.

    Returns:
        Feature-enriched historical frame.
    """

    enriched = build_historical_handedness_features(frame)
    enriched = build_umpire_history_features(enriched)

    if "weather_known_flag" not in enriched.columns:
        enriched["weather_known_flag"] = 0

    # Historical raw extracts generally lack complete weather/roof snapshots.
    # Keep these neutral until a historical weather backfill is materialized.
    venue_payload = normalize_venue_payload({"roof_state": enriched.get("roof_state")})
    for key, value in venue_payload.items():
        if key not in enriched.columns:
            enriched[key] = value

    enriched = ensure_live_feature_defaults(enriched)
    return enriched


def merge_live_feature_frame(
    predictions: pd.DataFrame,
    live_features: pd.DataFrame,
    *,
    join_keys: tuple[str, ...] = ("pitcher_id", "opponent_team"),
) -> pd.DataFrame:
    """Merge normalized live features onto prediction rows.

    Args:
        predictions: Inference rows before model scoring.
        live_features: Feature frame from live-context service.
        join_keys: Merge keys present in both frames.

    Returns:
        Prediction frame with merged live-context columns.
    """

    if live_features.empty:
        return ensure_live_feature_defaults(predictions)

    keys = [
        key
        for key in join_keys
        if key in predictions.columns and key in live_features.columns
    ]
    if not keys:
        logger.warning(
            "No common live-feature join keys found. Using neutral defaults."
        )
        return ensure_live_feature_defaults(predictions)

    keep_cols = keys + [
        col for col in LIVE_CONTEXT_FEATURE_COLUMNS if col in live_features.columns
    ]
    deduped = live_features[keep_cols].drop_duplicates(subset=keys, keep="last")
    merged = predictions.merge(deduped, on=keys, how="left")
    return ensure_live_feature_defaults(merged)


def coverage_metrics(frame: pd.DataFrame) -> Mapping[str, float]:
    """Compute feature availability coverage diagnostics for logging."""

    if frame.empty:
        return {
            "weather_known_pct": 0.0,
            "roof_known_pct": 0.0,
            "umpire_known_pct": 0.0,
            "handedness_known_pct": 0.0,
        }

    weather_known = pd.to_numeric(
        frame.get("weather_known_flag"), errors="coerce"
    ).fillna(0)
    roof_series = frame.get("roof_state", pd.Series("unknown", index=frame.index))
    roof_known = (~roof_series.astype(str).str.lower().eq("unknown")).astype(float)
    umpire_known = pd.to_numeric(
        frame.get("umpire_known_flag"), errors="coerce"
    ).fillna(0)
    handedness_known = (
        pd.to_numeric(frame.get("pitcher_throws_encoded"), errors="coerce")
        .fillna(0)
        .abs()
        .gt(0)
        .astype(float)
    )

    return {
        "weather_known_pct": float(weather_known.mean()),
        "roof_known_pct": float(roof_known.mean()),
        "umpire_known_pct": float(umpire_known.mean()),
        "handedness_known_pct": float(handedness_known.mean()),
    }
