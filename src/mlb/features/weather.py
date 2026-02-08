"""Weather feature normalization helpers for MLB strikeout modeling."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

NEUTRAL_WEATHER: dict[str, float | int] = {
    "game_temp_f": 72.0,
    "humidity_pct": 50.0,
    "wind_speed_mph": 7.5,
    "wind_out_to_cf_flag": 0,
}


def _coerce_float(value: object, *, default: float) -> float:
    """Return ``value`` coerced to float with a default fallback.

    Args:
        value: Input value from a provider payload.
        default: Value used when coercion fails.

    Returns:
        Coerced floating-point value.
    """

    try:
        numeric = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    if pd.isna(numeric):
        return default
    return numeric


def _wind_out_flag(raw_direction: object) -> int:
    """Map free-form wind direction text into a center-field out flag."""

    if not isinstance(raw_direction, str):
        return 0
    normalized = raw_direction.strip().lower()
    if not normalized:
        return 0
    out_tokens = {
        "out",
        "out to cf",
        "out to center",
        "out to center field",
        "blowing out",
        "to center",
        "toward cf",
    }
    return int(normalized in out_tokens)


def normalize_weather_payload(payload: Mapping[str, object]) -> dict[str, float | int]:
    """Normalize weather provider values into model feature keys.

    Args:
        payload: Raw provider payload.

    Returns:
        Normalized weather features with stable defaults.
    """

    game_temp_f = _coerce_float(payload.get("game_temp_f"), default=72.0)
    humidity_pct = _coerce_float(payload.get("humidity_pct"), default=50.0)
    wind_speed_mph = _coerce_float(payload.get("wind_speed_mph"), default=7.5)
    wind_out_to_cf_flag = _wind_out_flag(payload.get("wind_direction"))
    if "wind_out_to_cf_flag" in payload:
        wind_out_to_cf_flag = int(
            _coerce_float(payload.get("wind_out_to_cf_flag"), default=0.0) > 0
        )

    return {
        "game_temp_f": game_temp_f,
        "humidity_pct": max(0.0, min(humidity_pct, 100.0)),
        "wind_speed_mph": max(0.0, wind_speed_mph),
        "wind_out_to_cf_flag": int(wind_out_to_cf_flag),
    }


def add_weather_derived_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Add composite weather columns used by the model.

    Args:
        frame: Input frame containing weather base columns.

    Returns:
        Copy of ``frame`` with derived weather features.
    """

    enriched = frame.copy()
    enriched["weather_run_env_idx"] = (
        0.0025 * enriched["game_temp_f"]
        + 0.0015 * enriched["humidity_pct"]
        + 0.004 * enriched["wind_speed_mph"] * enriched["wind_out_to_cf_flag"]
    )
    enriched["humidity_x_temp"] = enriched["humidity_pct"] * enriched["game_temp_f"]
    return enriched
