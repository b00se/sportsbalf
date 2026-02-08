"""Venue and roof-state helpers for MLB live context features."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

ROOF_STATES = {
    "open",
    "closed",
    "retractable_open",
    "retractable_closed",
    "unknown",
}


def normalize_roof_state(raw_value: object) -> str:
    """Map provider roof text into canonical states."""

    if not isinstance(raw_value, str):
        return "unknown"
    text = raw_value.strip().lower()
    if not text:
        return "unknown"

    if "closed" in text and "retract" in text:
        return "retractable_closed"
    if "open" in text and "retract" in text:
        return "retractable_open"
    if text in {"open", "outdoor", "outside"}:
        return "open"
    if text in {"closed", "indoors", "indoor", "dome"}:
        return "closed"
    if "closed" in text:
        return "closed"
    if "open" in text:
        return "open"
    return "unknown"


def normalize_venue_payload(payload: Mapping[str, object]) -> dict[str, object]:
    """Normalize venue payload into model-ready roof features."""

    state = normalize_roof_state(payload.get("roof_state"))
    closed_flag = int(state in {"closed", "retractable_closed"})
    weather_effective_flag = 1 - closed_flag
    return {
        "roof_state": state,
        "roof_closed_flag": closed_flag,
        "weather_effective_flag": weather_effective_flag,
    }


def add_roof_interactions(frame: pd.DataFrame) -> pd.DataFrame:
    """Add weather interaction columns gated by roof state."""

    enriched = frame.copy()
    enriched["wind_speed_effective"] = (
        enriched["wind_speed_mph"] * enriched["weather_effective_flag"]
    )
    enriched["humidity_effective"] = (
        enriched["humidity_pct"] * enriched["weather_effective_flag"]
    )
    return enriched
