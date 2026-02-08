"""Typed config loading and validation utilities for modular pipelines."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.core.contracts import PipelineConfig
from src.utils.io import load_config


class ConfigValidationError(ValueError):
    """Raised when a config file is missing required sections."""


def extract_stat_section(
    raw_config: dict[str, Any],
    sport: str,
    stat: str,
) -> dict[str, Any]:
    """Return the resolved sport/stat section from a config payload.

    Supports both the new sectioned schema and legacy flat schema.
    """

    normalized_sport = sport.strip().lower()
    normalized_stat = stat.strip().lower()

    pipeline_section = raw_config.get("pipeline")
    has_new_schema = isinstance(pipeline_section, dict)

    if not has_new_schema:
        # Legacy fallback: entire config acts as the stat section.
        return dict(raw_config)

    sport_section = raw_config.get(normalized_sport)
    if not isinstance(sport_section, dict):
        raise ConfigValidationError(
            f"Missing sport section '{normalized_sport}' in config."
        )

    stat_section = sport_section.get(normalized_stat)
    if not isinstance(stat_section, dict):
        raise ConfigValidationError(
            "Missing stat section " f"'{normalized_sport}.{normalized_stat}' in config."
        )

    return dict(stat_section)


def _resolve_pipeline_identity(
    raw_config: dict[str, Any],
    sport_override: str | None,
    stat_override: str | None,
) -> tuple[str, str]:
    pipeline_section = raw_config.get("pipeline")

    if not isinstance(pipeline_section, dict):
        # Legacy behavior defaults to MLB strikeouts when schema is flat.
        return (sport_override or "mlb", stat_override or "strikeouts")

    sport = sport_override or pipeline_section.get("sport")
    stat = stat_override or pipeline_section.get("stat")

    if not isinstance(sport, str) or not sport.strip():
        raise ConfigValidationError(
            "Config is missing required field 'pipeline.sport'."
        )
    if not isinstance(stat, str) or not stat.strip():
        raise ConfigValidationError("Config is missing required field 'pipeline.stat'.")

    return sport.strip().lower(), stat.strip().lower()


def load_pipeline_config(
    config_path: str,
    *,
    sport_override: str | None = None,
    stat_override: str | None = None,
) -> PipelineConfig:
    """Load and validate YAML config for the requested sport/stat pipeline."""

    path = Path(config_path)
    raw = load_config(str(path))

    if not isinstance(raw, dict):
        raise ConfigValidationError("Config root must be a mapping.")

    sport, stat = _resolve_pipeline_identity(raw, sport_override, stat_override)
    section = extract_stat_section(raw, sport, stat)

    return PipelineConfig(
        config_path=path,
        sport=sport,
        stat=stat,
        raw=raw,
        section=section,
    )
