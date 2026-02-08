"""Typed config loading and validation utilities for modular pipelines."""

from __future__ import annotations

from collections.abc import Callable
from numbers import Integral
from pathlib import Path
from typing import Any

from src.core.contracts import PipelineConfig
from src.utils.io import load_config


class ConfigValidationError(ValueError):
    """Raised when a config file is missing required sections."""


_MLB_RUNTIME_CRITICAL_STATS: frozenset[str] = frozenset(
    {
        "strikeouts",
        "outs_recorded",
        "earned_runs",
        "hits_allowed",
        "bb_allowed",
    }
)
_MLB_REQUIRED_KEYS: tuple[str, ...] = ("pitch_data_path", "model_path", "lines_path")
_SECTIONED_SCHEMA_MESSAGE = (
    "Config must use sectioned schema. Required fields: 'pipeline.sport', "
    "'pipeline.stat', and '{sport}.{stat}'. Legacy flat config is not supported."
)


def _validate_sectioned_schema_root(raw_config: dict[str, Any]) -> dict[str, Any]:
    """Validate root schema and return the required `pipeline` mapping.

    Args:
        raw_config: Loaded config root mapping.

    Returns:
        The validated `pipeline` subsection.

    Raises:
        ConfigValidationError: If `pipeline` is missing or not a mapping.
    """

    pipeline_section = raw_config.get("pipeline")
    if not isinstance(pipeline_section, dict):
        raise ConfigValidationError(_SECTIONED_SCHEMA_MESSAGE)
    return pipeline_section


def _validate_required_str(section: dict[str, Any], path: str) -> str:
    """Validate a required non-empty string field from a mapping.

    Args:
        section: Parent mapping containing the field.
        path: Full dotted path to the field.

    Returns:
        Normalized (trimmed) field value.

    Raises:
        ConfigValidationError: If the field is missing, not a string, or blank.
    """

    key = path.rsplit(".", maxsplit=1)[-1]
    value = section.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigValidationError(
            f"Invalid required field '{path}': expected non-empty string."
        )
    return value.strip()


def _validate_required_str_key(section: dict[str, Any], path: str) -> None:
    """Validate a required string key for runtime-critical config fields.

    Args:
        section: Parent mapping containing the key.
        path: Full dotted path to the key.

    Raises:
        ConfigValidationError: If key is missing or not a non-empty string.
    """

    _validate_required_str(section, path)


def _validate_required_non_empty_int_list(section: dict[str, Any], path: str) -> None:
    """Validate a required non-empty list of integers (excluding bool).

    Args:
        section: Parent mapping containing the list.
        path: Full dotted path to the list.

    Raises:
        ConfigValidationError: If the value is missing/empty/not a list, or contains
            non-integer values.
    """

    key = path.rsplit(".", maxsplit=1)[-1]
    value = section.get(key)
    if not isinstance(value, list):
        raise ConfigValidationError(
            f"Invalid required field '{path}': expected non-empty list[int]."
        )
    if not value:
        raise ConfigValidationError(
            f"Invalid required field '{path}': expected non-empty list[int]."
        )

    for idx, item in enumerate(value):
        if isinstance(item, bool) or not isinstance(item, Integral):
            raise ConfigValidationError(
                f"Invalid required field '{path}[{idx}]': expected integer."
            )


def _validate_mlb_stat_section(section: dict[str, Any], path: str) -> None:
    """Validate MLB runtime-critical keys for implemented stats.

    Args:
        section: Active stat config subsection.
        path: Full dotted section path, such as `mlb.strikeouts`.
    """

    for key in _MLB_REQUIRED_KEYS:
        _validate_required_str_key(section, f"{path}.{key}")


def _validate_nfl_pass_attempts_section(section: dict[str, Any], path: str) -> None:
    """Validate NFL pass-attempt runtime-critical keys.

    Args:
        section: Active stat config subsection.
        path: Full dotted section path, such as `nfl.pass_attempts`.
    """

    _validate_required_non_empty_int_list(section, f"{path}.training_years")


_VALIDATORS: dict[tuple[str, str], Callable[[dict[str, Any], str], None]] = {
    **{
        ("mlb", stat): _validate_mlb_stat_section
        for stat in _MLB_RUNTIME_CRITICAL_STATS
    },
    ("nfl", "pass_attempts"): _validate_nfl_pass_attempts_section,
}


def extract_stat_section(
    raw_config: dict[str, Any],
    sport: str,
    stat: str,
) -> dict[str, Any]:
    """Return the resolved sport/stat section from a config payload.

    Requires sectioned schema and resolves the active sport/stat section.
    """

    normalized_sport = sport.strip().lower()
    normalized_stat = stat.strip().lower()

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
    pipeline_section = _validate_sectioned_schema_root(raw_config)
    pipeline_sport = _validate_required_str(pipeline_section, "pipeline.sport")
    pipeline_stat = _validate_required_str(pipeline_section, "pipeline.stat")

    sport = sport_override or pipeline_sport
    stat = stat_override or pipeline_stat

    if not isinstance(sport, str) or not sport.strip():
        raise ConfigValidationError(
            "Invalid required field 'pipeline.sport': expected non-empty string."
        )
    if not isinstance(stat, str) or not stat.strip():
        raise ConfigValidationError(
            "Invalid required field 'pipeline.stat': expected non-empty string."
        )

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
    validator = _VALIDATORS.get((sport, stat))
    if validator is not None:
        validator(section, f"{sport}.{stat}")

    return PipelineConfig(
        config_path=path,
        sport=sport,
        stat=stat,
        raw=raw,
        section=section,
    )
