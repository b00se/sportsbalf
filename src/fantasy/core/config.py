"""Unified fantasy config loading and validation for Phase 0 core."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.fantasy.core.contracts import (
    ContestConfig,
    DerivedMetricSpec,
    MarketDefinition,
)
from src.fantasy.core.validation import (
    BaseMetricEntry,
    FantasyConfigValidationError,
    normalize_mode,
    parse_base_metrics,
    parse_derived_metrics,
    parse_market_definitions,
    validate_derived_dependencies,
    validate_mapping_section,
    validate_metric_horizon_wiring,
    validate_mode_config_shape,
)
from src.utils.io import load_config


@dataclass(frozen=True, slots=True)
class BaseMetricConfig:
    """Normalized base metric config row."""

    metric_id: str
    horizon: str
    adapter_key: str


@dataclass(frozen=True, slots=True)
class MappingConfig:
    """Player-id mapping config details."""

    player_id_map_path: str
    unresolved_policy: str


@dataclass(frozen=True, slots=True)
class UnifiedFantasyConfig:
    """Validated Phase-0 unified fantasy config payload."""

    config_path: Path
    contest: ContestConfig
    market_mode: str
    markets: tuple[MarketDefinition, ...]
    base_metrics: tuple[BaseMetricConfig, ...]
    derived_metrics: tuple[DerivedMetricSpec, ...]
    mapping: MappingConfig
    raw: dict[str, Any] = field(default_factory=dict)


def _require_mapping(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise FantasyConfigValidationError(
            f"Invalid required section '{key}': expected mapping."
        )
    return value


def _require_non_empty_str(payload: dict[str, Any], key: str, *, path: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise FantasyConfigValidationError(
            f"Invalid required field '{path}.{key}': expected non-empty string."
        )
    return value.strip()


def _normalize_metadata(raw_metadata: Any) -> dict[str, str | int | float]:
    if raw_metadata is None:
        return {}
    if not isinstance(raw_metadata, dict):
        raise FantasyConfigValidationError(
            "Invalid field 'contest.metadata': expected mapping."
        )
    normalized: dict[str, str | int | float] = {}
    for key, value in raw_metadata.items():
        if not isinstance(key, str) or not key.strip():
            raise FantasyConfigValidationError(
                "Invalid field 'contest.metadata': expected non-empty string keys."
            )
        if isinstance(value, str | int | float) and not isinstance(value, bool):
            normalized[key.strip()] = value
            continue
        raise FantasyConfigValidationError(
            "Invalid field "
            f"'contest.metadata[{key}]': expected value type str|int|float."
        )
    return normalized


def _normalize_base_metrics(
    base_metrics: tuple[BaseMetricEntry, ...]
) -> tuple[BaseMetricConfig, ...]:
    return tuple(
        BaseMetricConfig(
            metric_id=entry.metric_id,
            horizon=entry.horizon,
            adapter_key=entry.adapter_key,
        )
        for entry in base_metrics
    )


def load_unified_fantasy_config(config_path: str) -> UnifiedFantasyConfig:
    """Load and validate a Phase-0 unified fantasy configuration.

    Args:
        config_path: YAML path for unified fantasy config.

    Returns:
        Strongly typed config payload used by fantasy-core orchestration.

    Raises:
        FantasyConfigValidationError: If the config is invalid.
    """

    path = Path(config_path)
    raw = load_config(str(path))
    if not isinstance(raw, dict):
        raise FantasyConfigValidationError("Config root must be a mapping.")

    contest_raw = _require_mapping(raw, "contest")
    metrics_raw = _require_mapping(raw, "metrics")
    mapping_raw = _require_mapping(raw, "mapping")
    markets_raw = _require_mapping(raw, "markets")

    contest_id = _require_non_empty_str(contest_raw, "contest_id", path="contest")
    provider = _require_non_empty_str(contest_raw, "provider", path="contest").lower()
    sport = _require_non_empty_str(contest_raw, "sport", path="contest").lower()
    mode = normalize_mode(
        _require_non_empty_str(contest_raw, "mode", path="contest"),
        path="contest.mode",
    )

    scoring_ruleset_raw = contest_raw.get("scoring_ruleset_id")
    scoring_ruleset_id: str | None
    if scoring_ruleset_raw is None:
        scoring_ruleset_id = None
    else:
        if not isinstance(scoring_ruleset_raw, str) or not scoring_ruleset_raw.strip():
            raise FantasyConfigValidationError(
                "Invalid field 'contest.scoring_ruleset_id': expected string or null."
            )
        scoring_ruleset_id = scoring_ruleset_raw.strip()

    mode_config = contest_raw.get("mode_config", {})
    if not isinstance(mode_config, dict):
        raise FantasyConfigValidationError(
            "Invalid field 'contest.mode_config': expected mapping."
        )

    base_metrics = parse_base_metrics(metrics_raw)
    derived_metrics = parse_derived_metrics(metrics_raw)
    validate_derived_dependencies(base_metrics, derived_metrics)
    validate_mapping_section(mapping_raw)
    market_mode, markets = parse_market_definitions(
        raw_markets=markets_raw,
        provider=provider,
        sport=sport,
        mode=mode,
    )
    validate_metric_horizon_wiring(
        base_metrics=base_metrics,
        derived_metrics=derived_metrics,
        markets=markets,
        scoring_ruleset_id=scoring_ruleset_id,
    )
    validate_mode_config_shape(mode, mode_config)

    mapping = MappingConfig(
        player_id_map_path=_require_non_empty_str(
            mapping_raw, "player_id_map_path", path="mapping"
        ),
        unresolved_policy=_require_non_empty_str(
            mapping_raw, "unresolved_policy", path="mapping"
        ).lower(),
    )

    metadata = _normalize_metadata(contest_raw.get("metadata", {}))
    contest = ContestConfig(
        contest_id=contest_id,
        provider=provider,
        sport=sport,
        mode=mode,
        scoring_ruleset_id=scoring_ruleset_id,
        market_definitions=markets,
        mode_config=dict(mode_config),
        metadata=metadata,
    )
    return UnifiedFantasyConfig(
        config_path=path,
        contest=contest,
        market_mode=market_mode,
        markets=markets,
        base_metrics=_normalize_base_metrics(base_metrics),
        derived_metrics=derived_metrics,
        mapping=mapping,
        raw=raw,
    )
    normalize_mode,
