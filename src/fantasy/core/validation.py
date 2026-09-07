"""Validation helpers for the unified fantasy projection config."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from src.fantasy.core.contracts import DerivedMetricSpec, MarketDefinition
from src.fantasy.core.derived import validate_derived_metric_dependencies

ALLOWED_HORIZONS: frozenset[str] = frozenset({"game", "day", "week", "slate", "season"})
ALLOWED_MODES: frozenset[str] = frozenset(
    {
        "season_long_tournament",
        "short_slate_fantasy",
        "single_game_pickem",
        "season_long_stat_pickem",
    }
)
ALLOWED_UNRESOLVED_POLICIES: frozenset[str] = frozenset({"fail", "warn", "drop"})
_MODE_SHAPE_RULES: dict[str, tuple[str, ...]] = {
    "season_long_tournament": ("roster", "advancement", "payouts"),
    "short_slate_fantasy": ("roster", "slate_selection_rule"),
    "single_game_pickem": ("slip_constraints", "payout_ladder"),
    "season_long_stat_pickem": ("slip_constraints", "payout_ladder"),
}


class FantasyConfigValidationError(ValueError):
    """Raised when fantasy unified config wiring is structurally invalid."""


@dataclass(frozen=True, slots=True)
class BaseMetricEntry:
    """Normalized declaration for a base metric adapter source."""

    metric_id: str
    horizon: str
    adapter_key: str


def _require_mapping(
    payload: Mapping[str, Any],
    key: str,
    *,
    path: str,
) -> Mapping[str, Any]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        raise FantasyConfigValidationError(f"Invalid required mapping '{path}.{key}'.")
    return value


def _require_non_empty_str(payload: Mapping[str, Any], key: str, *, path: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise FantasyConfigValidationError(
            f"Invalid required field '{path}.{key}': expected non-empty string."
        )
    return value.strip()


def _normalize_horizon(value: str, *, path: str) -> str:
    normalized = value.strip().lower()
    if normalized not in ALLOWED_HORIZONS:
        raise FantasyConfigValidationError(
            f"Invalid horizon '{path}': '{value}'. Allowed={sorted(ALLOWED_HORIZONS)}."
        )
    return normalized


def _normalize_mode(value: str, *, path: str) -> str:
    normalized = value.strip().lower()
    if normalized not in ALLOWED_MODES:
        raise FantasyConfigValidationError(
            f"Invalid mode '{path}': '{value}'. Allowed={sorted(ALLOWED_MODES)}."
        )
    return normalized


def normalize_mode(value: str, *, path: str) -> str:
    """Return validated normalized mode value."""

    return _normalize_mode(value, path=path)


def parse_base_metrics(raw_metrics: Mapping[str, Any]) -> tuple[BaseMetricEntry, ...]:
    """Parse and validate base metric declarations."""

    base_metrics = raw_metrics.get("base_metrics")
    if not isinstance(base_metrics, list) or not base_metrics:
        raise FantasyConfigValidationError(
            "Invalid required field 'metrics.base_metrics': expected non-empty list."
        )

    parsed: list[BaseMetricEntry] = []
    for idx, raw_entry in enumerate(base_metrics):
        if not isinstance(raw_entry, Mapping):
            raise FantasyConfigValidationError(
                f"Invalid field 'metrics.base_metrics[{idx}]': expected mapping."
            )
        metric_id = _require_non_empty_str(
            raw_entry, "metric_id", path=f"metrics.base_metrics[{idx}]"
        ).lower()
        horizon = _normalize_horizon(
            _require_non_empty_str(
                raw_entry, "horizon", path=f"metrics.base_metrics[{idx}]"
            ),
            path=f"metrics.base_metrics[{idx}].horizon",
        )
        adapter_key = _require_non_empty_str(
            raw_entry, "adapter_key", path=f"metrics.base_metrics[{idx}]"
        ).lower()
        parsed.append(
            BaseMetricEntry(
                metric_id=metric_id, horizon=horizon, adapter_key=adapter_key
            )
        )
    return tuple(parsed)


def parse_derived_metrics(
    raw_metrics: Mapping[str, Any],
) -> tuple[DerivedMetricSpec, ...]:
    """Parse and validate derived metric declarations."""

    derived_metrics = raw_metrics.get("derived_metrics", [])
    if derived_metrics is None:
        return ()
    if not isinstance(derived_metrics, list):
        raise FantasyConfigValidationError(
            "Invalid field 'metrics.derived_metrics': expected list."
        )

    parsed: list[DerivedMetricSpec] = []
    for idx, raw_entry in enumerate(derived_metrics):
        if not isinstance(raw_entry, Mapping):
            raise FantasyConfigValidationError(
                f"Invalid field 'metrics.derived_metrics[{idx}]': expected mapping."
            )
        derived_metric_id = _require_non_empty_str(
            raw_entry,
            "derived_metric_id",
            path=f"metrics.derived_metrics[{idx}]",
        ).lower()
        input_metric_ids = raw_entry.get("input_metric_ids")
        if not isinstance(input_metric_ids, list) or not input_metric_ids:
            raise FantasyConfigValidationError(
                "Invalid field "
                f"'metrics.derived_metrics[{idx}].input_metric_ids': "
                "expected non-empty list."
            )
        normalized_input_ids: list[str] = []
        for jdx, metric in enumerate(input_metric_ids):
            if not isinstance(metric, str) or not metric.strip():
                raise FantasyConfigValidationError(
                    "Invalid field "
                    f"'metrics.derived_metrics[{idx}].input_metric_ids[{jdx}]': "
                    "expected non-empty string."
                )
            normalized_input_ids.append(metric.strip().lower())
        transform_id = _require_non_empty_str(
            raw_entry, "transform_id", path=f"metrics.derived_metrics[{idx}]"
        ).lower()
        raw_params = raw_entry.get("transform_params", {})
        if not isinstance(raw_params, Mapping):
            raise FantasyConfigValidationError(
                "Invalid field "
                f"'metrics.derived_metrics[{idx}].transform_params': expected mapping."
            )
        params: dict[str, float] = {}
        for key, value in raw_params.items():
            if not isinstance(key, str) or not key.strip():
                raise FantasyConfigValidationError(
                    "Invalid field "
                    f"'metrics.derived_metrics[{idx}].transform_params': "
                    "expected string keys."
                )
            try:
                params[key.strip()] = float(value)
            except (TypeError, ValueError) as exc:
                raise FantasyConfigValidationError(
                    "Invalid field "
                    f"'metrics.derived_metrics[{idx}].transform_params[{key}]': "
                    "expected numeric value."
                ) from exc

        parsed.append(
            DerivedMetricSpec(
                derived_metric_id=derived_metric_id,
                input_metric_ids=tuple(normalized_input_ids),
                transform_id=transform_id,
                transform_params=params,
            )
        )
    return tuple(parsed)


def validate_derived_dependencies(
    base_metrics: Iterable[BaseMetricEntry],
    derived_metrics: Iterable[DerivedMetricSpec],
) -> None:
    """Validate that derived metrics only reference declared base metrics."""

    base_metric_ids = {entry.metric_id for entry in base_metrics}
    try:
        validate_derived_metric_dependencies(
            tuple(derived_metrics),
            declared_base_metric_ids=base_metric_ids,
        )
    except ValueError as exc:
        raise FantasyConfigValidationError(
            f"Invalid derived metric dependency wiring: {exc}"
        ) from exc


def validate_mapping_section(raw_mapping: Mapping[str, Any]) -> None:
    """Validate mapping section required fields and policy."""

    _require_non_empty_str(raw_mapping, "player_id_map_path", path="mapping")
    unresolved_policy = _require_non_empty_str(
        raw_mapping, "unresolved_policy", path="mapping"
    )
    policy = unresolved_policy.strip().lower()
    if policy not in ALLOWED_UNRESOLVED_POLICIES:
        raise FantasyConfigValidationError(
            "Invalid field 'mapping.unresolved_policy': "
            f"'{unresolved_policy}' not in {sorted(ALLOWED_UNRESOLVED_POLICIES)}."
        )


def parse_market_definitions(
    *,
    raw_markets: Mapping[str, Any],
    provider: str,
    sport: str,
    mode: str,
) -> tuple[str, tuple[MarketDefinition, ...]]:
    """Parse market mode and definitions."""

    market_mode = _normalize_mode(
        _require_non_empty_str(raw_markets, "mode", path="markets"),
        path="markets.mode",
    )
    if market_mode != mode:
        raise FantasyConfigValidationError(
            "Invalid market mode wiring: 'markets.mode' must match 'contest.mode'."
        )

    definitions = raw_markets.get("definitions")
    if not isinstance(definitions, list) or not definitions:
        raise FantasyConfigValidationError(
            "Invalid required field 'markets.definitions': expected non-empty list."
        )

    parsed: list[MarketDefinition] = []
    for idx, raw_definition in enumerate(definitions):
        if not isinstance(raw_definition, Mapping):
            raise FantasyConfigValidationError(
                f"Invalid field 'markets.definitions[{idx}]': expected mapping."
            )
        market_id = _require_non_empty_str(
            raw_definition,
            "market_id",
            path=f"markets.definitions[{idx}]",
        ).lower()
        metric_id = _require_non_empty_str(
            raw_definition,
            "metric_id",
            path=f"markets.definitions[{idx}]",
        ).lower()
        horizon = _normalize_horizon(
            _require_non_empty_str(
                raw_definition, "horizon", path=f"markets.definitions[{idx}]"
            ),
            path=f"markets.definitions[{idx}].horizon",
        )
        operator_raw = raw_definition.get("operator")
        operator: str | None
        if operator_raw is None:
            operator = None
        else:
            if not isinstance(operator_raw, str) or not operator_raw.strip():
                raise FantasyConfigValidationError(
                    f"Invalid field 'markets.definitions[{idx}].operator': "
                    "expected string."
                )
            operator = operator_raw.strip().lower()

        line_value_raw = raw_definition.get("line_value")
        line_value: float | None
        if line_value_raw is None:
            line_value = None
        else:
            try:
                line_value = float(line_value_raw)
            except (TypeError, ValueError) as exc:
                raise FantasyConfigValidationError(
                    f"Invalid field 'markets.definitions[{idx}].line_value': "
                    "expected number."
                ) from exc
            if operator is None:
                raise FantasyConfigValidationError(
                    "Invalid market definition: operator is required when "
                    "line_value is set."
                )

        window_start = _require_non_empty_str(
            raw_definition,
            "window_start",
            path=f"markets.definitions[{idx}]",
        )
        window_end = _require_non_empty_str(
            raw_definition,
            "window_end",
            path=f"markets.definitions[{idx}]",
        )
        game_id_raw = raw_definition.get("game_id")
        game_id = None if game_id_raw is None else str(game_id_raw).strip() or None

        parsed.append(
            MarketDefinition(
                market_id=market_id,
                provider=provider,
                sport=sport,
                mode=mode,
                metric_id=metric_id,
                horizon=horizon,
                operator=operator,
                line_value=line_value,
                window_start=window_start,
                window_end=window_end,
                game_id=game_id,
            )
        )
    return market_mode, tuple(parsed)


def validate_metric_horizon_wiring(
    *,
    base_metrics: Iterable[BaseMetricEntry],
    derived_metrics: Iterable[DerivedMetricSpec],
    markets: Iterable[MarketDefinition],
    scoring_ruleset_id: str | None,
) -> None:
    """Validate market references to declared base/derived metric horizons."""

    base_horizons: dict[str, set[str]] = {}
    for base_metric in base_metrics:
        base_horizons.setdefault(base_metric.metric_id, set()).add(base_metric.horizon)

    derived_horizons: dict[str, set[str]] = {}
    for spec in derived_metrics:
        input_horizons = [
            base_horizons.get(metric_id, set()) for metric_id in spec.input_metric_ids
        ]
        if not input_horizons:
            derived_horizons[spec.derived_metric_id] = set()
            continue
        allowed_horizons = set.intersection(*input_horizons)
        derived_horizons[spec.derived_metric_id] = allowed_horizons

    for market in markets:
        allowed_base_horizons = base_horizons.get(market.metric_id)
        if allowed_base_horizons is not None:
            if market.horizon not in allowed_base_horizons:
                raise FantasyConfigValidationError(
                    "Invalid metric/horizon market wiring for "
                    f"metric_id='{market.metric_id}', horizon='{market.horizon}'."
                )
            continue

        allowed_derived_horizons = derived_horizons.get(market.metric_id)
        if (
            allowed_derived_horizons is None
            or market.horizon not in allowed_derived_horizons
        ):
            raise FantasyConfigValidationError(
                "Invalid metric/horizon market wiring for "
                f"metric_id='{market.metric_id}', horizon='{market.horizon}'."
            )
        if not scoring_ruleset_id:
            raise FantasyConfigValidationError(
                "Invalid market wiring: scoring_ruleset_id is required for "
                f"derived metric '{market.metric_id}'."
            )


def validate_mode_config_shape(mode: str, mode_config: Mapping[str, Any]) -> None:
    """Apply Phase-0 soft shape validation for mode config fields."""

    expected_fields = _MODE_SHAPE_RULES.get(mode, ())
    for field_name in expected_fields:
        if field_name not in mode_config:
            continue
        value = mode_config[field_name]
        if field_name in {"slate_selection_rule"}:
            if not isinstance(value, str | Mapping):
                raise FantasyConfigValidationError(
                    f"Invalid mode_config.{field_name}: expected string or mapping."
                )
            continue
        if not isinstance(value, Mapping | list):
            raise FantasyConfigValidationError(
                f"Invalid mode_config.{field_name}: expected mapping or list."
            )
