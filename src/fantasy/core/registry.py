"""Fantasy adapter registries for projections, transforms, and exports."""

from __future__ import annotations

from typing import TypedDict

from src.fantasy.core.contracts import (
    DerivedMetricAdapter,
    ExportAdapter,
    MarketTransformAdapter,
    SportProjectionAdapter,
)


class ProjectionAdapterNotFoundError(LookupError):
    """Raised when a projection adapter key is missing."""


class DerivedMetricAdapterNotFoundError(LookupError):
    """Raised when a derived metric adapter key is missing."""


class MarketTransformAdapterNotFoundError(LookupError):
    """Raised when a market transform adapter key is missing."""


class ExportAdapterNotFoundError(LookupError):
    """Raised when an export adapter key is missing."""


class RegistrySummary(TypedDict):
    """Stable inventory of normalized registry keys."""

    projection_keys: tuple[tuple[str, str, str], ...]
    derived_metric_keys: tuple[str, ...]
    market_transform_keys: tuple[tuple[str, str, str], ...]
    export_keys: tuple[tuple[str, str, str], ...]


_PROJECTION_REGISTRY: dict[tuple[str, str, str], SportProjectionAdapter] = {}
_DERIVED_REGISTRY: dict[str, DerivedMetricAdapter] = {}
_MARKET_TRANSFORM_REGISTRY: dict[tuple[str, str, str], MarketTransformAdapter] = {}
_EXPORT_REGISTRY: dict[tuple[str, str, str], ExportAdapter] = {}


def _normalize(value: str) -> str:
    return value.strip().lower()


def _normalize_optional(value: str | None) -> str:
    if value is None:
        return ""
    return _normalize(value)


def register_projection_adapter(
    sport: str,
    metric_id: str,
    horizon: str,
    adapter: SportProjectionAdapter,
) -> None:
    """Register a projection adapter by `(sport, metric_id, horizon)`."""

    key = (_normalize(sport), _normalize(metric_id), _normalize(horizon))
    _PROJECTION_REGISTRY[key] = adapter


def get_projection_adapter(
    sport: str,
    metric_id: str,
    horizon: str,
) -> SportProjectionAdapter:
    """Resolve a projection adapter by normalized key."""

    key = (_normalize(sport), _normalize(metric_id), _normalize(horizon))
    adapter = _PROJECTION_REGISTRY.get(key)
    if adapter is None:
        raise ProjectionAdapterNotFoundError(
            "Projection adapter not found for "
            f"sport='{sport}', metric_id='{metric_id}', horizon='{horizon}'."
        )
    return adapter


def register_derived_metric_adapter(
    derived_metric_id: str,
    adapter: DerivedMetricAdapter,
) -> None:
    """Register a derived metric adapter by `derived_metric_id`."""

    key = _normalize(derived_metric_id)
    _DERIVED_REGISTRY[key] = adapter


def get_derived_metric_adapter(derived_metric_id: str) -> DerivedMetricAdapter:
    """Resolve a derived metric adapter by normalized id."""

    key = _normalize(derived_metric_id)
    adapter = _DERIVED_REGISTRY.get(key)
    if adapter is None:
        raise DerivedMetricAdapterNotFoundError(
            "Derived metric adapter not found for "
            f"derived_metric_id='{derived_metric_id}'."
        )
    return adapter


def register_market_transform_adapter(
    provider: str,
    mode: str,
    operator: str | None,
    adapter: MarketTransformAdapter,
) -> None:
    """Register a market transform adapter by `(provider, mode, operator)`."""

    key = (_normalize(provider), _normalize(mode), _normalize_optional(operator))
    _MARKET_TRANSFORM_REGISTRY[key] = adapter


def get_market_transform_adapter(
    provider: str,
    mode: str,
    operator: str | None,
) -> MarketTransformAdapter:
    """Resolve a market transform adapter by normalized key."""

    key = (_normalize(provider), _normalize(mode), _normalize_optional(operator))
    adapter = _MARKET_TRANSFORM_REGISTRY.get(key)
    if adapter is None:
        raise MarketTransformAdapterNotFoundError(
            "Market transform adapter not found for "
            f"provider='{provider}', mode='{mode}', operator='{operator}'."
        )
    return adapter


def register_export_adapter(
    provider: str,
    mode: str,
    export_kind: str,
    adapter: ExportAdapter,
) -> None:
    """Register an export adapter by `(provider, mode, export_kind)`."""

    key = (_normalize(provider), _normalize(mode), _normalize(export_kind))
    _EXPORT_REGISTRY[key] = adapter


def get_export_adapter(provider: str, mode: str, export_kind: str) -> ExportAdapter:
    """Resolve an export adapter by normalized key."""

    key = (_normalize(provider), _normalize(mode), _normalize(export_kind))
    adapter = _EXPORT_REGISTRY.get(key)
    if adapter is None:
        raise ExportAdapterNotFoundError(
            "Export adapter not found for "
            f"provider='{provider}', mode='{mode}', export_kind='{export_kind}'."
        )
    return adapter


def list_registered_fantasy_adapters() -> RegistrySummary:
    """Return normalized registry keys in stable order."""

    return {
        "projection_keys": tuple(sorted(_PROJECTION_REGISTRY.keys())),
        "derived_metric_keys": tuple(sorted(_DERIVED_REGISTRY.keys())),
        "market_transform_keys": tuple(sorted(_MARKET_TRANSFORM_REGISTRY.keys())),
        "export_keys": tuple(sorted(_EXPORT_REGISTRY.keys())),
    }


def clear_fantasy_registry() -> None:
    """Clear all fantasy adapter registries (test helper)."""

    _PROJECTION_REGISTRY.clear()
    _DERIVED_REGISTRY.clear()
    _MARKET_TRANSFORM_REGISTRY.clear()
    _EXPORT_REGISTRY.clear()
