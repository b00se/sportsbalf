"""Registry for sport/stat pipeline implementations."""

from __future__ import annotations

from collections.abc import Callable

from src.core.contracts import SportStatPipeline

PipelineFactory = Callable[[], SportStatPipeline]


class UnknownPipelineError(LookupError):
    """Raised when no pipeline is registered for a sport/stat pair."""


_REGISTRY: dict[tuple[str, str], PipelineFactory] = {}


def _normalize(value: str) -> str:
    return value.strip().lower()


def register_pipeline(sport: str, stat: str, factory: PipelineFactory) -> None:
    """Register a pipeline factory for a sport/stat pair."""

    key = (_normalize(sport), _normalize(stat))
    _REGISTRY[key] = factory


def get_pipeline(sport: str, stat: str) -> SportStatPipeline:
    """Resolve and instantiate a sport/stat pipeline implementation."""

    key = (_normalize(sport), _normalize(stat))
    factory = _REGISTRY.get(key)
    if factory is None:
        raise UnknownPipelineError(
            f"No pipeline registered for sport='{sport}' stat='{stat}'."
        )
    return factory()


def clear_registry() -> None:
    """Clear registry entries (test helper)."""

    _REGISTRY.clear()
