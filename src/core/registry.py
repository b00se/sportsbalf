"""Registry for sport/stat pipeline implementations."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from src.core.contracts import SportStatPipeline

PipelineFactory = Callable[[], SportStatPipeline]


class UnknownPipelineError(LookupError):
    """Raised when no pipeline is registered for a sport/stat pair."""


_REGISTRY: dict[tuple[str, str], PipelineFactory] = {}


@dataclass(frozen=True, slots=True)
class RegisteredPipeline:
    """Normalized metadata for a registered sport/stat pipeline.

    Attributes:
        sport: Lower-cased sport identifier.
        stat: Lower-cased stat identifier.
        factory: Pipeline factory registered for this pair.
    """

    sport: str
    stat: str
    factory: PipelineFactory


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


def list_registered_pipelines() -> tuple[RegisteredPipeline, ...]:
    """List normalized registration entries in stable key order."""

    return tuple(
        RegisteredPipeline(sport=sport, stat=stat, factory=factory)
        for (sport, stat), factory in sorted(_REGISTRY.items())
    )


def is_registered(sport: str, stat: str) -> bool:
    """Return whether a sport/stat pair exists in the registry."""

    key = (_normalize(sport), _normalize(stat))
    return key in _REGISTRY
