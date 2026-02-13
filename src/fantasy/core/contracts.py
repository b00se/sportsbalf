"""Shared contracts for fantasy projection and market surfaces."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

import pandas as pd


@dataclass(frozen=True, slots=True)
class ProjectionKey:
    """Identity for a projection value on a metric and horizon."""

    entity_id: str
    sport: str
    metric_id: str
    horizon: str
    window_start: str
    window_end: str
    game_id: str | None


@dataclass(frozen=True, slots=True)
class ProjectionDistribution:
    """Distribution summary for a projected metric."""

    mean: float
    p10: float
    p50: float
    p90: float
    stddev: float | None
    params: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ProjectionRow:
    """Single projection row including provenance metadata."""

    key: ProjectionKey
    distribution: ProjectionDistribution
    availability_confidence: float
    source_model_version: str
    source_snapshot_id: str


@dataclass(frozen=True, slots=True)
class DerivedMetricSpec:
    """Specification for deriving a metric from base metrics."""

    derived_metric_id: str
    input_metric_ids: tuple[str, ...]
    transform_id: str
    transform_params: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MarketDefinition:
    """Market definition over a metric/horizon pair."""

    market_id: str
    provider: str
    sport: str
    mode: str
    metric_id: str
    horizon: str
    operator: str | None
    line_value: float | None
    window_start: str
    window_end: str
    game_id: str | None


@dataclass(frozen=True, slots=True)
class ContestConfig:
    """Contest-level configuration with scoped market definitions."""

    contest_id: str
    provider: str
    sport: str
    mode: str
    scoring_ruleset_id: str | None
    market_definitions: tuple[MarketDefinition, ...]
    mode_config: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, str | int | float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ProviderPlayerMapping:
    """Mapping row between provider and internal player identities."""

    provider: str
    sport: str
    provider_player_id: str
    internal_player_id: str
    provider_player_name: str
    canonical_name: str
    is_active: bool
    source: str
    updated_at_utc: str


class SportProjectionAdapter(Protocol):
    """Adapter protocol for base stat projection generation."""

    def project(self, config: ContestConfig) -> pd.DataFrame:
        """Generate base projections for a contest scope."""


class DerivedMetricAdapter(Protocol):
    """Adapter protocol for derived metric computation."""

    def derive(
        self,
        base_projections: pd.DataFrame,
        spec: DerivedMetricSpec,
    ) -> pd.DataFrame:
        """Derive metric rows from base projection rows."""


class MarketTransformAdapter(Protocol):
    """Adapter protocol that transforms projections to market surfaces."""

    def transform(
        self,
        projections: pd.DataFrame,
        market: MarketDefinition,
        config: ContestConfig,
    ) -> pd.DataFrame:
        """Transform projections for one market definition."""


class ExportAdapter(Protocol):
    """Adapter protocol for provider/export-surface rendering."""

    def export(self, surface: pd.DataFrame, config: ContestConfig) -> pd.DataFrame:
        """Export transformed surface to provider schema."""
