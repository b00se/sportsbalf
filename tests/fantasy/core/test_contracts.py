"""Tests for fantasy core contracts."""

from __future__ import annotations

from typing import cast

import pandas as pd
from src.fantasy.core.contracts import (
    ContestConfig,
    DerivedMetricAdapter,
    DerivedMetricSpec,
    ExportAdapter,
    MarketDefinition,
    MarketTransformAdapter,
    ProjectionDistribution,
    ProjectionKey,
    ProjectionRow,
    ProviderPlayerMapping,
    SportProjectionAdapter,
)


class _ProjectionAdapter:
    def project(self, config: ContestConfig) -> pd.DataFrame:
        return pd.DataFrame([{"contest_id": config.contest_id}])


class _DerivedAdapter:
    def derive(
        self, base_projections: pd.DataFrame, spec: DerivedMetricSpec
    ) -> pd.DataFrame:
        frame = base_projections.copy()
        frame["metric_id"] = spec.derived_metric_id
        return frame


class _MarketAdapter:
    def transform(
        self,
        projections: pd.DataFrame,
        market: MarketDefinition,
        config: ContestConfig,
    ) -> pd.DataFrame:
        frame = projections.copy()
        frame["market_id"] = market.market_id
        frame["contest_id"] = config.contest_id
        return frame


class _ExportAdapter:
    def export(self, surface: pd.DataFrame, config: ContestConfig) -> pd.DataFrame:
        frame = surface.copy()
        frame["exported_for"] = config.provider
        return frame


def test_contract_dataclasses_round_trip() -> None:
    key = ProjectionKey(
        entity_id="player-1",
        sport="mlb",
        metric_id="hits",
        horizon="season",
        window_start="2026-03-01",
        window_end="2026-10-01",
        game_id=None,
    )
    distribution = ProjectionDistribution(
        mean=100.0,
        p10=80.0,
        p50=98.0,
        p90=120.0,
        stddev=10.0,
        params={"alpha": 1.0},
    )
    row = ProjectionRow(
        key=key,
        distribution=distribution,
        availability_confidence=0.95,
        source_model_version="model-v1",
        source_snapshot_id="snapshot-2026-02-12",
    )

    assert row.key.entity_id == "player-1"
    assert row.distribution.mean == 100.0


def test_protocol_shapes_are_usable() -> None:
    market = MarketDefinition(
        market_id="market-1",
        provider="underdog",
        sport="mlb",
        mode="single_game_pickem",
        metric_id="hits",
        horizon="game",
        operator="over",
        line_value=1.5,
        window_start="2026-06-01",
        window_end="2026-06-01",
        game_id="game-1",
    )
    contest = ContestConfig(
        contest_id="contest-1",
        provider="underdog",
        sport="mlb",
        mode="single_game_pickem",
        scoring_ruleset_id="default",
        market_definitions=(market,),
        mode_config={"slip_constraints": {"min_legs": 2}},
        metadata={"season": 2026},
    )
    spec = DerivedMetricSpec(
        derived_metric_id="fantasy_points",
        input_metric_ids=("hits",),
        transform_id="sum",
        transform_params={"hits_weight": 3.0},
    )

    projection_adapter = cast(SportProjectionAdapter, _ProjectionAdapter())
    derived_adapter = cast(DerivedMetricAdapter, _DerivedAdapter())
    market_adapter = cast(MarketTransformAdapter, _MarketAdapter())
    export_adapter = cast(ExportAdapter, _ExportAdapter())

    projected = projection_adapter.project(contest)
    derived = derived_adapter.derive(projected, spec)
    transformed = market_adapter.transform(derived, market, contest)
    exported = export_adapter.export(transformed, contest)

    assert projected.loc[0, "contest_id"] == "contest-1"
    assert derived.loc[0, "metric_id"] == "fantasy_points"
    assert transformed.loc[0, "market_id"] == "market-1"
    assert exported.loc[0, "exported_for"] == "underdog"


def test_provider_mapping_contract_fields() -> None:
    mapping = ProviderPlayerMapping(
        provider="underdog",
        sport="mlb",
        provider_player_id="123",
        internal_player_id="mlb-123",
        provider_player_name="A. Player",
        canonical_name="A Player",
        is_active=True,
        source="fixture",
        updated_at_utc="2026-02-12T00:00:00Z",
    )

    assert mapping.is_active is True
    assert mapping.internal_player_id == "mlb-123"
