"""Tests for fantasy adapter registries."""

from __future__ import annotations

import pandas as pd
import pytest
from src.fantasy.core.contracts import (
    ContestConfig,
    DerivedMetricSpec,
    MarketDefinition,
)
from src.fantasy.core.registry import (
    DerivedMetricAdapterNotFoundError,
    ExportAdapterNotFoundError,
    MarketTransformAdapterNotFoundError,
    ProjectionAdapterNotFoundError,
    clear_fantasy_registry,
    get_derived_metric_adapter,
    get_export_adapter,
    get_market_transform_adapter,
    get_projection_adapter,
    list_registered_fantasy_adapters,
    register_derived_metric_adapter,
    register_export_adapter,
    register_market_transform_adapter,
    register_projection_adapter,
)


class _ProjectionV1:
    def project(self, config: ContestConfig) -> pd.DataFrame:
        return pd.DataFrame([{"impl": "v1"}])


class _ProjectionV2:
    def project(self, config: ContestConfig) -> pd.DataFrame:
        return pd.DataFrame([{"impl": "v2"}])


class _DerivedAdapter:
    def derive(
        self, base_projections: pd.DataFrame, spec: DerivedMetricSpec
    ) -> pd.DataFrame:
        return base_projections.copy()


class _MarketAdapter:
    def transform(
        self,
        projections: pd.DataFrame,
        market: MarketDefinition,
        config: ContestConfig,
    ) -> pd.DataFrame:
        return projections.copy()


class _ExportAdapter:
    def export(self, surface: pd.DataFrame, config: ContestConfig) -> pd.DataFrame:
        return surface.copy()


def _contest() -> ContestConfig:
    market = MarketDefinition(
        market_id="m1",
        provider="underdog",
        sport="mlb",
        mode="season_long_tournament",
        metric_id="hits",
        horizon="season",
        operator=None,
        line_value=None,
        window_start="2026-03-01",
        window_end="2026-10-01",
        game_id=None,
    )
    return ContestConfig(
        contest_id="c1",
        provider="underdog",
        sport="mlb",
        mode="season_long_tournament",
        scoring_ruleset_id="rules",
        market_definitions=(market,),
        mode_config={},
        metadata={},
    )


def setup_function() -> None:
    clear_fantasy_registry()


def test_duplicate_registration_overwrites_projection_adapter() -> None:
    contest = _contest()
    register_projection_adapter(" mlb ", " hits ", " season ", _ProjectionV1())
    register_projection_adapter("MLB", "HITS", "SEASON", _ProjectionV2())

    adapter = get_projection_adapter("mlb", "hits", "season")
    projected = adapter.project(contest)

    assert projected.loc[0, "impl"] == "v2"


def test_missing_lookup_raises_typed_errors() -> None:
    with pytest.raises(ProjectionAdapterNotFoundError):
        get_projection_adapter("mlb", "hits", "season")
    with pytest.raises(DerivedMetricAdapterNotFoundError):
        get_derived_metric_adapter("fantasy_points")
    with pytest.raises(MarketTransformAdapterNotFoundError):
        get_market_transform_adapter("underdog", "single_game_pickem", "over")
    with pytest.raises(ExportAdapterNotFoundError):
        get_export_adapter("underdog", "single_game_pickem", "csv")


def test_list_registered_fantasy_adapters_has_normalized_keys() -> None:
    register_projection_adapter("MLB", "Hits", "Season", _ProjectionV1())
    register_derived_metric_adapter("Fantasy_Points", _DerivedAdapter())
    register_market_transform_adapter(
        "Underdog", "single_game_pickem", "Over", _MarketAdapter()
    )
    register_export_adapter("Underdog", "single_game_pickem", "CSV", _ExportAdapter())

    summary = list_registered_fantasy_adapters()

    assert summary["projection_keys"] == (("mlb", "hits", "season"),)
    assert summary["derived_metric_keys"] == ("fantasy_points",)
    assert summary["market_transform_keys"] == (
        ("underdog", "single_game_pickem", "over"),
    )
    assert summary["export_keys"] == (("underdog", "single_game_pickem", "csv"),)
