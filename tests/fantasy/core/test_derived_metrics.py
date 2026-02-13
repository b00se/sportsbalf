"""Tests for derived metric dependency resolution."""

from __future__ import annotations

import pandas as pd
import pytest
from src.fantasy.core.contracts import DerivedMetricSpec
from src.fantasy.core.derived import (
    DerivedMetricDependencyError,
    NoOpDerivedMetricAdapter,
    validate_derived_metric_dependencies,
)


def test_dependency_resolution_succeeds_when_all_inputs_declared() -> None:
    spec = DerivedMetricSpec(
        derived_metric_id="fantasy_points",
        input_metric_ids=("hits", "runs"),
        transform_id="weighted_sum",
        transform_params={"hits": 3.0, "runs": 2.0},
    )

    validate_derived_metric_dependencies(
        (spec,), declared_base_metric_ids={"hits", "runs"}
    )


def test_dependency_resolution_fails_on_missing_base_metric() -> None:
    spec = DerivedMetricSpec(
        derived_metric_id="fantasy_points",
        input_metric_ids=("hits", "rbi"),
        transform_id="weighted_sum",
        transform_params={"hits": 3.0, "rbi": 1.0},
    )

    with pytest.raises(
        DerivedMetricDependencyError, match="missing declared base metrics"
    ):
        validate_derived_metric_dependencies((spec,), declared_base_metric_ids={"hits"})


def test_noop_derived_adapter_returns_copy_with_metric_id() -> None:
    frame = pd.DataFrame([{"entity_id": "p1", "mean": 10.0}])
    spec = DerivedMetricSpec(
        derived_metric_id="fantasy_points",
        input_metric_ids=("hits",),
        transform_id="noop",
        transform_params={},
    )

    adapter = NoOpDerivedMetricAdapter()
    result = adapter.derive(frame, spec)

    assert result.loc[0, "derived_metric_id"] == "fantasy_points"
    assert frame.equals(pd.DataFrame([{"entity_id": "p1", "mean": 10.0}]))
