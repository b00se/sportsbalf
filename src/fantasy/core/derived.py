"""Derived metric scaffolding and dependency validation."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from src.fantasy.core.contracts import DerivedMetricSpec


class DerivedMetricDependencyError(ValueError):
    """Raised when a derived metric references undeclared base inputs."""


def validate_derived_metric_dependencies(
    specs: Iterable[DerivedMetricSpec],
    *,
    declared_base_metric_ids: set[str],
) -> None:
    """Ensure each derived metric references existing base metrics.

    Args:
        specs: Derived metric specifications.
        declared_base_metric_ids: Declared base metric identifiers.

    Raises:
        DerivedMetricDependencyError: If any derived metric references missing
            base metrics.
    """

    normalized_base_ids = {
        metric.strip().lower() for metric in declared_base_metric_ids
    }
    for spec in specs:
        missing = sorted(
            {
                metric.strip().lower()
                for metric in spec.input_metric_ids
                if metric.strip().lower() not in normalized_base_ids
            }
        )
        if missing:
            raise DerivedMetricDependencyError(
                "Derived metric "
                f"'{spec.derived_metric_id}' is missing declared base metrics: "
                f"{missing}"
            )


class NoOpDerivedMetricAdapter:
    """Simple adapter placeholder for wiring and registry tests."""

    def derive(
        self,
        base_projections: pd.DataFrame,
        spec: DerivedMetricSpec,
    ) -> pd.DataFrame:
        """Return a copied frame tagged with the derived metric id."""

        result = base_projections.copy()
        result["derived_metric_id"] = spec.derived_metric_id
        return result
