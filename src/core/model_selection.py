"""Reusable model-selection contracts and metric filtering helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import pandas as pd

METRIC_COLUMN_MAP: dict[str, str] = {
    "mae": "mean_mae",
    "rmse": "mean_rmse",
    "r2": "mean_r2",
}
MAXIMIZE_METRICS: set[str] = {"r2"}


class StatAdapter(Protocol):
    """Stat-specific adapter contract for reusable tournament code."""

    target_col: str
    feature_columns: list[str]


class BucketStrategy(Protocol):
    """Bucket strategy contract used by segmented tournaments."""

    name: str

    def assign(self, frame: pd.DataFrame) -> pd.Series:
        """Return bucket labels for each row in ``frame``."""

    def metadata(self) -> dict[str, Any]:
        """Return serializable strategy metadata."""


@dataclass(frozen=True, slots=True)
class SelectionPolicy:
    """Metric ranking policy for champion selection."""

    primary_metric: str = "mae"
    tie_breakers: tuple[str, ...] = ("rmse", "r2")
    epsilon: float = 1e-6

    def metric_priority(self) -> list[str]:
        """Return ordered metric priority with primary metric first."""

        return [self.primary_metric, *self.tie_breakers]


def apply_metric_filters(
    leaderboard: pd.DataFrame,
    *,
    policy: SelectionPolicy,
) -> pd.DataFrame:
    """Return leaderboard candidates that survive metric tie-break filters.

    Args:
        leaderboard: Aggregate tournament leaderboard.
        policy: Metric ranking policy.

    Returns:
        Filtered candidate dataframe.

    Raises:
        ValueError: If metrics are unsupported or leaderboard is empty.
    """

    if leaderboard.empty:
        raise ValueError("Cannot rank candidates from an empty leaderboard.")

    metric_priority = policy.metric_priority()
    unknown = [m for m in metric_priority if m not in METRIC_COLUMN_MAP]
    if unknown:
        raise ValueError(f"Unsupported metric(s) in selection policy: {unknown}")

    candidates = leaderboard.copy()
    for metric_name in metric_priority:
        column = METRIC_COLUMN_MAP[metric_name]
        if metric_name in MAXIMIZE_METRICS:
            best = float(candidates[column].max())
            candidates = candidates[candidates[column] >= best - policy.epsilon]
        else:
            best = float(candidates[column].min())
            candidates = candidates[candidates[column] <= best + policy.epsilon]
    return candidates


def normalize_metric_list(metrics: Sequence[str] | None) -> list[str]:
    """Normalize metric names to lowercase string list."""

    if not metrics:
        return []
    return [str(metric).strip().lower() for metric in metrics]


def selection_policy_from_config(raw: Mapping[str, Any]) -> SelectionPolicy:
    """Build ``SelectionPolicy`` from config mapping."""

    primary = str(raw.get("primary_metric", "mae")).strip().lower()
    tie_breakers = tuple(normalize_metric_list(raw.get("tie_breakers", ["rmse", "r2"])))
    epsilon = float(raw.get("tie_epsilon", 1e-6))
    return SelectionPolicy(
        primary_metric=primary,
        tie_breakers=tie_breakers,
        epsilon=epsilon,
    )
