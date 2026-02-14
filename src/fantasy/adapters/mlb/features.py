"""Feature utilities for MLB Phase 1 fantasy projections."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from src.utils.io import read_csv

logger = logging.getLogger(__name__)

_REQUIRED_RAW_COLUMNS: tuple[str, ...] = (
    "plate_appearances",
    "hits",
    "total_bases",
    "walks",
    "strikeouts",
    "hard_hit_rate",
    "pa_vs_lhp",
    "pa_vs_rhp",
)

_BASE_FEATURE_COLUMNS: tuple[str, ...] = (
    "plate_appearances",
    "hits",
    "total_bases",
    "walks",
    "strikeouts",
    "hard_hit_rate",
    "pa_vs_lhp",
    "pa_vs_rhp",
    "hard_hit_events",
    "hit_rate",
    "walk_rate",
    "strikeout_rate",
    "slugging_proxy",
)

DIRECT_COUNT_METRICS: tuple[str, ...] = (
    "plate_appearances",
    "hits",
    "total_bases",
    "walks",
    "strikeouts",
    "pa_vs_lhp",
    "pa_vs_rhp",
    "hard_hit_events",
)

DERIVED_RATE_INPUTS: dict[str, tuple[str, str]] = {
    "hit_rate": ("hits", "plate_appearances"),
    "walk_rate": ("walks", "plate_appearances"),
    "strikeout_rate": ("strikeouts", "plate_appearances"),
    "slugging_proxy": ("total_bases", "plate_appearances"),
}

_TARGET_LEAKAGE_BLOCKLIST: dict[str, tuple[str, ...]] = {
    "plate_appearances": (
        "plate_appearances",
        "pa_vs_lhp",
        "pa_vs_rhp",
        "hard_hit_events",
        "hit_rate",
        "walk_rate",
        "strikeout_rate",
        "slugging_proxy",
    ),
    "hits": ("hits", "hit_rate"),
    "total_bases": ("total_bases", "slugging_proxy"),
    "walks": ("walks", "walk_rate"),
    "strikeouts": ("strikeouts", "strikeout_rate"),
    "pa_vs_lhp": ("pa_vs_lhp", "plate_appearances", "pa_vs_rhp"),
    "pa_vs_rhp": ("pa_vs_rhp", "plate_appearances", "pa_vs_lhp"),
    "hard_hit_events": ("hard_hit_events", "hard_hit_rate", "plate_appearances"),
    "hit_rate": ("hit_rate", "hits", "plate_appearances"),
    "walk_rate": ("walk_rate", "walks", "plate_appearances"),
    "strikeout_rate": ("strikeout_rate", "strikeouts", "plate_appearances"),
    "slugging_proxy": ("slugging_proxy", "total_bases", "plate_appearances"),
}


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Return a stable ratio series with zero fallback for non-positive denominators."""

    num = pd.to_numeric(numerator, errors="coerce").fillna(0.0)
    den = pd.to_numeric(denominator, errors="coerce").fillna(0.0)
    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = np.where(den > 0.0, num / den, 0.0)
    return pd.Series(ratio, index=numerator.index, dtype="float64")


def prepare_mlb_projection_frame(
    input_dataset_path: str,
    *,
    entity_id_col: str,
    date_col: str,
) -> pd.DataFrame:
    """Load and normalize an MLB batter-game frame for adapter use.

    Args:
        input_dataset_path: CSV/Parquet path with batter-game rows.
        entity_id_col: Internal entity identifier column.
        date_col: Date column name.

    Returns:
        Normalized frame with required and derived projection columns.
    """

    frame = read_csv(input_dataset_path)
    normalized = frame.copy()

    if entity_id_col not in normalized.columns:
        normalized[entity_id_col] = ""
    normalized[entity_id_col] = normalized[entity_id_col].astype(str).str.strip()

    if date_col not in normalized.columns:
        normalized[date_col] = pd.NaT
    normalized[date_col] = pd.to_datetime(normalized[date_col], errors="coerce")

    for column in _REQUIRED_RAW_COLUMNS:
        if column not in normalized.columns:
            logger.info(
                "MLB projection input missing optional column '%s'; "
                "using zero fallback.",
                column,
            )
            normalized[column] = 0.0

    for column in _REQUIRED_RAW_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce").fillna(
            0.0
        )

    normalized["hard_hit_events"] = (
        normalized["hard_hit_rate"] * normalized["plate_appearances"]
    ).clip(lower=0.0)
    normalized["hit_rate"] = _safe_ratio(
        normalized["hits"], normalized["plate_appearances"]
    )
    normalized["walk_rate"] = _safe_ratio(
        normalized["walks"], normalized["plate_appearances"]
    )
    normalized["strikeout_rate"] = _safe_ratio(
        normalized["strikeouts"], normalized["plate_appearances"]
    )
    normalized["slugging_proxy"] = _safe_ratio(
        normalized["total_bases"], normalized["plate_appearances"]
    )

    normalized = normalized[normalized[entity_id_col].ne("")].copy()
    normalized = normalized.sort_values([entity_id_col, date_col], kind="stable")
    return normalized


def base_feature_columns() -> list[str]:
    """Return the ordered feature set for Phase 1 estimator usage."""

    return list(_BASE_FEATURE_COLUMNS)


def model_feature_columns_for_metric(metric_id: str) -> list[str]:
    """Return leakage-safe ordered model features for one metric.

    Args:
        metric_id: Target metric identifier.

    Returns:
        Ordered feature list with target-leaking columns removed.
    """

    metric_key = metric_id.strip().lower()
    blocked = set(_TARGET_LEAKAGE_BLOCKLIST.get(metric_key, (metric_key,)))
    return [column for column in _BASE_FEATURE_COLUMNS if column not in blocked]


def is_derived_rate_metric(metric_id: str) -> bool:
    """Return whether a metric is derived from count predictions."""

    return metric_id.strip().lower() in DERIVED_RATE_INPUTS


def rate_metric_inputs(metric_id: str) -> tuple[str, str]:
    """Return `(numerator_metric, denominator_metric)` for derived rate metrics."""

    metric_key = metric_id.strip().lower()
    return DERIVED_RATE_INPUTS[metric_key]
