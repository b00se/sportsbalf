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
