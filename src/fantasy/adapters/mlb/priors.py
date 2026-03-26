"""Pybaseball seasonal-prior helpers for Phase 1.5 MLB projection features."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from src.utils.io import read_csv


def load_cached_priors(cache_path: str) -> pd.DataFrame:
    """Load cached priors table from CSV/Parquet path."""

    return read_csv(cache_path)


def attach_priors_to_snapshots(
    *,
    snapshots: pd.DataFrame,
    priors: pd.DataFrame,
    prior_columns: Sequence[str],
) -> pd.DataFrame:
    """Join priors to snapshot rows with deterministic league-median fallback.

    Args:
        snapshots: Snapshot rows containing `fg_id` and optional `season`.
        priors: Prior table keyed by `fg_id` and optional `season`.
        prior_columns: Prior numeric columns to project onto snapshots.

    Returns:
        Snapshot frame with prior columns and `prior_imputed_flag`.
    """

    if snapshots.empty:
        result = snapshots.copy()
        result["prior_imputed_flag"] = pd.Series(dtype="int64")
        return result

    result = snapshots.copy()
    if priors.empty:
        for column in prior_columns:
            result[column] = 0.0
        result["prior_imputed_flag"] = 1
        return result

    median_values: dict[str, float] = {}
    for column in prior_columns:
        source = pd.to_numeric(
            priors.get(column, pd.Series(dtype="float64")), errors="coerce"
        )
        median = float(source.median()) if not source.dropna().empty else 0.0
        median_values[column] = median

    if "fg_id" not in result.columns or "fg_id" not in priors.columns:
        merged = result.copy()
        for column in prior_columns:
            merged[column] = median_values[column]
        merged["prior_imputed_flag"] = 1
        return merged

    merge_on: list[str] = ["fg_id"]
    if "season" in result.columns and "season" in priors.columns:
        merge_on.append("season")

    prior_lookup = priors.copy()
    for column in prior_columns:
        prior_lookup[column] = pd.to_numeric(
            prior_lookup.get(column, pd.Series(dtype="float64")),
            errors="coerce",
        )
    prior_lookup = (
        prior_lookup[merge_on + list(prior_columns)]
        .groupby(merge_on, dropna=False, as_index=False)
        .median(numeric_only=True)
    )

    merged = result.merge(
        prior_lookup,
        on=merge_on,
        how="left",
        suffixes=("", "_prior"),
    )

    missing_mask = pd.Series(False, index=merged.index)
    for column in prior_columns:
        numeric = pd.to_numeric(merged[column], errors="coerce")
        missing_mask = missing_mask | numeric.isna()
        merged[column] = numeric.fillna(median_values[column]).astype("float64")

    merged["prior_imputed_flag"] = missing_mask.astype("int64")
    return merged
