"""Uncertainty helpers for MLB fantasy projection rows."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd


def summarize_empirical_uncertainty(
    mean_by_entity: pd.Series,
    sample_size_by_entity: pd.Series,
    residuals: pd.Series,
) -> pd.DataFrame:
    """Build deterministic quantile and stddev summaries from residual history.

    Args:
        mean_by_entity: Projected means indexed by entity id.
        sample_size_by_entity: Inference sample size indexed by entity id.
        residuals: Historical residual samples.

    Returns:
        DataFrame containing `p10`, `p50`, `p90`, and `stddev` indexed by entity id.
    """

    valid_residuals = pd.to_numeric(residuals, errors="coerce").dropna()

    if valid_residuals.empty:
        q10 = 0.0
        q50 = 0.0
        q90 = 0.0
        residual_std = 0.0
    else:
        values = valid_residuals.to_numpy(dtype="float64")
        q10 = float(np.quantile(values, 0.10))
        q50 = float(np.quantile(values, 0.50))
        q90 = float(np.quantile(values, 0.90))
        residual_std = float(np.std(values, ddof=1)) if values.size > 1 else 0.0

    sample_sizes = pd.to_numeric(sample_size_by_entity, errors="coerce").fillna(0.0)
    scale = sample_sizes.clip(lower=1.0)

    p10 = mean_by_entity + (q10 * scale)
    p50 = mean_by_entity + (q50 * scale)
    p90 = mean_by_entity + (q90 * scale)

    ordered_lower = pd.concat([p10, p50], axis=1).min(axis=1)
    ordered_mid = p50.copy()
    ordered_upper = pd.concat([ordered_mid, p90], axis=1).max(axis=1)
    ordered_mid = pd.concat([ordered_lower, ordered_mid, ordered_upper], axis=1).iloc[
        :, 1
    ]

    stddev = scale.map(lambda count: residual_std * math.sqrt(float(count)))

    return pd.DataFrame(
        {
            "p10": ordered_lower.astype("float64"),
            "p50": ordered_mid.astype("float64"),
            "p90": ordered_upper.astype("float64"),
            "stddev": stddev.astype("float64").clip(lower=0.0),
        },
        index=mean_by_entity.index,
    )


def availability_confidence_by_entity(
    frame: pd.DataFrame,
    *,
    entity_id_col: str,
    date_col: str,
    min_history_games: int,
) -> pd.Series:
    """Estimate deterministic availability confidence from games and recent volume."""

    if frame.empty:
        return pd.Series(dtype="float64")

    grouped = frame.groupby(entity_id_col, dropna=False)
    games = grouped[date_col].size().astype("float64")

    latest_date = pd.to_datetime(frame[date_col], errors="coerce").max()
    if pd.isna(latest_date):
        recent = grouped["plate_appearances"].sum().astype("float64")
    else:
        recent_cutoff = pd.Timestamp(latest_date) - pd.Timedelta(days=30)
        recent_frame = frame[frame[date_col] >= recent_cutoff].copy()
        recent = (
            recent_frame.groupby(entity_id_col, dropna=False)["plate_appearances"]
            .sum()
            .astype("float64")
        )

    recent = recent.reindex(games.index, fill_value=0.0)

    games_score = (games / max(float(min_history_games), 1.0)).clip(
        lower=0.0, upper=1.0
    )
    recent_score = (recent / 25.0).clip(lower=0.0, upper=1.0)
    confidence = (0.7 * games_score) + (0.3 * recent_score)
    return confidence.astype("float64").clip(lower=0.0, upper=1.0)
