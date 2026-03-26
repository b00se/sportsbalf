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


def summarize_bucketed_uncertainty(
    *,
    mean_by_entity: pd.Series,
    sample_size_by_entity: pd.Series,
    residuals_by_bucket: dict[str, pd.Series],
    bucket_by_entity: pd.Series,
) -> pd.DataFrame:
    """Build uncertainty using per-bucket residual distributions."""

    by_entity_rows: dict[str, dict[str, float]] = {}
    for entity_id, mean_value in mean_by_entity.items():
        bucket = str(bucket_by_entity.get(entity_id, "default"))
        residuals = residuals_by_bucket.get(bucket)
        if residuals is None:
            residuals = residuals_by_bucket.get("default", pd.Series(dtype="float64"))
        local = summarize_empirical_uncertainty(
            mean_by_entity=pd.Series([mean_value], index=[entity_id], dtype="float64"),
            sample_size_by_entity=pd.Series(
                [sample_size_by_entity.get(entity_id, 1.0)],
                index=[entity_id],
                dtype="float64",
            ),
            residuals=residuals,
        )
        by_entity_rows[str(entity_id)] = {
            "p10": float(local.loc[entity_id, "p10"]),
            "p50": float(local.loc[entity_id, "p50"]),
            "p90": float(local.loc[entity_id, "p90"]),
            "stddev": float(local.loc[entity_id, "stddev"]),
        }

    if not by_entity_rows:
        return pd.DataFrame(columns=["p10", "p50", "p90", "stddev"])
    output = pd.DataFrame.from_dict(by_entity_rows, orient="index")
    output.index.name = mean_by_entity.index.name
    return output.reindex(mean_by_entity.index)


def summarize_hit_rate_uncertainty_from_counts(
    *,
    hit_mean_by_entity: pd.Series,
    pa_mean_by_entity: pd.Series,
    sample_size_by_entity: pd.Series,
    hit_residuals: pd.Series,
    pa_residuals: pd.Series,
    seed: int,
    draws: int = 500,
    residual_scale_global: float = 1.0,
    residual_scale_by_bucket: dict[str, float] | None = None,
    bucket_by_entity: pd.Series | None = None,
    hit_residuals_by_bucket: dict[str, pd.Series] | None = None,
    pa_residuals_by_bucket: dict[str, pd.Series] | None = None,
    min_bucket_residual_count: int = 100,
) -> pd.DataFrame:
    """Build bounded hit-rate intervals from count residual simulation."""

    entity_index = hit_mean_by_entity.index.union(pa_mean_by_entity.index)
    if entity_index.empty:
        return pd.DataFrame(columns=["p10", "p50", "p90", "stddev"])

    hit_mean = hit_mean_by_entity.reindex(entity_index).fillna(0.0).astype("float64")
    pa_mean = pa_mean_by_entity.reindex(entity_index).fillna(0.0).astype("float64")
    sample_sizes = (
        pd.to_numeric(sample_size_by_entity, errors="coerce")
        .reindex(entity_index)
        .fillna(1.0)
        .clip(lower=1.0)
    )
    hit_res = pd.to_numeric(hit_residuals, errors="coerce").dropna().astype("float64")
    pa_res = pd.to_numeric(pa_residuals, errors="coerce").dropna().astype("float64")
    if hit_res.empty:
        hit_res = pd.Series([0.0], dtype="float64")
    if pa_res.empty:
        pa_res = pd.Series([0.0], dtype="float64")

    residual_scale = max(float(residual_scale_global), 0.0)
    bucket_scales = {
        str(bucket): max(float(scale), 0.0)
        for bucket, scale in (residual_scale_by_bucket or {}).items()
    }
    entity_buckets = pd.Series("default", index=entity_index, dtype="object")
    if bucket_by_entity is not None and not bucket_by_entity.empty:
        bucket_values = bucket_by_entity.reindex(entity_index).fillna("default")
        entity_buckets = bucket_values.astype(str)

    hit_bucket_map: dict[str, pd.Series] = {}
    for bucket, series in (hit_residuals_by_bucket or {}).items():
        cleaned = pd.to_numeric(series, errors="coerce").dropna().astype("float64")
        if int(cleaned.shape[0]) >= int(max(min_bucket_residual_count, 1)):
            hit_bucket_map[str(bucket)] = cleaned
    pa_bucket_map: dict[str, pd.Series] = {}
    for bucket, series in (pa_residuals_by_bucket or {}).items():
        cleaned = pd.to_numeric(series, errors="coerce").dropna().astype("float64")
        if int(cleaned.shape[0]) >= int(max(min_bucket_residual_count, 1)):
            pa_bucket_map[str(bucket)] = cleaned

    hit_default_values = hit_res.to_numpy(dtype="float64")
    pa_default_values = pa_res.to_numpy(dtype="float64")

    rng = np.random.default_rng(seed)
    simulations = np.zeros((len(entity_index), int(max(draws, 1))), dtype="float64")
    for idx, entity_id in enumerate(entity_index):
        bucket = str(entity_buckets.loc[entity_id])
        bucket_scale = bucket_scales.get(bucket, 1.0)
        scale_multiplier = residual_scale * bucket_scale
        hit_res_values = hit_bucket_map.get(bucket, hit_res).to_numpy(dtype="float64")
        pa_res_values = pa_bucket_map.get(bucket, pa_res).to_numpy(dtype="float64")
        if hit_res_values.size == 0:
            hit_res_values = hit_default_values
        if pa_res_values.size == 0:
            pa_res_values = pa_default_values
        scale = math.sqrt(float(sample_sizes.loc[entity_id]))
        sampled_hit = rng.choice(
            hit_res_values, size=simulations.shape[1], replace=True
        )
        sampled_pa = rng.choice(pa_res_values, size=simulations.shape[1], replace=True)
        draw_hits = np.maximum(
            hit_mean.loc[entity_id] + (sampled_hit * scale * scale_multiplier), 0.0
        )
        draw_pa = np.maximum(
            pa_mean.loc[entity_id] + (sampled_pa * scale * scale_multiplier), 0.0
        )
        draw_hits = np.minimum(draw_hits, draw_pa)
        with np.errstate(divide="ignore", invalid="ignore"):
            draw_rates = np.where(draw_pa > 0.0, draw_hits / draw_pa, 0.0)
        simulations[idx, :] = np.clip(draw_rates, 0.0, 1.0)

    p10 = np.quantile(simulations, 0.10, axis=1)
    p50 = np.quantile(simulations, 0.50, axis=1)
    p90 = np.quantile(simulations, 0.90, axis=1)
    stddev = np.std(simulations, axis=1, ddof=1 if simulations.shape[1] > 1 else 0)

    output = pd.DataFrame(
        {
            "p10": p10,
            "p50": p50,
            "p90": p90,
            "stddev": stddev,
        },
        index=entity_index,
    )
    output["p10"] = output["p10"].clip(lower=0.0, upper=1.0)
    output["p50"] = output["p50"].clip(lower=0.0, upper=1.0)
    output["p90"] = output["p90"].clip(lower=0.0, upper=1.0)
    output["p50"] = output[["p10", "p50"]].max(axis=1)
    output["p90"] = output[["p50", "p90"]].max(axis=1)
    output["stddev"] = output["stddev"].clip(lower=0.0)
    return output


def availability_confidence_by_entity(
    frame: pd.DataFrame,
    *,
    entity_id_col: str,
    date_col: str,
    min_history_games: int,
) -> pd.Series:
    """Estimate deterministic availability confidence.

    The score blends historical game count and recency-weighted plate appearances.
    """

    if frame.empty:
        return pd.Series(dtype="float64")

    grouped = frame.groupby(entity_id_col, dropna=False)
    games = grouped[date_col].size().astype("float64")

    plate_appearances = pd.to_numeric(
        frame.get("plate_appearances", 0.0), errors="coerce"
    ).fillna(0.0)
    latest_date = pd.to_datetime(frame[date_col], errors="coerce").max()
    if pd.isna(latest_date):
        recent = plate_appearances.groupby(frame[entity_id_col], dropna=False).sum()
        recent = recent.astype("float64")
    else:
        dated_frame = frame.copy()
        dated_frame["_confidence_date"] = pd.to_datetime(
            frame[date_col], errors="coerce"
        )
        dated_frame["_plate_appearances"] = plate_appearances
        days_since_latest = (
            pd.Timestamp(latest_date) - dated_frame["_confidence_date"]
        ).dt.days
        days_since_latest = days_since_latest.fillna(3650).clip(lower=0.0)
        # Exponential half-life keeps this sensitive to recent form without hard cutoff.
        dated_frame["_recency_weight"] = np.power(0.5, days_since_latest / 30.0)
        dated_frame["_weighted_pa"] = (
            dated_frame["_plate_appearances"] * dated_frame["_recency_weight"]
        )
        recent = (
            dated_frame.groupby(entity_id_col, dropna=False)["_weighted_pa"]
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
