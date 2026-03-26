"""Walk-forward backtest helpers for Phase 1.5 MLB fantasy projections."""

from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True, slots=True)
class WalkForwardFold:
    """Single walk-forward fold definition."""

    train_seasons: tuple[int, ...]
    test_season: int


def generate_walk_forward_folds(
    seasons: tuple[int, ...]
) -> tuple[WalkForwardFold, ...]:
    """Generate `(train through N-1, test on N)` folds."""

    ordered = tuple(sorted({int(season) for season in seasons}))
    if len(ordered) < 2:
        return ()

    folds: list[WalkForwardFold] = []
    for index in range(1, len(ordered)):
        train = ordered[:index]
        test = ordered[index]
        folds.append(WalkForwardFold(train_seasons=train, test_season=test))
    return tuple(folds)


def aggregate_metric_scores(predictions: pd.DataFrame) -> pd.DataFrame:
    """Aggregate MAE/RMSE/bias by metric id from row-level predictions."""

    required = {"metric_id", "prediction", "actual"}
    if not required.issubset(predictions.columns):
        return pd.DataFrame(columns=["metric_id", "mae", "rmse", "bias", "n"])
    if predictions.empty:
        return pd.DataFrame(columns=["metric_id", "mae", "rmse", "bias", "n"])

    frame = predictions.copy()
    frame["prediction"] = pd.to_numeric(frame["prediction"], errors="coerce")
    frame["actual"] = pd.to_numeric(frame["actual"], errors="coerce")
    frame = frame.dropna(subset=["prediction", "actual"]).copy()
    if frame.empty:
        return pd.DataFrame(columns=["metric_id", "mae", "rmse", "bias", "n"])

    frame["error"] = frame["prediction"] - frame["actual"]
    frame["abs_error"] = frame["error"].abs()
    frame["sq_error"] = frame["error"] ** 2

    grouped = frame.groupby("metric_id", dropna=False)
    summary = grouped.agg(
        mae=("abs_error", "mean"),
        mse=("sq_error", "mean"),
        bias=("error", "mean"),
        n=("error", "size"),
    ).reset_index()
    summary["rmse"] = summary["mse"].map(lambda value: math.sqrt(float(value)))
    return summary[["metric_id", "mae", "rmse", "bias", "n"]]


def build_hit_rate_red_flag_dashboard(
    predictions: pd.DataFrame,
    *,
    baseline_scores: pd.DataFrame | None = None,
    coverage_target: float = 0.80,
) -> pd.DataFrame:
    """Build per-fold hit-rate red-flag diagnostics from walk-forward rows."""

    required = {
        "entity_id",
        "fold",
        "metric_id",
        "prediction",
        "actual",
        "p10",
        "p90",
    }
    if not required.issubset(predictions.columns):
        return pd.DataFrame()
    frame = predictions.copy()
    frame["metric_id"] = frame["metric_id"].astype(str).str.strip().str.lower()
    frame = frame[frame["metric_id"].isin({"hit_rate", "hits", "plate_appearances"})]
    if frame.empty:
        return pd.DataFrame()

    frame["prediction"] = pd.to_numeric(frame["prediction"], errors="coerce")
    frame["actual"] = pd.to_numeric(frame["actual"], errors="coerce")
    frame["p10"] = pd.to_numeric(frame["p10"], errors="coerce")
    frame["p90"] = pd.to_numeric(frame["p90"], errors="coerce")
    frame = frame.dropna(subset=["prediction", "actual"]).copy()
    if frame.empty:
        return pd.DataFrame()

    frame["abs_error"] = (frame["prediction"] - frame["actual"]).abs()
    frame["error"] = frame["prediction"] - frame["actual"]
    frame["covered"] = (frame["actual"] >= frame["p10"]) & (
        frame["actual"] <= frame["p90"]
    )
    frame["invalid_prediction"] = frame["prediction"].lt(0.0)
    hit_rows = frame[frame["metric_id"] == "hit_rate"].copy()
    if hit_rows.empty:
        return pd.DataFrame()
    hit_rows["invalid_prediction"] = hit_rows["invalid_prediction"] | hit_rows[
        "prediction"
    ].gt(1.0)
    hit_rows["extreme_rate"] = hit_rows["prediction"].gt(0.45) | hit_rows[
        "prediction"
    ].lt(0.10)
    hit_rows["interval_width"] = (hit_rows["p90"] - hit_rows["p10"]).clip(lower=0.0)

    metadata_cols = {
        "selected_model_family": "selected_model_family",
        "active_uncertainty_scale": "active_uncertainty_scale",
    }
    available_metadata = {
        source: target
        for source, target in metadata_cols.items()
        if source in hit_rows.columns
    }

    agg_spec: dict[str, tuple[str, str | object]] = {
        "hit_rate_mae": ("abs_error", "mean"),
        "hit_rate_bias": ("error", "mean"),
        "hit_rate_coverage": ("covered", "mean"),
        "hit_rate_invalid_predictions": ("invalid_prediction", "sum"),
        "hit_rate_extreme_fraction": ("extreme_rate", "mean"),
        "hit_rate_n": ("entity_id", "size"),
        "hit_rate_bias_gt_0p03_fraction": (
            "error",
            lambda s: (s.abs() > 0.03).mean(),
        ),
        "hit_rate_interval_width_median": ("interval_width", "median"),
        "hit_rate_interval_width_p95": (
            "interval_width",
            lambda s: s.quantile(0.95),
        ),
    }
    for source_col, target_col in available_metadata.items():
        agg_spec[target_col] = (
            source_col,
            lambda s: next((str(value) for value in s if pd.notna(value)), ""),
        )

    summary = (
        hit_rows.groupby("fold", dropna=False)
        .agg(**agg_spec)
        .reset_index()
    )
    summary["hit_rate_coverage_error_vs_target"] = (
        summary["hit_rate_coverage"] - float(coverage_target)
    ).abs()
    if baseline_scores is not None and not baseline_scores.empty:
        baseline = baseline_scores.copy()
        baseline["fold"] = baseline["fold"].astype(str)
        baseline["baseline_hit_rate_mae"] = pd.to_numeric(
            baseline.get("baseline_hit_rate_mae", float("nan")), errors="coerce"
        )
        summary["fold"] = summary["fold"].astype(str)
        summary = summary.merge(
            baseline.loc[:, ["fold", "baseline_hit_rate_mae"]],
            on="fold",
            how="left",
        )
        summary["mae_delta_vs_baseline"] = (
            summary["hit_rate_mae"] - summary["baseline_hit_rate_mae"]
        )
    return summary.sort_values("fold", kind="stable").reset_index(drop=True)
