"""Phase 1.5b uncertainty and calibration tests."""

from __future__ import annotations

import pandas as pd


def test_hit_rate_uncertainty_residual_scale_changes_interval_width() -> None:
    from src.fantasy.adapters.mlb.uncertainty import (
        summarize_hit_rate_uncertainty_from_counts,
    )

    hit_mean = pd.Series([120.0], index=["p1"], dtype="float64")
    pa_mean = pd.Series([400.0], index=["p1"], dtype="float64")
    sample_sizes = pd.Series([1.0], index=["p1"], dtype="float64")
    hit_residuals = pd.Series([-20.0, -10.0, 10.0, 20.0], dtype="float64")
    pa_residuals = pd.Series([-50.0, -25.0, 25.0, 50.0], dtype="float64")

    wide = summarize_hit_rate_uncertainty_from_counts(
        hit_mean_by_entity=hit_mean,
        pa_mean_by_entity=pa_mean,
        sample_size_by_entity=sample_sizes,
        hit_residuals=hit_residuals,
        pa_residuals=pa_residuals,
        seed=2026,
        draws=2000,
        residual_scale_global=1.0,
    )
    narrow = summarize_hit_rate_uncertainty_from_counts(
        hit_mean_by_entity=hit_mean,
        pa_mean_by_entity=pa_mean,
        sample_size_by_entity=sample_sizes,
        hit_residuals=hit_residuals,
        pa_residuals=pa_residuals,
        seed=2026,
        draws=2000,
        residual_scale_global=0.25,
    )

    wide_width = float(wide.loc["p1", "p90"] - wide.loc["p1", "p10"])
    narrow_width = float(narrow.loc["p1", "p90"] - narrow.loc["p1", "p10"])

    assert (
        0.0
        <= float(wide.loc["p1", "p10"])
        <= float(wide.loc["p1", "p50"])
        <= float(wide.loc["p1", "p90"])
        <= 1.0
    )
    assert (
        0.0
        <= float(narrow.loc["p1", "p10"])
        <= float(narrow.loc["p1", "p50"])
        <= float(narrow.loc["p1", "p90"])
        <= 1.0
    )
    assert narrow_width < wide_width


def test_hit_rate_uncertainty_bucket_fallback_uses_default_when_sparse() -> None:
    from src.fantasy.adapters.mlb.uncertainty import (
        summarize_hit_rate_uncertainty_from_counts,
    )

    hit_mean = pd.Series([120.0], index=["p1"], dtype="float64")
    pa_mean = pd.Series([400.0], index=["p1"], dtype="float64")
    sample_sizes = pd.Series([1.0], index=["p1"], dtype="float64")

    output = summarize_hit_rate_uncertainty_from_counts(
        hit_mean_by_entity=hit_mean,
        pa_mean_by_entity=pa_mean,
        sample_size_by_entity=sample_sizes,
        hit_residuals=pd.Series([-15.0, 15.0, -10.0, 10.0, 0.0], dtype="float64"),
        pa_residuals=pd.Series([-40.0, 40.0, -20.0, 20.0, 0.0], dtype="float64"),
        hit_residuals_by_bucket={"sparse": pd.Series([0.0], dtype="float64")},
        pa_residuals_by_bucket={"sparse": pd.Series([0.0], dtype="float64")},
        bucket_by_entity=pd.Series(["sparse"], index=["p1"]),
        min_bucket_residual_count=5,
        seed=2026,
        draws=1500,
    )

    width = float(output.loc["p1", "p90"] - output.loc["p1", "p10"])
    assert width > 0.0


def test_red_flag_dashboard_emits_phase15b_calibration_columns() -> None:
    from src.fantasy.adapters.mlb.backtest import build_hit_rate_red_flag_dashboard

    predictions = pd.DataFrame(
        {
            "entity_id": ["a", "b", "c"],
            "fold": ["2025", "2025", "2025"],
            "metric_id": ["hit_rate", "hit_rate", "hit_rate"],
            "prediction": [0.30, 0.24, 0.28],
            "actual": [0.35, 0.25, 0.26],
            "p10": [0.20, 0.18, 0.20],
            "p90": [0.42, 0.36, 0.34],
        }
    )

    summary = build_hit_rate_red_flag_dashboard(predictions, coverage_target=0.80)

    row = summary.iloc[0]
    assert "hit_rate_interval_width_median" in summary.columns
    assert "hit_rate_interval_width_p95" in summary.columns
    assert "hit_rate_coverage_error_vs_target" in summary.columns
    assert float(row["hit_rate_interval_width_median"]) > 0.0
    assert float(row["hit_rate_interval_width_p95"]) >= float(
        row["hit_rate_interval_width_median"]
    )
    assert float(row["hit_rate_coverage_error_vs_target"]) == abs(
        float(row["hit_rate_coverage"]) - 0.80
    )
