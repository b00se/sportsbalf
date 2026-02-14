"""Phase 1.5 walk-forward backtest tests."""

from __future__ import annotations

import pandas as pd


def test_walk_forward_folds_train_through_prior_season() -> None:
    from src.fantasy.adapters.mlb.backtest import generate_walk_forward_folds

    folds = generate_walk_forward_folds((2021, 2022, 2023))

    assert len(folds) == 2
    assert folds[0].train_seasons == (2021,)
    assert folds[0].test_season == 2022
    assert folds[1].train_seasons == (2021, 2022)
    assert folds[1].test_season == 2023


def test_backtest_metric_aggregation_computes_mae_rmse_and_bias() -> None:
    from src.fantasy.adapters.mlb.backtest import aggregate_metric_scores

    frame = pd.DataFrame(
        {
            "metric_id": ["hits", "hits", "walks", "walks"],
            "prediction": [10.0, 8.0, 5.0, 4.0],
            "actual": [12.0, 7.0, 5.0, 7.0],
        }
    )

    scores = aggregate_metric_scores(frame)

    hits = scores[scores["metric_id"] == "hits"].iloc[0]
    assert float(hits["mae"]) == 1.5
    assert float(hits["bias"]) == -0.5
    assert float(hits["rmse"]) > 0.0


def test_bucketed_uncertainty_keeps_quantiles_ordered() -> None:
    from src.fantasy.adapters.mlb.uncertainty import summarize_bucketed_uncertainty

    mean = pd.Series({"101": 10.0, "202": 12.0}, dtype="float64")
    sample_size = pd.Series({"101": 30.0, "202": 40.0}, dtype="float64")
    buckets = pd.Series({"101": "low", "202": "high"})
    residuals = {
        "low": pd.Series([-1.0, 0.0, 1.0], dtype="float64"),
        "high": pd.Series([-3.0, 0.0, 2.0], dtype="float64"),
        "default": pd.Series([0.0], dtype="float64"),
    }

    summary = summarize_bucketed_uncertainty(
        mean_by_entity=mean,
        sample_size_by_entity=sample_size,
        residuals_by_bucket=residuals,
        bucket_by_entity=buckets,
    )

    assert (summary["p10"] <= summary["p50"]).all()
    assert (summary["p50"] <= summary["p90"]).all()
    assert (summary["stddev"] >= 0.0).all()
