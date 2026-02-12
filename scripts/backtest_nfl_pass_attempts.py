"""Run walk-forward backtest for NFL pass-attempt model."""
# ruff: noqa: I001, E402

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.config import load_pipeline_config  # noqa: E402
from src.nfl.models import NFL_FEATURES, WalkForwardConfig, run_walk_forward_backtest  # noqa: E402


def _summary(folds: pd.DataFrame) -> dict[str, Any]:
    if folds.empty:
        return {
            "fold_count": 0,
            "mean_rmse": None,
            "mean_mae": None,
            "mean_r2": None,
            "mean_baseline_rmse": None,
            "mean_baseline_mae": None,
            "mean_baseline_r2": None,
        }
    return {
        "fold_count": int(len(folds)),
        "mean_rmse": float(folds["rmse"].mean()),
        "mean_mae": float(folds["mae"].mean()),
        "mean_r2": float(folds["r2"].mean()),
        "mean_baseline_rmse": float(folds["baseline_rmse"].mean()),
        "mean_baseline_mae": float(folds["baseline_mae"].mean()),
        "mean_baseline_r2": float(folds["baseline_r2"].mean()),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/nfl.yaml", help="Config YAML path")
    parser.add_argument(
        "--fold-metrics-out",
        default="runtime/nfl_pass_attempts_walk_forward_folds.csv",
        help="CSV output path for fold-level metrics",
    )
    parser.add_argument(
        "--summary-out",
        default="runtime/nfl_pass_attempts_walk_forward_summary.json",
        help="JSON output path for aggregate summary",
    )
    parser.add_argument(
        "--min-train-weeks",
        type=int,
        default=32,
        help="Minimum number of distinct season/week keys in training window",
    )
    parser.add_argument(
        "--step-weeks",
        type=int,
        default=1,
        help="Number of distinct season/week keys per test fold",
    )
    parser.add_argument(
        "--max-folds",
        type=int,
        default=20,
        help="Maximum folds to evaluate (use <=0 for no cap)",
    )
    args = parser.parse_args()

    config = load_pipeline_config(
        args.config,
        sport_override="nfl",
        stat_override="pass_attempts",
    )
    section = config.section
    dataset_path = str(section.get("dataset_path", "data/qb_attempts_dataset.parquet"))
    dataset = pd.read_parquet(dataset_path).sort_values(["season", "week"]).copy()

    max_folds = args.max_folds if args.max_folds > 0 else None
    wf_config = WalkForwardConfig(
        min_train_weeks=args.min_train_weeks,
        step_weeks=args.step_weeks,
        max_folds=max_folds,
        model_params=section.get("model_params"),
    )
    folds = run_walk_forward_backtest(
        dataset,
        features=NFL_FEATURES,
        target_col="pass_attempts",
        config=wf_config,
    )
    summary = _summary(folds)
    summary["config_path"] = args.config
    summary["dataset_path"] = dataset_path
    summary["min_train_weeks"] = args.min_train_weeks
    summary["step_weeks"] = args.step_weeks
    summary["max_folds"] = max_folds

    fold_out = Path(args.fold_metrics_out)
    summary_out = Path(args.summary_out)
    fold_out.parent.mkdir(parents=True, exist_ok=True)
    summary_out.parent.mkdir(parents=True, exist_ok=True)

    folds.to_csv(fold_out, index=False)
    summary_out.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("NFL Pass Attempts Walk-Forward Backtest")
    print(f"Dataset: {dataset_path}")
    print(f"Folds: {summary['fold_count']}")
    print(
        "Mean model metrics: "
        f"RMSE={summary['mean_rmse']}  "
        f"MAE={summary['mean_mae']}  "
        f"R2={summary['mean_r2']}"
    )
    print(
        "Mean baseline metrics: "
        f"RMSE={summary['mean_baseline_rmse']}  "
        f"MAE={summary['mean_baseline_mae']}  R2={summary['mean_baseline_r2']}"
    )
    print(f"Fold metrics CSV: {fold_out}")
    print(f"Summary JSON: {summary_out}")


if __name__ == "__main__":
    main()
