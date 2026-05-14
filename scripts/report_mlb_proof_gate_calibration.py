#!/usr/bin/env python
# ruff: noqa: I001, E402
"""Build a lightweight calibration report for MLB proof-run confidence gates."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.build_mlb_live_betslips import _confidence_thresholds_by_stat
from src.mlb.models.strategy import predict_with_strategy_artifact
from src.mlb.pitcher_props.descriptors import STAT_DESCRIPTORS, get_stat_descriptor
from src.mlb.pitcher_props.pipeline import (
    _build_training_games,
    _clean_for_model,
    _model_features,
    _train_or_load,
)
from src.utils.io import load_config

DEFAULT_CONFIG_PATH = "config/mlb.yaml"
DEFAULT_LINES_DIR = Path("data/lines")
DEFAULT_OUTPUT_PATH = Path("models/mlb_proof_gate_calibration.json")


def parse_args() -> argparse.Namespace:
    """Parse CLI args for proof-gate calibration reporting."""

    parser = argparse.ArgumentParser(
        description="Report quick MLB proof-gate calibration diagnostics.",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help="Path to the MLB config file.",
    )
    parser.add_argument(
        "--lines-dir",
        type=Path,
        default=DEFAULT_LINES_DIR,
        help="Directory containing dated live line snapshots.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Where to write the calibration JSON report.",
    )
    parser.add_argument(
        "--stat",
        action="append",
        dest="stats",
        default=[],
        help="Limit the report to one or more MLB prop stats.",
    )
    return parser.parse_args()


def _snapshot_paths(lines_dir: Path, stat: str) -> list[Path]:
    """Return sorted dated snapshot paths for a stat."""

    return sorted(lines_dir.glob(f"{stat}_20??-??-??.csv"))


def _line_distribution_stats(
    lines: pd.Series,
    actual: pd.Series,
) -> list[dict[str, float | int]]:
    """Return empirical under/over rates for each unique live line level."""

    distribution: list[dict[str, float | int]] = []
    clean_actual = pd.to_numeric(actual, errors="coerce").dropna()
    if clean_actual.empty:
        return distribution

    for line_value in sorted(
        pd.to_numeric(lines, errors="coerce").dropna().astype(float).unique().tolist()
    ):
        distribution.append(
            {
                "line": float(line_value),
                "empirical_under_rate": float((clean_actual < line_value).mean()),
                "empirical_over_rate": float((clean_actual > line_value).mean()),
                "sample_size": int(clean_actual.shape[0]),
            }
        )
    return distribution


def _stat_report(
    *,
    stat: str,
    section: dict[str, Any],
    lines_dir: Path,
) -> dict[str, Any]:
    """Build calibration diagnostics for one stat."""

    descriptor = get_stat_descriptor(stat)
    training_games = _build_training_games(section, descriptor)
    model_frame = _clean_for_model(training_games, descriptor)
    model, model_name, strategy_name = _train_or_load(
        model_frame,
        section=section,
        descriptor=descriptor,
        retrain=False,
    )
    actual = pd.to_numeric(model_frame[descriptor.target_col], errors="coerce")
    predicted = pd.to_numeric(
        predict_with_strategy_artifact(
            model_frame,
            features=_model_features(descriptor),
            name="prediction",
            artifact=model,
        ),
        errors="coerce",
    )
    residuals = actual - predicted

    snapshots: list[dict[str, Any]] = []
    for path in _snapshot_paths(lines_dir, stat):
        frame = pd.read_csv(path)
        if descriptor.line_col not in frame.columns:
            continue
        lines = pd.to_numeric(frame[descriptor.line_col], errors="coerce")
        snapshots.append(
            {
                "path": str(path),
                "date": path.stem.removeprefix(f"{stat}_"),
                "rows": int(len(frame)),
                "line_min": float(lines.min()) if lines.notna().any() else None,
                "line_max": float(lines.max()) if lines.notna().any() else None,
                "line_levels": _line_distribution_stats(lines, actual),
            }
        )

    return {
        "stat": stat,
        "model_name": model_name,
        "strategy_name": strategy_name,
        "thresholds": _confidence_thresholds_by_stat().get(stat, {}),
        "training_rows": int(len(model_frame)),
        "actual_mean": float(actual.mean()),
        "actual_std": float(actual.std(ddof=1)),
        "predicted_mean": float(predicted.mean()),
        "predicted_std": float(predicted.std(ddof=1)),
        "residual_std": float(residuals.std(ddof=1)),
        "mae": float((actual - predicted).abs().mean()),
        "p90_actual": float(actual.quantile(0.90)),
        "p90_predicted": float(predicted.quantile(0.90)),
        "snapshots": snapshots,
    }


def main() -> dict[str, Any]:
    """Write and return the proof-gate calibration report."""

    args = parse_args()
    config = load_config(args.config)
    stats = [stat.strip().lower() for stat in args.stats] or list(STAT_DESCRIPTORS)
    invalid = sorted(stat for stat in stats if stat not in STAT_DESCRIPTORS)
    if invalid:
        raise ValueError(f"Unsupported stats: {invalid}")

    report = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds"),
        "config_path": str(args.config),
        "lines_dir": str(args.lines_dir),
        "stats": [
            _stat_report(
                stat=stat,
                section=dict(config["mlb"][stat]),
                lines_dir=args.lines_dir,
            )
            for stat in stats
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), "stats": stats}, sort_keys=True))
    return report


if __name__ == "__main__":
    main()
