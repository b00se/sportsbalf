"""Outs-only probability calibration reporting for MLB pitcher props."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.core.simulation import MonteCarloConfig, apply_simulations
from src.mlb.features.feature_store import ensure_live_feature_defaults
from src.mlb.models.evaluation import probability_calibration_report
from src.mlb.pitcher_props.descriptors import StatDescriptor
from src.mlb.pitcher_props.outs_features import ensure_outs_feature_defaults
from src.utils.io import read_csv

logger = logging.getLogger(__name__)


def _load_calibration_frame(path: str) -> pd.DataFrame:
    """Load a labeled calibration frame from CSV or Parquet."""

    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Calibration data file not found: {file_path}")
    if file_path.suffix.lower() == ".parquet":
        return pd.read_parquet(file_path)
    return read_csv(str(file_path))


def _default_calibration_report_path(model_path: str) -> Path:
    """Return the default outs calibration report path next to the model file."""

    model_file = Path(model_path)
    return model_file.with_name(f"{model_file.stem}_calibration.csv")


def _prepare_calibration_frame(
    frame: pd.DataFrame,
    *,
    descriptor: StatDescriptor,
    features: list[str],
) -> pd.DataFrame:
    """Return a model-ready calibration frame with outs defaults applied."""

    prepared = ensure_live_feature_defaults(ensure_outs_feature_defaults(frame))
    if "pitcher_id" not in prepared.columns and "pitcher" in prepared.columns:
        prepared["pitcher_id"] = prepared["pitcher"]
    if "pitcher_id" not in prepared.columns:
        prepared["pitcher_id"] = prepared.index.astype(str)
    if "game_date" in prepared.columns:
        prepared["game_date"] = pd.to_datetime(prepared["game_date"], errors="coerce")

    required = [descriptor.target_col, descriptor.line_col]
    prepared = prepared.dropna(subset=required).copy()
    prepared = prepared.replace([np.inf, -np.inf], np.nan)
    missing = [column for column in features if column not in prepared.columns]
    if missing:
        raise ValueError(
            "Calibration frame missing required feature columns: "
            f"{missing}"
        )
    return prepared


def persist_outs_calibration_report(
    *,
    model: Any,
    section: dict[str, object],
    descriptor: StatDescriptor,
    features: list[str],
    std_dev: float,
) -> dict[str, float] | None:
    """Write an outs calibration report from a labeled holdout dataset.

    Args:
        model: Trained model artifact.
        section: Resolved outs config section.
        descriptor: Stats descriptor for ``outs_recorded``.
        features: Ordered model feature list.
        std_dev: Residual spread used for simulation.

    Returns:
        Calibration summary metrics when a dataset is configured, otherwise None.
    """

    if descriptor.stat != "outs_recorded":
        return None

    calibration_data_path = section.get("calibration_data_path")
    if not calibration_data_path:
        return None

    try:
        frame = _load_calibration_frame(str(calibration_data_path))
    except FileNotFoundError:
        logger.warning(
            "Outs calibration dataset not found at '%s'; skipping report.",
            calibration_data_path,
        )
        return None

    prepared = _prepare_calibration_frame(
        frame,
        descriptor=descriptor,
        features=features,
    )

    report_path = Path(
        str(
            section.get(
                "calibration_report_path",
                _default_calibration_report_path(str(section["model_path"])),
            )
        )
    )

    if prepared.empty:
        summary, by_bin = probability_calibration_report(
            pd.DataFrame(columns=["actual_over", "prob_over"]),
            actual_col="actual_over",
            probability_col="prob_over",
        )
    else:
        from src.mlb.models.strategy import predict_with_strategy_artifact

        scored = prepared.copy()
        scored["prediction"] = predict_with_strategy_artifact(
            scored,
            artifact=model,
            features=features,
            name="prediction",
        )
        scored = apply_simulations(
            scored,
            mean_col="prediction",
            std_dev=std_dev,
            config=MonteCarloConfig(
                simulations=int(section.get("monte_carlo_simulations", 10_000)),
                random_seed=section.get("monte_carlo_seed"),
            ),
            line_col=descriptor.line_col,
            id_col="pitcher_id",
        )
        scored["actual_over"] = (
            pd.to_numeric(scored[descriptor.target_col], errors="coerce")
            > pd.to_numeric(scored[descriptor.line_col], errors="coerce")
        ).astype(float)
        summary, by_bin = probability_calibration_report(
            scored,
            actual_col="actual_over",
            probability_col="prob_over",
        )

    report = by_bin.rename(
        columns={
            "bin": "prob_bin",
            "mean_probability": "mean_predicted_over",
            "observed_rate": "observed_over_rate",
            "abs_calibration_gap": "abs_gap",
        }
    ).copy()
    for key in ("rows", "brier_score", "log_loss", "ece"):
        report[key] = summary[key]
    report.insert(0, "stat", descriptor.stat)

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(report_path, index=False)
    logger.info(
        "Wrote outs calibration report to %s (rows=%s, ece=%s, brier=%s, log_loss=%s)",
        report_path,
        summary["rows"],
        summary["ece"],
        summary["brier_score"],
        summary["log_loss"],
    )
    return summary
