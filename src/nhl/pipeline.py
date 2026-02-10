"""NHL shots-on-goal orchestration shim."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.core.config import load_pipeline_config
from src.core.contracts import PipelineConfig
from src.core.simulation import MonteCarloConfig, apply_simulations
from src.utils.io import read_csv

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path("config/nhl.yaml")

REQUIRED_INPUT_COLUMNS: tuple[str, ...] = (
    "player_id",
    "player_name",
    "team",
    "opponent",
    "game_id",
    "sog_line",
)

REQUIRED_OUTPUT_COLUMNS: tuple[str, ...] = (
    "player_id",
    "player_name",
    "team",
    "opponent",
    "game_id",
    "sog_line",
    "predicted_shots_on_goal",
    "prob_over",
    "prob_under",
    "prob_push",
    "ev_over",
    "ev_under",
    "edge_over",
    "edge_under",
    "run_mode",
    "lines_status",
)


def _empty_output_frame() -> pd.DataFrame:
    """Build an empty output DataFrame with the stable NHL PR8 schema."""

    return pd.DataFrame(columns=list(REQUIRED_OUTPUT_COLUMNS))


def _safe_read_inference_input(inference_input_path: str | None) -> pd.DataFrame:
    """Read inference rows, returning an empty frame on expected failures.

    Args:
        inference_input_path: Configured CSV/Parquet path.

    Returns:
        Input DataFrame when readable; otherwise an empty DataFrame.
    """

    if not isinstance(inference_input_path, str) or not inference_input_path.strip():
        return pd.DataFrame()

    try:
        frame = read_csv(inference_input_path)
    except (FileNotFoundError, OSError, ValueError, KeyError):
        logger.warning(
            "NHL inference input unavailable; using empty fallback frame.",
            extra={"inference_input_path": inference_input_path},
        )
        return pd.DataFrame()

    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame()

    return frame


def _coerce_inference_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Ensure required NHL inference columns exist with compatible types.

    Args:
        frame: Raw loaded inference rows.

    Returns:
        Coerced inference frame.
    """

    coerced = frame.copy()
    for column in REQUIRED_INPUT_COLUMNS:
        if column not in coerced.columns:
            coerced[column] = pd.NA

    coerced["sog_line"] = pd.to_numeric(coerced["sog_line"], errors="coerce")
    return coerced


def _optional_numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    """Return an index-aligned numeric series for an optional input column."""

    if column in frame.columns:
        values = frame[column]
    else:
        values = pd.Series(index=frame.index, dtype="float64")
    return pd.to_numeric(values, errors="coerce")


def run_shots_on_goal_pipeline(
    config: PipelineConfig,
    retrain: bool = False,
) -> pd.DataFrame:
    """Execute NHL shots-on-goal inference with deterministic offline simulation.

    Args:
        config: Resolved pipeline config payload.
        retrain: Compatibility flag for shared adapter contract.

    Returns:
        NHL predictions with PR8-stable output schema.
    """

    del retrain

    section = config.section
    input_rows = _safe_read_inference_input(section.get("inference_input_path"))
    if input_rows.empty:
        return _empty_output_frame()

    inference_frame = _coerce_inference_columns(input_rows)

    default_over_price = float(section.get("default_over_decimal_price", 1.91))
    default_under_price = float(section.get("default_under_decimal_price", 1.91))
    fallback_prediction = float(section.get("fallback_prediction", 2.5))

    inference_frame["over_decimal_price"] = _optional_numeric_series(
        inference_frame, "over_decimal_price"
    ).fillna(default_over_price)
    inference_frame["under_decimal_price"] = _optional_numeric_series(
        inference_frame, "under_decimal_price"
    ).fillna(default_under_price)

    inferred_prediction = _optional_numeric_series(
        inference_frame, "predicted_shots_on_goal"
    )
    inference_frame["predicted_shots_on_goal"] = inferred_prediction.fillna(
        fallback_prediction
    )

    sim_config = MonteCarloConfig(
        simulations=int(section.get("monte_carlo_simulations", 10_000)),
        random_seed=section.get("monte_carlo_seed"),
    )

    simulated = apply_simulations(
        inference_frame,
        mean_col="predicted_shots_on_goal",
        std_dev=float(section.get("fallback_std", 1.0)),
        config=sim_config,
        line_col="sog_line",
        id_col="player_id",
    )

    simulated["run_mode"] = "prediction"
    simulated["lines_status"] = "present"

    return simulated.loc[:, list(REQUIRED_OUTPUT_COLUMNS)].copy()


def run(config_path: str | Path | None = None, retrain: bool = False) -> pd.DataFrame:
    """Compatibility shim for callers importing ``src.nhl.pipeline.run``.

    Args:
        config_path: Optional NHL config path.
        retrain: Compatibility retrain flag from the engine.

    Returns:
        NHL shots-on-goal pipeline output DataFrame.
    """

    config = load_pipeline_config(
        str(config_path or DEFAULT_CONFIG_PATH),
        sport_override="nhl",
        stat_override="shots_on_goal",
    )
    return run_shots_on_goal_pipeline(config=config, retrain=retrain)


__all__ = ["run", "run_shots_on_goal_pipeline"]
