"""NHL shots-on-goal orchestration shim."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from src.core.config import load_pipeline_config
from src.core.contracts import PipelineConfig
from src.core.simulation import MonteCarloConfig, apply_simulations
from src.nhl.data.moneypuck_ingest import refresh_skater_games_snapshot
from src.nhl.data.providers import get_provider
from src.nhl.features.shots_on_goal import (
    build_sog_inference_features,
    build_sog_training_features,
)
from src.nhl.models import (
    NHL_FEATURES,
    SOGResidualBootstrapper,
    load_model,
    predict_sog,
    residual_std,
    save_model,
    train_model,
)
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

ADDITIVE_OUTPUT_COLUMNS: tuple[str, ...] = (
    "baseline_predicted_shots_on_goal",
    "model_residual_std",
    "training_rmse",
    "training_mae",
    "training_r2",
    "model_name",
)


def _empty_output_frame() -> pd.DataFrame:
    """Build an empty output DataFrame with stable NHL schema."""

    return pd.DataFrame(columns=list(REQUIRED_OUTPUT_COLUMNS + ADDITIVE_OUTPUT_COLUMNS))


def _safe_read_inference_input(inference_input_path: str | None) -> pd.DataFrame:
    """Read inference rows, returning an empty frame on expected failures."""

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
    """Ensure required inference columns exist with compatible types."""

    coerced = frame.copy()
    for column in REQUIRED_INPUT_COLUMNS:
        if column not in coerced.columns:
            coerced[column] = pd.NA

    coerced["sog_line"] = pd.to_numeric(coerced["sog_line"], errors="coerce")
    coerced["player_id"] = coerced["player_id"].astype("string")
    return coerced


def _optional_numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    """Return an index-aligned numeric series for an optional input column."""

    if column in frame.columns:
        values = frame[column]
    else:
        values = pd.Series(index=frame.index, dtype="float64")
    return pd.to_numeric(values, errors="coerce")


def _prepare_training_frame(
    skater_games: pd.DataFrame,
    section: dict[str, Any],
) -> pd.DataFrame:
    """Build leakage-safe training frame filtered by configured seasons/history."""

    training_seasons = [
        int(value)
        for value in section.get(
            "training_seasons", section.get("provider_seasons", [])
        )
    ]
    filtered = skater_games.copy()
    if training_seasons:
        filtered = filtered.loc[
            filtered["season"].astype(int).isin(training_seasons)
        ].copy()

    training = build_sog_training_features(
        skater_games=filtered,
        rolling_windows=[
            int(value)
            for value in section.get("feature_rolling_windows", [5, 10])
        ],
    )

    min_games = int(section.get("min_training_games_per_player", 5))
    player_counts = training.groupby("player_id", dropna=False)[
        "shots_on_goal"
    ].transform("count")
    training = training.loc[player_counts >= min_games].copy()
    training = training.dropna(subset=NHL_FEATURES + ["shots_on_goal"])
    return training


def _train_or_load_model(
    *,
    training: pd.DataFrame,
    section: dict[str, Any],
    retrain: bool,
) -> tuple[Any, dict[str, float], pd.Series, float, SOGResidualBootstrapper | None]:
    """Load compatible model artifact or retrain when required."""

    model_path = Path(str(section.get("model_path")))
    model_name = str(section.get("model_name", "xgboost"))

    model = None
    force_retrain = bool(retrain)
    if model_path.exists() and not force_retrain:
        try:
            model, _ = load_model(
                model_path,
                expected_feature_columns=NHL_FEATURES,
            )
        except ValueError:
            force_retrain = True

    if model is None or force_retrain:
        model = train_model(
            training,
            params=section.get("model_params"),
            model_name=model_name,
        )
        save_model(
            model,
            model_path,
            feature_columns=NHL_FEATURES,
            model_name=model_name,
        )

    train_predictions = predict_sog(training, model)
    rmse = float(
        np.sqrt(mean_squared_error(training["shots_on_goal"], train_predictions))
    )
    mae = float(mean_absolute_error(training["shots_on_goal"], train_predictions))
    r2 = float(r2_score(training["shots_on_goal"], train_predictions))

    residual_frame = training[["player_id", "shots_on_goal"]].copy()
    residual_frame["prediction"] = train_predictions.to_numpy()
    residual_frame["residual"] = (
        residual_frame["shots_on_goal"].to_numpy(dtype=float)
        - residual_frame["prediction"].to_numpy(dtype=float)
    )

    sigma_stats = (
        residual_frame.groupby("player_id", dropna=False)["residual"]
        .agg(["std", "count"])
        .dropna(subset=["std"])
    )
    sigma_min_history = int(section.get("sigma_min_history", 5))
    sigma_by_player = sigma_stats.loc[sigma_stats["count"] >= sigma_min_history, "std"]
    sigma_by_player = pd.to_numeric(sigma_by_player, errors="coerce")

    global_sigma = residual_std(training["shots_on_goal"], train_predictions)

    bootstrapper: SOGResidualBootstrapper | None = None
    if bool(section.get("bootstrap_enabled", True)):
        try:
            bootstrapper = SOGResidualBootstrapper.from_games(
                residual_frame,
                prediction_col="prediction",
                min_history=sigma_min_history,
                mix_global_prob=float(section.get("bootstrap_mix_global_prob", 0.25)),
                min_sigma=float(section.get("bootstrap_min_sigma", 0.25)),
            )
        except Exception:
            bootstrapper = None

    return (
        model,
        {
            "training_rmse": rmse,
            "training_mae": mae,
            "training_r2": r2,
        },
        sigma_by_player,
        float(global_sigma),
        bootstrapper,
    )


def _resolve_sigma_series(
    inference_frame: pd.DataFrame,
    *,
    sigma_by_player: pd.Series,
    global_sigma: float,
    section: dict[str, Any],
) -> pd.Series:
    """Resolve per-row simulation sigma with fallback hierarchy and clipping."""

    fallback_sigma = float(section.get("fallback_std", 1.0))
    resolved_global = global_sigma
    if not np.isfinite(resolved_global) or resolved_global <= 0:
        resolved_global = fallback_sigma

    sigma_series = inference_frame["player_id"].map(sigma_by_player)
    sigma_series = pd.to_numeric(sigma_series, errors="coerce").fillna(resolved_global)

    min_sigma = float(section.get("min_sigma", 0.5))
    if min_sigma > 0:
        sigma_series = sigma_series.clip(lower=min_sigma)

    max_sigma = section.get("max_sigma")
    if max_sigma is not None:
        try:
            sigma_series = sigma_series.clip(upper=float(max_sigma))
        except (TypeError, ValueError):
            pass

    return sigma_series


def run_shots_on_goal_pipeline(
    config: PipelineConfig,
    retrain: bool = False,
) -> pd.DataFrame:
    """Execute NHL shots-on-goal inference with residual-aware simulation."""

    section = config.section
    input_rows = _safe_read_inference_input(section.get("inference_input_path"))
    if input_rows.empty:
        return _empty_output_frame()

    inference_frame = _coerce_inference_columns(input_rows)

    default_over_price = float(section.get("default_over_decimal_price", 1.91))
    default_under_price = float(section.get("default_under_decimal_price", 1.91))
    fallback_prediction = float(section.get("fallback_prediction", 2.5))
    provider_seasons = [int(value) for value in section.get("provider_seasons", [])]

    try:
        if bool(section.get("auto_refresh_snapshot", False)):
            refresh_skater_games_snapshot(
                snapshot_path=str(section["moneypuck_skater_games_snapshot_path"]),
                curated_cache_path=str(
                    section["moneypuck_skater_games_curated_cache_path"]
                ),
                seasons=provider_seasons,
            )

        provider = get_provider(
            section.get("provider"),
            curated_cache_path=str(
                section["moneypuck_skater_games_curated_cache_path"]
            ),
        )
        provider_result = provider.load_skater_games(provider_seasons)
    except Exception as exc:
        if bool(section.get("fail_on_provider_error", True)):
            raise RuntimeError(f"NHL provider load failure: {exc}") from exc
        logger.warning("NHL provider load failed; using fallback predictions.")
        provider_result = None

    if provider_result is None:
        inference_frame["baseline_predicted_shots_on_goal"] = fallback_prediction
        inference_frame["predicted_shots_on_goal"] = fallback_prediction
        inference_frame["model_residual_std"] = float(section.get("fallback_std", 1.0))
        inference_frame["training_rmse"] = np.nan
        inference_frame["training_mae"] = np.nan
        inference_frame["training_r2"] = np.nan
        inference_frame["model_name"] = str(section.get("model_name", "xgboost"))
        inference_frame["over_decimal_price"] = _optional_numeric_series(
            inference_frame,
            "over_decimal_price",
        ).fillna(default_over_price)
        inference_frame["under_decimal_price"] = _optional_numeric_series(
            inference_frame,
            "under_decimal_price",
        ).fillna(default_under_price)
        sim_config = MonteCarloConfig(
            simulations=int(section.get("monte_carlo_simulations", 10_000)),
            random_seed=section.get("monte_carlo_seed"),
        )
        simulated = apply_simulations(
            inference_frame,
            mean_col="predicted_shots_on_goal",
            std_dev="model_residual_std",
            config=sim_config,
            line_col="sog_line",
            id_col="player_id",
        )
        simulated["run_mode"] = "prediction"
        simulated["lines_status"] = "present"
        output_columns = list(REQUIRED_OUTPUT_COLUMNS + ADDITIVE_OUTPUT_COLUMNS)
        return simulated.loc[:, output_columns].copy()

    inference_frame = build_sog_inference_features(
        inference_rows=inference_frame,
        skater_games=provider_result.data,
        rolling_windows=[
            int(value)
            for value in section.get("feature_rolling_windows", [5, 10])
        ],
        fallback_prediction=fallback_prediction,
    )

    training_frame = _prepare_training_frame(provider_result.data, section)
    training_is_empty = training_frame.empty

    inference_frame["predicted_shots_on_goal"] = pd.to_numeric(
        inference_frame["baseline_predicted_shots_on_goal"],
        errors="coerce",
    ).fillna(fallback_prediction)

    if training_is_empty:
        sigma_series = pd.Series(
            float(section.get("fallback_std", 1.0)),
            index=inference_frame.index,
            dtype="float64",
        )
        training_metrics = {
            "training_rmse": np.nan,
            "training_mae": np.nan,
            "training_r2": np.nan,
        }
        bootstrapper = None
    else:
        (
            model,
            training_metrics,
            sigma_by_player,
            global_sigma,
            bootstrapper,
        ) = _train_or_load_model(
            training=training_frame,
            section=section,
            retrain=retrain,
        )

        valid_model_rows = inference_frame[NHL_FEATURES].notna().all(axis=1)
        if valid_model_rows.any():
            model_predictions = predict_sog(
                inference_frame.loc[valid_model_rows],
                model,
            )
            inference_frame.loc[valid_model_rows, "predicted_shots_on_goal"] = (
                model_predictions.to_numpy()
            )

        sigma_series = _resolve_sigma_series(
            inference_frame,
            sigma_by_player=sigma_by_player,
            global_sigma=global_sigma,
            section=section,
        )

    inference_frame["over_decimal_price"] = _optional_numeric_series(
        inference_frame,
        "over_decimal_price",
    ).fillna(default_over_price)
    inference_frame["under_decimal_price"] = _optional_numeric_series(
        inference_frame,
        "under_decimal_price",
    ).fillna(default_under_price)

    inference_frame["model_residual_std"] = sigma_series.to_numpy()

    sim_config = MonteCarloConfig(
        simulations=int(section.get("monte_carlo_simulations", 10_000)),
        random_seed=section.get("monte_carlo_seed"),
    )

    simulated = apply_simulations(
        inference_frame,
        mean_col="predicted_shots_on_goal",
        std_dev="model_residual_std",
        config=sim_config,
        sampler=bootstrapper if bool(section.get("bootstrap_enabled", True)) else None,
        line_col="sog_line",
        id_col="player_id",
    )

    simulated["run_mode"] = "prediction"
    simulated["lines_status"] = "present"
    simulated["training_rmse"] = training_metrics["training_rmse"]
    simulated["training_mae"] = training_metrics["training_mae"]
    simulated["training_r2"] = training_metrics["training_r2"]
    simulated["model_name"] = str(section.get("model_name", "xgboost"))

    output_columns = list(REQUIRED_OUTPUT_COLUMNS + ADDITIVE_OUTPUT_COLUMNS)
    return simulated.loc[:, output_columns].copy()


def run(config_path: str | Path | None = None, retrain: bool = False) -> pd.DataFrame:
    """Compatibility shim for callers importing ``src.nhl.pipeline.run``."""

    config = load_pipeline_config(
        str(config_path or DEFAULT_CONFIG_PATH),
        sport_override="nhl",
        stat_override="shots_on_goal",
    )
    return run_shots_on_goal_pipeline(config=config, retrain=retrain)


__all__ = ["run", "run_shots_on_goal_pipeline"]
