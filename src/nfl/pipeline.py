"""NFL QB pass attempt prediction pipeline."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor

from src.core.config import load_pipeline_config
from src.core.contracts import PipelineConfig
from src.mlb.models.monte_carlo import MonteCarloConfig, apply_simulations
from src.nfl.data.providers import get_provider
from src.nfl.data.qb_attempts import build_qb_attempts_dataset
from src.nfl.data.underdog import PASS_ATTEMPTS_ALGOLIA_ID, import_ud_pass_attempt_lines
from src.nfl.models import (
    DEFAULT_MODEL_PATH,
    NFL_FEATURES,
    QBResidualBootstrapper,
    load_model,
    predict_attempts,
    residual_std,
    save_model,
    train_model,
)

DEFAULT_CONFIG_PATH = Path("config/nfl.yaml")


def _normalize_name(value: str | None) -> str:
    if not isinstance(value, str):
        return ""
    cleaned = re.sub(r"[^a-zA-Z\s]", " ", value).lower()
    return " ".join(cleaned.split())


def _maybe_build_dataset(config: Mapping[str, Any]) -> Path:
    dataset_path = Path(config.get("dataset_path", "data/qb_attempts_dataset.parquet"))
    if dataset_path.exists() and not config.get("rebuild_dataset"):
        return dataset_path

    years = config.get("dataset_years")
    if not years:
        start = int(config.get("start_year", 2015))
        end = int(config.get("end_year", 2024))
        years = list(range(start, end + 1))
    else:
        years = list(years)

    build_qb_attempts_dataset(
        years=years,
        output_path=dataset_path,
        provider=get_provider(config.get("provider")),
    )
    return dataset_path


def _load_dataset(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df.sort_values(["season", "week"], inplace=True)
    return df


def _split_years(df: pd.DataFrame, years: list[int]) -> pd.DataFrame:
    return df[df["season"].isin(years)].copy()


def _train_if_needed(
    dataset: pd.DataFrame,
    config: Mapping[str, Any],
    retrain: bool,
) -> tuple[
    pd.DataFrame,
    pd.Series,
    XGBRegressor,
    dict[str, float],
    pd.Series,
    QBResidualBootstrapper | None,
]:
    train_years = list(config.get("training_years", []))
    if not train_years:
        raise ValueError("Config must define training_years")

    train_df = _split_years(dataset, train_years)
    features, target = train_df[NFL_FEATURES].copy(), train_df["pass_attempts"].copy()
    mask = features.notna().all(axis=1) & target.notna()
    train_df = train_df.loc[mask].copy()

    model_path = Path(config.get("model_path", DEFAULT_MODEL_PATH))

    if model_path.exists() and not retrain:
        model = load_model(model_path)
    else:
        params = config.get("model_params")
        model = train_model(train_df, params=params)
        model_path.parent.mkdir(parents=True, exist_ok=True)
        save_model(model, model_path)

    train_preds = predict_attempts(train_df, model)
    metrics = {
        "rmse": float(
            np.sqrt(mean_squared_error(train_df["pass_attempts"], train_preds))
        ),
        "mae": float(mean_absolute_error(train_df["pass_attempts"], train_preds)),
        "r2": float(r2_score(train_df["pass_attempts"], train_preds)),
    }

    residuals = train_df["pass_attempts"].to_numpy() - train_preds.to_numpy()
    train_df["_residual"] = residuals
    residual_stats = (
        train_df.groupby("qb_id")["_residual"]
        .agg(["std", "count"])
        .dropna(subset=["std"])
    )
    sigma_min_history = int(config.get("sigma_min_history", 4))
    sigma_by_qb = residual_stats.loc[
        residual_stats["count"] >= sigma_min_history, "std"
    ]
    sigma_by_qb = sigma_by_qb.astype(float)

    bootstrapper = None
    try:
        residual_games = train_df[["qb_id", "pass_attempts"]].copy()
        residual_games["prediction"] = train_preds.to_numpy()
        bootstrapper = QBResidualBootstrapper.from_games(
            residual_games,
            prediction_col="prediction",
            min_history=int(config.get("bootstrap_min_history", 5)),
            mix_global_prob=float(config.get("bootstrap_mix_global_prob", 0.25)),
            min_sigma=float(config.get("bootstrap_min_sigma", 0.5)),
        )
    except Exception:
        bootstrapper = None

    train_df.drop(columns=["_residual"], inplace=True)

    return train_df, train_preds, model, metrics, sigma_by_qb, bootstrapper


def _prepare_inference_frame(
    dataset: pd.DataFrame,
    config: Mapping[str, Any],
) -> pd.DataFrame:
    inference_years = list(config.get("inference_years", [])) or list(
        config.get("validation_years", [])
    )
    if not inference_years:
        inference_years = [int(dataset["season"].max())]

    frame = _split_years(dataset, inference_years)
    if frame.empty:
        return frame

    # Use the most recent record per QB as the feature template.
    latest = (
        frame.sort_values(["qb_id", "season", "week"])
        .drop_duplicates("qb_id", keep="last")
        .copy()
    )
    return latest


def _attach_live_ud_lines(
    inference_df: pd.DataFrame,
    config: Mapping[str, Any],
) -> pd.DataFrame:
    algolia_id = config.get("ud_algolia_id", PASS_ATTEMPTS_ALGOLIA_ID)
    try:
        live_lines = import_ud_pass_attempt_lines(algolia_object_id=algolia_id)
    except Exception:  # pragma: no cover - network dependency
        return inference_df

    if live_lines.empty or inference_df.empty:
        return inference_df

    dataset_map = (
        inference_df[["qb_id", "qb_name", "team"]]
        .drop_duplicates("qb_id")
        .assign(name_key=lambda df: df["qb_name"].map(_normalize_name))
    )

    live_lines = live_lines.copy()
    live_lines["name_key"] = live_lines["player_name"].map(_normalize_name)
    live_lines = live_lines.merge(
        dataset_map[["name_key", "qb_id"]],
        on="name_key",
        how="left",
    )
    live_lines.dropna(subset=["qb_id"], inplace=True)
    if live_lines.empty:
        return inference_df

    live_lines.rename(columns={"line": "ud_line"}, inplace=True)
    live_lines["ud_line"] = pd.to_numeric(live_lines["ud_line"], errors="coerce")
    live_lines["game_id"] = live_lines["game_id"].astype(str)

    latest_lines = live_lines.sort_values(
        ["qb_id", "scheduled_at"], na_position="last"
    ).drop_duplicates("qb_id", keep="last")
    latest_lines.rename(columns={"game_id": "ud_game_id"}, inplace=True)

    merge_columns = [
        "qb_id",
        "ud_line",
        "ud_game_id",
        "scheduled_at",
        "player_ud_id",
        "over_decimal_price",
        "over_payout_multiplier",
        "over_american_price",
        "under_decimal_price",
        "under_payout_multiplier",
        "under_american_price",
    ]
    available_merge_columns = [
        col for col in merge_columns if col in latest_lines.columns
    ]

    merged = inference_df.merge(
        latest_lines[available_merge_columns],
        on="qb_id",
        how="left",
    )
    if "ud_game_id" in merged.columns:
        merged["ud_game_id"] = merged["ud_game_id"].astype(str)
        if "game_id" in merged.columns:
            merged["game_id"] = merged["ud_game_id"].combine_first(merged["game_id"])
        else:
            merged["game_id"] = merged["ud_game_id"]
        merged.drop(columns=["ud_game_id"], inplace=True)
    if "ud_line_y" in merged.columns:
        if "ud_line" in merged.columns:
            merged["ud_line"] = merged["ud_line_y"].combine_first(merged["ud_line"])
        else:
            merged["ud_line"] = merged["ud_line_y"]
        merged.drop(columns=["ud_line_y"], inplace=True)
    if "ud_line_x" in merged.columns:
        merged.drop(columns=["ud_line_x"], inplace=True)

    price_columns = [
        "over_decimal_price",
        "over_payout_multiplier",
        "over_american_price",
        "under_decimal_price",
        "under_payout_multiplier",
        "under_american_price",
    ]
    for col in price_columns:
        x_col = f"{col}_x"
        y_col = f"{col}_y"
        if y_col in merged.columns:
            if col in merged.columns:
                merged[col] = merged[y_col].combine_first(merged[col])
            else:
                merged[col] = merged[y_col]
            merged.drop(columns=[y_col], inplace=True)
        if x_col in merged.columns:
            if col not in merged.columns:
                merged[col] = merged[x_col]
            merged.drop(columns=[x_col], inplace=True)

    if "game_id" in merged.columns:
        merged["game_id"] = merged["game_id"].astype(str)
    if "ud_line" not in merged.columns:
        merged["ud_line"] = pd.NA
    return merged


def run_pass_attempts_pipeline(
    config: PipelineConfig,
    retrain: bool = False,
) -> pd.DataFrame:
    """Execute the NFL pass-attempt workflow and return enriched lines."""
    section = config.section
    dataset_path = _maybe_build_dataset(section)
    dataset = _load_dataset(dataset_path)

    train_df, train_preds, model, train_metrics, sigma_by_qb, bootstrapper = (
        _train_if_needed(dataset, section, retrain=retrain)
    )

    inference_df = _prepare_inference_frame(dataset, section)
    inference_df = _attach_live_ud_lines(inference_df, section)
    inference_df = inference_df[inference_df["ud_line"].notna()]

    inference_df = inference_df[inference_df[NFL_FEATURES].notna().all(axis=1)].copy()
    if inference_df.empty:
        return inference_df

    predictions = predict_attempts(inference_df, model)
    inference_df["prediction"] = predictions

    global_sigma = residual_std(train_df["pass_attempts"], train_preds)
    fallback_sigma = float(section.get("fallback_std", 1.0))
    if not global_sigma or np.isnan(global_sigma) or global_sigma <= 0:
        global_sigma = fallback_sigma

    sigma_map = sigma_by_qb.copy()
    sigma_map = sigma_map[sigma_map > 0]

    sigma_series = inference_df["qb_id"].map(sigma_map)
    sigma_series = pd.to_numeric(sigma_series, errors="coerce")
    sigma_series = sigma_series.fillna(global_sigma)

    min_sigma = float(section.get("min_sigma", 1.5))
    if min_sigma > 0:
        sigma_series = sigma_series.clip(lower=min_sigma)
    max_sigma = section.get("max_sigma")
    if max_sigma is not None:
        try:
            sigma_series = sigma_series.clip(upper=float(max_sigma))
        except (TypeError, ValueError):
            pass

    sim_config = MonteCarloConfig(
        simulations=int(section.get("monte_carlo_simulations", 10_000)),
        random_seed=section.get("monte_carlo_seed"),
    )

    sim_input = inference_df.copy()
    sim_input["k_line"] = sim_input["ud_line"]
    sim_input["pitcher_id"] = sim_input["qb_id"]
    sim_input["simulation_sigma"] = sigma_series

    simulated = apply_simulations(
        sim_input,
        mean_col="prediction",
        std_dev="simulation_sigma",
        config=sim_config,
        sampler=bootstrapper,
    )

    simulated.rename(
        columns={
            "ud_line": "attempts_line",
            "prediction": "predicted_pass_attempts",
        },
        inplace=True,
    )

    simulated["model_residual_std"] = sigma_series.to_numpy()
    simulated["training_rmse"] = train_metrics["rmse"]
    simulated["training_mae"] = train_metrics["mae"]
    simulated["training_r2"] = train_metrics["r2"]

    simulated.drop(columns=["k_line", "pitcher_id"], inplace=True, errors="ignore")

    return simulated


def run(config_path: str | Path | None = None, retrain: bool = False) -> pd.DataFrame:
    """Compatibility shim for callers still importing ``src.nfl.pipeline.run``."""
    config = load_pipeline_config(
        str(config_path or DEFAULT_CONFIG_PATH),
        sport_override="nfl",
        stat_override="pass_attempts",
    )
    return run_pass_attempts_pipeline(config=config, retrain=retrain)


__all__ = ["run"]
