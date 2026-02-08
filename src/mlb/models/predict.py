"""Model utilities for MLB strikeout predictions."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from src.mlb.models.registry import get_model_spec
from src.mlb.models.trainers import fit_estimator, predict_estimator

FEATURES: list[str] = [
    "rolling_K_avg_3",
    "rolling_K_avg_5",
    "rolling_pitch_count_5",
    "rolling_K_rate",
    "opponent_k_pct",
    "opponent_k_rate",
    "park_factor_K",
    "rest_days",
    "pitcher_throws_encoded",
    "projected_batter_stand_mix_L",
    "projected_batter_stand_mix_R",
    "same_hand_matchup_rate",
    "umpire_k_boost_expanding",
    "umpire_sample_size",
    "umpire_known_flag",
    "game_temp_f",
    "humidity_pct",
    "wind_speed_mph",
    "wind_out_to_cf_flag",
    "weather_run_env_idx",
    "humidity_x_temp",
    "weather_known_flag",
    "roof_closed_flag",
    "weather_effective_flag",
    "wind_speed_effective",
    "humidity_effective",
]

DEFAULT_MODEL_PATH = Path("models/xgb_tuned_pitcher_k_model.joblib")
DEFAULT_PARAMS: Mapping[str, float | int] = dict(
    get_model_spec("xgboost").default_params
)


def load_model(model_path: str | Path | None = None) -> Any:
    """Load a trained model artifact from disk."""

    path = Path(model_path) if model_path else DEFAULT_MODEL_PATH
    return joblib.load(path)


def train_model(
    df: pd.DataFrame,
    params: Mapping[str, float | int] | None = None,
    model_name: str = "xgboost",
) -> Any:
    """Train a model using the configured feature set.

    Args:
        df: Training frame.
        params: Optional model parameter overrides.
        model_name: Model registry identifier.

    Returns:
        Fitted model.
    """

    spec = get_model_spec(model_name)
    return fit_estimator(df, spec=spec, features=FEATURES, params=params)


def predict_strikeouts(df: pd.DataFrame, model: Any) -> pd.Series:
    """Predict strikeouts for each row in ``df`` using the supplied model."""

    return predict_estimator(df, model=model, features=FEATURES, name="prediction")


def residual_std(actual: Iterable[float], predicted: Iterable[float]) -> float:
    """Compute the sample standard deviation of model residuals."""

    actual_arr = np.asarray(actual, dtype=float)
    predicted_arr = np.asarray(predicted, dtype=float)
    residuals = actual_arr - predicted_arr
    if residuals.size <= 1:
        return float(np.std(residuals, ddof=0))
    return float(np.std(residuals, ddof=1))


def save_model(model: Any, path: str | Path) -> None:
    """Persist a trained model to disk."""

    path_obj = Path(path)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, path_obj)
