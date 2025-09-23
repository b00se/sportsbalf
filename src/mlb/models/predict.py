"""Model utilities for MLB strikeout predictions."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Mapping, Optional

import joblib
import numpy as np
import pandas as pd
from xgboost import XGBRegressor

FEATURES: list[str] = [
    "rolling_K_avg_3",
    "rolling_K_avg_5",
    "rolling_pitch_count_5",
    "rolling_K_rate",
    "opponent_k_pct",
    "opponent_k_rate",
    "park_factor_K",
    "rest_days",
]

DEFAULT_MODEL_PATH = Path("models/xgb_tuned_pitcher_k_model.joblib")
DEFAULT_PARAMS: Mapping[str, float | int] = {
    "learning_rate": 0.1,
    "max_depth": 3,
    "n_estimators": 300,
    "subsample": 0.9,
    "colsample_bytree": 0.9,
    "reg_lambda": 1.0,
    "random_state": 42,
}


def load_model(model_path: str | Path | None = None) -> XGBRegressor:
    """Load the tuned XGBoost model from disk."""
    path = Path(model_path) if model_path else DEFAULT_MODEL_PATH
    return joblib.load(path)


def _ensure_features(df: pd.DataFrame) -> pd.DataFrame:
    missing = [col for col in FEATURES if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required feature columns: {missing}")
    return df[FEATURES]


def train_model(
    df: pd.DataFrame,
    params: Optional[Mapping[str, float | int]] = None,
) -> XGBRegressor:
    """Train an XGBoost model using the configured feature set."""

    features = _ensure_features(df)
    target = df["strikeouts"]

    config = {**DEFAULT_PARAMS}
    if params:
        config.update(params)

    model = XGBRegressor(**config)
    model.fit(features, target)
    return model


def predict_strikeouts(df: pd.DataFrame, model: XGBRegressor) -> pd.Series:
    """Predict strikeouts using the provided XGBoost model."""
    features = _ensure_features(df)
    preds = model.predict(features)
    return pd.Series(preds, index=df.index, name="prediction")


def residual_std(actual: Iterable[float], predicted: Iterable[float]) -> float:
    """Compute the sample standard deviation of model residuals."""
    actual_arr = np.asarray(actual, dtype=float)
    predicted_arr = np.asarray(predicted, dtype=float)
    residuals = actual_arr - predicted_arr
    if residuals.size <= 1:
        return float(np.std(residuals, ddof=0))
    return float(np.std(residuals, ddof=1))


def save_model(model: XGBRegressor, path: str | Path) -> None:
    """Persist the trained model to disk."""
    path_obj = Path(path)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, path_obj)
