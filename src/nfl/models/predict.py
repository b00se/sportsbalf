"""Model utilities for NFL QB pass attempt predictions."""
from __future__ import annotations

from pathlib import Path
from typing import Mapping, Optional

import joblib
import numpy as np
import pandas as pd
from xgboost import XGBRegressor

NFL_FEATURES: list[str] = [
    "prev_attempts",
    "rolling3_attempts",
    "season_avg_attempts",
    "season_avg_attempts_to_date",
    "career_avg_attempts",
    "plays_per_game",
    "pass_rate",
    "neutral_pass_rate",
    "pass_rate_over_expected",
    "plays_faced",
    "opponent_pass_rate_allowed",
    "opponent_neutral_pass_rate",
    "qb_dropbacks",
    "avg_cpoe",
    "epa_per_dropback",
    "air_yards_per_attempt",
    "qb_rush_attempts",
    "ngs_avg_time_to_throw",
    "ngs_avg_air_yards",
    "ngs_cpoe",
    "spread",
    "total",
    "rest_days",
    "short_week",
    "is_divisional",
    "home",
]

DEFAULT_MODEL_PATH = Path("models/xgb_qb_attempts.joblib")
DEFAULT_PARAMS: Mapping[str, float | int] = {
    "learning_rate": 0.08,
    "max_depth": 4,
    "n_estimators": 400,
    "subsample": 0.9,
    "colsample_bytree": 0.9,
    "reg_lambda": 1.25,
    "random_state": 42,
}


def load_model(model_path: str | Path | None = None) -> XGBRegressor:
    """Load a persisted pass-attempt model from disk."""
    path = Path(model_path) if model_path else DEFAULT_MODEL_PATH
    return joblib.load(path)


def save_model(model: XGBRegressor, path: str | Path) -> None:
    """Persist the trained model to disk."""
    joblib.dump(model, Path(path))


def _ensure_features(df: pd.DataFrame) -> pd.DataFrame:
    missing = [col for col in NFL_FEATURES if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required feature columns: {missing}")
    features = df[NFL_FEATURES].copy()
    for column in features.columns:
        if features[column].dtype == bool:
            features[column] = features[column].astype(float)
    return features


def _sanitize_training_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Return feature matrix and target vector with NaNs dropped."""
    features = _ensure_features(df)
    features = features.apply(pd.to_numeric, errors="coerce")
    target = pd.to_numeric(df["pass_attempts"], errors="coerce")
    mask = features.notna().all(axis=1) & target.notna()
    return features.loc[mask], target.loc[mask]


def train_model(
    df: pd.DataFrame,
    params: Optional[Mapping[str, float | int]] = None,
) -> XGBRegressor:
    """Train an XGBoost model using the configured feature set."""

    features, target = _sanitize_training_frame(df)
    config = {**DEFAULT_PARAMS}
    if params:
        config.update(params)

    model = XGBRegressor(**config)
    model.fit(features, target)
    return model


def predict_attempts(df: pd.DataFrame, model: XGBRegressor) -> pd.Series:
    """Predict pass attempts using the provided XGBoost model."""
    features = _ensure_features(df)
    preds = model.predict(features)
    return pd.Series(preds, index=df.index, name="prediction")


def residual_std(actual: pd.Series, predicted: pd.Series) -> float:
    """Compute the sample standard deviation of model residuals."""
    actual_arr = np.asarray(actual, dtype=float)
    predicted_arr = np.asarray(predicted, dtype=float)
    residuals = actual_arr - predicted_arr
    if residuals.size <= 1:
        return float(np.std(residuals, ddof=0))
    return float(np.std(residuals, ddof=1))
