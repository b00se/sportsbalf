"""Model utilities for NHL shots-on-goal predictions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from hashlib import sha256
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from xgboost import XGBRegressor

NHL_FEATURES: list[str] = [
    "sog_avg_last_5",
    "sog_avg_last_10",
    "sog_avg_season_to_date",
    "toi_avg_last_5",
    "toi_avg_last_10",
    "games_played_to_date",
    "days_since_last_game",
    "team_sog_for_avg_last_5",
    "opponent_sog_allowed_avg_last_5",
]

DEFAULT_MODEL_PATH = Path("models/nhl_shots_on_goal_model.joblib")
DEFAULT_PARAMS: Mapping[str, float | int] = {
    "learning_rate": 0.08,
    "max_depth": 4,
    "n_estimators": 300,
    "subsample": 0.9,
    "colsample_bytree": 0.9,
    "reg_lambda": 1.25,
    "random_state": 42,
}


def compute_feature_schema_hash(feature_columns: Sequence[str]) -> str:
    """Return a stable hash for the ordered feature schema."""

    payload = "||".join(str(column) for column in feature_columns)
    return sha256(payload.encode("utf-8")).hexdigest()


def artifact_is_compatible(
    metadata: Mapping[str, Any],
    *,
    expected_feature_columns: Sequence[str],
) -> bool:
    """Return whether persisted metadata matches expected feature schema."""

    actual_hash = metadata.get("feature_schema_hash")
    expected_hash = compute_feature_schema_hash(expected_feature_columns)
    return isinstance(actual_hash, str) and actual_hash == expected_hash


def _ensure_features(df: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in NHL_FEATURES if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required NHL feature columns: {missing}")

    features = df.loc[:, NHL_FEATURES].copy()
    for column in features.columns:
        if features[column].dtype == bool:
            features[column] = features[column].astype(float)
    return features.apply(pd.to_numeric, errors="coerce")


def _sanitize_training_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    features = _ensure_features(df)
    target = pd.to_numeric(df["shots_on_goal"], errors="coerce")
    mask = features.notna().all(axis=1) & target.notna()
    return features.loc[mask], target.loc[mask]


def train_model(
    df: pd.DataFrame,
    params: Mapping[str, float | int] | None = None,
    model_name: str = "xgboost",
) -> Any:
    """Train an NHL model using the configured feature set."""

    if model_name != "xgboost":
        raise ValueError(f"Unsupported NHL model_name '{model_name}'.")

    features, target = _sanitize_training_frame(df)
    config = {**DEFAULT_PARAMS}
    if params:
        config.update(params)

    model = XGBRegressor(**config)
    model.fit(features, target)
    return model


def predict_sog(df: pd.DataFrame, model: Any) -> pd.Series:
    """Predict shots on goal for each input row."""

    features = _ensure_features(df)
    predictions = model.predict(features)
    return pd.Series(predictions, index=df.index, name="prediction")


def residual_std(actual: pd.Series, predicted: pd.Series) -> float:
    """Compute sample standard deviation of residuals."""

    actual_arr = np.asarray(actual, dtype=float)
    predicted_arr = np.asarray(predicted, dtype=float)
    residuals = actual_arr - predicted_arr
    if residuals.size <= 1:
        return float(np.std(residuals, ddof=0))
    return float(np.std(residuals, ddof=1))


def save_model(
    model: Any,
    path: str | Path,
    *,
    feature_columns: Sequence[str],
    model_name: str,
) -> None:
    """Persist NHL model artifact with compatibility metadata."""

    path_obj = Path(path)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "artifact_version": 1,
        "model": model,
        "model_name": str(model_name),
        "feature_columns": list(feature_columns),
        "feature_schema_hash": compute_feature_schema_hash(feature_columns),
    }
    joblib.dump(payload, path_obj)


def load_model(
    model_path: str | Path | None = None,
    *,
    expected_feature_columns: Sequence[str] | None = None,
) -> tuple[Any, dict[str, Any]]:
    """Load NHL model artifact and validate schema compatibility."""

    path = Path(model_path) if model_path else DEFAULT_MODEL_PATH
    try:
        payload = joblib.load(path)
    except Exception as exc:  # pragma: no cover - exercised in tests via type
        raise ValueError(f"Failed to load NHL model artifact from '{path}'.") from exc

    if not isinstance(payload, dict) or "model" not in payload:
        raise ValueError("Failed to load NHL model artifact: unsupported format.")

    metadata = {
        "artifact_version": payload.get("artifact_version"),
        "model_name": payload.get("model_name"),
        "feature_columns": payload.get("feature_columns"),
        "feature_schema_hash": payload.get("feature_schema_hash"),
    }

    if expected_feature_columns is not None and not artifact_is_compatible(
        metadata,
        expected_feature_columns=expected_feature_columns,
    ):
        raise ValueError("NHL model artifact schema mismatch.")

    return payload["model"], metadata


__all__ = [
    "DEFAULT_MODEL_PATH",
    "NHL_FEATURES",
    "artifact_is_compatible",
    "compute_feature_schema_hash",
    "load_model",
    "predict_sog",
    "residual_std",
    "save_model",
    "train_model",
]
