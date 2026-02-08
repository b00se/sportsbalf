"""Estimator-agnostic training and prediction helpers for MLB models."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd
from sklearn.base import RegressorMixin
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.mlb.features.feature_store import LIVE_CONTEXT_DEFAULTS
from src.mlb.models.registry import ModelSpec


def _ordered_features(frame: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    """Return ordered feature frame and validate required columns.

    Args:
        frame: Input dataframe.
        features: Required feature list in canonical order.

    Returns:
        Feature-only dataframe.

    Raises:
        ValueError: If required features are missing.
    """

    ordered_frame = frame.copy()
    missing = [column for column in features if column not in ordered_frame.columns]
    unsupported = [column for column in missing if column not in LIVE_CONTEXT_DEFAULTS]
    if unsupported:
        raise ValueError(f"Missing required feature columns: {unsupported}")

    for column in missing:
        ordered_frame[column] = LIVE_CONTEXT_DEFAULTS[column]

    ordered = ordered_frame[features].replace([np.inf, -np.inf], np.nan).copy()
    return ordered


def build_estimator(spec: ModelSpec, params: Mapping[str, Any] | None = None) -> Any:
    """Build a configured estimator instance for the given spec.

    Args:
        spec: Model specification with defaults and preprocessing mode.
        params: Optional parameter overrides.

    Returns:
        A regressor, potentially wrapped in a sklearn pipeline.
    """

    config: dict[str, Any] = dict(spec.default_params)
    if params:
        config.update(params)

    estimator = spec.factory(**config)

    steps: list[tuple[str, Any]] = [("imputer", SimpleImputer(strategy="median"))]
    if spec.preprocess_linear:
        steps.append(("scaler", StandardScaler()))
    steps.append(("model", estimator))
    return Pipeline(steps)


def fit_estimator(
    frame: pd.DataFrame,
    *,
    spec: ModelSpec,
    features: list[str],
    target_col: str = "strikeouts",
    params: Mapping[str, Any] | None = None,
) -> RegressorMixin:
    """Fit a model spec on the supplied dataframe.

    Args:
        frame: Training dataframe.
        spec: Model specification.
        features: Ordered features used for training.
        target_col: Target column name.
        params: Optional params overriding model defaults.

    Returns:
        Fitted sklearn-compatible regressor.
    """

    x_train = _ordered_features(frame, features)
    y_train = pd.to_numeric(frame[target_col], errors="coerce")
    valid = y_train.notna()
    x_train = x_train.loc[valid]
    y_train = y_train.loc[valid]

    model = build_estimator(spec, params=params)
    model.fit(x_train, y_train)
    return model


def predict_estimator(
    frame: pd.DataFrame,
    *,
    model: RegressorMixin,
    features: list[str],
    name: str = "prediction",
) -> pd.Series:
    """Generate predictions using a fitted model.

    Args:
        frame: Input dataframe.
        model: Fitted estimator implementing ``predict``.
        features: Ordered feature list used at train time.
        name: Name for returned prediction series.

    Returns:
        Predictions aligned to ``frame`` index.
    """

    x_input = _ordered_features(frame, features)
    preds = model.predict(x_input)
    return pd.Series(preds, index=frame.index, name=name)
