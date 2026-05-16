"""Strategy-aware training and inference for MLB strikeout models."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.mlb.models.buckets import SegmentationConfig, fit_bucket_model
from src.mlb.models.registry import ModelSpec
from src.mlb.models.trainers import fit_estimator, predict_estimator
from src.mlb.pitcher_props.outs_features import (
    OUTS_FEATURE_COLUMNS,
    ensure_outs_feature_defaults,
)


def train_strategy_artifact(
    frame: pd.DataFrame,
    *,
    spec: ModelSpec,
    features: list[str],
    target_col: str,
    strategy_name: str,
    segmentation: SegmentationConfig,
    model_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Train a model artifact for global or segmented strategy routing."""

    base_model = fit_estimator(
        frame,
        spec=spec,
        features=features,
        target_col=target_col,
        params=model_params,
    )

    if strategy_name == "global":
        return {
            "strategy_name": "global",
            "effective_strategy": "global",
            "model_name": spec.name,
            "features": list(features),
            "bucket_model": None,
            "bucket_models": {"global": base_model},
        }

    effective_strategy = strategy_name
    bucket_model: Any | None = None
    try:
        bucket_model = fit_bucket_model(strategy_name, frame, settings=segmentation)
    except ValueError:
        if strategy_name == "kmeans":
            bucket_model = fit_bucket_model("quantile3", frame, settings=segmentation)
            effective_strategy = "kmeans->quantile3"
        else:
            return {
                "strategy_name": strategy_name,
                "effective_strategy": f"{strategy_name}->global",
                "model_name": spec.name,
                "features": list(features),
                "bucket_model": None,
                "bucket_models": {"global": base_model},
            }

    train_labels = bucket_model.assign(frame)
    bucket_models: dict[str, Any] = {"fallback": base_model}
    for bucket in sorted(train_labels.unique()):
        bucket_idx = train_labels[train_labels == bucket].index
        if len(bucket_idx) < segmentation.min_bucket_size:
            continue
        bucket_models[bucket] = fit_estimator(
            frame.loc[bucket_idx],
            spec=spec,
            features=features,
            target_col=target_col,
            params=model_params,
        )

    return {
        "strategy_name": strategy_name,
        "effective_strategy": effective_strategy,
        "model_name": spec.name,
        "features": list(features),
        "bucket_model": bucket_model,
        "bucket_models": bucket_models,
    }


def predict_with_strategy_artifact(
    frame: pd.DataFrame,
    *,
    artifact: Any,
    features: list[str],
    name: str = "prediction",
) -> pd.Series:
    """Predict using either a raw estimator or strategy artifact."""

    if any(column in features for column in OUTS_FEATURE_COLUMNS):
        frame = ensure_outs_feature_defaults(frame)

    # Backward compatibility for older artifacts that store estimator only.
    if not isinstance(artifact, dict) or "bucket_models" not in artifact:
        preds = predict_estimator(frame, model=artifact, features=features, name=name)
        return preds

    strategy_name = str(artifact.get("strategy_name", "global"))
    bucket_models = artifact.get("bucket_models") or {}
    global_model = bucket_models.get("global") or bucket_models.get("fallback")
    if global_model is None:
        raise ValueError("Invalid strategy artifact: missing global/fallback model.")

    if strategy_name == "global" or artifact.get("bucket_model") is None:
        return predict_estimator(
            frame,
            model=global_model,
            features=features,
            name=name,
        )

    bucket_model = artifact["bucket_model"]
    labels = bucket_model.assign(frame)

    preds = pd.Series(index=frame.index, dtype=float, name=name)
    for bucket in sorted(labels.unique()):
        bucket_idx = labels[labels == bucket].index
        model = bucket_models.get(bucket, bucket_models.get("fallback", global_model))
        bucket_preds = predict_estimator(
            frame.loc[bucket_idx],
            model=model,
            features=features,
            name=name,
        )
        preds.loc[bucket_idx] = bucket_preds.values
    return preds


def strategy_metadata(artifact: Any) -> dict[str, Any]:
    """Return serializable strategy metadata summary."""

    if not isinstance(artifact, dict) or "bucket_models" not in artifact:
        return {
            "strategy_name": "global",
            "effective_strategy": "global",
            "bucket_model": None,
            "bucket_model_keys": ["global"],
        }

    bucket_model = artifact.get("bucket_model")
    return {
        "strategy_name": artifact.get("strategy_name", "global"),
        "effective_strategy": artifact.get("effective_strategy", "global"),
        "bucket_model": bucket_model.metadata() if bucket_model else None,
        "bucket_model_keys": sorted(list((artifact.get("bucket_models") or {}).keys())),
    }


def strategy_candidates_from_config(segmentation: SegmentationConfig) -> list[str]:
    """Return strategy list used for tournament scoring."""

    if not segmentation.enabled:
        return ["global"]

    candidates = ["global"]
    for method in segmentation.bucket_methods:
        normalized = str(method).strip().lower()
        if normalized in {"quantile3", "kmeans"} and normalized not in candidates:
            candidates.append(normalized)
    return candidates
