"""Segmentation bucket strategies for MLB model tournaments."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler


@dataclass(frozen=True, slots=True)
class SegmentationConfig:
    """Configuration for segmented strategy evaluation and training."""

    enabled: bool = False
    bucket_methods: tuple[str, ...] = ("quantile3", "kmeans")
    min_bucket_size: int = 150
    workload_col: str = "rolling_pitch_count_5"
    kmeans_n_clusters: int = 3
    kmeans_features: tuple[str, ...] = (
        "rolling_pitch_count_5",
        "rolling_K_avg_5",
        "rest_days",
    )
    random_seed: int = 42


@dataclass(frozen=True, slots=True)
class QuantileBucketModel:
    """Fitted quantile bucket strategy."""

    workload_col: str
    q1: float
    q2: float

    @property
    def name(self) -> str:
        return "quantile3"

    def assign(self, frame: pd.DataFrame) -> pd.Series:
        """Assign rows to low/mid/high quantile workload buckets."""

        workload = pd.to_numeric(frame.get(self.workload_col), errors="coerce").fillna(
            (self.q1 + self.q2) / 2
        )

        labels = pd.Series("bucket_1", index=frame.index, dtype="object")
        labels.loc[workload <= self.q1] = "bucket_0"
        labels.loc[workload > self.q2] = "bucket_2"
        return labels

    def metadata(self) -> dict[str, Any]:
        """Return serializable quantile strategy metadata."""

        return {
            "method": self.name,
            "workload_col": self.workload_col,
            "thresholds": {
                "q1": float(self.q1),
                "q2": float(self.q2),
            },
        }


@dataclass(frozen=True, slots=True)
class KMeansBucketModel:
    """Fitted k-means bucket strategy."""

    feature_cols: tuple[str, ...]
    workload_col: str
    scaler: StandardScaler
    kmeans: KMeans
    fill_values: dict[str, float]
    cluster_to_bucket: dict[int, str]

    @property
    def name(self) -> str:
        return "kmeans"

    def _prepare(self, frame: pd.DataFrame) -> pd.DataFrame:
        prepared = frame.loc[:, list(self.feature_cols)].replace(
            [np.inf, -np.inf], np.nan
        )
        for col in self.feature_cols:
            prepared[col] = pd.to_numeric(prepared[col], errors="coerce")
            prepared[col] = prepared[col].fillna(self.fill_values[col])
        return prepared

    def assign(self, frame: pd.DataFrame) -> pd.Series:
        """Assign rows to k-means cluster buckets."""

        x = self._prepare(frame)
        scaled = self.scaler.transform(x)
        clusters = self.kmeans.predict(scaled)
        return pd.Series(clusters, index=frame.index).map(self.cluster_to_bucket)

    def metadata(self) -> dict[str, Any]:
        """Return serializable k-means strategy metadata."""

        centers = []
        workload_idx = (
            self.feature_cols.index(self.workload_col)
            if self.workload_col in self.feature_cols
            else 0
        )
        for cluster_id, center in enumerate(self.kmeans.cluster_centers_):
            centers.append(
                {
                    "cluster": int(cluster_id),
                    "bucket": self.cluster_to_bucket.get(cluster_id, "unknown"),
                    "center_scaled": [float(v) for v in center.tolist()],
                    "workload_center_scaled": float(center[workload_idx]),
                }
            )
        return {
            "method": self.name,
            "feature_cols": list(self.feature_cols),
            "fill_values": self.fill_values,
            "cluster_to_bucket": self.cluster_to_bucket,
            "centers": centers,
        }


def segmentation_config_from_model_selection(
    raw: Mapping[str, Any],
) -> SegmentationConfig:
    """Build segmentation settings from model-selection config."""

    seg = raw.get("segmentation")
    if not isinstance(seg, Mapping):
        seg = {}

    methods_raw = seg.get("bucket_methods", ["quantile3", "kmeans"])
    methods = tuple(str(method).strip().lower() for method in methods_raw)

    kmeans = seg.get("kmeans")
    if not isinstance(kmeans, Mapping):
        kmeans = {}

    features_raw = kmeans.get(
        "features",
        ["rolling_pitch_count_5", "rolling_K_avg_5", "rest_days"],
    )

    return SegmentationConfig(
        enabled=bool(seg.get("enabled", False)),
        bucket_methods=methods,
        min_bucket_size=max(1, int(seg.get("min_bucket_size", 150))),
        workload_col=str(seg.get("workload_col", "rolling_pitch_count_5")),
        kmeans_n_clusters=max(2, int(kmeans.get("n_clusters", 3))),
        kmeans_features=tuple(str(col) for col in features_raw),
        random_seed=int(raw.get("random_seed", 42)),
    )


def fit_quantile_bucket_model(
    frame: pd.DataFrame,
    *,
    workload_col: str,
    min_bucket_size: int,
) -> QuantileBucketModel:
    """Fit deterministic 3-quantile workload buckets."""

    workload = pd.to_numeric(frame.get(workload_col), errors="coerce").dropna()
    if workload.empty:
        raise ValueError("Cannot fit quantile buckets without workload values.")

    q1 = float(workload.quantile(1 / 3))
    q2 = float(workload.quantile(2 / 3))
    model = QuantileBucketModel(workload_col=workload_col, q1=q1, q2=q2)

    labels = model.assign(frame)
    counts = labels.value_counts()
    if (counts < min_bucket_size).any():
        raise ValueError(
            "Quantile buckets below min_bucket_size: "
            f"{counts.to_dict()} (min={min_bucket_size})"
        )
    return model


def fit_kmeans_bucket_model(
    frame: pd.DataFrame,
    *,
    feature_cols: Sequence[str],
    workload_col: str,
    n_clusters: int,
    min_bucket_size: int,
    random_seed: int,
) -> KMeansBucketModel:
    """Fit k-means buckets and map clusters by workload center ordering."""

    missing = [col for col in feature_cols if col not in frame.columns]
    if missing:
        raise ValueError(f"Missing k-means feature columns: {missing}")

    x = frame.loc[:, list(feature_cols)].replace([np.inf, -np.inf], np.nan).copy()
    fill_values: dict[str, float] = {}
    for col in feature_cols:
        x[col] = pd.to_numeric(x[col], errors="coerce")
        value = float(x[col].median(skipna=True))
        if np.isnan(value):
            value = 0.0
        fill_values[col] = value
        x[col] = x[col].fillna(value)

    scaler = StandardScaler()
    scaled = scaler.fit_transform(x)
    kmeans = KMeans(n_clusters=n_clusters, n_init=10, random_state=random_seed)
    clusters = kmeans.fit_predict(scaled)

    if len(set(clusters)) < n_clusters:
        raise ValueError("K-means produced fewer clusters than requested.")

    workload_idx = (
        list(feature_cols).index(workload_col) if workload_col in feature_cols else 0
    )
    centers = kmeans.cluster_centers_[:, workload_idx]
    order = np.argsort(centers)
    cluster_to_bucket = {
        int(cluster_id): f"bucket_{bucket_idx}"
        for bucket_idx, cluster_id in enumerate(order.tolist())
    }

    labels = pd.Series(clusters, index=frame.index).map(cluster_to_bucket)
    counts = labels.value_counts()
    if (counts < min_bucket_size).any():
        raise ValueError(
            "K-means buckets below min_bucket_size: "
            f"{counts.to_dict()} (min={min_bucket_size})"
        )

    return KMeansBucketModel(
        feature_cols=tuple(feature_cols),
        workload_col=workload_col,
        scaler=scaler,
        kmeans=kmeans,
        fill_values=fill_values,
        cluster_to_bucket=cluster_to_bucket,
    )


def fit_bucket_model(
    method: str,
    frame: pd.DataFrame,
    *,
    settings: SegmentationConfig,
) -> QuantileBucketModel | KMeansBucketModel:
    """Fit requested segmentation strategy model."""

    normalized = method.strip().lower()
    if normalized == "quantile3":
        return fit_quantile_bucket_model(
            frame,
            workload_col=settings.workload_col,
            min_bucket_size=settings.min_bucket_size,
        )
    if normalized == "kmeans":
        return fit_kmeans_bucket_model(
            frame,
            feature_cols=settings.kmeans_features,
            workload_col=settings.workload_col,
            n_clusters=settings.kmeans_n_clusters,
            min_bucket_size=settings.min_bucket_size,
            random_seed=settings.random_seed,
        )
    raise ValueError(f"Unsupported bucket method: {method}")
