"""Leakage-safe NFL game/player feature-store adapter.

This is an inference boundary: callers provide canonical, snapshot-backed
tables and the adapter refuses evidence that cannot be dated and audited.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

import pandas as pd


class FeatureStoreError(ValueError):
    """Raised when an inference feature store cannot be built safely."""


@dataclass(frozen=True, slots=True)
class SnapshotEvidence:
    """A normalized source table and its immutable snapshot identity."""

    frame: pd.DataFrame
    snapshot_id: str
    snapshot_sha256: str
    manifest_ref: str
    global_safe: bool = False

    def __post_init__(self) -> None:
        if not all(
            isinstance(value, str) and value.strip()
            for value in (self.snapshot_id, self.snapshot_sha256, self.manifest_ref)
        ):
            raise FeatureStoreError(
                "snapshot_id, snapshot_sha256, and manifest_ref are required"
            )

    @classmethod
    def from_frame(
        cls,
        frame: pd.DataFrame,
        *,
        snapshot_id: str | None = None,
        snapshot_sha256: str | None = None,
        manifest_ref: str | None = None,
        global_safe: bool = False,
    ) -> SnapshotEvidence:
        """Create evidence from a frame carrying optional snapshot attrs."""
        identifier = snapshot_id or str(frame.attrs.get("snapshot_id", ""))
        digest = snapshot_sha256 or str(frame.attrs.get("snapshot_sha256", ""))
        manifest = manifest_ref or str(frame.attrs.get("manifest_ref", ""))
        return cls(frame, identifier, digest, manifest, global_safe)


FEATURE_COLUMNS: tuple[str, ...] = (
    "plays",
    "pace",
    "neutral_pass_rate",
    "opportunity_share",
    "routes",
    "snaps",
    "opponent_strength",
    "rest_days",
    "weather",
    "spread",
    "total",
    "role",
    "availability",
    "projection",
)
NUMERIC_FEATURES = frozenset(FEATURE_COLUMNS) - {"role", "availability"}


def _strict_timestamp(frame: pd.DataFrame, column: str, label: str) -> pd.Series:
    if column not in frame.columns:
        raise FeatureStoreError(f"{label} missing {column}")
    values = frame[column]
    if not pd.api.types.is_datetime64_any_dtype(values):
        raise FeatureStoreError(
            f"{label} timestamp must be timezone-aware datetime values"
        )
    if getattr(values.dt, "tz", None) is None:
        raise FeatureStoreError(f"{label} timestamp must be timezone-aware")
    if values.isna().any():
        raise FeatureStoreError(f"{label} contains a missing timestamp")
    return values


def _canonical(frame: pd.DataFrame, label: str) -> None:
    if "nflverse_id" not in frame.columns:
        raise FeatureStoreError(f"{label} requires canonical nflverse_id")
    ids = frame["nflverse_id"]
    if ids.isna().any() or ids.astype(str).str.strip().eq("").any():
        raise FeatureStoreError(f"{label} contains unresolved nflverse_id")


def _as_evidence(name: str, value: SnapshotEvidence | pd.DataFrame) -> SnapshotEvidence:
    if isinstance(value, SnapshotEvidence):
        return value
    return SnapshotEvidence.from_frame(value)


def _hash_frame(frame: pd.DataFrame) -> str:
    """Return a deterministic digest for a source when callers need one."""
    payload = frame.to_csv(index=False).encode("utf-8")
    return sha256(payload).hexdigest()


def build_nfl_feature_store(
    targets: pd.DataFrame,
    sources: Mapping[str, SnapshotEvidence | pd.DataFrame],
    *,
    availability: SnapshotEvidence | pd.DataFrame | None = None,
    projections: SnapshotEvidence | pd.DataFrame | None = None,
    require_evidence: bool = True,
) -> pd.DataFrame:
    """Build pre-target rolling features for canonical player/game rows.

    Every source is required to be snapshot-backed. Source rows must use a
    timezone-aware datetime column named ``source_timestamp_utc``. If a source
    contains ``game_id`` it is event-scoped; unscoped rows are rejected unless
    explicitly marked ``global_safe``. Rolling numeric features use prior rows
    only; missing values receive deterministic fallback and missingness rates.
    """
    if targets is None or targets.empty:
        raise FeatureStoreError("targets must be nonempty")
    _canonical(targets, "targets")
    cutoff = _strict_timestamp(targets, "as_of_utc", "targets")
    if not sources and availability is None and projections is None:
        if require_evidence:
            raise FeatureStoreError("at least one source snapshot is required")
        return targets.copy()
    all_sources: dict[str, SnapshotEvidence] = {
        name: _as_evidence(name, evidence) for name, evidence in sources.items()
    }
    if availability is not None:
        all_sources["availability"] = _as_evidence("availability", availability)
    if projections is not None:
        all_sources["projections"] = _as_evidence("projections", projections)
    normalized: list[tuple[str, SnapshotEvidence, pd.DataFrame]] = []
    for name, evidence in all_sources.items():
        frame = evidence.frame.copy()
        _canonical(frame, f"source {name}")
        _strict_timestamp(frame, "source_timestamp_utc", f"source {name}")
        if "provenance" not in frame.columns or frame["provenance"].isna().any():
            raise FeatureStoreError(f"source {name} requires nonempty provenance")
        if "game_id" not in frame.columns and not evidence.global_safe:
            raise FeatureStoreError(
                f"source {name} is unscoped; mark it global_safe explicitly"
            )
        normalized.append((name, evidence, frame))

    result = targets.copy()
    result["as_of_utc"] = cutoff
    rows: list[dict[str, Any]] = []
    for target_index, target in result.iterrows():
        eligible: list[tuple[str, SnapshotEvidence, pd.Series]] = []
        for name, evidence, frame in normalized:
            candidates = frame.loc[
                (frame["nflverse_id"] == target["nflverse_id"])
                & (frame["source_timestamp_utc"] <= target["as_of_utc"])
            ]
            if "game_id" in frame.columns and "game_id" in result.columns:
                candidates = candidates.loc[candidates["game_id"] != target["game_id"]]
            elif not evidence.global_safe:
                raise FeatureStoreError(f"source {name} lacks safe target scope")
            if candidates.empty:
                continue
            for _, candidate in candidates.iterrows():
                eligible.append((name, evidence, candidate))
        if require_evidence and not eligible:
            raise FeatureStoreError(
                f"no snapshot-backed prior evidence for target {target['nflverse_id']}"
            )
        output: dict[str, Any] = {}
        for feature in FEATURE_COLUMNS:
            values = [
                row[feature]
                for _, _, row in eligible
                if feature in row.index and pd.notna(row[feature])
            ]
            if feature in NUMERIC_FEATURES:
                numeric = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
                output[f"{feature}_missing_rate"] = 1.0 - (
                    len(numeric) / max(len(eligible), 1)
                )
                if not numeric.empty:
                    output[feature] = float(numeric.mean())
                    output[f"{feature}_fallback"] = False
                else:
                    output[feature] = pd.NA
                    output[f"{feature}_fallback"] = True
            else:
                output[f"{feature}_missing_rate"] = 1.0 - (
                    len(values) / max(len(eligible), 1)
                )
                output[feature] = values[-1] if values else pd.NA
                output[f"{feature}_fallback"] = not values
        snapshots = sorted(
            {
                (evidence.snapshot_id, evidence.snapshot_sha256, evidence.manifest_ref)
                for _, evidence, _ in eligible
            }
        )
        output["source_snapshot_id"] = "|".join(item[0] for item in snapshots)
        output["source_snapshot_sha256"] = "|".join(item[1] for item in snapshots)
        output["source_manifest_ref"] = "|".join(item[2] for item in snapshots)
        output["source_count"] = len(snapshots)
        output["as_of_utc"] = target["as_of_utc"]
        rows.append(output)
    features = pd.DataFrame(rows, index=result.index)
    return pd.concat([result, features.drop(columns=["as_of_utc"])], axis=1)


__all__ = [
    "FEATURE_COLUMNS",
    "FeatureStoreError",
    "SnapshotEvidence",
    "build_nfl_feature_store",
]
