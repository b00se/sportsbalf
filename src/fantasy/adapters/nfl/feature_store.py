"""Leakage-safe NFL game/player feature-store adapter."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

import pandas as pd


class FeatureStoreError(ValueError):
    """Raised when an inference feature store cannot be built safely."""


@dataclass(frozen=True, slots=True)
class SnapshotEvidence:
    """A defensively copied source table and its retrieval metadata.

    ``snapshot_sha256`` is optional audit metadata.  The production gate is
    the supplied snapshot plus dated observations, not a cryptographic hash.
    """

    frame: pd.DataFrame
    snapshot_id: str
    retrieved_at: pd.Timestamp
    row_count: int
    manifest_ref: str = ""
    snapshot_sha256: str | None = None
    global_safe: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.frame, pd.DataFrame):
            raise FeatureStoreError("snapshot frame must be a DataFrame")
        if not isinstance(self.snapshot_id, str) or not self.snapshot_id.strip():
            raise FeatureStoreError("snapshot source identity is required")
        if not isinstance(self.row_count, int) or self.row_count < 0:
            raise FeatureStoreError("snapshot row_count must be a nonnegative integer")
        if self.row_count != len(self.frame):
            raise FeatureStoreError("snapshot row_count does not match source frame")
        retrieved = pd.Timestamp(self.retrieved_at)
        if retrieved.tzinfo is None:
            raise FeatureStoreError("snapshot retrieved_at must be timezone-aware")
        if self.snapshot_sha256 is not None and not re.fullmatch(
            r"[0-9a-f]{64}", self.snapshot_sha256
        ):
            raise FeatureStoreError(
                "snapshot_sha256 must be lowercase 64-character hex"
            )
        copied = self.frame.copy(deep=True)
        object.__setattr__(self, "frame", copied)
        object.__setattr__(self, "retrieved_at", retrieved)

    @classmethod
    def from_frame(cls, frame: pd.DataFrame) -> SnapshotEvidence:
        """Create evidence from required snapshot metadata stored in attrs."""
        return cls(
            frame=frame,
            snapshot_id=str(frame.attrs.get("snapshot_id", "")),
            retrieved_at=frame.attrs.get("retrieved_at"),
            row_count=frame.attrs.get("row_count", -1),
            manifest_ref=str(frame.attrs.get("manifest_ref", "")),
            snapshot_sha256=frame.attrs.get("snapshot_sha256"),
            global_safe=bool(frame.attrs.get("global_safe", False)),
        )


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
NUMERIC_FEATURES = frozenset(FEATURE_COLUMNS) - {
    "weather",
    "role",
    "availability",
}
SCOPES = ("game_id", "event_id", "slate_id")


def _strict_timestamp(frame: pd.DataFrame, column: str, label: str) -> pd.Series:
    if column not in frame.columns:
        raise FeatureStoreError(f"{label} missing {column}")
    values = frame[column]
    if not pd.api.types.is_datetime64_any_dtype(values):
        raise FeatureStoreError(
            f"{label} timestamp must be timezone-aware datetime values"
        )
    if getattr(values.dt, "tz", None) is None or values.isna().any():
        raise FeatureStoreError(
            f"{label} timestamp must be timezone-aware and nonmissing"
        )
    return values


def _canonical(frame: pd.DataFrame, label: str) -> None:
    if "nflverse_id" not in frame.columns:
        raise FeatureStoreError(f"{label} requires canonical nflverse_id")
    if (
        frame["nflverse_id"].isna().any()
        or frame["nflverse_id"].astype(str).str.strip().eq("").any()
    ):
        raise FeatureStoreError(f"{label} contains unresolved nflverse_id")


def _validate_scopes(frame: pd.DataFrame, label: str) -> None:
    for scope in SCOPES:
        if scope in frame.columns:
            values = frame[scope]
            if values.isna().any() or values.astype(str).str.strip().eq("").any():
                raise FeatureStoreError(f"{label} contains blank {scope}")


def _as_evidence(value: SnapshotEvidence | pd.DataFrame) -> SnapshotEvidence:
    return (
        value
        if isinstance(value, SnapshotEvidence)
        else SnapshotEvidence.from_frame(value)
    )


def _exclude_target_scopes(
    frame: pd.DataFrame, target: pd.Series, *, label: str
) -> pd.DataFrame:
    """Remove all source rows that share any declared target scope."""
    eligible = frame
    for scope in SCOPES:
        if scope not in frame.columns:
            continue
        if scope not in target.index:
            raise FeatureStoreError(f"{label} requires target {scope}")
        eligible = eligible.loc[eligible[scope] != target[scope]]
    return eligible


def _hash_frame(frame: pd.DataFrame) -> str:
    """Return optional deterministic audit metadata for callers that want it."""
    return sha256(frame.to_csv(index=False).encode("utf-8")).hexdigest()


def _fallback_values(
    feature: str,
    target: pd.Series,
    normalized: list[tuple[str, SnapshotEvidence, pd.DataFrame]],
    *,
    cutoff: pd.Timestamp,
) -> Any:
    """Return a snapshot-backed prior-season value, then a global prior."""
    prior: list[Any] = []
    global_prior: list[Any] = []
    for name, _, frame in normalized:
        eligible = frame.loc[frame["source_timestamp_utc"] <= cutoff]
        eligible = _exclude_target_scopes(eligible, target, label=f"source {name}")
        if feature not in eligible.columns:
            continue
        global_prior.extend(
            pd.to_numeric(eligible[feature], errors="coerce").dropna().tolist()
        )
        if "season" in frame.columns and "season" in target.index:
            prior.extend(
                pd.to_numeric(
                    eligible.loc[eligible["season"] < target["season"], feature],
                    errors="coerce",
                )
                .dropna()
                .tolist()
            )
    if prior:
        return float(pd.Series(prior).mean())
    return float(pd.Series(global_prior).mean()) if global_prior else pd.NA


def _latest_categorical(
    feature: str, eligible: list[tuple[str, SnapshotEvidence, pd.Series]]
) -> Any:
    values = [
        (name, row)
        for name, _, row in eligible
        if feature in row.index and pd.notna(row[feature])
    ]
    if not values:
        return pd.NA
    latest = max(row["source_timestamp_utc"] for _, row in values)
    tied = [
        (name, row[feature])
        for name, row in values
        if row["source_timestamp_utc"] == latest
    ]
    distinct = {str(value) for _, value in tied}
    if len(distinct) != 1:
        raise FeatureStoreError(f"unresolved categorical tie for {feature} at {latest}")
    return tied[0][1]


def build_nfl_feature_store(
    targets: pd.DataFrame,
    sources: Mapping[str, SnapshotEvidence | pd.DataFrame],
    *,
    availability: SnapshotEvidence | pd.DataFrame | None = None,
    projections: SnapshotEvidence | pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build features using only supplied observations dated before each cutoff."""
    if targets is None or targets.empty:
        raise FeatureStoreError("targets must be nonempty")
    _canonical(targets, "targets")
    _validate_scopes(targets, "targets")
    cutoff = _strict_timestamp(targets, "as_of_utc", "targets")
    if not sources and availability is None and projections is None:
        raise FeatureStoreError("at least one source snapshot is required")
    all_sources = {name: _as_evidence(value) for name, value in sources.items()}
    if availability is not None:
        all_sources["availability"] = _as_evidence(availability)
    if projections is not None:
        all_sources["projections"] = _as_evidence(projections)

    normalized: list[tuple[str, SnapshotEvidence, pd.DataFrame]] = []
    for name, evidence in sorted(all_sources.items()):
        frame = evidence.frame.copy(deep=True)
        _canonical(frame, f"source {name}")
        _validate_scopes(frame, f"source {name}")
        _strict_timestamp(frame, "source_timestamp_utc", f"source {name}")
        if (
            "provenance" not in frame.columns
            or frame["provenance"].isna().any()
            or frame["provenance"].astype(str).str.strip().eq("").any()
        ):
            raise FeatureStoreError(f"source {name} requires nonempty provenance")
        if "game_id" not in frame.columns and not evidence.global_safe:
            raise FeatureStoreError(
                f"source {name} is unscoped; mark it global_safe explicitly"
            )
        duplicate_key = ["nflverse_id", "source_timestamp_utc"]
        if "game_id" in frame.columns:
            duplicate_key.append("game_id")
        if frame.duplicated(subset=duplicate_key).any():
            raise FeatureStoreError(f"source {name} contains duplicate observations")
        normalized.append((name, evidence, frame))

    result = targets.copy()
    result["as_of_utc"] = cutoff
    rows: list[dict[str, Any]] = []
    for _, target in result.iterrows():
        eligible: list[tuple[str, SnapshotEvidence, pd.Series]] = []
        active: list[tuple[str, SnapshotEvidence]] = []
        for name, evidence, frame in normalized:
            dated = frame.loc[frame["source_timestamp_utc"] <= target["as_of_utc"]]
            dated = _exclude_target_scopes(dated, target, label=f"source {name}")
            if not dated.empty:
                active.append((name, evidence))
            candidates = dated.loc[dated["nflverse_id"] == target["nflverse_id"]]
            eligible.extend((name, evidence, row) for _, row in candidates.iterrows())
        if not active:
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
                output[f"{feature}_missing_rate"] = 1.0 - len(numeric) / max(
                    len(eligible), 1
                )
                feature_value = (
                    float(numeric.mean())
                    if not numeric.empty
                    else _fallback_values(
                        feature, target, normalized, cutoff=target["as_of_utc"]
                    )
                )
                if pd.isna(feature_value):
                    raise FeatureStoreError(
                        "no usable snapshot-backed evidence for "
                        f"feature {feature} and target {target['nflverse_id']}"
                    )
                output[feature] = feature_value
                output[f"{feature}_fallback"] = numeric.empty
            else:
                output[f"{feature}_missing_rate"] = 1.0 - len(values) / max(
                    len(eligible), 1
                )
                output[feature] = _latest_categorical(feature, eligible)
                output[f"{feature}_fallback"] = not values
        snapshots = sorted(
            {
                (e.snapshot_id, e.snapshot_sha256 or "", e.manifest_ref, e.row_count)
                for _, e in active
            }
        )
        output["source_snapshot_id"] = "|".join(item[0] for item in snapshots)
        output["source_snapshot_sha256"] = "|".join(item[1] for item in snapshots)
        output["source_manifest_ref"] = "|".join(item[2] for item in snapshots)
        output["source_row_count"] = sum(item[3] for item in snapshots)
        output["source_count"] = len(snapshots)
        output["feature_missingness_summary"] = json.dumps(
            {feature: output[f"{feature}_missing_rate"] for feature in FEATURE_COLUMNS},
            sort_keys=True,
        )
        output["season"] = target.get("season", pd.NA)
        rows.append(output)
    return pd.concat([result, pd.DataFrame(rows, index=result.index)], axis=1)


__all__ = [
    "FEATURE_COLUMNS",
    "FeatureStoreError",
    "SnapshotEvidence",
    "build_nfl_feature_store",
]
