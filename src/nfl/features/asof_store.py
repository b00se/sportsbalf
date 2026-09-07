"""Leakage-safe, deterministic as-of feature store for NFL player data.

The store deliberately accepts already-normalized evidence frames.  Each frame
must carry a durable ``nflverse_id`` and a timezone-aware source timestamp.
External identifiers belong in the identity packet and must be resolved before
they enter this boundary.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pandas as pd


class AsOfFeatureError(ValueError):
    """Raised when feature evidence cannot be used safely."""


@dataclass(frozen=True, slots=True)
class SourceSpec:
    """Description of one evidence frame and its audit fields."""

    name: str
    frame: pd.DataFrame
    timestamp_col: str = "source_timestamp_utc"
    value_columns: tuple[str, ...] | None = None
    provenance_col: str = "provenance"


def _utc_series(frame: pd.DataFrame, column: str, *, label: str) -> pd.Series:
    """Parse timestamps strictly, rejecting missing and naive values."""
    if column not in frame.columns:
        raise AsOfFeatureError(f"{label} missing required timestamp column {column!r}")
    values = frame[column]
    for value in values:
        if pd.isna(value):
            raise AsOfFeatureError(f"{label} contains a missing timestamp")
        if isinstance(value, datetime) and value.tzinfo is None:
            raise AsOfFeatureError(f"{label} contains a naive timestamp")
        if isinstance(value, pd.Timestamp) and value.tzinfo is None:
            raise AsOfFeatureError(f"{label} contains a naive timestamp")
        if isinstance(value, str):
            parsed = pd.to_datetime(value, errors="coerce")
            if pd.isna(parsed):
                raise AsOfFeatureError(f"{label} contains an invalid timestamp")
            if parsed.tzinfo is None:
                raise AsOfFeatureError(f"{label} contains a naive timestamp")
    parsed = pd.to_datetime(values, errors="coerce", utc=True)
    if parsed.isna().any():
        raise AsOfFeatureError(f"{label} contains an invalid timestamp")
    return parsed


def _require_ids(frame: pd.DataFrame, *, label: str) -> None:
    if "nflverse_id" not in frame.columns:
        raise AsOfFeatureError(f"{label} requires canonical nflverse_id")
    blank = frame["nflverse_id"].astype(str).str.strip() == ""
    if frame["nflverse_id"].isna().any() or blank.any():
        raise AsOfFeatureError(f"{label} contains an unresolved canonical identity")


def _specs(
    evidence: Mapping[str, pd.DataFrame] | None,
    specs: tuple[SourceSpec, ...],
) -> tuple[SourceSpec, ...]:
    if evidence:
        specs += tuple(SourceSpec(name, frame) for name, frame in evidence.items())
    names = [spec.name for spec in specs]
    if len(set(names)) != len(names):
        raise AsOfFeatureError("source names must be unique")
    return specs


@dataclass(frozen=True, slots=True)
class AsOfFeatureStore:
    """Materialized feature rows with source provenance and freshness."""

    frame: pd.DataFrame

    @classmethod
    def build(
        cls,
        targets: pd.DataFrame,
        evidence: Mapping[str, pd.DataFrame] | None = None,
        *,
        source_specs: tuple[SourceSpec, ...] = (),
        cutoff_col: str = "as_of_utc",
        max_age: timedelta | None = None,
        require_evidence: bool = False,
    ) -> AsOfFeatureStore:
        """Build features using only evidence available at each target cutoff.

        Args:
            targets: Rows keyed by ``nflverse_id`` with ``as_of_utc``.
            evidence: Named normalized evidence frames.
            source_specs: Optional per-source timestamp/value configuration.
            cutoff_col: Explicit timezone-aware target cutoff column.
            max_age: Optional freshness limit relative to each cutoff.
            require_evidence: Require one eligible row from every source.
        """
        if targets is None or targets.empty:
            return cls(targets.copy() if targets is not None else pd.DataFrame())
        _require_ids(targets, label="targets")
        result = targets.copy()
        result[cutoff_col] = _utc_series(result, cutoff_col, label="targets")
        specs = _specs(evidence, source_specs)
        for spec in specs:
            _require_ids(spec.frame, label=f"source {spec.name}")
            source = spec.frame.copy()
            source[spec.timestamp_col] = _utc_series(
                source, spec.timestamp_col, label=f"source {spec.name}"
            )
            if spec.provenance_col not in source.columns:
                raise AsOfFeatureError(f"source {spec.name} missing provenance")
            if source[spec.provenance_col].isna().any():
                raise AsOfFeatureError(
                    f"source {spec.name} contains missing provenance"
                )
            values = spec.value_columns or tuple(
                col
                for col in source.columns
                if col not in {"nflverse_id", spec.timestamp_col, spec.provenance_col}
            )
            missing = [col for col in values if col not in source.columns]
            if missing:
                raise AsOfFeatureError(
                    f"source {spec.name} missing value columns: {missing}"
                )
            scope_columns = [
                column
                for column in ("event_id", "game_id", "slate_id")
                if column in result.columns and column in source.columns
            ]
            duplicate_key = ["nflverse_id", spec.timestamp_col, *scope_columns]
            if source.duplicated(duplicate_key).any():
                raise AsOfFeatureError(
                    f"source {spec.name} has duplicate timestamped identities"
                )
            source = source.sort_values(["nflverse_id", spec.timestamp_col])
            found = []
            for _, target in result.iterrows():
                candidates = source.loc[
                    (source["nflverse_id"] == target["nflverse_id"])
                    & (source[spec.timestamp_col] <= target[cutoff_col])
                ]
                for column in scope_columns:
                    candidates = candidates.loc[candidates[column] == target[column]]
                if max_age is not None:
                    candidates = candidates.loc[
                        target[cutoff_col] - candidates[spec.timestamp_col] <= max_age
                    ]
                if candidates.empty:
                    if require_evidence:
                        raise AsOfFeatureError(
                            f"source {spec.name} has no eligible evidence for "
                            f"{target['nflverse_id']}"
                        )
                    found.append(None)
                else:
                    found.append(candidates.iloc[-1])
            prefix = f"{spec.name}_"
            result[f"{prefix}source_timestamp_utc"] = [
                row[spec.timestamp_col] if row is not None else pd.NaT for row in found
            ]
            result[f"{prefix}provenance"] = [
                row[spec.provenance_col] if row is not None else pd.NA for row in found
            ]
            result[f"{prefix}freshness_seconds"] = [
                (target[cutoff_col] - row[spec.timestamp_col]).total_seconds()
                if row is not None
                else pd.NA
                for target, row in zip(result.to_dict("records"), found)
            ]
            for column in values:
                output = f"{prefix}{column}"
                if output in result.columns:
                    raise AsOfFeatureError(f"feature column collision: {output}")
                result[output] = [
                    row[column] if row is not None else pd.NA for row in found
                ]
        return cls(result)

    def to_frame(self) -> pd.DataFrame:
        """Return a defensive copy of the materialized feature frame."""
        return self.frame.copy()


def build_as_of_feature_store(
    targets: pd.DataFrame,
    evidence: Mapping[str, pd.DataFrame] | None = None,
    **kwargs: Any,
) -> AsOfFeatureStore:
    """Convenience wrapper for :meth:`AsOfFeatureStore.build`."""
    return AsOfFeatureStore.build(targets, evidence, **kwargs)


__all__ = [
    "AsOfFeatureError",
    "AsOfFeatureStore",
    "SourceSpec",
    "build_as_of_feature_store",
]
