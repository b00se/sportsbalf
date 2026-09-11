"""Immutable, manifest-backed snapshots for nflverse provider responses."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd


class ArchiveIntegrityError(ValueError):
    """Raised when a snapshot or its manifest cannot be verified."""


@dataclass(frozen=True, slots=True)
class ArchiveWriteResult:
    """Paths for an immutable payload and its manifest."""

    payload_path: Path
    manifest_path: Path


def _canonical_json(value: dict[str, Any]) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()


def _package_version() -> str:
    for name in ("nflreadpy", "nfl_data_py"):
        try:
            return importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            continue
    return "unknown"


def _logical_dtype(dtype: Any) -> str:
    """Map pandas storage dtypes to CSV-stable schema semantics."""
    text = str(dtype).lower()
    if "datetime" in text:
        return "datetime"
    if "bool" in text:
        return "boolean"
    if "int" in text or text == "uint":
        return "integer"
    if "float" in text or "decimal" in text:
        return "number"
    return "string"


class ArchiveAdapter:
    """Write and verify immutable CSV snapshots supplied by provider code.

    The cache directory is caller-owned (and may be outside the repository).
    Existing payload or manifest paths are never replaced.
    """

    _REQUIRED = frozenset(
        {
            "dataset", "source_url", "source_version", "retrieved_at_utc",
            "requested_seasons", "available_seasons", "cutoff_semantics",
            "row_count", "byte_size", "sha256", "package_version", "schema_fingerprint",
        }
    )

    def __init__(self, cache_dir: str | Path):
        self.cache_dir = Path(cache_dir)

    @staticmethod
    def schema_fingerprint(frame: pd.DataFrame) -> str:
        """Return a deterministic hash of sorted column names and dtypes."""
        schema = []
        for column in sorted(frame.columns, key=str):
            series = frame[column]
            logical = _logical_dtype(series.dtype)
            if logical == "number":
                numeric = pd.to_numeric(series, errors="coerce").dropna()
                if numeric.empty:
                    logical = "string"
                elif (numeric % 1 == 0).all():
                    logical = "integer"
            schema.append([str(column), logical])
        return hashlib.sha256(_canonical_json({"columns": schema})).hexdigest()

    @staticmethod
    def _payload(frame: pd.DataFrame) -> bytes:
        ordered = frame.loc[:, sorted(frame.columns, key=str)]
        return ordered.to_csv(index=False, lineterminator="\n").encode("utf-8")

    def write(
        self,
        dataset: str,
        frame: pd.DataFrame,
        *,
        requested_seasons: Sequence[int],
        source_url: str = "https://github.com/nflverse/nflverse-data",
        source_version: str = "unknown",
        cutoff_semantics: str = (
            "observations are restricted to rows strictly before the declared cutoff"
        ),
        retrieved_at_utc: str | None = None,
        package_version: str | None = None,
        payload_path: str | Path | None = None,
    ) -> ArchiveWriteResult:
        """Serialize a provider dataframe and create its manifest exclusively."""
        if not dataset or not isinstance(frame, pd.DataFrame):
            raise TypeError("dataset must be non-empty and frame must be a DataFrame")
        if not cutoff_semantics:
            raise ValueError("cutoff_semantics must be explicit")
        requested = sorted({int(year) for year in requested_seasons})
        available: list[int] = []
        if "season" in frame.columns:
            values = pd.to_numeric(frame["season"], errors="coerce").dropna()
            available = sorted(
                {int(value) for value in values if float(value).is_integer()}
            )
        payload = self._payload(frame)
        path = (
            Path(payload_path)
            if payload_path is not None
            else self.cache_dir / f"{dataset}.csv"
        )
        manifest_path = path.with_suffix(path.suffix + ".manifest.json")
        path.parent.mkdir(parents=True, exist_ok=True)
        retrieved = retrieved_at_utc or datetime.now(UTC).isoformat().replace(
            "+00:00", "Z"
        )
        manifest = {
            "dataset": dataset,
            "source_url": source_url,
            "source_version": source_version,
            "retrieved_at_utc": retrieved,
            "requested_seasons": requested,
            "available_seasons": available,
            "cutoff_semantics": cutoff_semantics,
            "row_count": len(frame),
            "byte_size": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "package_version": package_version or _package_version(),
            "schema_fingerprint": self.schema_fingerprint(frame),
        }
        with path.open("xb") as handle:
            handle.write(payload)
        try:
            with manifest_path.open("xb") as handle:
                handle.write(_canonical_json(manifest))
        except Exception:
            # Do not remove the payload: immutable artifacts are recoverable and
            # a subsequent attempt must not overwrite it.
            raise
        return ArchiveWriteResult(path, manifest_path)

    def load(self, payload_path: str | Path) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Load a snapshot only after validating all manifest claims."""
        path = Path(payload_path)
        manifest_path = path.with_suffix(path.suffix + ".manifest.json")
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ArchiveIntegrityError("manifest is missing or malformed") from exc
        if not isinstance(manifest, dict) or not self._REQUIRED <= manifest.keys():
            raise ArchiveIntegrityError("manifest is missing required fields")
        for key in (
            "dataset", "source_url", "source_version", "cutoff_semantics",
            "package_version", "schema_fingerprint", "sha256",
        ):
            if not isinstance(manifest[key], str) or not manifest[key].strip():
                raise ArchiveIntegrityError(f"manifest {key} is invalid")
        if (
            not isinstance(manifest["row_count"], int)
            or isinstance(manifest["row_count"], bool)
            or manifest["row_count"] < 0
        ):
            raise ArchiveIntegrityError("manifest row count is invalid")
        if not isinstance(manifest["byte_size"], int) or manifest["byte_size"] < 0:
            raise ArchiveIntegrityError("manifest byte size is invalid")
        if not isinstance(manifest["sha256"], str) or len(manifest["sha256"]) != 64:
            raise ArchiveIntegrityError("manifest SHA-256 is invalid")
        for key in ("requested_seasons", "available_seasons"):
            if not isinstance(manifest[key], list) or not all(
                isinstance(year, int) and not isinstance(year, bool)
                for year in manifest[key]
            ):
                raise ArchiveIntegrityError(f"manifest {key} is invalid")
        try:
            timestamp = datetime.fromisoformat(
                str(manifest["retrieved_at_utc"]).replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise ArchiveIntegrityError(
                "manifest retrieval timestamp is invalid"
            ) from exc
        if timestamp.tzinfo is None or timestamp.utcoffset() != UTC.utcoffset(
            timestamp
        ):
            raise ArchiveIntegrityError("manifest retrieval timestamp is not UTC")
        try:
            payload = path.read_bytes()
        except OSError as exc:
            raise ArchiveIntegrityError("payload is missing or unreadable") from exc
        if manifest["byte_size"] != len(payload):
            raise ArchiveIntegrityError("byte size mismatch")
        if manifest["sha256"] != hashlib.sha256(payload).hexdigest():
            raise ArchiveIntegrityError("SHA-256 mismatch")
        try:
            frame = pd.read_csv(path)
        except Exception as exc:
            raise ArchiveIntegrityError("payload cannot be parsed") from exc
        if manifest["row_count"] != len(frame):
            raise ArchiveIntegrityError("row count mismatch")
        if manifest["schema_fingerprint"] != self.schema_fingerprint(frame):
            raise ArchiveIntegrityError("schema fingerprint mismatch")
        return frame, manifest

    def load_cache_hit(
        self, payload_path: str | Path
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Load an existing cache entry through the same integrity gate."""
        return self.load(payload_path)


def write_archive_snapshot(*args: Any, **kwargs: Any) -> ArchiveWriteResult:
    """Convenience wrapper around :class:`ArchiveAdapter.write`."""
    cache_dir = kwargs.pop("cache_dir")
    return ArchiveAdapter(cache_dir).write(*args, **kwargs)


def load_archive_snapshot(
    payload_path: str | Path,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Convenience wrapper around :class:`ArchiveAdapter.load`."""
    return ArchiveAdapter(Path(payload_path).parent).load(payload_path)
