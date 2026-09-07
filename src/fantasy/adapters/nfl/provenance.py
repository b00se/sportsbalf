"""Deterministic provenance manifests for NFL daily rankings artifacts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any

_SENSITIVE_KEY_TOKENS = frozenset(
    {
        "account",
        "api_key",
        "apikey",
        "cookie",
        "credential",
        "password",
        "secret",
        "token",
    }
)
_UTC_TIMESTAMP_ERROR = "Snapshot inputs require an ISO-8601 UTC timestamp ending in Z."


@dataclass(frozen=True, slots=True)
class SnapshotInput:
    """Immutable source input recorded in a rankings snapshot.

    Args:
        name: Stable source artifact name.
        content: Exact bytes supplied by the source.
        source_timestamp_utc: Source-provided or operator-recorded UTC timestamp.
    """

    name: str
    content: bytes
    source_timestamp_utc: str


@dataclass(frozen=True, slots=True)
class SnapshotManifest:
    """Canonical, serializable manifest for a deterministic rankings run."""

    snapshot_id: str
    payload: Mapping[str, Any]

    def to_json_bytes(self) -> bytes:
        """Serialize the manifest using its canonical byte representation."""

        return _canonical_json_bytes(_thaw(self.payload))


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(nested) for key, nested in value.items()})
    if isinstance(value, list | tuple):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw(nested) for key, nested in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _validate_utc_timestamp(value: str) -> None:
    if not value.endswith("Z"):
        raise ValueError(_UTC_TIMESTAMP_ERROR)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(
            _UTC_TIMESTAMP_ERROR
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(_UTC_TIMESTAMP_ERROR)


def _validate_no_sensitive_keys(value: Any, *, path: str = "") -> None:
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        for index, nested_value in enumerate(value):
            _validate_no_sensitive_keys(nested_value, path=f"{path}[{index}]")
        return
    if not isinstance(value, Mapping):
        return
    for key, nested_value in value.items():
        if not isinstance(key, str):
            raise ValueError(f"Manifest {path or 'payload'} keys must be strings.")
        normalized = key.lower()
        if any(token in normalized for token in _SENSITIVE_KEY_TOKENS):
            raise ValueError(f"Manifest contains sensitive metadata key '{key}'.")
        child_path = f"{path}.{key}" if path else key
        _validate_no_sensitive_keys(nested_value, path=child_path)


def _artifact_hashes(
    artifacts: Mapping[str, bytes], *, label: str
) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    for name, content in sorted(artifacts.items()):
        if not isinstance(name, str) or not name:
            raise ValueError(f"{label} artifact names must be non-empty strings.")
        if not isinstance(content, bytes):
            raise ValueError(f"{label} artifact '{name}' must contain bytes.")
        records.append({"name": name, "sha256": _sha256(content)})
    return records


def build_snapshot_manifest(
    *,
    inputs: tuple[SnapshotInput, ...],
    configuration: Mapping[str, Any],
    outputs: Mapping[str, bytes],
    run_metadata: Mapping[str, Any],
) -> SnapshotManifest:
    """Build a byte-stable manifest without recording sensitive account data.

    Args:
        inputs: Immutable source artifacts and their source timestamps.
        configuration: Validated contest configuration used by the run.
        outputs: Generated artifacts keyed by stable artifact name.
        run_metadata: Non-sensitive metadata such as seed and code commit.

    Returns:
        Canonical manifest with a content-derived snapshot identifier.

    Raises:
        ValueError: If values are malformed or sensitive metadata is supplied.
    """

    _validate_no_sensitive_keys(configuration, path="configuration")
    _validate_no_sensitive_keys(run_metadata, path="run_metadata")
    input_records: list[dict[str, str]] = []
    for source in sorted(inputs, key=lambda item: item.name):
        if not source.name or not source.source_timestamp_utc:
            raise ValueError(
                "Snapshot inputs require non-empty names and UTC timestamps."
            )
        _validate_utc_timestamp(source.source_timestamp_utc)
        input_records.append(
            {
                "name": source.name,
                "sha256": _sha256(source.content),
                "source_timestamp_utc": source.source_timestamp_utc,
            }
        )
    payload: dict[str, Any] = {
        "configuration": dict(configuration),
        "configuration_sha256": _sha256(_canonical_json_bytes(configuration)),
        "inputs": input_records,
        "outputs": _artifact_hashes(outputs, label="Output"),
        "run_metadata": dict(run_metadata),
        "schema_version": 1,
    }
    snapshot_id = _sha256(_canonical_json_bytes(payload))
    payload["snapshot_id"] = snapshot_id
    return SnapshotManifest(snapshot_id=snapshot_id, payload=_freeze(payload))
