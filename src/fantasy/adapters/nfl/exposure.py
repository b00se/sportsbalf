"""Strict parser and roster reconstruction for Underdog exposure exports."""

from __future__ import annotations

import csv
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import IO

# The provider export is intentionally treated as an opaque record: metadata columns
# are retained so a schema addition cannot silently discard audit information.
EXPOSURE_COLUMNS: tuple[str, ...] = (
    "Draft Entry", "Draft ID", "Pool ID", "Draft Date", "Contest Name",
    "Contest ID", "Entry Fee", "Prize Pool", "Draft Status", "Draft Position",
    "Pick Number", "Round", "Player Name", "Player ID", "Position", "Team",
    "Projection", "Fantasy Points", "Rank", "ADP", "Username", "User ID",
    "Tournament Rank", "Tournament Score", "Payout", "Winner", "Notes",
)
_REQUIRED = {"Draft Entry", "Draft ID", "Pool ID", "Pick Number", "Player Name"}


class ExposureSchemaError(ValueError):
    """Raised when exposure CSV shape or entry integrity is invalid."""


@dataclass(frozen=True, slots=True)
class ExposureExport:
    """Exposure rows, with empty cells represented as ``None``."""

    columns: tuple[str, ...]
    rows: tuple[dict[str, str | None], ...]


@dataclass(frozen=True, slots=True)
class ExposurePick:
    """One user selection and its complete source row."""

    pick_number: int
    player_name: str
    raw: Mapping[str, str | None]


@dataclass(frozen=True, slots=True)
class UserEntry:
    """Reconstructed entry ordered by pick number."""

    draft_entry: str
    draft_id: str
    pool_id: str
    picks: tuple[ExposurePick, ...]


def parse_exposure_csv(source: str | Path | IO[str]) -> ExposureExport:
    """Parse the exact 27-column exposure contract."""

    close = False
    if isinstance(source, (str, Path)):
        handle = open(source, newline="", encoding="utf-8")
        close = True
    else:
        handle = source
    try:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ExposureSchemaError("exposure CSV is missing its header row")
        if tuple(reader.fieldnames) != EXPOSURE_COLUMNS:
            raise ExposureSchemaError(
                "exposure CSV must contain exactly 27 columns in provider order; "
                f"got {reader.fieldnames!r}"
            )
        rows: list[dict[str, str | None]] = []
        for number, row in enumerate(reader, 2):
            if None in row or any(value is None for value in row.values()):
                raise ExposureSchemaError(
                    f"exposure row {number} has the wrong column count"
                )
            rows.append(
                {
                    key: (value if value is not None and value.strip() else None)
                    for key, value in row.items()
                }
            )
        if not rows:
            raise ExposureSchemaError("exposure CSV contains zero picks")
        return ExposureExport(EXPOSURE_COLUMNS, tuple(rows))
    finally:
        if close:
            handle.close()


def reconstruct_user_entries(export: ExposureExport) -> dict[str, UserEntry]:
    """Group rows by Draft Entry and reconstruct exact pick order."""

    if export.columns != EXPOSURE_COLUMNS:
        raise ExposureSchemaError("exposure export has unexpected columns")
    grouped: dict[str, list[dict[str, str | None]]] = {}
    for row in export.rows:
        entry = row["Draft Entry"]
        if not entry:
            raise ExposureSchemaError("exposure row has missing Draft Entry")
        grouped.setdefault(entry, []).append(row)

    result: dict[str, UserEntry] = {}
    for entry, rows in grouped.items():
        ids = {row["Draft ID"] for row in rows}
        pools = {row["Pool ID"] for row in rows}
        if None in ids or len(ids) != 1:
            raise ExposureSchemaError(f"entry {entry!r} has mixed or missing Draft ID")
        if None in pools or len(pools) != 1:
            raise ExposureSchemaError(f"entry {entry!r} has mixed or missing pool id")
        picks: list[ExposurePick] = []
        for row in rows:
            raw_pick = row["Pick Number"]
            if raw_pick is None:
                raise ExposureSchemaError(f"entry {entry!r} has a missing Pick Number")
            try:
                pick_number = int(raw_pick)
            except ValueError as exc:
                raise ExposureSchemaError(
                    f"entry {entry!r} has non-numeric Pick Number"
                ) from exc
            if pick_number < 1:
                raise ExposureSchemaError(
                    f"entry {entry!r} has invalid Pick Number {pick_number}"
                )
            player = row["Player Name"]
            if not player:
                raise ExposureSchemaError(f"entry {entry!r} has a missing Player Name")
            picks.append(ExposurePick(pick_number, player, row))
        numbers = [pick.pick_number for pick in picks]
        if len(set(numbers)) != len(numbers):
            raise ExposureSchemaError(
                f"entry {entry!r} has duplicate Pick Number values"
            )
        expected = list(range(1, len(numbers) + 1))
        if sorted(numbers) != expected:
            raise ExposureSchemaError(f"entry {entry!r} has missing Pick Number values")
        picks.sort(key=lambda pick: pick.pick_number)
        result[entry] = UserEntry(
            entry, next(iter(ids)), next(iter(pools)), tuple(picks)
        )
    return result
