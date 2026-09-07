"""Strict, offline Underdog NFL rankings CSV contract."""

from __future__ import annotations

import csv
import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import IO

from src.nfl.data.availability import (
    AvailabilityDecision,
    AvailabilityStatus,
    check_unattended_availability,
)
from src.nfl.data.identity import IdentityGraph

RANKINGS_COLUMNS: tuple[str, ...] = (
    "id",
    "playerId",
    "firstName",
    "lastName",
    "adp",
    "projectedPoints",
    "salary",
    "positionRank",
    "slotName",
    "teamName",
    "lineupStatus",
    "byeWeek",
)
_NUMERIC_COLUMNS = {"adp", "projectedPoints", "salary", "byeWeek"}
_POSITIONS = {"QB", "RB", "WR", "TE", "K", "DST", "DEF"}


class RankingsSchemaError(ValueError):
    """Raised when a rankings CSV violates the frozen source contract."""


@dataclass(frozen=True, slots=True)
class RankingsTable:
    """Parsed rankings rows, retaining source cell text exactly."""

    columns: tuple[str, ...]
    rows: tuple[dict[str, str], ...]
    source_path: Path | None = None


def _reader(source: str | Path | IO[str]) -> tuple[list[str], list[dict[str, str]]]:
    close = False
    if isinstance(source, (str, Path)):
        handle = open(source, newline="", encoding="utf-8")
        close = True
    else:
        handle = source
    try:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise RankingsSchemaError("rankings CSV is missing its header row")
        if tuple(reader.fieldnames) != RANKINGS_COLUMNS:
            raise RankingsSchemaError(
                "rankings CSV columns must exactly match "
                f"{list(RANKINGS_COLUMNS)!r}; got {reader.fieldnames!r}"
            )
        rows: list[dict[str, str]] = []
        for number, row in enumerate(reader, 2):
            if None in row or any(value is None for value in row.values()):
                raise RankingsSchemaError(
                    f"rankings row {number} has the wrong column count"
                )
            rows.append({column: row[column] for column in RANKINGS_COLUMNS})
        return list(RANKINGS_COLUMNS), rows
    finally:
        if close:
            handle.close()


def parse_rankings_csv(source: str | Path | IO[str]) -> RankingsTable:
    """Parse and strictly validate a 12-column rankings CSV."""

    columns, rows = _reader(source)
    if not rows:
        raise RankingsSchemaError("rankings CSV contains zero players")
    seen: set[str] = set()
    for number, row in enumerate(rows, 2):
        player_id = row["id"]
        if not player_id:
            raise RankingsSchemaError(f"rankings row {number} has an empty id")
        if player_id in seen:
            raise RankingsSchemaError(
                f"rankings row {number} duplicates id {player_id!r}"
            )
        seen.add(player_id)
        if not row["playerId"]:
            raise RankingsSchemaError(f"rankings row {number} has an empty playerId")
        for field in _NUMERIC_COLUMNS:
            if row[field]:
                try:
                    numeric_value = float(row[field])
                except ValueError as exc:
                    raise RankingsSchemaError(
                        f"rankings row {number} field {field!r} must be numeric; "
                        f"got {row[field]!r}"
                    ) from exc
                if not math.isfinite(numeric_value):
                    raise RankingsSchemaError(
                        f"rankings row {number} field {field!r} must be finite; "
                        f"got {row[field]!r}"
                    )
    source_path = Path(source).resolve() if isinstance(source, (str, Path)) else None
    return RankingsTable(tuple(columns), tuple(rows), source_path)


def normalize_rankings(table: RankingsTable) -> RankingsTable:
    """Validate normalized position values while preserving every source value."""

    if table.columns != RANKINGS_COLUMNS:
        raise RankingsSchemaError("rankings table has unexpected columns")
    for number, row in enumerate(table.rows, 2):
        position = row["slotName"].strip().upper()
        if position and position not in _POSITIONS:
            raise RankingsSchemaError(
                f"rankings row {number} has unknown position {row['slotName']!r}"
            )
    return table


def reorder_rankings(table: RankingsTable, ids: Sequence[str]) -> RankingsTable:
    """Return rows in ``ids`` order, with no value transformations."""

    if len(ids) != len(table.rows) or len(set(ids)) != len(ids):
        raise RankingsSchemaError(
            "rankings order must contain each player id exactly once"
        )
    by_id = {row["id"]: row for row in table.rows}
    missing = [player_id for player_id in ids if player_id not in by_id]
    if missing:
        raise RankingsSchemaError(f"rankings order contains unknown id(s): {missing!r}")
    return RankingsTable(
        table.columns,
        tuple(by_id[player_id].copy() for player_id in ids),
        table.source_path,
    )


def export_rankings_csv(
    table: RankingsTable,
    destination: str | Path | IO[str] | None = None,
    *,
    identity_graph: IdentityGraph | None = None,
    season: int | None = None,
    unattended: bool = False,
) -> str:
    """Serialize rankings for manual/low-level use.

    Call :func:`export_unattended_rankings_csv` for any automated output.  The
    optional gate arguments remain here only for backwards compatibility with
    existing callers; new unattended code must use the dedicated wrapper.
    """

    if table.columns != RANKINGS_COLUMNS:
        raise RankingsSchemaError("rankings table has unexpected columns")
    if unattended:
        if identity_graph is None or season is None:
            raise RankingsSchemaError(
                "unattended rankings export requires identity_graph and season"
            )
        try:
            identity_graph.check_material_players(table.rows, season, unattended=True)
        except ValueError as exc:
            raise RankingsSchemaError(str(exc)) from exc
    output = StringIO(newline="")
    writer = csv.DictWriter(
        output, fieldnames=list(RANKINGS_COLUMNS), lineterminator="\n"
    )
    writer.writeheader()
    writer.writerows(
        {column: row[column] for column in RANKINGS_COLUMNS} for row in table.rows
    )
    text = output.getvalue()
    if destination is not None:
        if isinstance(destination, (str, Path)):
            destination_path = Path(destination).resolve()
            if table.source_path is not None and destination_path == table.source_path:
                raise RankingsSchemaError(
                    "rankings export destination must not overwrite its input source"
                )
            with open(destination_path, "w", newline="", encoding="utf-8") as handle:
                handle.write(text)
        else:
            destination.write(text)
    return text


def export_unattended_rankings_csv(
    table: RankingsTable,
    destination: str | Path | IO[str],
    *,
    identity_graph: IdentityGraph,
    season: int,
    availability_decisions: dict[str, AvailabilityDecision] | None = None,
    availability_player_ids: Sequence[str] | None = None,
    high_impact_players: Sequence[str] | None = None,
    availability_as_of_utc: datetime | None = None,
    material_rank_cutoff: float = 50.0,
) -> str:
    """Export only after the mandatory material-player identity gate.

    This is the single entry point for unattended output; validation happens
    before opening the destination, so a blocked export cannot create a file.
    """
    if availability_decisions is None or high_impact_players is None:
        raise RankingsSchemaError(
            "unattended export blocked: availability gate requires decisions "
            "and high-impact players"
        )
    if not isinstance(availability_as_of_utc, datetime) or (
        availability_as_of_utc.tzinfo is None
        or availability_as_of_utc.utcoffset() is None
    ):
        raise RankingsSchemaError(
            "unattended export blocked: availability gate: "
            "availability_as_of_utc must be timezone-aware"
        )

    material_ids = _material_canonical_ids(
        table, identity_graph, season, material_rank_cutoff
    )
    if (
        availability_player_ids is not None
        and set(availability_player_ids) != material_ids
    ):
        raise RankingsSchemaError(
            "availability gate: supplied player IDs do not match "
            "table-derived material players"
        )
    if set(availability_decisions) != material_ids:
        missing = sorted(material_ids - set(availability_decisions))
        extra = sorted(set(availability_decisions) - material_ids)
        raise RankingsSchemaError(
            "availability gate: decisions do not exactly cover "
            "table-derived material players; "
            f"missing={missing!r}, extra={extra!r}"
        )
    if not set(high_impact_players).issubset(material_ids):
        raise RankingsSchemaError(
            "availability gate: incomplete high-impact player set"
        )
    for key, decision in availability_decisions.items():
        if not isinstance(decision, AvailabilityDecision):
            raise RankingsSchemaError("availability gate: invalid decision type")
        if not isinstance(decision.status, AvailabilityStatus):
            raise RankingsSchemaError("availability gate: invalid decision status")
        if key != decision.nflverse_id:
            raise RankingsSchemaError("availability gate: decision key mismatch")
        if decision.season != season:
            raise RankingsSchemaError("availability gate: season mismatch")
        if not isinstance(decision.as_of_utc, datetime) or (
            decision.as_of_utc.tzinfo is None
            or decision.as_of_utc.utcoffset() is None
        ):
            raise RankingsSchemaError(
                "availability gate: decision as_of must be timezone-aware"
            )
        if decision.as_of_utc != availability_as_of_utc:
            raise RankingsSchemaError("availability gate: as_of mismatch")
    try:
        check_unattended_availability(
            material_ids,
            availability_decisions,
            high_impact=high_impact_players,
        )
    except ValueError as exc:
        raise RankingsSchemaError(f"availability gate: {exc}") from exc
    return export_rankings_csv(
        table,
        destination,
        identity_graph=identity_graph,
        season=season,
        unattended=True,
    )


def _material_canonical_ids(
    table: RankingsTable,
    identity_graph: IdentityGraph,
    season: int,
    rank_cutoff: float,
) -> set[str]:
    """Resolve material export rows and return their canonical player IDs.

    ``is_material`` is an optional producer flag.  When absent, the numeric
    ``adp`` value is used with a deterministic top-50 fallback.  Resolution is
    deliberately performed at the export boundary so callers cannot provide a
    narrower availability set than the rows being exported.
    """
    if not isinstance(rank_cutoff, (int, float)) or isinstance(rank_cutoff, bool):
        raise RankingsSchemaError("material rank cutoff must be numeric")
    material: list[dict[str, str]] = []
    for row in table.rows:
        flag = row.get("is_material")
        if flag is not None and str(flag).strip() != "":
            normalized = str(flag).strip().casefold()
            if normalized in {"1", "true", "yes", "y"}:
                is_material = True
            elif normalized in {"0", "false", "no", "n"}:
                is_material = False
            else:
                raise RankingsSchemaError(
                    f"invalid is_material flag: {flag!r}"
                )
        else:
            try:
                rank = float(row.get("adp", ""))
            except (TypeError, ValueError) as exc:
                raise RankingsSchemaError(
                    "material rank cutoff fallback requires numeric adp"
                ) from exc
            is_material = math.isfinite(rank) and rank <= rank_cutoff
        if is_material:
            material.append(row)

    canonical_ids: set[str] = set()
    for row in material:
        context = {
            key: row.get(key)
            for key in ("game_id", "slate_id", "as_of", "week")
            if row.get(key) not in (None, "")
        }
        references = (
            ("nflverse_id", "nflverse_id"),
            ("ud_player_id", "ud_player_id"),
            ("appearance_id", "appearance_id"),
            ("id", None),
            ("playerId", None),
        )
        outcomes = [
            identity_graph.resolve_player(
                row.get(key), season, source=source, **context
            )
            for key, source in references
            if row.get(key) not in (None, "")
        ]
        resolved = {out.nflverse_id for out in outcomes if out.nflverse_id}
        if len(resolved) != 1 or any(
            out.status.value == "ambiguous" for out in outcomes
        ):
            raise RankingsSchemaError(
                "unattended export blocked: material player identity unresolved "
                f"for {row.get('id') or row.get('playerId')!r}"
            )
        canonical_ids.update(resolved)
    if not canonical_ids:
        raise RankingsSchemaError("unattended export blocked: no material players")
    return canonical_ids


load_rankings_csv = parse_rankings_csv
