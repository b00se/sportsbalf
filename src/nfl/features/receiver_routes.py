"""Conservative NFLverse WR/TE route-row materialization.

NFLverse participation labels a play with one categorical route, rather than
assigning a route to every offensive participant.  This module therefore
counts only route rows whose PBP receiver identity can be proven to be a
WR/TE in the aligned participation lists (or in a week/team roster).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

_PARTICIPATION_REQUIRED = {"nflverse_game_id", "play_id", "route"}
_PBP_REQUIRED = {
    "game_id",
    "play_id",
    "season",
    "week",
    "play_type",
    "pass_attempt",
    "receiver_player_id",
    "posteam",
    "season_type",
}
_SEASON_LIMITS = (1920, 2100)
_WEEK_LIMITS = (1, 22)
_PLAY_ID_LIMITS = (0, 1_000_000)


class ReceiverRouteSchemaError(ValueError):
    """Raised when route inputs cannot be joined or aligned safely."""


@dataclass(frozen=True, slots=True)
class ReceiverRouteDiagnostics:
    """Counts describing route source coverage and exclusions."""

    source_pass_route_rows: int
    linked_receiver_rows: int
    unlinked_receiver_rows: int
    linked_wr_te_rows: int
    excluded_non_wr_te_rows: int
    unmatched_participation_route_rows: int

    @property
    def linked_coverage(self) -> float:
        """Return the fraction of route rows with a linked receiver."""
        if self.source_pass_route_rows == 0:
            return 0.0
        return self.linked_receiver_rows / self.source_pass_route_rows

    @property
    def wr_te_coverage(self) -> float:
        """Return the fraction of source route rows retained as WR/TE."""
        if self.source_pass_route_rows == 0:
            return 0.0
        return self.linked_wr_te_rows / self.source_pass_route_rows


@dataclass(frozen=True, slots=True)
class ReceiverRouteMaterialization:
    """Materialized lower-bound routes and source-quality diagnostics."""

    frame: pd.DataFrame
    diagnostics: ReceiverRouteDiagnostics


def _require_columns(frame: pd.DataFrame, required: set[str], name: str) -> None:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError(f"{name} must be a pandas DataFrame")
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ReceiverRouteSchemaError(f"{name} missing required columns: {missing}")


def _key_series(frame: pd.DataFrame, column: str, name: str) -> pd.Series:
    values = frame[column].astype("string").str.strip()
    if values.isna().any() or values.eq("").any():
        raise ReceiverRouteSchemaError(f"{name} {column} must be non-empty")
    return values


def _calendar(frame: pd.DataFrame, name: str) -> pd.DataFrame:
    result = frame.copy()
    season = _integer_values(result["season"], name=f"{name} season")
    week = _integer_values(result["week"], name=f"{name} week")
    if season.isna().any() or week.isna().any():
        raise ReceiverRouteSchemaError(f"{name} season and week must be numeric")
    if ((season % 1) != 0).any() or ((week % 1) != 0).any():
        raise ReceiverRouteSchemaError(f"{name} season and week must be integers")
    if (season < _SEASON_LIMITS[0]).any() or (season > _SEASON_LIMITS[1]).any():
        raise ReceiverRouteSchemaError(
            f"{name} season must be between {_SEASON_LIMITS[0]} and {_SEASON_LIMITS[1]}"
        )
    if (week < _WEEK_LIMITS[0]).any() or (week > _WEEK_LIMITS[1]).any():
        raise ReceiverRouteSchemaError(
            f"{name} week must be between {_WEEK_LIMITS[0]} and {_WEEK_LIMITS[1]}"
        )
    result["season"] = season.astype("int64")
    result["week"] = week.astype("int64")
    return result


def _integer_values(series: pd.Series, *, name: str) -> pd.Series:
    """Return integer-valued data while rejecting booleans and fractions."""
    if (
        pd.api.types.is_bool_dtype(series)
        or series.map(lambda value: isinstance(value, (bool, np.bool_))).any()
    ):
        raise ReceiverRouteSchemaError(f"{name} must be an integer, not boolean")
    values = pd.to_numeric(series, errors="coerce")
    if values.isna().any() or (~np.isfinite(values)).any() or (values % 1 != 0).any():
        raise ReceiverRouteSchemaError(f"{name} must be non-null finite integers")
    return values.astype("int64")


def _validate_play_keys(
    frame: pd.DataFrame, *, name: str, game_column: str
) -> pd.DataFrame:
    result = frame.copy()
    result[game_column] = _key_series(result, game_column, name)
    result["play_id"] = _integer_values(result["play_id"], name=f"{name} play_id")
    if (result["play_id"] < _PLAY_ID_LIMITS[0]).any() or (
        result["play_id"] > _PLAY_ID_LIMITS[1]
    ).any():
        raise ReceiverRouteSchemaError(
            f"{name} play_id must be between {_PLAY_ID_LIMITS[0]} and "
            f"{_PLAY_ID_LIMITS[1]}"
        )
    if result[[game_column, "play_id"]].duplicated().any():
        raise ReceiverRouteSchemaError(f"{name} contains duplicate game/play keys")
    return result


def _split_aligned(value: Any, *, column: str, index: Any) -> list[str]:
    if not isinstance(value, str):
        reason = "missing" if value is None or value is pd.NA else "must be a string"
        raise ReceiverRouteSchemaError(f"{reason} {column} alignment at row {index}")
    values = [item.strip() for item in value.split(";")]
    if not values or any(not item for item in values):
        raise ReceiverRouteSchemaError(f"malformed {column} alignment at row {index}")
    return values


def _alignment_cell_missing(value: Any) -> bool:
    """Return whether an alignment cell is absent rather than malformed."""
    if value is None or value is pd.NA:
        return True
    if isinstance(value, str):
        return not value.strip()
    missing = pd.isna(value)
    return isinstance(missing, (bool, np.bool_)) and bool(missing)


def _position_from_alignment(row: pd.Series, receiver_id: str) -> str | None:
    players = _split_aligned(
        row["offense_players"], column="offense_players", index=row.name
    )
    positions = _split_aligned(
        row["offense_positions"], column="offense_positions", index=row.name
    )
    if len(players) != len(positions):
        raise ReceiverRouteSchemaError(
            f"offense player/position alignment mismatch at row {row.name}"
        )
    if len(set(players)) != len(players):
        raise ReceiverRouteSchemaError(
            f"duplicate offense player identity at row {row.name}"
        )
    try:
        position = positions[players.index(receiver_id)]
    except ValueError:
        return None
    return position.upper()


def _roster_positions(roster: pd.DataFrame, keys: pd.DataFrame) -> pd.Series:
    """Resolve only roster rows that exactly match candidate receivers.

    The weekly roster archive can contain unrelated players with incomplete
    identity fields. Those rows are intentionally discarded before strict
    roster validation; a row that does match a candidate key is validated and
    duplicate matches fail closed.
    """
    required = {"season", "week", "team", "position"}
    _require_columns(roster, required, "roster")
    id_column = "gsis_id" if "gsis_id" in roster.columns else "player_id"
    if id_column not in roster.columns:
        raise ReceiverRouteSchemaError("roster missing player identity column")
    # Filter on the complete candidate identity before validating the roster
    # payload. Blank IDs or malformed calendar values on unrelated roster rows
    # must not poison a valid candidate lookup.
    if keys.empty:
        return pd.Series(pd.NA, index=keys.index, dtype="string")
    candidate_keys = pd.MultiIndex.from_frame(
        keys[["season", "week", "posteam", "receiver_player_id"]].rename(
            columns={"posteam": "team", "receiver_player_id": id_column}
        )
    )
    roster_keys = pd.MultiIndex.from_arrays(
        [
            pd.to_numeric(roster["season"], errors="coerce"),
            pd.to_numeric(roster["week"], errors="coerce"),
            roster["team"].astype("string").str.strip(),
            roster[id_column].astype("string").str.strip(),
        ],
        names=["season", "week", "team", id_column],
    )
    roster_frame = roster.loc[roster_keys.isin(candidate_keys)].copy()
    if roster_frame.empty:
        return pd.Series(pd.NA, index=keys.index, dtype="string")
    roster_frame = _calendar(roster_frame, "roster")
    roster_frame["team"] = _key_series(roster_frame, "team", "roster")
    roster_frame[id_column] = _key_series(roster_frame, id_column, "roster")
    roster_frame["position"] = (
        roster_frame["position"].astype("string").str.strip().str.upper()
    )
    if roster_frame["position"].isna().any() or roster_frame["position"].eq("").any():
        raise ReceiverRouteSchemaError("roster position must be non-empty")
    if roster_frame[["season", "week", "team", id_column]].duplicated().any():
        raise ReceiverRouteSchemaError(
            "roster contains duplicate season/week/team/player keys"
        )
    lookup = roster_frame.set_index(["season", "week", "team", id_column])["position"]
    values = [
        lookup.get((row.season, row.week, row.posteam, row.receiver_player_id))
        for row in keys.itertuples(index=False)
    ]
    return pd.Series(values, index=keys.index, dtype="string")


def materialize_identified_receiver_routes(
    participation: pd.DataFrame,
    pbp: pd.DataFrame,
    roster: pd.DataFrame | None = None,
) -> ReceiverRouteMaterialization:
    """Count conservative identified WR/TE route rows by player/week.

    Args:
        participation: NFLverse play-level participation rows.
        pbp: NFLverse PBP rows containing receiver identity and pass flags.
        roster: Optional weekly roster used when aligned participation
            positions are unavailable. It must contain season, week, team,
            position, and ``gsis_id`` (or ``player_id``).

    Returns:
        Materialized rows with ``player_id``, ``season``, ``week``, ``team``,
        ``position``, and ``identified_receiver_routes`` plus diagnostics.

    Raises:
        ReceiverRouteSchemaError: If source keys, route schema, or player list
            alignment is malformed. Unlinked rows are excluded, never imputed.
    """
    _require_columns(participation, _PARTICIPATION_REQUIRED, "participation")
    _require_columns(pbp, _PBP_REQUIRED, "pbp")
    part = _validate_play_keys(
        participation, name="participation", game_column="nflverse_game_id"
    )
    pbp_frame = _calendar(
        _validate_play_keys(pbp, name="pbp", game_column="game_id"), "pbp"
    )
    if pd.api.types.is_numeric_dtype(part["route"]):
        raise ReceiverRouteSchemaError("participation route must be categorical")
    route_values = part["route"].dropna()
    if not route_values.map(lambda value: isinstance(value, str)).all():
        raise ReceiverRouteSchemaError(
            "participation route must be categorical strings"
        )
    part["route"] = part["route"].astype("string").str.strip()
    season_type = pbp_frame["season_type"]
    if not season_type.map(lambda value: isinstance(value, str)).all():
        raise ReceiverRouteSchemaError("pbp season_type must be a nonempty string")
    season_type = season_type.str.strip().str.upper()
    if season_type.isna().any() or season_type.eq("").any():
        raise ReceiverRouteSchemaError("pbp season_type must be a nonempty string")
    pbp_frame["season_type"] = season_type
    route_part = part.loc[part["route"].notna() & part["route"].ne("")]
    matched = route_part[["nflverse_game_id", "play_id"]].merge(
        pbp_frame[["game_id", "play_id"]],
        left_on=["nflverse_game_id", "play_id"],
        right_on=["game_id", "play_id"],
        how="left",
        indicator=True,
        validate="one_to_one",
    )
    unmatched_participation_route_rows = int(matched["_merge"].eq("left_only").sum())
    joined = pbp_frame.merge(
        part,
        left_on=["game_id", "play_id"],
        right_on=["nflverse_game_id", "play_id"],
        how="inner",
        validate="one_to_one",
        suffixes=("", "_participation"),
    )
    joined = joined.loc[joined["season_type"].eq("REG")].copy()
    pass_attempt = pd.to_numeric(joined["pass_attempt"], errors="coerce")
    route_mask = (
        joined["play_type"].astype("string").str.strip().str.lower().eq("pass")
        & pass_attempt.eq(1)
        & joined["route"].notna()
        & joined["route"].ne("")
    )
    candidates = joined.loc[route_mask].copy()
    source_count = len(candidates)
    receiver = candidates["receiver_player_id"].astype("string").str.strip()
    candidates = candidates.loc[receiver.notna() & receiver.ne("")].copy()
    candidates["receiver_player_id"] = receiver.loc[candidates.index]
    # Validate posteam only after candidate filtering. Null/malformed values
    # on plays that cannot contribute a route are irrelevant to this lower
    # bound and must not fail materialization.
    if not candidates["posteam"].map(lambda value: isinstance(value, str)).all():
        raise ReceiverRouteSchemaError("pbp posteam must be a nonempty string")
    candidates["posteam"] = _key_series(candidates, "posteam", "pbp")
    linked_position = pd.Series(pd.NA, index=candidates.index, dtype="string")
    has_alignment = {"offense_players", "offense_positions"}.issubset(
        candidates.columns
    )
    if has_alignment:
        fallback_indices = []
        for index, row in candidates.iterrows():
            alignment_values = (row["offense_players"], row["offense_positions"])
            if any(_alignment_cell_missing(value) for value in alignment_values):
                fallback_indices.append(index)
                continue
            linked_position.at[index] = _position_from_alignment(
                row, row["receiver_player_id"]
            )
        if fallback_indices:
            if roster is None:
                raise ReceiverRouteSchemaError(
                    "participation requires aligned offense_players/"
                    "offense_positions or roster"
                )
            fallback_keys = candidates.loc[fallback_indices]
            linked_position.loc[fallback_indices] = _roster_positions(
                roster, fallback_keys
            )
    elif roster is not None:
        linked_position = _roster_positions(roster, candidates)
    else:
        raise ReceiverRouteSchemaError(
            "participation requires aligned offense_players/offense_positions or roster"
        )
    linked = linked_position.notna()
    wr_te = linked & linked_position.isin(["WR", "TE"])
    output = candidates.loc[
        wr_te, ["season", "week", "posteam", "receiver_player_id"]
    ].copy()
    output.rename(
        columns={"posteam": "team", "receiver_player_id": "player_id"},
        inplace=True,
    )
    output["position"] = linked_position.loc[wr_te].to_numpy()
    result = (
        output.groupby(
            ["player_id", "season", "week", "team", "position"], as_index=False
        )
        .size()
        .rename(columns={"size": "identified_receiver_routes"})
    )
    diagnostics = ReceiverRouteDiagnostics(
        source_pass_route_rows=source_count,
        linked_receiver_rows=int(linked.sum()),
        unlinked_receiver_rows=source_count - int(linked.sum()),
        linked_wr_te_rows=int(wr_te.sum()),
        excluded_non_wr_te_rows=int((linked & ~wr_te).sum()),
        unmatched_participation_route_rows=unmatched_participation_route_rows,
    )
    result.attrs["route_diagnostics"] = diagnostics
    return ReceiverRouteMaterialization(result, diagnostics)


def materialize_identified_receiver_route_history(
    participation: pd.DataFrame,
    pbp: pd.DataFrame,
    as_of: tuple[int, int],
    roster: pd.DataFrame | None = None,
) -> ReceiverRouteMaterialization:
    """Materialize only route history strictly before an outer target week."""
    if not isinstance(as_of, tuple) or len(as_of) != 2:
        raise ValueError("as_of must be a (season, week) tuple")
    season, week = as_of
    if isinstance(season, (bool, np.bool_)) or isinstance(week, (bool, np.bool_)):
        raise ValueError("as_of must contain integer season and week")
    try:
        season_value, week_value = int(season), int(week)
    except (TypeError, ValueError):
        raise ValueError("as_of must contain integer season and week") from None
    if season_value != season or week_value != week or not 1 <= week_value <= 22:
        raise ValueError("as_of must contain integer season and week 1..22")
    pbp_validated = _calendar(
        _validate_play_keys(pbp, name="pbp", game_column="game_id"), "pbp"
    )
    season_type = pbp_validated["season_type"]
    if not season_type.map(lambda value: isinstance(value, str)).all():
        raise ReceiverRouteSchemaError("pbp season_type must be a nonempty string")
    season_type = season_type.str.strip().str.upper()
    if season_type.isna().any() or season_type.eq("").any():
        raise ReceiverRouteSchemaError("pbp season_type must be a nonempty string")
    cutoff = (pbp_validated["season"] < season_value) | (
        (pbp_validated["season"] == season_value) & (pbp_validated["week"] < week_value)
    )
    prior_pbp = pbp_validated.loc[cutoff].copy()
    prior_games = set(prior_pbp["game_id"])
    part_keys = participation["nflverse_game_id"].astype("string").str.strip()
    prior_participation = participation.loc[part_keys.isin(prior_games)].copy()
    full = materialize_identified_receiver_routes(
        prior_participation, prior_pbp, roster
    )
    frame = full.frame.loc[
        (full.frame["season"] < season_value)
        | ((full.frame["season"] == season_value) & (full.frame["week"] < week_value))
    ].copy()
    frame.attrs["route_diagnostics"] = full.diagnostics
    return ReceiverRouteMaterialization(frame, full.diagnostics)
