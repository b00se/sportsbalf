"""Strict adapter from nflverse weekly rows to component-projector inputs.

The adapter deliberately accepts only completed player-week observations.  It
does not infer routes or other unavailable statistics, and it never creates a
target frame containing outcome fields.  Callers can therefore pass the
returned history frames to the existing leakage-safe projectors.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd

from src.nfl.models.qb_components import COMPONENTS as QB_COMPONENTS
from src.nfl.models.rb_components import RB_COMPONENTS
from src.nfl.models.receiver_components import RECEIVER_COMPONENTS

SUPPORTED_POSITIONS: Final[frozenset[str]] = frozenset({"QB", "RB", "WR", "TE"})

_ALIASES: Final[dict[str, tuple[str, ...]]] = {
    "player_id": ("player_id", "player_gsis_id", "gsis_id"),
    "position": ("position", "player_position"),
    "season": ("season",),
    "week": ("week", "game_week"),
    "pass_attempts": ("pass_attempts", "attempts", "passing_attempts"),
    "completions": ("completions", "complete_passes"),
    "passing_yards": ("passing_yards", "pass_yards"),
    "passing_tds": ("passing_tds", "pass_tds", "passing_touchdowns"),
    "interceptions": ("interceptions", "passing_interceptions"),
    "rushing_attempts": ("rushing_attempts", "rush_attempts", "carries"),
    "rushing_yards": ("rushing_yards", "rush_yards"),
    "rushing_tds": ("rushing_tds", "rush_tds", "rushing_touchdowns"),
    "targets": ("targets", "receiving_targets"),
    "receptions": ("receptions", "rec", "catches"),
    "receiving_yards": ("receiving_yards", "rec_yards"),
    "receiving_tds": ("receiving_tds", "receiving_touchdowns", "rec_tds"),
    "routes": ("routes",),
}

_POSITION_COMPONENTS: Final[Mapping[str, tuple[str, ...]]] = {
    "QB": tuple(QB_COMPONENTS),
    "RB": tuple(RB_COMPONENTS),
    "WR": tuple(RECEIVER_COMPONENTS),
    "TE": tuple(RECEIVER_COMPONENTS),
}
_SIGNED: Final[frozenset[str]] = frozenset(
    {"passing_yards", "rushing_yards", "receiving_yards", "rare_rush_yards"}
)
_NONNEGATIVE: Final[frozenset[str]] = frozenset(
    set(QB_COMPONENTS) | set(RB_COMPONENTS) | set(RECEIVER_COMPONENTS)
) - _SIGNED


@dataclass(frozen=True, slots=True)
class ArchivedComponentFrames:
    """Projector-ready historical observations partitioned by position."""

    qb: pd.DataFrame
    rb: pd.DataFrame
    receivers: pd.DataFrame


def _strict_numeric(series: pd.Series, name: str) -> pd.Series:
    if series.map(lambda value: isinstance(value, (bool, np.bool_))).any():
        raise ValueError(f"{name} must not contain booleans")
    values = pd.to_numeric(series, errors="coerce")
    if values.isna().any() or (~np.isfinite(values)).any():
        raise ValueError(f"{name} must contain finite numeric values")
    return values.astype(float)


def _canonicalize(frame: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("weekly rows must be a pandas DataFrame")
    result = frame.copy(deep=True)
    for canonical, aliases in _ALIASES.items():
        present = [column for column in aliases if column in result.columns]
        if not present:
            continue
        values = [result[column] for column in present]
        if len(values) > 1:
            for alternate in values[1:]:
                equal = values[0].eq(alternate) | (
                    values[0].isna() & alternate.isna()
                )
                if (~equal).any():
                    raise ValueError(f"conflicting alternate values for {canonical}")
        result[canonical] = values[0]
    required = {"player_id", "position", "season", "week"}
    missing = sorted(required - set(result.columns))
    if missing:
        raise ValueError(f"weekly rows missing required columns: {missing}")
    if result[sorted(required)].isna().any().any():
        raise ValueError("player_id, position, season, and week are required")
    result["player_id"] = result["player_id"].astype(str).str.strip()
    if (result["player_id"] == "").any() or result["player_id"].eq("nan").any():
        raise ValueError("player_id must be non-empty")
    result["position"] = result["position"].astype(str).str.upper().str.strip()
    unsupported = sorted(set(result["position"]) - SUPPORTED_POSITIONS)
    if unsupported:
        raise ValueError(f"unsupported player positions: {unsupported}")
    result["season"] = _strict_numeric(result["season"], "season")
    result["week"] = _strict_numeric(result["week"], "week")
    if (
        (result["season"] % 1 != 0).any()
        or (result["season"] < 1920).any()
        or (result["week"] % 1 != 0).any()
        or (result["week"] < 1).any()
        or (result["week"] > 22).any()
    ):
        raise ValueError("season must be integer >= 1920 and week integer 1..22")
    result["season"] = result["season"].astype(int)
    result["week"] = result["week"].astype(int)
    if result.duplicated(["player_id", "season", "week"]).any():
        raise ValueError("duplicate supported player-week rows are not allowed")
    return result


def _component(frame: pd.DataFrame, name: str) -> pd.Series:
    aliases = _ALIASES.get(name, (name,))
    present = [column for column in aliases if column in frame.columns]
    if not present:
        raise ValueError(f"weekly rows missing required component: {name}")
    return _strict_numeric(frame[present[0]], name)


def _projector_frame(frame: pd.DataFrame, position: str) -> pd.DataFrame:
    components = _POSITION_COMPONENTS[position]
    output = pd.DataFrame(index=frame.index)
    output["season"] = frame["season"]
    output["week"] = frame["week"]
    output["position"] = frame["position"] if position in {"WR", "TE"} else None
    output["player_id"] = frame["player_id"]
    for name in components:
        source_name = name
        if name == "rush_attempts":
            source_name = "rushing_attempts"
        if name == "rare_rush_attempts":
            source_name = "rushing_attempts"
        elif name == "rare_rush_yards":
            source_name = "rushing_yards"
        elif name == "rare_rush_tds":
            source_name = "rushing_tds"
        output[name] = _component(frame, source_name)
        if name in _NONNEGATIVE and (output[name] < 0).any():
            raise ValueError(f"{name} must be nonnegative")
    if position == "QB":
        output = output.rename(columns={"player_id": "qb_id"})
        return output[["qb_id", "season", "week", *QB_COMPONENTS]]
    if position == "RB":
        output = output.rename(columns={"player_id": "rb_id"})
        return output[["rb_id", "season", "week", *RB_COMPONENTS]]
    receiver_columns = ["player_id", "position", "season", "week", *RECEIVER_COMPONENTS]
    return output[receiver_columns].rename(columns={"player_id": "receiver_id"})


def adapt_archived_weekly_components(weekly: pd.DataFrame) -> ArchivedComponentFrames:
    """Adapt canonical archived rows for the existing component projectors.

    All component values are observations from the represented week.  This
    function intentionally returns history only; callers must construct target
    identity/calendar rows separately, keeping target-week outcomes out of
    feature materialization.
    """
    canonical = _canonicalize(weekly)
    frames = {
        position: _projector_frame(
            canonical.loc[canonical["position"].eq(position)].copy(), position
        )
        for position in SUPPORTED_POSITIONS
    }
    return ArchivedComponentFrames(
        qb=frames["QB"], rb=frames["RB"], receivers=pd.concat(
            [frames["WR"], frames["TE"]], ignore_index=True
        ).sort_values(["season", "week", "receiver_id"], kind="mergesort")
    )


__all__ = [
    "ArchivedComponentFrames",
    "SUPPORTED_POSITIONS",
    "adapt_archived_weekly_components",
]
