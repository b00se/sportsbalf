"""Deterministic, leakage-safe WR/TE component projections."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd

RECEIVER_COMPONENTS: Final[tuple[str, ...]] = (
    "routes",
    "targets",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    "rare_rush_attempts",
    "rare_rush_yards",
    "rare_rush_tds",
)
_CAPS: Final[tuple[str, ...]] = (
    "team_pass_attempts",
    "team_routes",
    "team_targets",
    "team_rush_attempts",
)


@dataclass(frozen=True, slots=True)
class ReceiverComponentConfig:
    """Controls history windows and conservative priors for receivers."""

    min_history: int = 1
    window: int = 6
    fallback_routes: float = 25.0
    fallback_target_rate: float = 0.20
    fallback_catch_rate: float = 0.65
    fallback_yards_per_reception: float = 11.0
    fallback_td_rate: float = 0.04
    fallback_rare_rush_attempts: float = 0.25
    fallback_rare_rush_yards_per_attempt: float = 5.0
    fallback_rare_rush_td_rate: float = 0.01
    availability_probability: float = 1.0

    def __post_init__(self) -> None:
        for name in ("min_history", "window"):
            value = getattr(self, name)
            if isinstance(value, (bool, np.bool_)):
                raise ValueError(f"{name} must be a positive integer")
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                raise ValueError(f"{name} must be a positive integer") from None
            if not np.isfinite(numeric) or not numeric.is_integer() or numeric < 1:
                raise ValueError(f"{name} must be a positive integer")
            object.__setattr__(self, name, int(numeric))
        if self.window < self.min_history:
            raise ValueError("window must be >= min_history")
        bounds = {
            "fallback_routes": (0.0, 100.0),
            "fallback_target_rate": (0.0, 1.0),
            "fallback_catch_rate": (0.0, 1.0),
            "fallback_yards_per_reception": (0.0, 100.0),
            "fallback_td_rate": (0.0, 1.0),
            "fallback_rare_rush_attempts": (0.0, 20.0),
            "fallback_rare_rush_yards_per_attempt": (0.0, 100.0),
            "fallback_rare_rush_td_rate": (0.0, 1.0),
            "availability_probability": (0.0, 1.0),
        }
        for name, (lower, upper) in bounds.items():
            value = getattr(self, name)
            if isinstance(value, (bool, np.bool_)):
                raise ValueError(f"{name} must be finite and in [{lower}, {upper}]")
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                numeric = float("nan")
            if not np.isfinite(numeric) or not lower <= numeric <= upper:
                raise ValueError(f"{name} must be finite and in [{lower}, {upper}]")
            object.__setattr__(self, name, numeric)


def _calendar(frame: pd.DataFrame) -> pd.DataFrame:
    seasons = pd.to_numeric(frame["season"], errors="coerce")
    weeks = pd.to_numeric(frame["week"], errors="coerce")
    if (
        seasons.isna().any()
        or weeks.isna().any()
        or (seasons % 1 != 0).any()
        or (weeks % 1 != 0).any()
        or (seasons < 1920).any()
        or (weeks < 1).any()
        or (weeks > 22).any()
    ):
        raise ValueError(
            "season must be integer >= 1920 and week must be integer 1..22"
        )
    result = frame.copy()
    result["season"] = seasons.astype(int)
    result["week"] = weeks.astype(int)
    result["_key"] = result["season"] * 100 + result["week"]
    return result


def _normalise(frame: pd.DataFrame, *, require_stats: bool) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("frame must be a pandas DataFrame")
    required = {"receiver_id", "position", "season", "week"}
    if require_stats:
        required.update(RECEIVER_COMPONENTS)
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"missing required columns: {', '.join(missing)}")
    result = _calendar(frame)
    if result["receiver_id"].isna().any() or result["position"].isna().any():
        raise ValueError("receiver_id and position are required")
    result["position"] = result["position"].astype(str).str.upper()
    if (~result["position"].isin(["WR", "TE"])).any():
        raise ValueError("position must be WR or TE")
    keys = ["receiver_id", "position", "season", "week"]
    if result.duplicated(keys).any():
        raise ValueError("duplicate receiver/calendar rows are not allowed")
    supplied = set(result.columns)
    for column in [*RECEIVER_COMPONENTS, *_CAPS]:
        if column not in supplied:
            result[column] = np.nan
        result[column] = pd.to_numeric(result[column], errors="coerce")
        values = result[column]
        if (
            require_stats and column in RECEIVER_COMPONENTS and values.isna().any()
        ) or (column in _CAPS and column in supplied and values.isna().any()):
            raise ValueError(f"{column} must contain numeric non-null values")
        if np.isinf(values).any() or (values.dropna() < 0).any():
            raise ValueError(f"{column} must contain finite nonnegative values")
    if require_stats and (result["receiving_tds"] > result["receptions"]).any():
        raise ValueError("receiving_tds cannot exceed receptions")
    if require_stats and (result["rare_rush_tds"] > result["rare_rush_attempts"]).any():
        raise ValueError("rare_rush_tds cannot exceed rare_rush_attempts")
    if "availability_probability" in result:
        result["availability_probability"] = pd.to_numeric(
            result["availability_probability"], errors="coerce"
        )
        values = result["availability_probability"]
        if (
            values.isna().any()
            or np.isinf(values).any()
            or (~values.between(0, 1)).any()
        ):
            raise ValueError("availability_probability must be finite and in [0, 1]")
    return result


def _as_of_key(as_of: tuple[int, int] | None) -> int | None:
    if as_of is None:
        return None
    if not isinstance(as_of, tuple) or len(as_of) != 2:
        raise ValueError("as_of must be a (season, week) tuple")
    frame = _calendar(pd.DataFrame([{"season": as_of[0], "week": as_of[1]}]))
    return int(frame.iloc[0]["_key"])


def _prior(
    history: pd.DataFrame,
    target: pd.Series,
    cfg: ReceiverComponentConfig,
    as_of: int | None,
) -> pd.DataFrame:
    target_key = int(target["_key"])
    boundary = (
        target_key - 1 if target_key % 100 > 1 else (target_key // 100 - 1) * 100 + 22
    )
    if as_of is not None:
        boundary = min(boundary, as_of)
    rows = history.loc[
        (history["receiver_id"] == target["receiver_id"])
        & (history["position"] == target["position"])
        & (history["_key"] <= boundary)
    ]
    return (
        rows.sort_values("_key").tail(cfg.window)
        if len(rows) >= cfg.min_history
        else history.iloc[0:0]
    )


def _mean(prior: pd.DataFrame, column: str, fallback: float) -> float:
    values = prior[column].dropna()
    return float(values.mean()) if not values.empty else fallback


def _ratio(
    prior: pd.DataFrame, numerator: str, denominator: str, fallback: float
) -> float:
    if prior.empty:
        return fallback
    values = (prior[numerator] / prior[denominator].replace(0, np.nan)).dropna()
    return float(values.mean()) if not values.empty else fallback


def _project_one(
    history: pd.DataFrame,
    target: pd.Series,
    cfg: ReceiverComponentConfig,
    as_of: int | None,
) -> dict[str, object]:
    prior = _prior(history, target, cfg, as_of)
    routes = _mean(prior, "routes", cfg.fallback_routes)
    target_rate = _ratio(prior, "targets", "routes", cfg.fallback_target_rate)
    target_rate = float(np.clip(target_rate, 0, 1))
    catch_rate = float(
        np.clip(_ratio(prior, "receptions", "targets", cfg.fallback_catch_rate), 0, 1)
    )
    ypr = max(
        0.0,
        _ratio(
            prior, "receiving_yards", "receptions", cfg.fallback_yards_per_reception
        ),
    )
    td_rate = max(0.0, _ratio(prior, "receiving_tds", "targets", cfg.fallback_td_rate))
    rush = max(0.0, _mean(prior, "rare_rush_attempts", cfg.fallback_rare_rush_attempts))
    rush_ypr = max(
        0.0,
        _ratio(
            prior,
            "rare_rush_yards",
            "rare_rush_attempts",
            cfg.fallback_rare_rush_yards_per_attempt,
        ),
    )
    rush_td = max(
        0.0,
        _ratio(
            prior, "rare_rush_tds", "rare_rush_attempts", cfg.fallback_rare_rush_td_rate
        ),
    )

    def cap(name: str) -> float | None:
        return float(target[name]) if pd.notna(target.get(name)) else None

    pass_cap = cap("team_pass_attempts")
    target_cap = cap("team_targets")
    rush_cap = cap("team_rush_attempts")
    route_cap = cap("team_routes")
    if pass_cap is not None:
        routes = min(routes, pass_cap)
    if route_cap is not None:
        routes = min(routes, route_cap)
    targets = (
        min(routes * target_rate, target_cap)
        if target_cap is not None
        else routes * target_rate
    )
    rush = min(rush, rush_cap) if rush_cap is not None else rush
    availability = float(
        target.get("availability_probability", cfg.availability_probability)
    )
    scale = np.clip(availability, 0, 1)
    routes, targets, rush = routes * scale, targets * scale, rush * scale
    metadata_key = (
        min(
            int(target["_key"]) - 1
            if int(target["week"]) > 1
            else (int(target["season"]) - 1) * 100 + 22,
            as_of,
        )
        if as_of is not None
        else (
            int(target["_key"]) - 1
            if int(target["week"]) > 1
            else (int(target["season"]) - 1) * 100 + 22
        )
    )
    return {
        "receiver_id": target["receiver_id"],
        "position": target["position"],
        "season": int(target["season"]),
        "week": int(target["week"]),
        "routes": routes,
        "targets": targets,
        "receptions": min(targets, targets * catch_rate),
        "receiving_yards": min(targets, targets * catch_rate) * ypr,
        "receiving_tds": min(targets * td_rate, min(targets, targets * catch_rate)),
        "rare_rush_attempts": rush,
        "rare_rush_yards": rush * rush_ypr,
        "rare_rush_tds": min(rush * rush_td, rush),
        "availability_probability": availability,
        "as_of_season": int(metadata_key // 100),
        "as_of_week": int(metadata_key % 100),
    }


def project_receiver_components(
    history: pd.DataFrame,
    targets: pd.DataFrame | None = None,
    *,
    as_of: tuple[int, int] | None = None,
    config: ReceiverComponentConfig | None = None,
) -> pd.DataFrame:
    """Project WR/TE components using only strictly prior receiver observations."""
    cfg = config or ReceiverComponentConfig()
    observed = _normalise(history, require_stats=True)
    target_frame = (
        _normalise(targets, require_stats=False) if targets is not None else observed
    )
    cutoff = _as_of_key(as_of)
    rows = [
        _project_one(observed, row, cfg, cutoff) for _, row in target_frame.iterrows()
    ]
    columns = [
        "receiver_id",
        "position",
        "season",
        "week",
        *RECEIVER_COMPONENTS,
        "availability_probability",
        "as_of_season",
        "as_of_week",
    ]
    return pd.DataFrame(rows, columns=columns)


def rolling_origin_compare(
    frame: pd.DataFrame, *, config: ReceiverComponentConfig | None = None
) -> dict[str, float | int | bool]:
    """Compare historical trailing target-rate predictions to a global mean baseline."""
    cfg = config or ReceiverComponentConfig()
    observed = _normalise(frame, require_stats=True).sort_values(
        ["_key", "receiver_id"]
    )
    errors: list[float] = []
    baseline_errors: list[float] = []
    for key in sorted(observed["_key"].unique()):
        train = observed.loc[observed["_key"] < key]
        test = observed.loc[observed["_key"] == key]
        if len(train) < cfg.min_history:
            continue
        # Identity/calendar only: target-fold opportunity values stay out of evaluation.
        targets = test[["receiver_id", "position", "season", "week"]].copy()
        train_for_projection = train[
            ["receiver_id", "position", "season", "week", *RECEIVER_COMPONENTS]
        ]
        predictions = project_receiver_components(
            train_for_projection, targets, config=cfg
        )
        actual = test.reset_index(drop=True)
        errors.extend((predictions["targets"] - actual["targets"]).abs().tolist())
        baseline_errors.extend(
            (float(train["targets"].mean()) - actual["targets"]).abs().tolist()
        )
    model_mae = float(np.mean(errors)) if errors else float("nan")
    baseline_mae = float(np.mean(baseline_errors)) if baseline_errors else float("nan")
    return {
        "model_mae": model_mae,
        "baseline_mae": baseline_mae,
        "promoted": bool(errors) and model_mae <= baseline_mae,
        "folds": len(errors),
    }


__all__ = [
    "RECEIVER_COMPONENTS",
    "ReceiverComponentConfig",
    "project_receiver_components",
    "rolling_origin_compare",
]
