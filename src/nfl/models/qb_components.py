"""Leakage-safe, bottom-up NFL quarterback component projections."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd

COMPONENTS: Final[tuple[str, ...]] = (
    "pass_attempts",
    "completions",
    "passing_yards",
    "passing_tds",
    "interceptions",
    "rushing_attempts",
    "rushing_yards",
    "rushing_tds",
)
_ALIASES: Final[dict[str, tuple[str, ...]]] = {
    "pass_attempts": ("pass_attempts", "attempts"),
    "completions": ("completions", "complete_passes"),
    "passing_yards": ("passing_yards", "pass_yards"),
    "passing_tds": ("passing_tds", "pass_touchdowns"),
    "interceptions": ("interceptions", "interceptions_thrown"),
    "rushing_attempts": ("rushing_attempts", "rush_attempts"),
    "rushing_yards": ("rushing_yards", "rush_yards"),
    "rushing_tds": ("rushing_tds", "rush_touchdowns"),
}


@dataclass(frozen=True, slots=True)
class QBComponentConfig:
    """Controls history windows and conservative empty-history priors."""

    min_history: int = 1
    window: int = 6
    fallback_attempts: float = 30.0
    fallback_completion_rate: float = 0.62
    fallback_yards_per_attempt: float = 7.0
    fallback_pass_td_rate: float = 0.04
    fallback_interception_rate: float = 0.025
    fallback_rush_attempts: float = 3.0
    fallback_rush_yards_per_attempt: float = 4.0
    fallback_rush_td_rate: float = 0.02

    def __post_init__(self) -> None:
        try:
            min_history = float(self.min_history)
            window = float(self.window)
        except (TypeError, ValueError):
            raise ValueError(
                "window and min_history must be positive integers"
            ) from None
        if (
            isinstance(self.min_history, (bool, np.bool_))
            or isinstance(self.window, (bool, np.bool_))
            or not min_history.is_integer()
            or not window.is_integer()
            or min_history < 1
            or window < min_history
        ):
            raise ValueError("window must be >= min_history >= 1")
        object.__setattr__(self, "min_history", int(min_history))
        object.__setattr__(self, "window", int(window))
        bounded = {
            "fallback_attempts": (self.fallback_attempts, 0.0, 100.0),
            "fallback_completion_rate": (self.fallback_completion_rate, 0.0, 1.0),
            "fallback_yards_per_attempt": (self.fallback_yards_per_attempt, 0.0, 100.0),
            "fallback_pass_td_rate": (self.fallback_pass_td_rate, 0.0, 1.0),
            "fallback_interception_rate": (self.fallback_interception_rate, 0.0, 1.0),
            "fallback_rush_attempts": (self.fallback_rush_attempts, 0.0, 40.0),
            "fallback_rush_yards_per_attempt": (
                self.fallback_rush_yards_per_attempt,
                0.0,
                100.0,
            ),
            "fallback_rush_td_rate": (self.fallback_rush_td_rate, 0.0, 1.0),
        }
        for name, (value, lower, upper) in bounded.items():
            try:
                if isinstance(value, (bool, np.bool_)):
                    raise ValueError
                numeric_value = float(value)
            except (TypeError, ValueError):
                numeric_value = float("nan")
            if not np.isfinite(numeric_value) or not lower <= numeric_value <= upper:
                raise ValueError(f"{name} must be finite and in [{lower}, {upper}]")
            object.__setattr__(self, name, numeric_value)


def _key(frame: pd.DataFrame) -> pd.Series:
    return pd.to_numeric(frame["season"], errors="coerce") * 100 + pd.to_numeric(
        frame["week"], errors="coerce"
    )


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    for alias in _ALIASES[column]:
        if alias in frame:
            return pd.to_numeric(frame[alias], errors="coerce")
    return pd.Series(np.nan, index=frame.index, dtype=float)


def _normalise_history(history: pd.DataFrame) -> pd.DataFrame:
    required = {"qb_id", "season", "week"}
    missing = sorted(required - set(history.columns))
    if missing:
        raise KeyError(f"history missing required columns: {missing}")
    result = history.copy()
    seasons = pd.to_numeric(result["season"], errors="coerce")
    weeks = pd.to_numeric(result["week"], errors="coerce")
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
            "season must be an integer >= 1920 and week must be integer 1..22"
        )
    result["season"] = seasons.astype(int)
    result["week"] = weeks.astype(int)
    if result.duplicated(["qb_id", "season", "week"]).any():
        raise ValueError("duplicate (qb_id, season, week) rows are not allowed")
    result["_key"] = _key(result)
    if result["_key"].isna().any():
        raise ValueError("season and week must be numeric")
    for component in COMPONENTS:
        result[component] = _numeric(result, component)
    result["team_pass_attempts"] = pd.to_numeric(
        result.get("team_pass_attempts", pd.Series(np.nan, index=result.index)),
        errors="coerce",
    )
    for column in [*COMPONENTS, "team_pass_attempts"]:
        values = result[column]
        if np.isinf(values).any() or (values.dropna() < 0).any():
            raise ValueError(f"{column} must contain finite nonnegative values")
    return result


def _as_of_key(as_of: tuple[int, int] | None) -> float | None:
    if as_of is None:
        return None
    if len(as_of) != 2:
        raise ValueError("as_of must be a (season, week) tuple")
    season, week = as_of
    if (
        int(season) != season
        or int(week) != week
        or int(season) < 1920
        or not 1 <= int(week) <= 22
    ):
        raise ValueError("as_of must contain an integer season >= 1920 and week 1..22")
    return int(season) * 100 + int(week)


def _mean(series: pd.Series, fallback: float) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return float(values.mean()) if not values.empty else fallback


def _predecessor(season: int, week: int) -> tuple[int, int]:
    """Return the latest calendar boundary strictly before a target game."""
    return (season, week - 1) if week > 1 else (season - 1, 22)


def _effective_boundary(target: pd.Series, as_of_key: float | None) -> tuple[int, int]:
    predecessor = _predecessor(int(target["season"]), int(target["week"]))
    predecessor_key = predecessor[0] * 100 + predecessor[1]
    if as_of_key is None or as_of_key >= predecessor_key:
        return predecessor
    return (int(as_of_key // 100), int(as_of_key % 100))


def _project_one(
    history: pd.DataFrame,
    target: pd.Series,
    *,
    config: QBComponentConfig,
    as_of_key: float | None,
    effective_boundary: tuple[int, int],
) -> dict[str, float | int | str]:
    qb_history = history.loc[history["qb_id"].eq(target["qb_id"])]
    target_key = float(target["_key"])
    cutoff = target_key if as_of_key is None else min(target_key, as_of_key + 1e-6)
    prior = (
        qb_history.loc[qb_history["_key"] < cutoff]
        .sort_values("_key")
        .tail(config.window)
    )
    if len(prior) < config.min_history:
        prior = qb_history.iloc[0:0]

    attempts = _mean(prior["pass_attempts"], config.fallback_attempts)
    target_team_attempts = pd.to_numeric(
        target.get("team_pass_attempts"), errors="coerce"
    )
    if pd.notna(target_team_attempts) and not prior.empty:
        share = (
            prior["pass_attempts"] / prior["team_pass_attempts"].replace(0, np.nan)
        ).clip(0, 1)
        attempts = float(target_team_attempts) * _mean(share, 1.0)
    if pd.notna(target_team_attempts):
        attempts = min(attempts, max(0.0, float(target_team_attempts)))
    attempts = max(0.0, attempts)
    completion_rate = np.clip(
        _mean(
            prior["completions"] / prior["pass_attempts"].replace(0, np.nan),
            config.fallback_completion_rate,
        ),
        0,
        1,
    )
    yards_rate = max(
        0.0,
        _mean(
            prior["passing_yards"] / prior["pass_attempts"].replace(0, np.nan),
            config.fallback_yards_per_attempt,
        ),
    )
    td_rate = max(
        0.0,
        _mean(
            prior["passing_tds"] / prior["pass_attempts"].replace(0, np.nan),
            config.fallback_pass_td_rate,
        ),
    )
    int_rate = max(
        0.0,
        _mean(
            prior["interceptions"] / prior["pass_attempts"].replace(0, np.nan),
            config.fallback_interception_rate,
        ),
    )
    rush_attempts = max(
        0.0, _mean(prior["rushing_attempts"], config.fallback_rush_attempts)
    )
    rush_yards_rate = max(
        0.0,
        _mean(
            prior["rushing_yards"] / prior["rushing_attempts"].replace(0, np.nan),
            config.fallback_rush_yards_per_attempt,
        ),
    )
    rush_td_rate = max(
        0.0,
        _mean(
            prior["rushing_tds"] / prior["rushing_attempts"].replace(0, np.nan),
            config.fallback_rush_td_rate,
        ),
    )
    return {
        "qb_id": target["qb_id"],
        "season": int(target["season"]),
        "week": int(target["week"]),
        "pass_attempts": attempts,
        "completions": min(attempts, attempts * completion_rate),
        "passing_yards": attempts * yards_rate,
        "passing_tds": attempts * td_rate,
        "interceptions": attempts * int_rate,
        "rushing_attempts": rush_attempts,
        "rushing_yards": rush_attempts * rush_yards_rate,
        "rushing_tds": rush_attempts * rush_td_rate,
        "as_of_season": effective_boundary[0],
        "as_of_week": effective_boundary[1],
    }


def project_qb_components(
    history: pd.DataFrame,
    targets: pd.DataFrame | None = None,
    *,
    as_of: tuple[int, int] | None = None,
    config: QBComponentConfig | None = None,
) -> pd.DataFrame:
    """Project QB passing and rushing components using only prior observations.

    ``history`` is never mutated. Each target uses rows strictly earlier than its
    season/week; ``as_of`` can impose an earlier global information cutoff.
    """
    cfg = config or QBComponentConfig()
    observed = _normalise_history(history)
    target_frame = _normalise_history(targets) if targets is not None else observed
    cutoff = _as_of_key(as_of)
    boundaries = [
        _effective_boundary(row, cutoff) for _, row in target_frame.iterrows()
    ]
    rows = [
        _project_one(
            observed,
            row,
            config=cfg,
            as_of_key=(boundary[0] * 100 + boundary[1]),
            effective_boundary=boundary,
        )
        for (_, row), boundary in zip(target_frame.iterrows(), boundaries)
    ]
    return pd.DataFrame(
        rows,
        columns=["qb_id", "season", "week", *COMPONENTS, "as_of_season", "as_of_week"],
    )


def rolling_origin_compare(
    frame: pd.DataFrame, *, config: QBComponentConfig | None = None
) -> dict[str, float | int | bool]:
    """Compare the component projection with a historical-mean baseline."""
    cfg = config or QBComponentConfig()
    observed = _normalise_history(frame).sort_values(["_key", "qb_id"])
    errors: list[float] = []
    baseline_errors: list[float] = []
    keys = sorted(observed["_key"].unique())
    for key in keys:
        train = observed.loc[observed["_key"] < key]
        test = observed.loc[observed["_key"].eq(key)]
        if train.empty or len(train) < cfg.min_history:
            continue
        targets = test[["qb_id", "season", "week"]].copy()
        predictions = project_qb_components(train, targets, config=cfg)
        actual = test.reset_index(drop=True)
        errors.extend(
            (predictions["pass_attempts"] - actual["pass_attempts"]).abs().tolist()
        )
        baseline_errors.extend(
            (float(train["pass_attempts"].mean()) - actual["pass_attempts"])
            .abs()
            .tolist()
        )
    model_mae = float(np.mean(errors)) if errors else float("nan")
    baseline_mae = float(np.mean(baseline_errors)) if baseline_errors else float("nan")
    promoted = bool(errors) and model_mae <= baseline_mae
    return {
        "model_mae": model_mae,
        "baseline_mae": baseline_mae,
        "promoted": promoted,
        "folds": len(errors),
    }


__all__ = [
    "COMPONENTS",
    "QBComponentConfig",
    "project_qb_components",
    "rolling_origin_compare",
]
