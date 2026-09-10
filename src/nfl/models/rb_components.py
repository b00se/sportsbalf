"""Leakage-safe, deterministic NFL running-back component projections."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd

RB_COMPONENTS: Final[tuple[str, ...]] = (
    "rush_attempts",
    "targets",
    "rushing_yards",
    "receptions",
    "receiving_yards",
    "rushing_tds",
    "receiving_tds",
)
_RATES: Final[tuple[str, ...]] = (
    "rush_attempts",
    "targets",
    "rushing_yards_per_attempt",
    "reception_rate",
    "receiving_yards_per_reception",
    "rushing_td_rate",
    "receiving_td_rate",
)
_STATUS: Final[set[str]] = {
    "active",
    "available",
    "inactive",
    "out",
    "questionable",
    "doubtful",
    "limited",
}
_ALIASES: Final[dict[str, tuple[str, ...]]] = {
    "rush_attempts": ("rush_attempts", "rushing_attempts"),
    "targets": ("targets", "receiving_targets"),
    "rushing_yards": ("rushing_yards", "rush_yards"),
    "receptions": ("receptions", "catches"),
    "receiving_yards": ("receiving_yards", "receiving_yards_gained"),
    "rushing_tds": ("rushing_tds", "rush_touchdowns"),
    "receiving_tds": ("receiving_tds", "receiving_touchdowns"),
}


@dataclass(frozen=True, slots=True)
class RBComponentConfig:
    """Controls the history window and conservative rookie priors."""

    min_history: int = 1
    window: int = 6
    fallback_rush_attempts: float = 12.0
    fallback_targets: float = 4.0
    fallback_rushing_yards_per_attempt: float = 4.2
    fallback_reception_rate: float = 0.70
    fallback_receiving_yards_per_reception: float = 8.0
    fallback_rushing_td_rate: float = 0.04
    fallback_receiving_td_rate: float = 0.02

    def __post_init__(self) -> None:
        try:
            minimum = float(self.min_history)
            window = float(self.window)
        except (TypeError, ValueError):
            raise ValueError(
                "window and min_history must be positive integers"
            ) from None
        if (
            isinstance(self.min_history, (bool, np.bool_))
            or isinstance(self.window, (bool, np.bool_))
            or not minimum.is_integer()
            or not window.is_integer()
            or minimum < 1
            or window < minimum
        ):
            raise ValueError("window must be >= min_history >= 1")
        object.__setattr__(self, "min_history", int(minimum))
        object.__setattr__(self, "window", int(window))
        bounds = {
            "fallback_rush_attempts": (self.fallback_rush_attempts, 0.0, 100.0),
            "fallback_targets": (self.fallback_targets, 0.0, 50.0),
            "fallback_rushing_yards_per_attempt": (
                self.fallback_rushing_yards_per_attempt,
                0.0,
                100.0,
            ),
            "fallback_reception_rate": (self.fallback_reception_rate, 0.0, 1.0),
            "fallback_receiving_yards_per_reception": (
                self.fallback_receiving_yards_per_reception,
                0.0,
                100.0,
            ),
            "fallback_rushing_td_rate": (self.fallback_rushing_td_rate, 0.0, 1.0),
            "fallback_receiving_td_rate": (
                self.fallback_receiving_td_rate,
                0.0,
                1.0,
            ),
        }
        for name, (raw, low, high) in bounds.items():
            try:
                if isinstance(raw, (bool, np.bool_)):
                    raise ValueError
                value = float(raw)
            except (TypeError, ValueError):
                value = float("nan")
            if not np.isfinite(value) or not low <= value <= high:
                raise ValueError(f"{name} must be finite and in [{low}, {high}]")
            object.__setattr__(self, name, value)


def _number(frame: pd.DataFrame, name: str) -> pd.Series:
    return pd.to_numeric(
        frame.get(name, pd.Series(np.nan, index=frame.index)), errors="coerce"
    )


def _component_number(frame: pd.DataFrame, name: str) -> pd.Series:
    for alias in _ALIASES[name]:
        if alias in frame:
            return pd.to_numeric(frame[alias], errors="coerce")
    return pd.Series(np.nan, index=frame.index, dtype=float)


def _key(frame: pd.DataFrame) -> pd.Series:
    return _number(frame, "season") * 100 + _number(frame, "week")


def _normalise(frame: pd.DataFrame, *, require_components: bool) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("history and targets must be pandas DataFrames")
    required = {"rb_id", "season", "week"}
    missing = sorted(required - set(frame.columns))
    missing_components = (
        [
            column
            for column in RB_COMPONENTS
            if not any(alias in frame for alias in _ALIASES[column])
        ]
        if require_components
        else []
    )
    missing.extend(missing_components)
    if missing:
        raise ValueError(f"missing required columns: {', '.join(missing)}")
    result = frame.copy()
    for column in ("season", "week", "team_rush_attempts", "team_targets"):
        if (
            column in result
            and result[column]
            .map(lambda value: isinstance(value, (bool, np.bool_)))
            .any()
        ):
            raise ValueError(f"{column} must not contain booleans")
    seasons, weeks = _number(result, "season"), _number(result, "week")
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
    result["season"], result["week"] = seasons.astype(int), weeks.astype(int)
    if result.duplicated(["rb_id", "season", "week"]).any():
        raise ValueError("duplicate (rb_id, season, week) rows are not allowed")
    for column in RB_COMPONENTS:
        sources = [alias for alias in _ALIASES[column] if alias in result]
        if sources:
            parsed = [
                pd.to_numeric(result[source], errors="coerce") for source in sources
            ]
            for source, values in zip(sources[1:], parsed[1:]):
                equal = parsed[0].eq(values) | (parsed[0].isna() & values.isna())
                if (~equal).any():
                    raise ValueError(
                        f"conflicting values for {column} and alias {source}"
                    )
            result[column] = parsed[0]
        elif result.empty:
            result[column] = pd.Series(dtype=float)
    for column in ("team_rush_attempts", "team_targets"):
        if column in result:
            result[column] = _number(result, column)
    numeric_columns = [
        c for c in (*RB_COMPONENTS, "team_rush_attempts", "team_targets") if c in result
    ]
    for column in numeric_columns:
        values = result[column]
        if values.isna().any() and (
            require_components or column not in ("team_rush_attempts", "team_targets")
        ):
            raise ValueError(f"{column} must contain numeric values")
        if np.isinf(values).any() or (values.dropna() < 0).any():
            raise ValueError(f"{column} must contain finite nonnegative values")
    if (
        "receptions" in result
        and "targets" in result
        and (result["receptions"] > result["targets"]).any()
    ):
        raise ValueError("receptions cannot exceed targets")
    if (
        "rushing_tds" in result
        and "rush_attempts" in result
        and (result["rushing_tds"] > result["rush_attempts"]).any()
    ):
        raise ValueError("rushing_tds cannot exceed rush_attempts")
    if (
        "receiving_tds" in result
        and "receptions" in result
        and (result["receiving_tds"] > result["receptions"]).any()
    ):
        raise ValueError("receiving_tds cannot exceed receptions")
    if "availability_probability" in result:
        if (
            result["availability_probability"]
            .map(lambda value: isinstance(value, (bool, np.bool_)))
            .any()
        ):
            raise ValueError("availability_probability must be finite and in [0, 1]")
        probabilities = pd.to_numeric(
            result["availability_probability"], errors="coerce"
        )
        if probabilities.isna().any() or (~probabilities.between(0.0, 1.0)).any():
            raise ValueError("availability_probability must be finite and in [0, 1]")
        result["availability_probability"] = probabilities
    if "availability_status" in result:
        statuses = (
            result["availability_status"].fillna("").astype(str).str.lower().str.strip()
        )
        if (~statuses.isin(_STATUS | {""})).any():
            raise ValueError("unknown availability_status")
        result["availability_status"] = statuses
    result["_key"] = _key(result)
    return result


def _validate_probability(value: object) -> float:
    try:
        if isinstance(value, (bool, np.bool_)):
            raise ValueError
        numeric = float(value)
    except (TypeError, ValueError):
        raise ValueError(
            "availability_probability must be finite and in [0, 1]"
        ) from None
    if not np.isfinite(numeric) or not 0.0 <= numeric <= 1.0:
        raise ValueError("availability_probability must be finite and in [0, 1]")
    return numeric


def _availability(row: pd.Series, override: float | None) -> float:
    status = str(row.get("availability_status", "") or "").lower().strip()
    raw = (
        override
        if override is not None
        else row.get("availability_probability", np.nan)
    )
    probability = (
        _validate_probability(raw) if override is not None or pd.notna(raw) else None
    )
    if status in {"inactive", "out"}:
        return 0.0
    if probability is not None:
        return probability
    if status in {"questionable", "doubtful", "limited"}:
        return 0.5
    return 1.0


def _mean(values: pd.Series, fallback: float) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    result = float(clean.mean()) if not clean.empty else fallback
    if not np.isfinite(result):
        raise ValueError("projection inputs produce a non-finite rate")
    return result


def _project_one(
    history: pd.DataFrame,
    target: pd.Series,
    *,
    config: RBComponentConfig,
    cutoff: float | None,
    availability_probability: float | None,
) -> dict[str, float | int | str]:
    rb_history = history.loc[history["rb_id"].eq(target["rb_id"])]
    target_key = float(target["_key"])
    boundary = target_key if cutoff is None else min(target_key, cutoff + 1e-6)
    target_week = int(target["week"])
    metadata_key = (
        target_key - 1 if target_week > 1 else (int(target["season"]) - 1) * 100 + 22
    )
    if cutoff is not None:
        metadata_key = min(metadata_key, cutoff)
    prior = (
        rb_history.loc[rb_history["_key"] < boundary]
        .sort_values("_key")
        .tail(config.window)
    )
    if len(prior) < config.min_history:
        prior = rb_history.iloc[0:0]
    rush = _mean(prior["rush_attempts"], config.fallback_rush_attempts)
    targets = _mean(prior["targets"], config.fallback_targets)
    if (
        pd.notna(target.get("team_rush_attempts"))
        and not prior.empty
        and "team_rush_attempts" in prior
    ):
        share = (
            prior["rush_attempts"] / prior["team_rush_attempts"].replace(0, np.nan)
        ).clip(0, 1)
        rush = float(target["team_rush_attempts"]) * _mean(share, 1.0)
    if (
        pd.notna(target.get("team_targets"))
        and not prior.empty
        and "team_targets" in prior
    ):
        share = (prior["targets"] / prior["team_targets"].replace(0, np.nan)).clip(0, 1)
        targets = float(target["team_targets"]) * _mean(share, 1.0)
    availability = _availability(target, availability_probability)
    rush = max(0.0, rush) * availability
    targets = max(0.0, targets) * availability
    if pd.notna(target.get("team_rush_attempts")):
        rush = min(rush, max(0.0, float(target["team_rush_attempts"])))
    if pd.notna(target.get("team_targets")):
        targets = min(targets, max(0.0, float(target["team_targets"])))
    ypa = max(
        0.0,
        _mean(
            prior["rushing_yards"] / prior["rush_attempts"].replace(0, np.nan),
            config.fallback_rushing_yards_per_attempt,
        ),
    )
    rec_rate = np.clip(
        _mean(
            prior["receptions"] / prior["targets"].replace(0, np.nan),
            config.fallback_reception_rate,
        ),
        0,
        1,
    )
    ypr = max(
        0.0,
        _mean(
            prior["receiving_yards"] / prior["receptions"].replace(0, np.nan),
            config.fallback_receiving_yards_per_reception,
        ),
    )
    rush_td = max(
        0.0,
        _mean(
            prior["rushing_tds"] / prior["rush_attempts"].replace(0, np.nan),
            config.fallback_rushing_td_rate,
        ),
    )
    rec_td = max(
        0.0,
        _mean(
            prior["receiving_tds"] / prior["receptions"].replace(0, np.nan),
            config.fallback_receiving_td_rate,
        ),
    )
    receptions = min(targets, targets * rec_rate)
    output = {
        "rb_id": target["rb_id"],
        "season": int(target["season"]),
        "week": int(target["week"]),
        "rush_attempts": rush,
        "targets": targets,
        "rushing_yards": rush * ypa,
        "receptions": receptions,
        "receiving_yards": receptions * ypr,
        "rushing_tds": rush * rush_td,
        "receiving_tds": receptions * rec_td,
        "availability_probability": availability,
        "as_of_season": int(metadata_key // 100),
        "as_of_week": int(metadata_key % 100),
    }
    if not all(
        np.isfinite(float(value))
        for key, value in output.items()
        if key not in {"rb_id"}
    ):
        raise ValueError("projection inputs produce non-finite output")
    return output


def project_rb_components(
    history: pd.DataFrame,
    targets: pd.DataFrame | None = None,
    *,
    as_of: tuple[int, int] | None = None,
    config: RBComponentConfig | None = None,
    availability_probability: float | None = None,
    availability_status: str | None = None,
) -> pd.DataFrame:
    """Project RB components from rows strictly before each target boundary."""
    cfg = config or RBComponentConfig()
    if not isinstance(history, pd.DataFrame):
        raise TypeError("history and targets must be pandas DataFrames")
    if availability_status is not None:
        status = str(availability_status).lower().strip()
        if status not in _STATUS:
            raise ValueError("unknown availability_status")
    observed = _normalise(history, require_components=not history.empty)
    target_frame = (
        _normalise(targets, require_components=False)
        if targets is not None
        else observed
    )
    as_of_key = None
    if as_of is not None:
        if len(as_of) != 2 or any(
            isinstance(v, (bool, np.bool_)) or int(v) != v for v in as_of
        ):
            raise ValueError("as_of must be a (season, week) tuple")
        season, week = int(as_of[0]), int(as_of[1])
        if season < 1920 or not 1 <= week <= 22:
            raise ValueError(
                "as_of must contain an integer season >= 1920 and week 1..22"
            )
        as_of_key = season * 100 + week
    rows = []
    for _, row in target_frame.iterrows():
        candidate = row.copy()
        if availability_status is not None:
            candidate["availability_status"] = availability_status
        rows.append(
            _project_one(
                observed,
                candidate,
                config=cfg,
                cutoff=as_of_key,
                availability_probability=availability_probability,
            )
        )
    return pd.DataFrame(
        rows,
        columns=[
            "rb_id",
            "season",
            "week",
            *RB_COMPONENTS,
            "availability_probability",
            "as_of_season",
            "as_of_week",
        ],
    )


def rolling_origin_compare(
    frame: pd.DataFrame, *, config: RBComponentConfig | None = None
) -> dict[str, float | int | bool]:
    """Compare trailing-mean rush-attempt MAE with an expanding mean baseline."""
    cfg = config or RBComponentConfig()
    observed = _normalise(frame, require_components=not frame.empty).sort_values(
        ["_key", "rb_id"]
    )
    errors: list[float] = []
    baseline_errors: list[float] = []
    for key in sorted(observed["_key"].unique()):
        train = observed.loc[observed["_key"] < key]
        test = observed.loc[observed["_key"].eq(key)]
        if len(train) < cfg.min_history:
            continue
        targets = test[["rb_id", "season", "week"]].copy()
        predictions = project_rb_components(train, targets, config=cfg)
        actual = test.reset_index(drop=True)
        errors.extend(
            (predictions["rush_attempts"] - actual["rush_attempts"]).abs().tolist()
        )
        baseline_errors.extend(
            (float(train["rush_attempts"].mean()) - actual["rush_attempts"])
            .abs()
            .tolist()
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
    "RB_COMPONENTS",
    "RBComponentConfig",
    "project_rb_components",
    "rolling_origin_compare",
]
