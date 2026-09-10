"""Exact NFL fantasy scoring and a leakage-safe direct-FP benchmark.

Component scoring is deliberately a pure accounting transform.  The direct
benchmark is a separate trailing historical fantasy-point mean and is never
promoted by this module without an explicit, multi-metric external review.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd


class UnknownScoringError(ValueError):
    """Raised when a scoring ruleset is not explicitly supported."""


@dataclass(frozen=True, slots=True)
class FantasyScoringConfig:
    """Standard NFL scoring coefficients, defaulting to half-PPR."""

    ruleset: str = "half_ppr"
    reception: float | None = None
    receiving_yard: float = 0.1
    receiving_touchdown: float = 6.0
    rushing_yard: float = 0.1
    rushing_touchdown: float = 6.0
    passing_yard: float = 0.04
    passing_touchdown: float = 4.0
    interception: float = -1.0
    fumble_lost: float = -2.0
    two_point_conversion: float = 2.0

    def __post_init__(self) -> None:
        ruleset = self.ruleset.strip().lower() if isinstance(self.ruleset, str) else ""
        if ruleset == "ppr":
            ruleset = "full_ppr"
        if ruleset not in {"half_ppr", "full_ppr"}:
            raise UnknownScoringError(f"Unsupported scoring ruleset: {self.ruleset!r}")
        object.__setattr__(self, "ruleset", ruleset)
        reception = (
            (0.5 if ruleset == "half_ppr" else 1.0)
            if self.reception is None
            else self.reception
        )
        values = (
            reception,
            self.receiving_yard,
            self.receiving_touchdown,
            self.rushing_yard,
            self.rushing_touchdown,
            self.passing_yard,
            self.passing_touchdown,
            self.interception,
            self.fumble_lost,
            self.two_point_conversion,
        )
        try:
            invalid = any(
                isinstance(v, (bool, np.bool_)) or not np.isfinite(float(v))
                for v in values
            )
        except (TypeError, ValueError):
            invalid = True
        if invalid:
            raise ValueError("scoring coefficients must be finite non-booleans")
        numeric_values = tuple(float(value) for value in values)
        reception = numeric_values[0]
        if (
            reception < 0
            or any(value < 0 for value in numeric_values[1:7])
            or numeric_values[-1] < 0
        ):
            raise ValueError("positive scoring coefficients must be nonnegative")
        object.__setattr__(self, "reception", reception)
        for name in (
            "receiving_yard",
            "receiving_touchdown",
            "rushing_yard",
            "rushing_touchdown",
            "passing_yard",
            "passing_touchdown",
            "interception",
            "fumble_lost",
            "two_point_conversion",
        ):
            object.__setattr__(self, name, float(getattr(self, name)))


_NONNEGATIVE: Final[tuple[str, ...]] = (
    "pass_attempts",
    "completions",
    "passing_yards",
    "passing_tds",
    "interceptions",
    "rushing_attempts",
    "rushing_yards",
    "rushing_tds",
    "rush_attempts",
    "rush_yards",
    "targets",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    "receiving_touchdowns",
    "rare_rush_attempts",
    "rare_rush_yards",
    "rare_rush_tds",
    "fumbles_lost",
    "two_point_conversions",
    "two_point_conversion",
)
_ALIASES: Final[dict[str, str]] = {
    "receiving_touchdowns": "receiving_tds",
    "rush_attempts": "rushing_attempts",
    "rush_yards": "rushing_yards",
    "rush_touchdowns": "rushing_tds",
    "two_point_conversion": "two_point_conversions",
}


def _resolve_scoring(
    scoring: str | FantasyScoringConfig | Mapping[str, object] | None,
) -> FantasyScoringConfig:
    if scoring is None:
        return FantasyScoringConfig()
    if isinstance(scoring, FantasyScoringConfig):
        return scoring
    if hasattr(scoring, "ruleset") and hasattr(scoring, "reception"):
        return FantasyScoringConfig(
            ruleset=str(scoring.ruleset),
            reception=float(scoring.reception),
            receiving_yard=float(scoring.receiving_yard),
            receiving_touchdown=float(scoring.receiving_touchdown),
            rushing_yard=float(scoring.rushing_yard),
            rushing_touchdown=float(scoring.rushing_touchdown),
            passing_yard=float(scoring.passing_yard),
            passing_touchdown=float(scoring.passing_touchdown),
            interception=float(scoring.interception),
            fumble_lost=float(scoring.fumble_lost),
            two_point_conversion=float(scoring.two_point_conversion),
        )
    if isinstance(scoring, str):
        return FantasyScoringConfig(ruleset=scoring)
    if isinstance(scoring, Mapping):
        raw = dict(scoring)
        if "ruleset" not in raw:
            raw["ruleset"] = "half_ppr"
        return FantasyScoringConfig(**raw)
    raise UnknownScoringError(f"Unsupported scoring specification: {scoring!r}")


def _value(frame: pd.DataFrame, name: str) -> pd.Series:
    sources = [name, *[a for a, canonical in _ALIASES.items() if canonical == name]]
    present = [source for source in sources if source in frame]
    if len(present) > 1:
        parsed = [_value_single(frame, source) for source in present]
        if any(not parsed[0].eq(other).all() for other in parsed[1:]):
            raise ValueError(f"conflicting aliases for {name}")
    source = present[0] if present else None
    if source is None:
        return pd.Series(0.0, index=frame.index)
    return _value_single(frame, source)


def _value_single(frame: pd.DataFrame, source: str) -> pd.Series:
    raw = frame[source]
    if raw.map(lambda x: isinstance(x, (bool, np.bool_))).any():
        raise ValueError(f"{source} must not contain booleans")
    values = pd.to_numeric(raw, errors="coerce")
    if values.isna().any() or (~np.isfinite(values)).any() or (values < 0).any():
        raise ValueError(f"{source} must contain finite nonnegative values")
    return values.astype(float)


def _direct_points(frame: pd.DataFrame) -> pd.Series:
    raw = frame["fantasy_points"]
    if raw.map(lambda x: isinstance(x, (bool, np.bool_))).any():
        raise ValueError("fantasy_points must not contain booleans")
    values = pd.to_numeric(raw, errors="coerce")
    if values.isna().any() or (~np.isfinite(values)).any():
        raise ValueError("fantasy_points must contain finite numeric values")
    return values.astype(float)


def _validate_calendar(frame: pd.DataFrame, *, name: str) -> pd.DataFrame:
    missing = sorted({"player_id", "season", "week"} - set(frame.columns))
    if missing:
        raise KeyError(f"{name} missing required columns: {missing}")
    for column, lower, upper in (("season", 1920, 9999), ("week", 1, 22)):
        if frame[column].map(lambda value: isinstance(value, (bool, np.bool_))).any():
            raise ValueError(f"{name}.{column} must not contain booleans")
        values = pd.to_numeric(frame[column], errors="coerce")
        if (
            values.isna().any()
            or (~np.isfinite(values)).any()
            or (values % 1 != 0).any()
            or (values < lower).any()
            or (values > upper).any()
        ):
            raise ValueError(f"{name}.{column} must be a finite integer in range")
    normalized = frame.copy()
    normalized["season"] = pd.to_numeric(normalized["season"]).astype(int)
    normalized["week"] = pd.to_numeric(normalized["week"]).astype(int)
    if normalized.duplicated(["player_id", "season", "week"]).any():
        raise ValueError(f"{name} has duplicate player/calendar rows")
    return normalized


def _validate_frame(frame: pd.DataFrame) -> None:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("components must be a pandas DataFrame")
    if frame.empty:
        return
    if "position" in frame:
        positions = frame["position"].astype(str).str.upper()
        if (~positions.isin({"QB", "RB", "WR", "TE"})).any():
            raise ValueError("position must be one of QB, RB, WR, TE")
    rare_fields = {
        "rare_rush_attempts",
        "rare_rush_yards",
        "rare_rush_tds",
    }
    supplied_rare = rare_fields.intersection(frame.columns)
    if supplied_rare and supplied_rare != rare_fields:
        raise ValueError("rare rushing fields must be supplied as a complete stream")
    for name in _NONNEGATIVE:
        if name in frame:
            _value(frame, name)


def derive_fantasy_points(
    components: pd.DataFrame,
    scoring: str | FantasyScoringConfig | Mapping[str, object] | None = None,
) -> pd.DataFrame:
    """Derive exact fantasy points from projected or observed components.

    Missing optional categories are zero.  Position is metadata only; each
    supplied category is counted once, with rare receiver rushing fields used
    as rushing components when regular rushing fields are absent.
    """
    _validate_frame(components)
    cfg = _resolve_scoring(scoring)
    result = components.copy()
    attempts = _value(result, "pass_attempts")
    completions = _value(result, "completions")
    rush_attempts = _value(result, "rushing_attempts")
    rush_yards = _value(result, "rushing_yards")
    rush_tds = _value(result, "rushing_tds")
    if "rare_rush_attempts" in result:
        # Rare receiver rushing is a distinct opportunity stream.  Add it to
        # regular rushing when both are supplied; zero regular fields are not
        # allowed to erase the rare-rush projection.
        rare_attempts = _value(result, "rare_rush_attempts")
        rare_yards = _value(result, "rare_rush_yards")
        rare_tds = _value(result, "rare_rush_tds")
        if (rush_tds > rush_attempts).any():
            raise ValueError("canonical rushing touchdowns exceed attempts")
        if (rare_tds > rare_attempts).any():
            raise ValueError("rare rushing touchdowns exceed attempts")
        rush_attempts = rush_attempts + rare_attempts
        rush_yards = rush_yards + rare_yards
        rush_tds = rush_tds + rare_tds
    targets, receptions = _value(result, "targets"), _value(result, "receptions")
    receiving_yards, receiving_tds = (
        _value(result, "receiving_yards"),
        _value(result, "receiving_tds"),
    )
    passing_tds, interceptions = (
        _value(result, "passing_tds"),
        _value(result, "interceptions"),
    )
    passing_yards = _value(result, "passing_yards")
    fumbles, conversions = (
        _value(result, "fumbles_lost"),
        _value(result, "two_point_conversions"),
    )
    if (completions + interceptions > attempts).any() or (receptions > targets).any():
        raise ValueError("completions plus interceptions cannot exceed attempts")
    if (
        (passing_tds > attempts).any()
        or (interceptions > attempts).any()
        or (rush_tds > rush_attempts).any()
        or (receiving_tds > receptions).any()
        or (passing_tds > completions).any()
    ):
        raise ValueError("touchdowns and interceptions exceed available opportunities")
    if (fumbles > attempts + rush_attempts + receptions).any():
        raise ValueError("fumbles lost exceed available player opportunities")
    result["passing_points"] = (
        passing_yards * cfg.passing_yard + passing_tds * cfg.passing_touchdown
    )
    result["rushing_points"] = (
        rush_yards * cfg.rushing_yard + rush_tds * cfg.rushing_touchdown
    )
    result["receiving_points"] = (
        receiving_yards * cfg.receiving_yard
        + receiving_tds * cfg.receiving_touchdown
        + receptions * cfg.reception
    )
    result["turnover_points"] = (
        fumbles * cfg.fumble_lost + interceptions * cfg.interception
    )
    result["bonus_points"] = conversions * cfg.two_point_conversion
    result["fantasy_points"] = (
        result["passing_points"]
        + result["rushing_points"]
        + result["receiving_points"]
        + result["turnover_points"]
        + result["bonus_points"]
    )
    result["scoring_ruleset"] = cfg.ruleset
    if (~np.isfinite(result["fantasy_points"])).any():
        raise ValueError("fantasy points are non-finite")
    return result


def direct_fantasy_points_benchmark(
    history: pd.DataFrame,
    targets: pd.DataFrame | None = None,
    scoring: str | FantasyScoringConfig | Mapping[str, object] | None = None,
    *,
    window: int = 6,
) -> pd.DataFrame:
    """Return a distinct trailing direct-FP benchmark, never component-derived."""
    if (
        isinstance(window, (bool, np.bool_))
        or not isinstance(window, int)
        or window < 1
    ):
        raise ValueError("window must be a positive integer")
    _resolve_scoring(scoring)
    history = _validate_calendar(history, name="history")
    if "fantasy_points" in history:
        observed = history.copy()
        observed["fantasy_points"] = _direct_points(observed)
    else:
        observed = derive_fantasy_points(history, scoring)
    observed = observed.assign(
        _key=pd.to_numeric(observed["season"]) * 100 + pd.to_numeric(observed["week"])
    ).sort_values(["_key", "player_id"])
    target_frame = observed if targets is None else targets.copy()
    if targets is not None:
        target_frame = _validate_calendar(target_frame, name="targets")

    def key(frame: pd.DataFrame) -> pd.Series:
        return pd.to_numeric(frame["season"], errors="coerce") * 100 + pd.to_numeric(
            frame["week"], errors="coerce"
        )

    rows = []
    for _, target in target_frame.iterrows():
        prior = observed.loc[
            observed.get("player_id", pd.Series(index=observed.index)).eq(
                target["player_id"]
            )
        ]
        if "season" in target and "week" in target:
            cutoff = int(target["season"]) * 100 + int(target["week"])
            prior = prior.loc[key(prior) < cutoff]
        values = prior["fantasy_points"].tail(window)
        fallback_used = values.empty
        fallback_reason = "no_player_history" if fallback_used else "none"
        rows.append(
            {
                "player_id": target["player_id"],
                "season": target.get("season"),
                "week": target.get("week"),
                "fantasy_points": float(values.mean()) if not values.empty else 0.0,
                "direct_fantasy_points": float(values.mean())
                if not values.empty
                else 0.0,
                "benchmark_kind": "direct_fantasy_points",
                "fallback_used": fallback_used,
                "fallback_reason": fallback_reason,
            }
        )
    return pd.DataFrame(rows)


def rolling_origin_compare(
    frame: pd.DataFrame,
    scoring: str | FantasyScoringConfig | Mapping[str, object] | None = None,
    *,
    window: int = 6,
) -> dict[str, float | int | bool]:
    """Compare direct trailing FP MAE with a strict-fold historical baseline.

    Promotion remains false: R3.8 must perform multi-metric, multiple-comparison
    controlled selection in nested folds.
    """
    if not {"player_id", "season", "week"}.issubset(frame.columns):
        raise KeyError("frame requires player_id, season, and week")
    _resolve_scoring(scoring)
    frame = _validate_calendar(frame, name="frame")
    if "fantasy_points" in frame:
        observed = frame.copy()
        observed["fantasy_points"] = _direct_points(observed)
    else:
        observed = derive_fantasy_points(frame, scoring)
    observed = observed.assign(
        _key=pd.to_numeric(observed["season"]) * 100 + pd.to_numeric(observed["week"])
    ).sort_values(["_key", "player_id"])
    errors, baseline = [], []
    fallback_rows = 0
    for fold in sorted(observed["_key"].unique()):
        train = observed.loc[observed["_key"] < fold]
        test = observed.loc[observed["_key"] == fold]
        if train.empty:
            continue
        for _, row in test.iterrows():
            prior = train.loc[
                train["player_id"].eq(row["player_id"]), "fantasy_points"
            ].tail(window)
            prediction = (
                float(prior.mean())
                if not prior.empty
                else float(train["fantasy_points"].mean())
            )
            if prior.empty:
                fallback_rows += 1
            errors.append(abs(prediction - float(row["fantasy_points"])))
            baseline.append(
                abs(
                    float(train["fantasy_points"].mean()) - float(row["fantasy_points"])
                )
            )
    model_mae = float(np.mean(errors)) if errors else float("nan")
    baseline_mae = float(np.mean(baseline)) if baseline else float("nan")
    return {
        "model_mae": model_mae,
        "baseline_mae": baseline_mae,
        "folds": len(errors),
        "promoted": False,
        "fallback_rows": fallback_rows,
        "fallback_policy": "global_history_mean_for_unseen_player",
    }


score_fantasy_points = derive_fantasy_points

__all__ = [
    "FantasyScoringConfig",
    "UnknownScoringError",
    "derive_fantasy_points",
    "score_fantasy_points",
    "direct_fantasy_points_benchmark",
    "rolling_origin_compare",
]
