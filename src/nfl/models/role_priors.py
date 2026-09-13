"""Leakage-safe, conservative NFL rookie and new-role priors.

This module deliberately uses only supplied context and observations before a
target calendar boundary.  It is a V1 prior, not a player-transition model.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd

POSITIONS: Final[tuple[str, ...]] = ("QB", "RB", "WR", "TE")
_KEYS: Final[tuple[str, ...]] = ("player_id", "position", "season", "week")
_COMPONENTS: Final[dict[str, tuple[str, ...]]] = {
    "QB": (
        "pass_attempts",
        "completions",
        "passing_yards",
        "passing_tds",
        "interceptions",
        "rushing_attempts",
        "rushing_yards",
        "rushing_tds",
    ),
    "RB": (
        "rush_attempts",
        "targets",
        "rushing_yards",
        "receptions",
        "receiving_yards",
        "rushing_tds",
        "receiving_tds",
    ),
    "WR": (
        "routes",
        "targets",
        "receptions",
        "receiving_yards",
        "receiving_tds",
        "rare_rush_attempts",
        "rare_rush_yards",
        "rare_rush_tds",
    ),
    "TE": (
        "routes",
        "targets",
        "receptions",
        "receiving_yards",
        "receiving_tds",
        "rare_rush_attempts",
        "rare_rush_yards",
        "rare_rush_tds",
    ),
}
ROLE_COMPONENTS: Final[tuple[str, ...]] = tuple(
    dict.fromkeys(c for values in _COMPONENTS.values() for c in values)
)
_POSITION_REPLACEMENT: Final[dict[str, dict[str, float]]] = {
    "QB": {
        "pass_attempts": 28.0,
        "completions": 17.0,
        "passing_yards": 185.0,
        "passing_tds": 1.1,
        "interceptions": 0.8,
        "rushing_attempts": 2.0,
        "rushing_yards": 8.0,
        "rushing_tds": 0.05,
    },
    "RB": {
        "rush_attempts": 8.0,
        "targets": 2.5,
        "rushing_yards": 32.0,
        "receptions": 1.7,
        "receiving_yards": 13.0,
        "rushing_tds": 0.15,
        "receiving_tds": 0.02,
    },
    "WR": {
        "routes": 25.0,
        "targets": 3.5,
        "receptions": 2.2,
        "receiving_yards": 27.0,
        "receiving_tds": 0.12,
        "rare_rush_attempts": 0.1,
        "rare_rush_yards": 0.5,
        "rare_rush_tds": 0.005,
    },
    "TE": {
        "routes": 22.0,
        "targets": 3.0,
        "receptions": 1.9,
        "receiving_yards": 22.0,
        "receiving_tds": 0.12,
        "rare_rush_attempts": 0.05,
        "rare_rush_yards": 0.25,
        "rare_rush_tds": 0.002,
    },
}


@dataclass(frozen=True, slots=True)
class RolePriorConfig:
    """Controls history eligibility, shrinkage, and uncertainty widening."""

    min_history: int = 2
    window: int = 8
    rookie_uncertainty_multiplier: float = 1.75
    new_role_uncertainty_multiplier: float = 1.45
    established_uncertainty_multiplier: float = 1.0
    external_weight: float = 0.25

    def __post_init__(self) -> None:
        for name in ("min_history", "window"):
            value = getattr(self, name)
            if isinstance(value, (bool, np.bool_)):
                raise ValueError(f"{name} must be a positive integer")
            try:
                number = float(value)
            except (TypeError, ValueError):
                raise ValueError(f"{name} must be a positive integer") from None
            if not np.isfinite(number) or not number.is_integer() or number < 1:
                raise ValueError(f"{name} must be a positive integer")
            object.__setattr__(self, name, int(number))
        if self.window < self.min_history:
            raise ValueError("window must be >= min_history")
        for name in (
            "rookie_uncertainty_multiplier",
            "new_role_uncertainty_multiplier",
            "established_uncertainty_multiplier",
            "external_weight",
        ):
            value = getattr(self, name)
            if isinstance(value, (bool, np.bool_)):
                raise ValueError(f"{name} must be finite and in range")
            try:
                number = float(value)
            except (TypeError, ValueError):
                number = float("nan")
            upper = 1.0 if name == "external_weight" else 10.0
            if not np.isfinite(number) or number < 0 or number > upper:
                raise ValueError(f"{name} must be finite and in range")
            if name == "rookie_uncertainty_multiplier" and number < 1:
                raise ValueError("rookie uncertainty must be >= 1")
            if name == "new_role_uncertainty_multiplier" and number < 1:
                raise ValueError("new-role uncertainty must be >= 1")
            object.__setattr__(self, name, number)
        if (
            self.rookie_uncertainty_multiplier
            <= self.established_uncertainty_multiplier
        ):
            raise ValueError("rookie uncertainty must exceed established uncertainty")
        if (
            self.new_role_uncertainty_multiplier
            <= self.established_uncertainty_multiplier
        ):
            raise ValueError("new-role uncertainty must exceed established uncertainty")


def _calendar(frame: pd.DataFrame, *, require_components: bool) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("history and targets must be pandas DataFrames")
    missing = sorted(set(_KEYS) - set(frame.columns))
    if missing:
        raise ValueError(f"missing required columns: {', '.join(missing)}")
    result = frame.copy()
    if result["player_id"].isna().any() or result["position"].isna().any():
        raise ValueError("player_id and position are required")
    result["position"] = result["position"].astype(str).str.upper().str.strip()
    if (~result["position"].isin(POSITIONS)).any():
        raise ValueError("position must be QB, RB, WR, or TE")
    for name in ("season", "week"):
        if result[name].map(lambda value: isinstance(value, (bool, np.bool_))).any():
            raise ValueError(f"{name} must not contain booleans")
    season = pd.to_numeric(result["season"], errors="coerce")
    week = pd.to_numeric(result["week"], errors="coerce")
    if (
        season.isna().any()
        or week.isna().any()
        or (season % 1 != 0).any()
        or (week % 1 != 0).any()
        or (season < 1920).any()
        or (season > 3000).any()
        or (week < 1).any()
        or (week > 22).any()
    ):
        raise ValueError(
            "season must be integer >= 1920 and week must be integer 1..22"
        )
    result["season"], result["week"] = season.astype(int), week.astype(int)
    if result.duplicated(list(_KEYS)).any():
        raise ValueError("duplicate semantic player/calendar rows are not allowed")
    for component in ROLE_COMPONENTS:
        if component not in result:
            result[component] = np.nan
        if (
            result[component]
            .map(lambda value: isinstance(value, (bool, np.bool_)))
            .any()
        ):
            raise ValueError(f"{component} must not contain booleans")
        values = pd.to_numeric(result[component], errors="coerce")
        required_for_rows = result["position"].isin(
            [
                position
                for position, components in _COMPONENTS.items()
                if component in components
            ]
        )
        if component in frame and values.loc[required_for_rows].isna().any():
            raise ValueError(f"{component} must contain numeric values")
        if require_components and values.loc[required_for_rows].isna().any():
            raise ValueError(f"{component} must contain numeric values")
        if np.isinf(values).any() or (values.dropna() < 0).any():
            raise ValueError(f"{component} must be finite and nonnegative")
        result[component] = values
    for name in (
        "draft_year",
        "draft_round",
        "draft_capital",
        "ud_projection",
        "consensus_projection",
    ):
        if name in result:
            if (
                result[name]
                .map(lambda value: isinstance(value, (bool, np.bool_)))
                .any()
            ):
                raise ValueError(f"{name} must not contain booleans")
            values = pd.to_numeric(result[name], errors="coerce")
            if values.isna().any():
                raise ValueError(f"{name} must contain numeric values")
            if name in {
                "draft_year",
                "draft_round",
                "draft_capital",
                "ud_projection",
                "consensus_projection",
            } and (np.isinf(values).any() or (values.dropna() < 0).any()):
                raise ValueError(f"{name} must be finite and nonnegative")
            result[name] = values
    if "draft_year" in result and (
        (result["draft_year"] % 1 != 0).any() or (result["draft_year"] < 1920).any()
    ):
        raise ValueError("draft_year must be an integer >= 1920")
    if "draft_round" in result and (
        (result["draft_round"] % 1 != 0).any()
        or (result["draft_round"] < 1).any()
        or (result["draft_round"] > 7).any()
    ):
        raise ValueError("draft_round must be an integer in 1..7")
    if "depth_chart_role" in result:
        if (
            result["depth_chart_role"]
            .map(lambda value: isinstance(value, (bool, np.bool_)))
            .any()
        ):
            raise ValueError("depth_chart_role must be a known role")
        roles = result["depth_chart_role"].astype(str).str.lower().str.strip()
        allowed_roles = {
            "starter",
            "start",
            "first_team",
            "first-team",
            "backup",
            "second_team",
            "second-team",
            "reserve",
        }
        if (
            result["depth_chart_role"].isna().any()
            or (~roles.isin(allowed_roles)).any()
        ):
            raise ValueError("depth_chart_role must be a known role")
        result["depth_chart_role"] = roles
    result["_key"] = result["season"] * 100 + result["week"]
    return result


def _cutoff_key(as_of: tuple[int, int] | None) -> int | None:
    if as_of is None:
        return None
    if not isinstance(as_of, tuple) or len(as_of) != 2:
        raise ValueError("as_of must be a (season, week) tuple")
    checked = _calendar(
        pd.DataFrame(
            [{"player_id": "_", "position": "RB", "season": as_of[0], "week": as_of[1]}]
        ),
        require_components=False,
    )
    return int(checked.iloc[0]["_key"])


def _context(row: pd.Series, *, has_history: bool) -> tuple[str, str, float]:
    explicit_rookie = row.get("is_rookie", row.get("rookie", False))
    explicit_new = row.get(
        "is_new_role", row.get("new_role", row.get("role_change", False))
    )
    context = str(row.get("role_context", "") or "").strip().lower()
    if context and context not in {
        "rookie",
        "new",
        "new_role",
        "promoted",
        "expanded",
        "established",
    }:
        raise ValueError("unknown role_context")
    if context in {"rookie", "new", "new_role", "promoted", "expanded"}:
        explicit_new = context != "rookie"
        explicit_rookie = context == "rookie"
    for name, value in (("rookie", explicit_rookie), ("new_role", explicit_new)):
        if not isinstance(value, (bool, np.bool_)):
            raise ValueError(f"{name} context must be boolean")
    if bool(explicit_rookie):
        return "rookie", "supplied_rookie_context", 1.75
    if bool(explicit_new):
        return "new_role", "supplied_new_role_context", 1.45
    if not has_history:
        # Lack of history is not evidence that a player is a rookie.  Keep a
        # separate label so future observations cannot silently classify him.
        return "no_history", "no_prior_player_history", 1.75
    return "established", "player_history", 1.0


def _external(row: pd.Series) -> float | None:
    values = []
    for name in ("ud_projection", "consensus_projection"):
        if pd.notna(row.get(name)):
            value = float(row[name])
            if not np.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and nonnegative")
            values.append(value)
    return float(np.mean(values)) if values else None


def build_role_priors(
    history: pd.DataFrame,
    targets: pd.DataFrame,
    *,
    as_of: tuple[int, int] | None = None,
    config: RolePriorConfig | None = None,
) -> pd.DataFrame:
    """Emit conservative component priors for target players.

    History is eligible only when its calendar key is strictly earlier than
    both the target boundary and an optional explicit ``as_of`` cutoff.
    """
    cfg = config or RolePriorConfig()
    observed = _calendar(history, require_components=not history.empty)
    target_frame = _calendar(targets, require_components=False)
    cutoff = _cutoff_key(as_of)
    rows: list[dict[str, object]] = []
    for _, target in target_frame.sort_values(list(_KEYS)).iterrows():
        boundary = int(target["_key"])
        if cutoff is not None:
            boundary = min(boundary, cutoff)
        eligible = (
            observed.loc[
                (observed["player_id"] == target["player_id"])
                & (observed["position"] == target["position"])
                & (observed["_key"] < boundary)
            ]
            .sort_values("_key")
            .tail(cfg.window)
        )
        eligible_count = len(eligible)
        has_history = eligible_count >= cfg.min_history
        prior_rows = eligible if has_history else observed.iloc[0:0]
        classification, source, multiplier = _context(target, has_history=has_history)
        if (
            eligible_count
            and not has_history
            and classification
            in {
                "established",
                "no_history",
            }
        ):
            classification, source, multiplier = (
                "limited_history",
                "limited_player_history",
                cfg.new_role_uncertainty_multiplier,
            )
        multiplier = {
            "rookie": cfg.rookie_uncertainty_multiplier,
            "no_history": cfg.rookie_uncertainty_multiplier,
            "limited_history": cfg.new_role_uncertainty_multiplier,
            "new_role": cfg.new_role_uncertainty_multiplier,
            "established": cfg.established_uncertainty_multiplier,
        }[classification]
        result: dict[str, object] = {
            "player_id": target["player_id"],
            "position": target["position"],
            "season": int(target["season"]),
            "week": int(target["week"]),
            "classification": classification,
            "prior_source": source,
            "uncertainty_multiplier": multiplier,
            "history_count": eligible_count,
            "as_of_season": int(boundary // 100),
            "as_of_week": int(boundary % 100),
        }
        external = _external(target)
        role = str(target.get("depth_chart_role", "") or "").strip().lower()
        context_adjustment = 1.0
        if role in {"starter", "start", "first_team", "first-team"}:
            context_adjustment = 1.08
        elif role in {"backup", "second_team", "second-team", "reserve"}:
            context_adjustment = 0.92
        result["context_adjustment"] = context_adjustment
        for component in ROLE_COMPONENTS:
            if component not in _COMPONENTS[target["position"]]:
                result[f"prior_{component}"] = 0.0
                continue
            replacement = _POSITION_REPLACEMENT[target["position"]][component]
            history_value = (
                float(prior_rows[component].mean())
                if not prior_rows.empty
                else replacement
            )
            if not np.isfinite(history_value) or history_value < 0:
                raise ValueError("prior history must produce finite nonnegative values")
            # UD/consensus is an optional scale prior, never a raw stat source.
            if external is not None:
                scale = np.clip(external / 100.0, 0.5, 1.5)
                history_value = (
                    1.0 - cfg.external_weight
                ) * history_value + cfg.external_weight * replacement * scale
            if classification in {"rookie", "no_history", "new_role"}:
                history_value = 0.75 * history_value + 0.25 * replacement
                history_value *= context_adjustment
            result[f"prior_{component}"] = max(0.0, history_value)
            result[f"uncertainty_{component}"] = multiplier
        rows.append(result)
    columns = (
        [
            "player_id",
            "position",
            "season",
            "week",
            "classification",
            "prior_source",
            "uncertainty_multiplier",
            "history_count",
            "context_adjustment",
            "as_of_season",
            "as_of_week",
        ]
        + [f"prior_{c}" for c in ROLE_COMPONENTS]
        + [f"uncertainty_{c}" for c in ROLE_COMPONENTS]
    )
    return pd.DataFrame(rows, columns=columns)


def classify_player_roles(
    history: pd.DataFrame,
    targets: pd.DataFrame,
    *,
    as_of: tuple[int, int] | None = None,
    config: RolePriorConfig | None = None,
) -> pd.DataFrame:
    """Return only the explicit rookie/new-role classification metadata."""
    result = build_role_priors(history, targets, as_of=as_of, config=config)
    return result[
        [
            "player_id",
            "position",
            "season",
            "week",
            "classification",
            "prior_source",
            "uncertainty_multiplier",
            "history_count",
            "context_adjustment",
        ]
    ].copy()


def rolling_origin_compare(
    frame: pd.DataFrame, *, config: RolePriorConfig | None = None
) -> dict[str, float | int | bool]:
    """Compare the prior's primary opportunity component to replacement mean."""
    cfg = config or RolePriorConfig()
    observed = _calendar(frame, require_components=True).sort_values(
        ["_key", "player_id"]
    )
    errors: list[float] = []
    baseline_errors: list[float] = []
    for key in sorted(observed["_key"].unique()):
        train = observed.loc[observed["_key"] < key]
        test = observed.loc[observed["_key"] == key]
        if len(train) < cfg.min_history:
            continue
        targets = test[["player_id", "position", "season", "week"]].copy()
        predictions = build_role_priors(train, targets, config=cfg)
        actual = test.reset_index(drop=True)
        for index, row in actual.iterrows():
            position = str(row["position"])
            metric = (
                "pass_attempts"
                if position == "QB"
                else ("rush_attempts" if position == "RB" else "targets")
            )
            value = float(row[metric])
            errors.append(
                abs(float(predictions.iloc[index][f"prior_{metric}"]) - value)
            )
            # Declared replacement baseline is position-specific and fixed;
            # it cannot absorb any target-fold facts.
            baseline_errors.append(abs(_POSITION_REPLACEMENT[position][metric] - value))
    model_mae = float(np.mean(errors)) if errors else float("nan")
    baseline_mae = float(np.mean(baseline_errors)) if baseline_errors else float("nan")
    return {
        "model_mae": model_mae,
        "baseline_mae": baseline_mae,
        "promoted": bool(errors) and model_mae <= baseline_mae,
        "folds": len(errors),
    }


project_role_priors = build_role_priors
evaluate_rolling_origin = rolling_origin_compare

__all__ = [
    "POSITIONS",
    "ROLE_COMPONENTS",
    "RolePriorConfig",
    "build_role_priors",
    "project_role_priors",
    "classify_player_roles",
    "rolling_origin_compare",
    "evaluate_rolling_origin",
]
