"""Outs-specific workload features for MLB pitcher props."""

from __future__ import annotations

import numpy as np
import pandas as pd

OUTS_FEATURE_COLUMNS: list[str] = [
    "batters_faced",
    "rolling_outs_recorded_5",
    "rolling_batters_faced_5",
    "rolling_outs_per_batter_faced_5",
    "prev_outs_recorded",
    "prev_batters_faced",
    "prev_pitch_count",
    "rolling_pitch_count_10",
    "season_avg_pitch_count_to_date",
    "career_avg_pitch_count",
]

OUTS_FEATURE_DEFAULTS: dict[str, float] = {
    "batters_faced": 24.0,
    "rolling_outs_recorded_5": 15.0,
    "rolling_batters_faced_5": 24.0,
    "rolling_outs_per_batter_faced_5": 0.75,
    "prev_outs_recorded": 15.0,
    "prev_batters_faced": 24.0,
    "prev_pitch_count": 85.0,
    "rolling_pitch_count_10": 85.0,
    "season_avg_pitch_count_to_date": 85.0,
    "career_avg_pitch_count": 85.0,
}


def _shifted_group_rolling_mean(
    frame: pd.DataFrame,
    *,
    column: str,
    window: int,
    default: float,
) -> pd.Series:
    """Return pitcher-level rolling means shifted to exclude the current game."""

    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype="float64")

    grouped = frame.groupby("pitcher")[column].rolling(window=window, min_periods=1)
    rolled = grouped.mean().groupby(level=0).shift(1).droplevel(0)
    return pd.to_numeric(rolled, errors="coerce").fillna(default)


def _shifted_group_rolling_sum(
    frame: pd.DataFrame,
    *,
    column: str,
    window: int,
) -> pd.Series:
    """Return pitcher-level rolling sums shifted to exclude the current game."""

    if column not in frame.columns:
        return pd.Series(np.nan, index=frame.index, dtype="float64")

    grouped = frame.groupby("pitcher")[column].rolling(window=window, min_periods=1)
    return grouped.sum().groupby(level=0).shift(1).droplevel(0)


def _shifted_group_previous_value(
    frame: pd.DataFrame,
    *,
    group_cols: list[str],
    column: str,
) -> pd.Series:
    """Return grouped prior-game values shifted to exclude the current game."""

    if column not in frame.columns:
        return pd.Series(np.nan, index=frame.index, dtype="float64")

    values = pd.to_numeric(frame[column], errors="coerce")
    grouped = values.groupby([frame[col] for col in group_cols], dropna=False)
    return grouped.shift(1)


def _shifted_group_running_mean(
    frame: pd.DataFrame,
    *,
    group_cols: list[str],
    column: str,
) -> pd.Series:
    """Return grouped running means shifted to exclude the current game."""

    if column not in frame.columns:
        return pd.Series(np.nan, index=frame.index, dtype="float64")

    values = pd.to_numeric(frame[column], errors="coerce")
    grouped = values.groupby([frame[col] for col in group_cols], dropna=False)
    shifted = grouped.shift(1)
    cumulative_sum = shifted.groupby(
        [frame[col] for col in group_cols], dropna=False
    ).cumsum()
    cumulative_count = shifted.notna().groupby(
        [frame[col] for col in group_cols], dropna=False
    ).cumsum()
    return (cumulative_sum / cumulative_count.replace(0, np.nan)).astype("float64")


def add_outs_workload_features(
    games: pd.DataFrame,
    *,
    default_batters_faced: float = 24.0,
    default_outs_per_batter_faced: float = 0.75,
    default_rolling_outs: float = 15.0,
    default_rolling_batters_faced: float = 24.0,
    default_rolling_rate: float = 0.75,
) -> pd.DataFrame:
    """Add outs-only workload features to pitcher-game rows.

    Args:
        games: Pitcher-game frame with `outs_recorded` and `batters_faced`.
        default_batters_faced: Neutral fallback for the historical workload proxy.
        default_outs_per_batter_faced: Neutral fallback for the historical rate proxy.
        default_rolling_outs: Neutral fallback for prior outs history.
        default_rolling_batters_faced: Neutral fallback for prior workload history.
        default_rolling_rate: Neutral fallback for the prior outs-per-batter rate.

    Returns:
        Frame with outs-specific workload features populated.
    """

    enriched = games.copy()
    if enriched.empty:
        for col in OUTS_FEATURE_COLUMNS:
            enriched[col] = pd.Series(dtype="float64")
        return enriched

    enriched["game_date"] = pd.to_datetime(enriched["game_date"], errors="coerce")
    sort_cols = ["pitcher", "game_date"]
    if "game_pk" in enriched.columns:
        sort_cols.append("game_pk")
    enriched = enriched.sort_values(sort_cols, kind="stable").reset_index(drop=True)

    if "batters_faced" not in enriched.columns:
        if "plate_appearances" in enriched.columns:
            enriched["batters_faced"] = pd.to_numeric(
                enriched["plate_appearances"], errors="coerce"
            )
        else:
            enriched["batters_faced"] = pd.Series(
                default_batters_faced, index=enriched.index, dtype="float64"
            )

    enriched["batters_faced"] = pd.to_numeric(
        enriched["batters_faced"], errors="coerce"
    ).fillna(default_batters_faced)
    outs_source = (
        enriched["outs_recorded"]
        if "outs_recorded" in enriched.columns
        else pd.Series(0.0, index=enriched.index, dtype="float64")
    )
    enriched["outs_recorded"] = pd.to_numeric(outs_source, errors="coerce").fillna(0.0)
    enriched["_batters_faced_source"] = enriched["batters_faced"]
    per_game_rate = (
        enriched["outs_recorded"]
        / enriched["_batters_faced_source"].replace(0.0, pd.NA)
    ).replace([np.inf, -np.inf], np.nan)
    enriched["_outs_per_batter_faced_source"] = per_game_rate

    # The exposed workload columns must be leakage-safe. They represent prior-game
    # rolling proxies, not current-game outcomes.
    enriched["batters_faced"] = _shifted_group_rolling_mean(
        enriched,
        column="_batters_faced_source",
        window=5,
        default=default_batters_faced,
    )
    enriched["rolling_outs_recorded_5"] = _shifted_group_rolling_mean(
        enriched,
        column="outs_recorded",
        window=5,
        default=default_rolling_outs,
    )
    enriched["rolling_batters_faced_5"] = _shifted_group_rolling_mean(
        enriched,
        column="_batters_faced_source",
        window=5,
        default=default_rolling_batters_faced,
    )

    outs_sum = _shifted_group_rolling_sum(
        enriched,
        column="outs_recorded",
        window=5,
    )
    bf_sum = _shifted_group_rolling_sum(
        enriched,
        column="_batters_faced_source",
        window=5,
    )
    rate = (outs_sum / bf_sum.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)
    enriched["rolling_outs_per_batter_faced_5"] = pd.to_numeric(
        rate, errors="coerce"
    ).fillna(default_rolling_rate)

    enriched["prev_outs_recorded"] = _shifted_group_previous_value(
        enriched,
        group_cols=["pitcher"],
        column="outs_recorded",
    ).fillna(15.0)
    enriched["prev_batters_faced"] = _shifted_group_previous_value(
        enriched,
        group_cols=["pitcher"],
        column="_batters_faced_source",
    ).fillna(default_batters_faced)
    enriched["prev_pitch_count"] = _shifted_group_previous_value(
        enriched,
        group_cols=["pitcher"],
        column="pitch_count",
    ).fillna(85.0)
    enriched["rolling_pitch_count_10"] = _shifted_group_rolling_mean(
        enriched,
        column="pitch_count",
        window=10,
        default=85.0,
    )
    if "season" not in enriched.columns:
        enriched["season"] = enriched["game_date"].dt.year
    enriched["season_avg_pitch_count_to_date"] = _shifted_group_running_mean(
        enriched,
        group_cols=["pitcher", "season"],
        column="pitch_count",
    ).fillna(85.0)
    enriched["career_avg_pitch_count"] = _shifted_group_running_mean(
        enriched,
        group_cols=["pitcher"],
        column="pitch_count",
    ).fillna(85.0)

    enriched = enriched.drop(
        columns=["_batters_faced_source", "_outs_per_batter_faced_source"]
    )

    return enriched


def ensure_outs_feature_defaults(frame: pd.DataFrame) -> pd.DataFrame:
    """Guarantee outs workload columns exist with neutral defaults."""

    enriched = frame.copy()
    for column in OUTS_FEATURE_COLUMNS:
        if column not in enriched.columns:
            enriched[column] = OUTS_FEATURE_DEFAULTS[column]
        enriched[column] = pd.to_numeric(
            enriched[column], errors="coerce"
        ).fillna(OUTS_FEATURE_DEFAULTS[column])
    return enriched
