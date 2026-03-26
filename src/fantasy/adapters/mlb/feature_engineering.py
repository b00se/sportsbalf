"""Phase 1.5 leakage-safe feature engineering for MLB batter projections."""

from __future__ import annotations

import numpy as np
import pandas as pd

ROLLING_WINDOWS: tuple[int, ...] = (7, 14, 30)
ROLL_BASE_COLUMNS: tuple[str, ...] = (
    "plate_appearances",
    "hits",
    "total_bases",
    "walks",
    "strikeouts",
    "hard_hit_events",
    "pa_vs_lhp",
    "pa_vs_rhp",
)


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    num = pd.to_numeric(numerator, errors="coerce").fillna(0.0)
    den = pd.to_numeric(denominator, errors="coerce").fillna(0.0)
    with np.errstate(divide="ignore", invalid="ignore"):
        values = np.where(den > 0.0, num / den, 0.0)
    return pd.Series(values, index=numerator.index, dtype="float64")


def add_phase15_rolling_features(
    frame: pd.DataFrame,
    *,
    entity_id_col: str,
    date_col: str,
) -> pd.DataFrame:
    """Add shifted rolling features that only use pre-anchor observations.

    Args:
        frame: Batter-game frame.
        entity_id_col: Entity id column.
        date_col: Date column.

    Returns:
        DataFrame with added leakage-safe rolling features.
    """

    if frame.empty:
        return frame.copy()

    engineered = frame.copy()
    engineered[date_col] = pd.to_datetime(engineered[date_col], errors="coerce")
    engineered = engineered.sort_values([entity_id_col, date_col], kind="stable")

    for base_column in ROLL_BASE_COLUMNS:
        if base_column not in engineered.columns:
            engineered[base_column] = 0.0
        engineered[base_column] = pd.to_numeric(
            engineered[base_column], errors="coerce"
        ).fillna(0.0)

    grouped = engineered.groupby(entity_id_col, dropna=False)
    for window in ROLLING_WINDOWS:
        for base_column in ROLL_BASE_COLUMNS:
            column_name = f"roll_{window}_{base_column}"
            engineered[column_name] = grouped[base_column].transform(
                lambda series: series.shift(1).rolling(window, min_periods=1).mean()
            )
            engineered[column_name] = pd.to_numeric(
                engineered[column_name], errors="coerce"
            ).fillna(0.0)

        plate_col = f"roll_{window}_plate_appearances"
        engineered[f"roll_{window}_hit_rate"] = _safe_ratio(
            engineered[f"roll_{window}_hits"], engineered[plate_col]
        )
        engineered[f"roll_{window}_walk_rate"] = _safe_ratio(
            engineered[f"roll_{window}_walks"], engineered[plate_col]
        )
        engineered[f"roll_{window}_strikeout_rate"] = _safe_ratio(
            engineered[f"roll_{window}_strikeouts"], engineered[plate_col]
        )
        engineered[f"roll_{window}_slugging_proxy"] = _safe_ratio(
            engineered[f"roll_{window}_total_bases"], engineered[plate_col]
        )
        engineered[f"roll_{window}_hard_hit_rate"] = _safe_ratio(
            engineered[f"roll_{window}_hard_hit_events"], engineered[plate_col]
        )
        engineered[f"roll_{window}_pa_vs_lhp_share"] = _safe_ratio(
            engineered[f"roll_{window}_pa_vs_lhp"], engineered[plate_col]
        )
        engineered[f"roll_{window}_pa_vs_rhp_share"] = _safe_ratio(
            engineered[f"roll_{window}_pa_vs_rhp"], engineered[plate_col]
        )

    date_series = pd.to_datetime(engineered[date_col], errors="coerce")
    prev_game = grouped[date_col].shift(1)
    prev_game = pd.to_datetime(prev_game, errors="coerce")
    engineered["days_since_last_game"] = (
        (date_series - prev_game).dt.days.fillna(365.0).clip(lower=0.0)
    )

    for window in (14, 30):
        engineered[f"games_played_last_{window}"] = grouped[
            "plate_appearances"
        ].transform(
            lambda series: series.shift(1).rolling(window, min_periods=1).count()
        )
        engineered[f"games_played_last_{window}"] = pd.to_numeric(
            engineered[f"games_played_last_{window}"], errors="coerce"
        ).fillna(0.0)
        engineered[f"pa_per_game_last_{window}"] = _safe_ratio(
            engineered[f"roll_{window}_plate_appearances"],
            engineered[f"games_played_last_{window}"],
        )

    engineered["team_games_seen_last_30"] = pd.to_numeric(
        engineered.get("games_played_last_30", 0.0), errors="coerce"
    ).fillna(0.0)
    engineered["player_game_share_last_30"] = _safe_ratio(
        engineered["games_played_last_30"], pd.Series(30.0, index=engineered.index)
    ).clip(lower=0.0, upper=1.0)

    def _shifted_consecutive_streak(series: pd.Series) -> pd.Series:
        games = pd.to_numeric(series, errors="coerce").fillna(0.0).gt(0.0)
        output = []
        streak = 0
        for played in games:
            output.append(float(streak))
            if played:
                streak += 1
            else:
                streak = 0
        return pd.Series(output, index=series.index, dtype="float64")

    engineered["recent_consecutive_games_played"] = grouped[
        "plate_appearances"
    ].transform(_shifted_consecutive_streak)

    return engineered
