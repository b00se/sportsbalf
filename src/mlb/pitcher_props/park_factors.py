"""Rolling park-factor helpers for MLB multi-stat pitcher props."""

from __future__ import annotations

import numpy as np
import pandas as pd


def add_rolling_park_factor(
    games: pd.DataFrame,
    *,
    target_col: str,
    park_col: str,
    min_samples: int = 20,
    half_life_games: int = 60,
) -> pd.DataFrame:
    """Attach leakage-safe recency-weighted park factors to pitcher-game rows.

    Args:
        games: Game-level pitcher frame with ``home_team`` and ``game_date``.
        target_col: Stat target column used to derive factor strength.
        park_col: Output park-factor column name.
        min_samples: Minimum prior team samples before using non-neutral factors.
        half_life_games: Half-life in games for exponential weighting.

    Returns:
        Frame with ``park_col`` populated.
    """

    enriched = games.copy()
    if (
        enriched.empty
        or "home_team" not in enriched.columns
        or target_col not in enriched.columns
    ):
        enriched[park_col] = 1.0
        return enriched

    enriched = enriched.copy()
    enriched["game_date"] = pd.to_datetime(enriched["game_date"], errors="coerce")
    enriched = enriched.dropna(subset=["game_date"]).copy()
    if enriched.empty:
        enriched[park_col] = 1.0
        return enriched

    sort_cols = ["game_date"]
    for candidate in ["game_pk", "pitcher_id", "pitcher"]:
        if candidate in enriched.columns:
            sort_cols.append(candidate)
    enriched = enriched.sort_values(sort_cols, kind="stable").reset_index(drop=True)

    target = pd.to_numeric(enriched[target_col], errors="coerce").fillna(0.0)
    enriched["_target_value"] = target

    league_daily = (
        enriched.groupby("game_date", as_index=False)["_target_value"]
        .mean()
        .sort_values("game_date", kind="stable")
    )
    league_daily["league_recent"] = (
        league_daily["_target_value"]
        .ewm(halflife=half_life_games, adjust=False, min_periods=1)
        .mean()
        .shift(1)
    )
    league_daily["league_recent"] = league_daily["league_recent"].fillna(
        league_daily["_target_value"].expanding(min_periods=1).mean().shift(1)
    )
    league_daily["league_recent"] = league_daily["league_recent"].fillna(
        max(float(target.mean()), 1e-6)
    )

    park_daily = (
        enriched.groupby(["home_team", "game_date"], as_index=False)
        .agg(day_mean=("_target_value", "mean"), day_games=("_target_value", "size"))
        .sort_values(["home_team", "game_date"], kind="stable")
    )
    park_daily["park_recent"] = park_daily.groupby("home_team", sort=False)[
        "day_mean"
    ].transform(
        lambda s: (
            s.ewm(halflife=half_life_games, adjust=False, min_periods=1).mean().shift(1)
        )
    )
    park_daily["park_samples"] = (
        park_daily.groupby("home_team", sort=False)["day_games"]
        .transform(lambda s: s.cumsum().shift(1))
        .fillna(0.0)
        .astype(float)
    )

    merged = enriched.merge(
        park_daily[["home_team", "game_date", "park_recent", "park_samples"]],
        on=["home_team", "game_date"],
        how="left",
    ).merge(
        league_daily[["game_date", "league_recent"]],
        on="game_date",
        how="left",
    )

    factor = (merged["park_recent"] / merged["league_recent"]).replace(
        [np.inf, -np.inf], np.nan
    )
    factor = factor.where(merged["park_samples"] >= float(min_samples), 1.0)
    factor = factor.fillna(1.0).clip(lower=0.5, upper=1.5)

    merged[park_col] = factor
    return merged.drop(columns=["_target_value"])


def park_factor_lookup(games: pd.DataFrame, park_col: str) -> dict[str, float]:
    """Create a latest-value lookup by home team for inference rows.

    Args:
        games: Pitcher-game table that includes ``home_team`` and ``park_col``.
        park_col: Park-factor column name.

    Returns:
        Mapping from team abbreviation to park factor.
    """

    if games.empty or "home_team" not in games.columns or park_col not in games.columns:
        return {}

    latest = (
        games.dropna(subset=["home_team"])
        .sort_values("game_date")
        .drop_duplicates(subset=["home_team"], keep="last")
    )
    return {
        str(row.home_team): float(getattr(row, park_col))
        for row in latest.itertuples(index=False)
        if pd.notna(getattr(row, park_col))
    }
