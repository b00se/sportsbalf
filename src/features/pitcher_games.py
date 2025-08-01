import pandas as pd
from features.mlb_features import aggregate_pitcher_games as _aggregate
from features.rolling import add_rolling_features as _add_rolling
from features.enrichments import add_park_factor as _add_park
from features.enrichments import add_opponent_k_pct as _add_opponent


def aggregate_pitcher_games(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate pitch-level data to game-level using existing helper."""
    return _aggregate(df)


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add rolling averages for strikeouts and pitch counts."""
    return _add_rolling(df)


def add_park_factor(df: pd.DataFrame, park_df: pd.DataFrame) -> pd.DataFrame:
    """Attach park factor data for the home team."""
    return _add_park(df, park_df)


def add_opponent_k_pct(df: pd.DataFrame, opp_df: pd.DataFrame) -> pd.DataFrame:
    """Merge opponent strikeout percent onto game logs."""
    return _add_opponent(df, opp_df)
