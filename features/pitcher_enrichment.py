import pandas as pd

from features.enrichments import add_park_factor
from features.mlb_features import aggregate_pitcher_games
from features.rolling import add_rolling_features


def _coerce_datetime(series: pd.Series) -> pd.Series:
    """Return a datetime64 series, coercing when necessary."""

    if not pd.api.types.is_datetime64_any_dtype(series):
        return pd.to_datetime(series)
    return series


def _coerce_numeric(df: pd.DataFrame, columns) -> None:
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")


def enrich_pitcher_games(player_df, name, mlbam_id, opponent_k_df, park_df):
    if player_df.empty:
        return None

    player_df = player_df.copy()
    player_df["game_date"] = _coerce_datetime(player_df["game_date"])

    games = aggregate_pitcher_games(player_df)
    games["game_date"] = _coerce_datetime(games["game_date"])
    games = games.sort_values(["pitcher", "game_date"]).reset_index(drop=True)

    numeric_cols = [
        "pitch_count",
        "strikeouts",
        "max_inning",
        "num_pitch_types",
        "whiff_rate",
        "csw_pct",
        "whiff_rate_expanding",
        "csw_pct_expanding",
        "rest_days",
    ]
    _coerce_numeric(games, numeric_cols)

    opponent_k_df = opponent_k_df.copy()
    if "game_date" in opponent_k_df.columns:
        opponent_k_df["game_date"] = _coerce_datetime(opponent_k_df["game_date"])
    _coerce_numeric(opponent_k_df, ["K_pct_so_far"])

    park_df = park_df.copy()
    _coerce_numeric(park_df, ["K_park_factor"])

    games = (
        games.merge(
            opponent_k_df,
            left_on=["game_date", "opponent_team"],
            right_on=["game_date", "Team"],
            how="left",
        )
        .rename(columns={"K_pct_so_far": "opponent_k_pct"})
        .drop(columns=["Team"], errors="ignore")
    )

    games = add_park_factor(games, park_df)
    games = add_rolling_features(games)
    games["pitcher_name"] = name
    games["pitcher_id"] = mlbam_id

    return games
