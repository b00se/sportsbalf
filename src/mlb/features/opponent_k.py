import pandas as pd


def add_opponent_k_rate(games: pd.DataFrame) -> pd.DataFrame:
    """Add opponent strikeout rate up to each game."""
    games = games.sort_values(["opponent_team", "game_date"]).copy()
    grouped = games.groupby("opponent_team", sort=False)
    prior_strikeouts = (
        grouped["strikeouts"].cumsum().groupby(games["opponent_team"]).shift(1)
    )
    prior_pitch_count = (
        grouped["pitch_count"].cumsum().groupby(games["opponent_team"]).shift(1)
    )
    games["opponent_k_rate"] = prior_strikeouts / prior_pitch_count
    fallback_rate = games["opponent_k_rate"].mean()
    if pd.isna(fallback_rate):
        fallback_rate = 0.0
    games["opponent_k_rate"] = games["opponent_k_rate"].fillna(fallback_rate)
    return games.reset_index(drop=True)
