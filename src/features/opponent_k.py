import pandas as pd


def add_opponent_k_rate(games: pd.DataFrame) -> pd.DataFrame:
    """Add opponent strikeout rate up to each game."""
    games = games.sort_values(["opponent_team", "game_date"]).copy()
    rate = (
        games.groupby("opponent_team")["strikeouts"].cumsum().shift(1)
        / games.groupby("opponent_team")["pitch_count"].cumsum().shift(1)
    )
    games["opponent_k_rate"] = rate
    games["opponent_k_rate"] = games["opponent_k_rate"].fillna(
        games["opponent_k_rate"].mean()
    )
    return games
