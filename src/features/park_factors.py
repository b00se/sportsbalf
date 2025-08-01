import pandas as pd


def adjust_for_park_factors(games: pd.DataFrame, park_df: pd.DataFrame) -> pd.DataFrame:
    """Merge park factors onto game logs."""
    df = games.merge(park_df, left_on="home_team", right_on="Team_abbr", how="left")
    df = df.rename(columns={"K_park_factor": "park_factor_K"}).drop(columns=["Team_abbr"])
    return df
