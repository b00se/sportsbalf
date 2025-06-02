import pandas as pd

from features.enrichments import add_park_factor
from features.mlb_features import aggregate_pitcher_games
from features.rolling import add_rolling_features


def enrich_pitcher_games(player_df, name, mlbam_id, opponent_k_df, park_df):
    if player_df.empty:
        return None

    games = aggregate_pitcher_games(player_df)
    games['game_date'] = pd.to_datetime(games['game_date'])

    games = games.merge(
        opponent_k_df,
        left_on=['game_date', 'opponent_team'],
        right_on=['game_date', 'Team'],
        how='left'
    ).rename(columns={'K_pct_so_far': 'opponent_k_pct'}).drop(columns=['Team'])

    games = add_park_factor(games, park_df)
    games = add_rolling_features(games)
    games['pitcher_name'] = name
    games['pitcher_id'] = mlbam_id

    return games