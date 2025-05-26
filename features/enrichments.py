def add_opponent_k_pct(games, team_k_table):
    games = games.merge(
        team_k_table, left_on='opponent_team', right_on='Team', how='left'
    ).rename(columns={'K_pct': 'opponent_k_pct'})
    return games.drop(columns=['Team'])

def add_park_factor(games, park_df):
    games = games.merge(
        park_df, left_on='home_team', right_on='Team_abbr', how='left'
    ).rename(columns={'K_park_factor': 'park_factor_K'})
    return games.drop(columns=['Team_abbr'])