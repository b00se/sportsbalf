import pandas as pd
from features.team_abbr_map import team_fix_map

def aggregate_pitcher_games(df: pd.DataFrame) -> pd.DataFrame:
    # Aggregate pitch-level data to game-level
    games = df.groupby(['game_date']).agg({
        'description': 'count',
        'events': lambda x: (x == 'strikeout').sum(),
        'inning': 'max',
        'pitch_type': pd.Series.nunique
    }).reset_index()

    games.rename(columns={
        'description': 'pitch_count',
        'events': 'strikeouts',
        'inning': 'max_inning',
        'pitch_type': 'num_pitch_types'
    }, inplace=True)

    # Extract metadata (one pitch per game)
    meta = df.groupby('game_date').first().reset_index()
    games['home_team'] = meta['home_team']
    games['away_team'] = meta['away_team']
    games['rest_days'] = meta['pitcher_days_since_prev_game'].fillna(5).astype(int)

    # Infer opponent team
    games['pitcher_team'] = meta.apply(
        lambda row: row['home_team'] if row['inning_topbot'] == 'Top' else row['away_team'], axis=1
    )
    games['opponent_team'] = meta.apply(
        lambda row: row['away_team'] if row['inning_topbot'] == 'Top' else row['home_team'], axis=1
    )

    for col in ['pitcher_team', 'opponent_team', 'home_team', 'away_team']:
        games[col] = games[col].replace(team_fix_map)

    return games