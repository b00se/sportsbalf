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

    whiff_count = df.groupby('game_date')['description'].apply(lambda x: (x == 'swinging_strike').sum())
    called_count = df.groupby('game_date')['description'].apply(lambda x: (x == 'called_strike').sum())

    games['whiff_rate'] = (whiff_count.shift(1).fillna(0) / games['pitch_count']).fillna(0)
    games['csw_pct'] = (((whiff_count + called_count).shift(1).fillna(0)) / games['pitch_count']).fillna(0)

    games['cum_whiffs'] = whiff_count.cumsum().shift(1).fillna(0)
    games['cum_called'] = called_count.cumsum().shift(1).fillna(0)
    games['cum_pitches'] = games['pitch_count'].cumsum().shift(1).fillna(0)
    games['whiff_rate_expanding'] = (games['cum_whiffs'] / games['cum_pitches']).fillna(0)
    games['csw_pct_expanding'] = ((games['cum_whiffs'] + games['cum_called']) / games['cum_pitches']).fillna(0)

    games.drop(columns=['cum_whiffs', 'cum_called', 'cum_pitches'], inplace=True)

    return games