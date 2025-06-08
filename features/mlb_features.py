import pandas as pd

from features.team_abbr_map import team_fix_map

def aggregate_pitcher_games(df: pd.DataFrame) -> pd.DataFrame:
    # Aggregate pitch-level data to game-level
    games = df.groupby(['pitcher', 'game_date']).agg({
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
    meta = df.groupby(['pitcher', 'game_date']).first().reset_index()
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

    whiff_count = df.groupby(['pitcher', 'game_date'])['description'].apply(
        lambda x: x.isin(['swinging_strike', 'swinging_strike_blocked']).sum()
    ).rename('whiff_count')
    swing_count = df.groupby(['pitcher', 'game_date'])['description'].apply(
        lambda x: x.isin(['swinging_strike', 'swinging_strike_blocked', 'foul', 'foul_tip', 'hit_into_play']).sum()
    ).rename('swing_count')

    called_count = df.groupby(['pitcher','game_date'])['description'].apply(
        lambda x: (x == 'called_strike').sum()
    ).rename('called_count')

    # Merge in counts
    games = games.merge(whiff_count, on=['pitcher', 'game_date'], how='left')
    games = games.merge(swing_count, on=['pitcher', 'game_date'], how='left')
    games = games.merge(called_count, on=['pitcher', 'game_date'], how='left')

    # Calculate metrics
    games['whiff_rate'] = (games['whiff_count'] / games['swing_count']).fillna(0)
    games['csw_pct'] = ((games['whiff_count'] + games['called_count']) / games['pitch_count']).fillna(0)

    games['cum_whiffs'] = games['whiff_count'].cumsum().fillna(0)
    games['cum_called'] = games['called_count'].cumsum().fillna(0)
    games['cum_pitches'] = games['pitch_count'].cumsum().fillna(0)
    games['whiff_rate_expanding'] = (games['cum_whiffs'] / games['cum_pitches']).fillna(0)
    games['csw_pct_expanding'] = ((games['cum_whiffs'] + games['cum_called']) / games['cum_pitches']).fillna(0)

    games.drop(columns=['whiff_count', 'swing_count', 'called_count', 'cum_whiffs', 'cum_called', 'cum_pitches'], inplace=True)

    return games