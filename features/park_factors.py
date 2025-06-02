import pandas as pd

from features.team_abbr_map import team_fix_map


def load_historic_park_factors(csv_path: str) -> pd.DataFrame:
    """
        Load a manually downloaded park factor CSV from FanGraphs Guts! page.
        Expects 'Team' and 'SO' columns where 'SO' is a percent (e.g. 105 = 1.05).

        Returns:
            DataFrame with columns ['Team_abbr', 'K_park_factor']
    """
    df = pd.read_csv(csv_path)
    df['K_park_factor'] = df['SO'] / 100.0

    team_map = {
        'Diamondbacks': 'ARI',
        'Braves': 'ATL',
        'Orioles': 'BAL',
        'Red Sox': 'BOS',
        'Cubs': 'CHC',
        'White Sox': 'CHW',
        'Reds': 'CIN',
        'Cleveland': 'CLE',
        'Guardians': 'CLE',
        'Rockies': 'COL',
        'Tigers': 'DET',
        'Astros': 'HOU',
        'Royals': 'KC',
        'Angels': 'LAA',
        'Dodgers': 'LAD',
        'Marlins': 'MIA',
        'Brewers': 'MIL',
        'Twins': 'MIN',
        'Mets': 'NYM',
        'Yankees': 'NYY',
        'Athletics': 'ATH',
        'Phillies': 'PHI',
        'Pirates': 'PIT',
        'Padres': 'SD',
        'Giants': 'SF',
        'Mariners': 'SEA',
        'Cardinals': 'STL',
        'Rays': 'TB',
        'Rangers': 'TEX',
        'Blue Jays': 'TOR',
        'Nationals': 'WSH'
    }

    df['Team_abbr'] = df['Team'].map(team_map)
    return df[["Team_abbr", "K_park_factor"]]

def load_live_park_factors(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    return df[['Team_abbr', 'K_park_factor']]

def compute_k_park_factors(start_date, end_date, source_df=None):
    if source_df is None:
        from pybaseball import statcast
        print("⚠️ No source_df provided — fetching from Statcast live.")
        df = statcast(start_date, end_date)
    else:
        df = source_df[
            (source_df['game_date'] >= start_date) &
            (source_df['game_date'] <= end_date)
        ].copy()

    df = df[df['pitch_type'].notnull()]
    df['is_k'] = df['events'] == 'strikeout'

    k_by_park = df.groupby('home_team').agg({
        'is_k': 'sum',
        'batter': 'count'
    }).reset_index()

    k_by_park['K_pct'] = k_by_park['is_k'] / k_by_park['batter']
    league_avg = df['is_k'].sum() / df['batter'].count()
    k_by_park['K_park_factor'] = k_by_park['K_pct'] / league_avg
    k_by_park['Team_abbr'] = k_by_park['home_team'].replace(team_fix_map).fillna(k_by_park['home_team'])

    return k_by_park[['Team_abbr', 'K_park_factor']]