import pandas as pd

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