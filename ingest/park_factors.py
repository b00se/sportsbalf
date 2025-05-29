from pybaseball import statcast, cache
from datetime import date, timedelta

from features.team_abbr_map import team_fix_map


def compute_k_park_factors(start, end=None):
    if end is None:
        end = (date.today() - timedelta(days=1)).isoformat()

    df = statcast(start, end)

    df = df[df['pitch_type'].notnull()]
    df['is_k'] = df['events'] == 'strikeout'
    k_by_park = df.groupby('home_team').agg({
        'is_k': 'sum',
        'batter': 'count' # rough PA proxy
    }).reset_index()

    k_by_park['k_pct'] = k_by_park['is_k'] / k_by_park['batter']
    league_avg = df['is_k'].sum() / df['batter'].count()
    k_by_park['K_park_factor'] = k_by_park['k_pct'] / league_avg

    k_by_park['Team_abbr'] = k_by_park['home_team'].map(team_fix_map).fillna(k_by_park['home_team'])

    k_by_park[['Team_abbr', 'K_park_factor']].to_csv("../data/raw/park_factors_2025.csv")