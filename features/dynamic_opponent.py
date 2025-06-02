import pandas as pd
from features.team_abbr_map import team_fix_map

def compute_opponent_k_pct_dynamic(start_date: str, end_date: str, default_k_pct = 0.055, source_df=None) -> pd.DataFrame:
    """
    Returns a DataFrame with one row per team & date, containing:
        ['game_date', 'Team', 'K_pct_so_far']
    where K_pct_so_far is the team's strikeout % over all prior dates.
    """
    if source_df is None:
        from pybaseball import statcast
        print("⚠️ No source_df provided — fetching from Statcast live.")
        pbp = statcast(start_date, end_date)
    else:
        pbp = source_df[
            (source_df['game_date'] >= start_date) &
            (source_df['game_date'] <= end_date)
        ].copy()

    pbp['pa'] = pbp['events'].notna()
    pbp['Team'] = pbp.apply(
        lambda r: r['home_team'] if r['inning_topbot'] == 'Bot' else r['away_team'],
        axis=1
    )
    pbp['Team'] = pbp['Team'].replace(team_fix_map)

    # Game-level team aggregation
    daily = (
        pbp.groupby(['game_date', 'Team'])
        .agg(so=('events', lambda x: x.eq('strikeout').sum()),
             pa=('pa', 'sum'))
        .reset_index()
    )

    daily = daily.sort_values(['Team', 'game_date'])
    daily['cum_so'] = daily.groupby('Team')['so'].cumsum().shift(1).fillna(0)
    daily['cum_pa'] = daily.groupby('Team')['pa'].cumsum().shift(1).fillna(0)

    daily['K_pct_so_far'] = daily['cum_so'] / daily['cum_pa']
    daily['K_pct_so_far'] = daily['K_pct_so_far'].fillna(default_k_pct)

    return daily[['game_date', 'Team', 'K_pct_so_far']]