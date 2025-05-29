#!/usr/bin/env python3
import argparse
import time
from datetime import date, timedelta

import pandas as pd
from pybaseball import statcast_pitcher, team_batting, playerid_reverse_lookup, cache

from features.enrichments import add_park_factor
from features.dynamic_opponent import compute_opponent_k_pct_dynamic
from features.mlb_features import aggregate_pitcher_games
from features.park_factors import load_historic_park_factors, load_live_park_factors
from features.rolling import add_rolling_features
from features.team_abbr_map import team_fix_map


def load_opponent_k_data(season=2023):
    df = team_batting(season)
    df['K_pct'] = df['SO'] / df['PA']
    df['Team'] = df['Team'].replace(team_fix_map)
    return df[['Team', 'K_pct']]


def get_pitchers_with_ids(csv_path: str, top_n: int = None) -> list:
    df = pd.read_csv(csv_path)
    if top_n:
        df = df.head(top_n)
    ids = df['IDfg'].tolist()

    lookup = playerid_reverse_lookup(ids, key_type='fangraphs')
    print(f"🔍 Lookup returned {lookup.shape[0]} rows for {len(ids)} Fangraphs IDs")
    merged = df.merge(lookup, left_on='IDfg', right_on='key_fangraphs')

    missing_ids = set(ids) - set(merged['IDfg'])
    if missing_ids:
        print(f"⚠️ Missing MLBAM IDs for {len(missing_ids)} pitchers: {list(missing_ids)[:5]}...")

    return list(zip(merged['Name'], merged['key_mlbam']))

def process_pitchers(name, mlbam_id, season, opponent_k_df, park_df, start_date, end_date):
    print(f"▶️ Processing {name} ({mlbam_id})")
    try:
        df = statcast_pitcher(start_date, end_date, mlbam_id)
        print(f"📦 Got {len(df)} rows for {name}")
        if df.empty:
            return None
        games = aggregate_pitcher_games(df)
        games['game_date'] = pd.to_datetime(games['game_date'])
        games = games.merge(
            opponent_k_df,
            left_on=['game_date', 'opponent_team'],
            right_on=['game_date', 'Team'],
            how='left'
        ).rename(columns={'K_pct_so_far':'opponent_k_pct'}).drop(columns=['Team'])

        # games = add_opponent_k_pct(games, opponent_k_df)
        games = add_park_factor(games, park_df)
        games = add_rolling_features(games)
        games['pitcher_name'] = name
        games['pitcher_id'] = mlbam_id
        return games
    except Exception as e:
        print(f"⚠️ Error processing {name}: {e}")
        return None


def main():
    cache.enable()
    p = argparse.ArgumentParser()
    p.add_argument('--season', type=int, required=True, help='Year to build')
    p.add_argument('--mode', choices=['historical', 'live'], default='historical')
    args = p.parse_args()

    season = args.season
    start_date = f"{season}-04-01"
    if args.mode == 'live':
        end_date = (date.today() - timedelta(days=1)).isoformat()
    else:
        end_date = f"{season}-10-01"

    war_csv_path = f"data/raw/top_starters_{season}.csv"
    output_path = f"data/processed/pitcher_game_data_{season}.parquet"
    if args.mode == 'live':
        park_csv_path = f"data/raw/park_factors_{season}.csv"
    else:
        park_csv_path = f"data/raw/fangraphs_park_factors_{season}.csv"

    opponent_k_df = compute_opponent_k_pct_dynamic(start_date, end_date)
    opponent_k_df['game_date'] = pd.to_datetime(opponent_k_df['game_date'])
    park_df = load_live_park_factors(park_csv_path) if args.mode == 'live' else load_historic_park_factors(park_csv_path)
    pitchers = get_pitchers_with_ids(war_csv_path)

    all_games = []
    for name, pid in pitchers:
        games = process_pitchers(name, pid, season, opponent_k_df, park_df, start_date, end_date)
        if games is not None:
            all_games.append(games)
        time.sleep(1)

    if all_games:
        full_df = pd.concat(all_games, ignore_index=True)
        full_df.to_parquet(output_path)
        print(f"✅ Saved {len(full_df)} rows to {output_path}")
    else:
        print("❌ No pitcher data collected.")


if __name__ == "__main__":
    main()
