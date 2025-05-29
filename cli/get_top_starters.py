#!/usr/bin/env python3
"""
Pull and save a list of top starting pitchers for a given season.
Filters for GS >= 10 and sorts by WAR.
"""

import argparse
from pybaseball import pitching_stats

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--season', required=True, type=int, help="Year to fetch")
    p.add_argument('--top', type=int, default=999, help="How many SPs")
    args = p.parse_args()

    season = args.season
    top_n = args.top
    out_path = f"data/raw/top_starters_{season}.csv"

    print(f"📊 Fetching pitching stats for {season}...")
    df = pitching_stats(season, qual=0)

    starters = df[df['IP'] >= 15].copy()
    starters = starters.sort_values('WAR', ascending=False)

    keep = ['Name', 'Team', 'IP', 'GS', 'WAR', 'ERA', 'K/9', 'IDfg']
    starters = starters[keep].head(top_n)

    starters.to_csv(out_path, index=False)
    print(f"✅ Saved {len(starters)} starters to {out_path}")

if __name__ == "__main__":
    main()