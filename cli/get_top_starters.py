#!/usr/bin/env python3
"""
Pull and save a list of top starting pitchers for a given season.
Filters for GS >= 10 and sorts by WAR.
"""

from pybaseball import pitching_stats

def main():
    season = 2023
    out_path = f"data/raw/top_starters_{season}.csv"

    print(f"📊 Fetching pitching stats for {season}...")
    df = pitching_stats(season, qual=0)

    starters = df[df['GS'] >= 10].copy()
    starters = starters.sort_values('WAR', ascending=False)

    keep = ['Name', 'Team', 'IP', 'GS', 'WAR', 'ERA', 'K/9', 'IDfg']
    starters = starters[keep]

    starters.to_csv(out_path, index=False)
    print(f"✅ Saved {len(starters)} starters to {out_path}")

if __name__ == "__main__":
    main()