import os

import pandas as pd
from pybaseball import playerid_reverse_lookup

from features.dynamic_opponent import compute_opponent_k_pct_dynamic
from features.park_factors import compute_k_park_factors
from features.pitcher_enrichment import enrich_pitcher_games

RAW_PATH = "data/raw/statcast"
OUTPUT_PATH = "data/processed"

def load_pitcher_ids(csv_path):
    df = pd.read_csv(csv_path)
    lookup = playerid_reverse_lookup(df['IDfg'].tolist(), key_type='fangraphs')
    merged = df.merge(lookup, left_on='IDfg', right_on='key_fangraphs')
    return list(zip(merged['Name'], merged['key_mlbam']))

def generate_dataset_from_raw(season):
    raw_file = os.path.join(RAW_PATH, f"statcast_raw_{season}.parquet")
    starter_csv = f"data/raw/top_starters_{season}.csv"
    output_file = os.path.join(OUTPUT_PATH, f"pitcher_game_data_{season}.parquet")

    print(f"📂 Loading raw statcast data from {raw_file}")
    df = pd.read_parquet(raw_file)
    df['game_date'] = pd.to_datetime(df['game_date'])

    print(f"📋 Loading starter list from {starter_csv}")
    pitchers = load_pitcher_ids(starter_csv)
    mlbam_ids = [pid for _, pid in pitchers]

    print(f"📆 Computing opponent K% from raw data...")
    opponent_k_df = compute_opponent_k_pct_dynamic(
        start_date=df['game_date'].min().strftime("%Y-%m-%d"),
        end_date=df['game_date'].max().strftime("%Y-%m-%d"),
        source_df=df
    )
    opponent_k_df['game_date'] = pd.to_datetime(opponent_k_df['game_date'])

    print(f"🏟️ Computing park factors from raw data...")
    park_df = compute_k_park_factors(
        start_date=df['game_date'].min().strftime("%Y-%m-%d"),
        end_date=df['game_date'].max().strftime("%Y-%m-%d"),
        source_df=df
    )

    print(f"🧠 Processing {len(pitchers)} pitchers...")
    all_games =[]
    for name, mlbam_id in pitchers:
        player_df = df[df['pitcher'] == mlbam_id]
        if player_df.empty:
            print(f"⛔ No data for {name} ({mlbam_id})")
            continue

        enriched = enrich_pitcher_games(player_df, name, mlbam_id, opponent_k_df, park_df)
        if enriched is not None:
            all_games.append(enriched)

    if not all_games:
        print("❌ No pitcher games generated.")
        return

    full_df = pd.concat(all_games, ignore_index=True)
    full_df.to_parquet(output_file, index=False)
    print(f"✅ Saved {len(full_df)} rows to {output_file}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    args = parser.parse_args()
    generate_dataset_from_raw(args.season)
