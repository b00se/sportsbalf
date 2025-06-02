import os
import pandas as pd
from datetime import datetime
from pybaseball import statcast

def fetch_statcast_raw(season, start="04-01", end="10-01", save_dir="data/raw/statcast"):
    os.makedirs(save_dir, exist_ok=True)

    start_date = f"{season}-{start}"
    end_date = f"{season}-{end}"

    print(f"📡 Fetching Statcast data from {start_date} to {end_date}")
    df = statcast(start_date, end_date)
    print(f"💾 Retrieved {len(df)} rows")

    out_path = os.path.join(save_dir, f"statcast_raw_{season}.parquet")
    df.to_parquet(out_path, index=False)
    print(f"✅ Saved to {out_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    args = parser.parse_args()

    fetch_statcast_raw(args.season)