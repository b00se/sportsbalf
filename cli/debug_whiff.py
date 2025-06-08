import pandas as pd

from features.mlb_features import aggregate_pitcher_games


def main():
    pitcher_id = 670102  # Bowden Francis
    statcast_path = "../data/raw/statcast/statcast_raw_2025.parquet"
    enriched_path = "../data/processed/pitcher_game_data_2025.parquet"

    # Load raw statcast and filter
    df = pd.read_parquet(statcast_path)
    pitcher = df[df['pitcher'] == pitcher_id]
    new_df = aggregate_pitcher_games(pitcher)
    print(new_df[["whiff_rate", "csw_pct"]])

def test():
    processed_path = "../data/processed/pitcher_game_data_2025.parquet"
    df = pd.read_parquet(processed_path)
    print(df[['whiff_rate', 'csw_pct', 'whiff_rate_expanding', 'csw_pct_expanding']].tail(10))

if __name__ == "__main__":
    test()
