import pandas as pd
from src.utils.io import read_csv

def load_pitcher_game_logs(path: str) -> pd.DataFrame:
    """Load pitcher game logs from CSV."""
    df = read_csv(path)
    df['game_date'] = pd.to_datetime(df['game_date'])
    return df
