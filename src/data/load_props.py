import pandas as pd
from src.utils.io import read_csv

def load_strikeout_lines(path: str) -> pd.DataFrame:
    """Load pitcher strikeout lines from CSV."""
    return read_csv(path)
