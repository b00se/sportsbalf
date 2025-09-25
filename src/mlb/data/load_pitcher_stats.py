"""Utility wrappers for pitcher historical datasets."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def load_pitcher_game_logs(path: str) -> pd.DataFrame:
    """Load historical pitcher game logs from CSV or Parquet."""

    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Pitcher game logs file not found: {file_path}")

    if file_path.suffix.lower() == ".parquet":
        return pd.read_parquet(file_path)

    return pd.read_csv(file_path)

