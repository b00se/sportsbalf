"""Helpers for loading Underdog strikeout prop lines."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from src.utils.io import read_csv

_REQUIRED_COLUMNS = {
    "player",
    "k_line",
    "over_decimal_price",
    "under_decimal_price",
}


def _coerce_numeric(df: pd.DataFrame, columns: Iterable[str]) -> None:
    for column in columns:
        if column not in df.columns:
            continue
        df[column] = pd.to_numeric(df[column], errors="coerce")


def load_strikeout_lines(path: str) -> pd.DataFrame:
    """Load raw Underdog strikeout lines and coerce numeric odds columns."""

    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Strikeout lines file not found: {file_path}")

    if file_path.suffix.lower() == ".parquet":
        df = pd.read_parquet(file_path)
    else:
        df = read_csv(str(file_path))

    missing = _REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            f"Strikeout lines missing required columns: {sorted(missing)}"
        )

    numeric_columns = [
        "k_line",
        "over_decimal_price",
        "over_payout_multiplier",
        "under_decimal_price",
        "under_payout_multiplier",
    ]
    _coerce_numeric(df, numeric_columns)

    df["player"] = df["player"].astype(str)
    return df
