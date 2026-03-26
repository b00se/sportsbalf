"""Helpers for loading MLB pitcher prop lines."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import numpy as np
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


def normalize_pitcher_prop_lines(df: pd.DataFrame, line_col: str) -> pd.DataFrame:
    """Normalize a pitcher-prop line frame for downstream loaders.

    Args:
        df: Raw line frame.
        line_col: Stat-specific line column name.

    Returns:
        Normalized line frame with required numeric columns coerced.

    Raises:
        ValueError: If required columns are missing.
    """

    work = df.copy()
    required = {"player", line_col}
    missing = required - set(work.columns)
    if missing:
        raise ValueError(
            f"Pitcher prop lines missing required columns: {sorted(missing)}"
        )

    for optional in (
        "over_decimal_price",
        "over_payout_multiplier",
        "under_decimal_price",
        "under_payout_multiplier",
    ):
        if optional not in work.columns:
            work[optional] = np.nan

    _coerce_numeric(
        work,
        [
            line_col,
            "over_decimal_price",
            "over_payout_multiplier",
            "under_decimal_price",
            "under_payout_multiplier",
        ],
    )
    work["player"] = work["player"].astype(str)
    return work


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
        raise ValueError(f"Strikeout lines missing required columns: {sorted(missing)}")

    return normalize_pitcher_prop_lines(df, "k_line")


def load_pitcher_prop_lines(path: str, line_col: str) -> pd.DataFrame:
    """Load generic MLB pitcher prop lines for a stat-specific line column.

    Args:
        path: CSV/Parquet path.
        line_col: Stat-specific line column (for example ``outs_line``).

    Returns:
        Normalized line frame.
    """

    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Pitcher prop lines file not found: {file_path}")

    if file_path.suffix.lower() == ".parquet":
        df = pd.read_parquet(file_path)
    else:
        df = read_csv(str(file_path))

    return normalize_pitcher_prop_lines(df, line_col)
