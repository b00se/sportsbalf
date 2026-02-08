from pathlib import Path
from typing import Any

import pandas as pd
import yaml


def load_config(path: str = "config/mlb.yaml") -> dict[str, Any]:
    """Load YAML configuration file."""
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def read_csv(path: str) -> pd.DataFrame:
    """Read a CSV or Parquet file into a DataFrame depending on extension."""
    resolved = Path(path)

    if resolved.suffix.lower() == ".parquet":
        return pd.read_parquet(resolved)

    return pd.read_csv(resolved)
