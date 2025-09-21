import yaml
import pandas as pd
from pathlib import Path
from typing import Any, Dict


def load_config(path: str = "config/mlb.yaml") -> Dict[str, Any]:
    """Load YAML configuration file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def read_csv(path: str) -> pd.DataFrame:
    """Read a CSV or Parquet file into a DataFrame depending on extension."""
    resolved = Path(path)

    if resolved.suffix.lower() == ".parquet":
        return pd.read_parquet(resolved)

    return pd.read_csv(resolved)
