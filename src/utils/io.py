import yaml
import pandas as pd
from pathlib import Path
from typing import Any, Dict


def load_config(path: str = "config/config.yaml") -> Dict[str, Any]:
    """Load YAML configuration file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def read_csv(path: str) -> pd.DataFrame:
    """Read a CSV file into a DataFrame."""
    return pd.read_csv(Path(path))
