import sys
import argparse
from pathlib import Path

# Ensure project root is on the import path when running this script directly
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.mlb.pipeline import run


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the MLB strikeout pipeline")
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Optional path to override the default config",
    )
    parser.add_argument(
        "--retrain",
        action="store_true",
        help="Retrain the XGBoost model before generating predictions",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result = run(config_path=args.config, retrain=args.retrain)
    print(result)
