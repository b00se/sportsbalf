import argparse
import sys
from pathlib import Path

# Ensure project root is on the import path when running this script directly
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a sport/stat prediction pipeline")
    parser.add_argument(
        "--sport",
        type=str,
        required=True,
        help="Sport key (for example: mlb, nfl).",
    )
    parser.add_argument(
        "--stat",
        type=str,
        required=True,
        help="Stat-line key (for example: strikeouts, pass_attempts).",
    )
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to YAML config (for example: config/mlb.yaml).",
    )
    parser.add_argument(
        "--retrain",
        action="store_true",
        help="Retrain the XGBoost model before generating predictions",
    )
    return parser.parse_args()


if __name__ == "__main__":
    from src.pipeline.engine import run_pipeline_with_overrides

    args = parse_args()
    result = run_pipeline_with_overrides(
        args.config,
        sport=args.sport,
        stat=args.stat,
        retrain=args.retrain,
    )
    print(result)
