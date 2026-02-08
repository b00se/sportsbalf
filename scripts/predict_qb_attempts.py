"""CLI to run the NFL QB pass attempt pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run NFL QB pass attempt predictions")
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Optional path to an alternate config file (defaults to config/nfl.yaml).",
    )
    parser.add_argument(
        "--retrain",
        action="store_true",
        help="Force retraining the model even if a saved artifact exists.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Optional path to persist the enriched lines (CSV or Parquet).",
    )
    return parser.parse_args()


def main() -> None:
    from src.nfl.pipeline import run

    args = parse_args()
    predictions = run(config_path=args.config, retrain=args.retrain)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.suffix.lower() == ".parquet":
            predictions.to_parquet(output_path, index=False)
        else:
            predictions.to_csv(output_path, index=False)
    print(
        "Generated predictions",
        {
            "rows": len(predictions),
            "columns": len(predictions.columns),
            "output": args.output,
        },
    )


if __name__ == "__main__":  # pragma: no cover - script entrypoint
    main()
