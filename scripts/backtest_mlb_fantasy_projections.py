"""Run a lightweight walk-forward summary for MLB fantasy projection metrics."""

from __future__ import annotations

import argparse

import pandas as pd
from src.fantasy.adapters.mlb.backtest import (
    aggregate_metric_scores,
    generate_walk_forward_folds,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--predictions",
        required=True,
        help="CSV path with metric_id,prediction,actual,season.",
    )
    parser.add_argument(
        "--output", required=True, help="Output CSV path for aggregated scores."
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    frame = pd.read_csv(args.predictions)
    seasons = tuple(int(value) for value in sorted(frame["season"].dropna().unique()))
    _folds = generate_walk_forward_folds(seasons)
    scores = aggregate_metric_scores(frame)
    scores.to_csv(args.output, index=False)


if __name__ == "__main__":
    main()
