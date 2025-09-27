"""CLI helper for building the NFL QB attempts dataset."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from typing import Sequence

import pandas as pd

from src.nfl.data.qb_attempts import (
    build_qb_attempts_dataset,
    load_ngs_passing_data,
    load_pbp_data,
)

DEFAULT_OUTPUT = "data/qb_attempts_dataset.parquet"


def _empty_loader(_: Sequence[int]) -> pd.DataFrame:  # pragma: no cover - CLI convenience
    return pd.DataFrame()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build QB pass attempts dataset from nfl_data_py + Underdog lines",
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="*",
        default=None,
        help="Explicit season years to include (overrides --start/--end).",
    )
    parser.add_argument(
        "--start",
        type=int,
        default=2015,
        help="First season to include when --years is not provided (inclusive).",
    )
    parser.add_argument(
        "--end",
        type=int,
        default=2024,
        help="Last season to include when --years is not provided (inclusive).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT,
        help="Destination parquet file for the assembled dataset.",
    )
    parser.add_argument(
        "--skip-pbp",
        action="store_true",
        help="Skip importing play-by-play data (useful for quick smoke tests).",
    )
    parser.add_argument(
        "--skip-ngs",
        action="store_true",
        help="Skip importing Next Gen Stats passing data.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.years:
        years = sorted(set(args.years))
    else:
        years = list(range(args.start, args.end + 1))

    pbp_loader = _empty_loader if args.skip_pbp else load_pbp_data
    ngs_loader = _empty_loader if args.skip_ngs else load_ngs_passing_data

    dataset = build_qb_attempts_dataset(
        years=years,
        output_path=args.output,
        pbp_loader=pbp_loader,
        ngs_loader=ngs_loader,
    )
    print(
        "Dataset built",
        {
            "years": f"{years[0]}-{years[-1]}",
            "rows": len(dataset),
            "features": len(dataset.columns),
            "output": args.output,
        },
    )


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    main()