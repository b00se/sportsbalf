"""CLI helper for building the NFL QB attempts dataset."""
from __future__ import annotations

import argparse

from src.nfl.data.qb_attempts import build_qb_attempts_dataset

DEFAULT_OUTPUT = "data/qb_attempts_dataset.parquet"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build QB pass attempts dataset from nfl_data_py + Underdog lines")
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.years:
        years = sorted(set(args.years))
    else:
        years = list(range(args.start, args.end + 1))

    dataset = build_qb_attempts_dataset(years=years, output_path=args.output)
    print(f"Saved {len(dataset)} rows to {args.output}")


if __name__ == "__main__":
    main()
