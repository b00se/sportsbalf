"""CLI helper: build NHL skater-game snapshot CSV from shot-level CSV."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_INPUT = "data/nhl/shots_2007-2024.csv"
DEFAULT_OUTPUT = "data/nhl/moneypuck_skater_games_full_snapshot_from_shots.csv"


def parse_args() -> argparse.Namespace:
    """Parse CLI args for snapshot build command."""

    parser = argparse.ArgumentParser(
        description="Build canonical NHL skater-game snapshot from shot-level CSV.",
    )
    parser.add_argument(
        "--input",
        type=str,
        default=DEFAULT_INPUT,
        help="Shot-level input CSV path.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT,
        help="Canonical output snapshot CSV path.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=500_000,
        help="CSV chunk size used for aggregation.",
    )
    return parser.parse_args()


def main() -> None:
    """Run CLI entrypoint."""

    from src.nhl.data.shot_snapshot import build_skater_snapshot_from_shots_csv

    args = parse_args()
    snapshot = build_skater_snapshot_from_shots_csv(
        input_path=args.input,
        output_path=args.output,
        chunk_size=args.chunk_size,
    )
    print(
        "Snapshot built",
        {
            "rows": len(snapshot),
            "players": int(snapshot["player_id"].nunique()) if len(snapshot) else 0,
            "seasons": int(snapshot["season"].nunique()) if len(snapshot) else 0,
            "output": args.output,
        },
    )


if __name__ == "__main__":  # pragma: no cover
    main()
