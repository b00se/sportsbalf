#!/usr/bin/env python
# ruff: noqa: I001, E402
"""Print saved MLB live slip artifacts in a human-readable format."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.mlb.slip_report import load_summary, render_slip_report


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the slip-printer script.

    Returns:
        Parsed CLI namespace.
    """

    parser = argparse.ArgumentParser(
        description="Print saved MLB live slip artifacts in a readable format.",
    )
    parser.add_argument(
        "summary_path",
        type=Path,
        help="Path to an MLB live summary JSON artifact.",
    )
    parser.add_argument(
        "--tag",
        choices=("conservative", "fullsend"),
        default=None,
        help="Optional slip set to print. Defaults to all saved sets.",
    )
    return parser.parse_args()


def main() -> str:
    """Load a summary artifact and print its saved slips.

    Returns:
        Rendered slip report text.
    """

    args = parse_args()
    summary = load_summary(Path(args.summary_path))
    tags = (args.tag,) if args.tag else None
    report = render_slip_report(summary, tags=tags)
    print(report)
    return report


if __name__ == "__main__":  # pragma: no cover - script entrypoint
    main()
