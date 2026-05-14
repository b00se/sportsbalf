#!/usr/bin/env python
# ruff: noqa: I001
"""Generate conservative and full-send bet slips from the MLB pipeline output."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _derive_target_date(df: pd.DataFrame) -> datetime:
    if "upcoming_game_date" in df.columns:
        dates = pd.to_datetime(df["upcoming_game_date"].dropna())
        if not dates.empty:
            return dates.min().to_pydatetime()
    return datetime.utcnow()


def main() -> None:
    from scripts.build_mlb_live_betslips import write_slip_artifacts
    from src.mlb.pipeline import run
    from src.mlb.slips import SlipBuilderConfig, build_slip_sets

    parser = argparse.ArgumentParser(description="Build bet slips from MLB predictions")
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Optional path to override the default MLB config",
    )
    parser.add_argument(
        "--retrain",
        action="store_true",
        help="Retrain the XGBoost model before generating slips",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=12,
        help="Number of top legs to consider when generating slips",
    )
    parser.add_argument(
        "--conservative-count",
        type=int,
        default=3,
        help="Maximum number of conservative slips to emit",
    )
    parser.add_argument(
        "--fullsend-count",
        type=int,
        default=5,
        help="Maximum number of full-send slips to emit",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("betslips"),
        help="Directory to write JSON slip files",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default="ud_slips",
        help="Filename prefix for generated slip files",
    )

    args = parser.parse_args()

    results = run(config_path=args.config, retrain=args.retrain)

    cfg = SlipBuilderConfig(
        top_n=args.top_n,
        conservative_count=args.conservative_count,
        fullsend_count=args.fullsend_count,
    )
    slip_sets = build_slip_sets(results, config=cfg)

    target_date = _derive_target_date(results).date().isoformat()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    paths = write_slip_artifacts(
        output_dir=output_dir,
        prefix=args.prefix,
        target_date=target_date,
        slip_sets=slip_sets,
        payload_common={
            "generated_at": datetime.utcnow().isoformat(timespec="seconds"),
            "config_path": args.config,
            "retrained": args.retrain,
        },
    )

    for tag, slips in slip_sets.items():
        print(f"Wrote {len(slips)} {tag} slips to {paths[tag]}")


if __name__ == "__main__":
    main()
