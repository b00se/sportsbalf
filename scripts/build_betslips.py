#!/usr/bin/env python
"""Generate bet slips from MLB and NFL pipelines."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.mlb.pipeline import run as run_mlb_pipeline
from src.mlb.slips import SlipBuilderConfig, build_slip_sets, prepare_long_df as prepare_mlb_long
from src.nfl.pipeline import run as run_nfl_pipeline
from src.nfl.slips import prepare_long_df as prepare_nfl_long


def _derive_target_date(df: pd.DataFrame) -> datetime:
    for column in ("game_date", "upcoming_game_date", "scheduled_at"):
        if column in df.columns:
            dates = pd.to_datetime(df[column].dropna(), errors="coerce")
            if not dates.empty:
                return dates.min().to_pydatetime()
    return datetime.utcnow()


def _write_payload(
    *,
    slips: Dict[str, List[dict]],
    output_dir: Path,
    prefix: str,
    target_date: datetime,
    config_path: str | None,
    retrained: bool,
    stdout: bool,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    date_token = target_date.date().isoformat()

    for tag, entries in slips.items():
        payload = {
            "generated_at": datetime.utcnow().isoformat(timespec="seconds"),
            "config_path": config_path,
            "retrained": retrained,
            "slips": entries,
        }
        filename = f"{prefix}_{date_token}_{tag}.json"
        path = output_dir / filename
        path.write_text(json.dumps(payload, indent=2))
        print(f"Wrote {len(entries)} {tag} slips to {path}")
        if stdout:
            print(json.dumps(payload, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Build bet slips from MLB and NFL predictions")
    parser.add_argument(
        "--sports",
        nargs="+",
        choices=["mlb", "nfl"],
        default=["mlb"],
        help="Sports to include when building slips",
    )
    parser.add_argument(
        "--mlb-config",
        type=str,
        default=None,
        help="Optional path to override the default MLB config",
    )
    parser.add_argument(
        "--nfl-config",
        type=str,
        default=None,
        help="Optional path to override the default NFL config",
    )
    parser.add_argument(
        "--retrain",
        action="store_true",
        help="Retrain models before generating slips",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=12,
        help="Number of top legs to consider when generating slips",
    )
    parser.add_argument(
        "--min-ev",
        type=float,
        default=0.0,
        help="Minimum leg EV required to include a prop",
    )
    parser.add_argument(
        "--conservative-count",
        type=int,
        default=3,
        help="Maximum number of conservative slips to emit per set",
    )
    parser.add_argument(
        "--fullsend-count",
        type=int,
        default=5,
        help="Maximum number of full-send slips to emit per set",
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
    parser.add_argument(
        "--combine",
        action="store_true",
        help="When multiple sports are selected, also generate mixed slips",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Print slip payloads to stdout in addition to writing files",
    )

    args = parser.parse_args()

    selected_sports = {sport.lower() for sport in args.sports}

    cfg = SlipBuilderConfig(
        top_n=args.top_n,
        conservative_count=args.conservative_count,
        fullsend_count=args.fullsend_count,
        min_leg_ev=args.min_ev,
    )

    long_frames: Dict[str, pd.DataFrame] = {}
    results_map: Dict[str, pd.DataFrame] = {}

    if "mlb" in selected_sports:
        mlb_results = run_mlb_pipeline(config_path=args.mlb_config, retrain=args.retrain)
        if not mlb_results.empty:
            long_frames["mlb"] = prepare_mlb_long(mlb_results, top_n=None, min_ev=0.0)
            results_map["mlb"] = mlb_results
        else:
            print("MLB pipeline returned no lines; skipping MLB slips")

    if "nfl" in selected_sports:
        nfl_results = run_nfl_pipeline(config_path=args.nfl_config, retrain=args.retrain)
        if not nfl_results.empty:
            long_frames["nfl"] = prepare_nfl_long(nfl_results, top_n=None, min_ev=0.0)
            results_map["nfl"] = nfl_results
        else:
            print("NFL pipeline returned no lines; skipping NFL slips")

    if not long_frames:
        print("No eligible props found for the requested sports")
        return

    for sport, long_df in long_frames.items():
        slip_sets = build_slip_sets(long_df=long_df, config=cfg)
        if not slip_sets["conservative"] and not slip_sets["fullsend"]:
            print(f"No qualifying slips produced for {sport.upper()}; skipping output")
            continue
        target_date = _derive_target_date(results_map[sport])
        _write_payload(
            slips=slip_sets,
            output_dir=args.output_dir,
            prefix=f"{args.prefix}_{sport}",
            target_date=target_date,
            config_path=args.mlb_config if sport == "mlb" else args.nfl_config,
            retrained=args.retrain,
            stdout=args.stdout,
        )

    if args.combine and len(long_frames) > 1:
        combined_long = pd.concat(long_frames.values(), ignore_index=True)
        combined_slips = build_slip_sets(long_df=combined_long, config=cfg)
        if combined_slips["conservative"] or combined_slips["fullsend"]:
            combined_results = pd.concat(results_map.values(), ignore_index=True)
            _write_payload(
                slips=combined_slips,
                output_dir=args.output_dir,
                prefix=f"{args.prefix}_multi",
                target_date=_derive_target_date(combined_results),
                config_path="combined",
                retrained=args.retrain,
                stdout=args.stdout,
            )
        else:
            print("No qualifying mixed-sport slips produced")


if __name__ == "__main__":
    main()
