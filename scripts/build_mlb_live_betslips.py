#!/usr/bin/env python
# ruff: noqa: I001, E402
"""Run the MLB live Underdog shadow workflow and write JSON slip artifacts."""

from __future__ import annotations

import argparse
import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.config import ConfigValidationError, load_pipeline_config
from src.core.contracts import PipelineConfig
from src.mlb.data.underdog import import_ud_mlb_lines
from src.mlb.pitcher_props.descriptors import STAT_DESCRIPTORS
from src.mlb.pitcher_props.live_lines import write_live_pitcher_prop_snapshot
from src.mlb.pitcher_props.slate import run_mlb_pitcher_prop_slate
from src.mlb.slips import SlipBuilderConfig, build_slip_sets

DEFAULT_CONFIG_PATH = "config/mlb.yaml"
DEFAULT_OUTPUT_DIR = Path("betslips/mlb_live")
DEFAULT_SNAPSHOT_DIR = Path("data/lines")
DEFAULT_PREFIX = "mlb_live"


def _parse_stat_id_pairs(stat_id_args: list[str]) -> dict[str, str]:
    """Return a stat-id mapping parsed from ``STAT=PickemStat_*`` CLI args."""

    mapping: dict[str, str] = {}
    for raw in stat_id_args:
        stat, separator, algolia_object_id = raw.partition("=")
        if not separator:
            raise ValueError(
                "Invalid --stat-id value: expected STAT=PickemStat_<stat-id>."
            )

        stat_name = stat.strip().lower()
        object_id = algolia_object_id.strip()
        if not stat_name or not object_id:
            raise ValueError(
                "Invalid --stat-id value: expected STAT=PickemStat_<stat-id>."
            )
        if stat_name in mapping:
            raise ValueError(f"Duplicate --stat-id entry for stat '{stat_name}'.")
        mapping[stat_name] = object_id

    return mapping


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the MLB live shadow workflow."""

    parser = argparse.ArgumentParser(
        description="Build MLB live Underdog shadow-run bet slips.",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help="Path to the MLB config file used for stat pipelines.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where JSON slip artifacts are written.",
    )
    parser.add_argument(
        "--snapshot-dir",
        type=Path,
        default=DEFAULT_SNAPSHOT_DIR,
        help="Directory where dated live line snapshots are written.",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default=DEFAULT_PREFIX,
        help="Filename prefix for generated JSON artifacts.",
    )
    parser.add_argument(
        "--retrain",
        action="store_true",
        help="Retrain each stat model before scoring the live slate.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=12,
        help="Number of top candidate legs to keep before slip generation.",
    )
    parser.add_argument(
        "--conservative-count",
        type=int,
        default=3,
        help="Maximum number of conservative slips to emit.",
    )
    parser.add_argument(
        "--fullsend-count",
        type=int,
        default=5,
        help="Maximum number of full-send slips to emit.",
    )
    parser.add_argument(
        "--stat-id",
        action="append",
        dest="stat_ids",
        default=[],
        metavar="STAT=PickemStat_ID",
        help="Map an MLB stat to its Underdog PickemStat id. Repeat per stat.",
    )
    return parser.parse_args()


def write_slip_artifacts(
    *,
    output_dir: Path,
    prefix: str,
    target_date: str,
    slip_sets: dict[str, list[dict[str, Any]]],
    payload_common: dict[str, Any],
) -> dict[str, Path]:
    """Write JSON slip artifacts and return their paths."""

    output_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    for tag, slips in slip_sets.items():
        path = output_dir / f"{prefix}_{target_date}_{tag}.json"
        payload = {**payload_common, "slips": slips}
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        paths[tag] = path
    return paths


def _target_date_from_frame(frame: pd.DataFrame) -> str:
    """Return the target slate date from scored or fetched rows."""

    if "scheduled_at" in frame.columns:
        dates = pd.to_datetime(frame["scheduled_at"], errors="coerce").dropna()
        if not dates.empty:
            return dates.min().date().isoformat()
    if "game_date" in frame.columns:
        dates = pd.to_datetime(frame["game_date"], errors="coerce").dropna()
        if not dates.empty:
            return dates.min().date().isoformat()
    return datetime.utcnow().date().isoformat()


def _is_expected_fetch_error(exc: Exception) -> bool:
    """Return whether a live-line fetch failure should be recorded per stat."""

    if isinstance(exc, json.JSONDecodeError):
        return True
    if isinstance(exc, RuntimeError):
        return str(exc).startswith("Underdog API request failed with status ")
    return isinstance(exc, OSError)


def run_live_shadow_workflow(args: argparse.Namespace) -> dict[str, Any]:
    """Fetch live lines, score the slate, and emit JSON artifacts."""

    stat_ids = _parse_stat_id_pairs(list(args.stat_ids))
    if not stat_ids:
        raise ValueError("At least one --stat-id mapping is required.")
    missing_stats = sorted(stat for stat in stat_ids if stat not in STAT_DESCRIPTORS)
    if missing_stats:
        supported = ", ".join(sorted(STAT_DESCRIPTORS))
        raise ValueError(
            "Unsupported MLB stat-id mapping(s): "
            f"{missing_stats}. Supported stats: {supported}"
        )

    snapshot_paths: dict[str, Path] = {}
    stat_configs: dict[str, PipelineConfig] = {}
    skipped_stats: dict[str, str] = {}
    failed_stats: dict[str, str] = {}
    target_date: str | None = None

    for stat, algolia_object_id in stat_ids.items():
        try:
            live_lines = import_ud_mlb_lines(algolia_object_id)
        except Exception as exc:  # pragma: no cover - exercised in regression test
            if not _is_expected_fetch_error(exc):
                raise
            failed_stats[stat] = str(exc)
            continue

        if live_lines.empty:
            skipped_stats[stat] = "no live lines returned"
            continue

        line_date = _target_date_from_frame(live_lines)
        target_date = line_date if target_date is None else min(target_date, line_date)
        try:
            snapshot_path = write_live_pitcher_prop_snapshot(
                live_lines,
                stat,
                output_dir=args.snapshot_dir,
                snapshot_date=line_date,
            )
        except OSError as exc:  # pragma: no cover - exercised in regression test
            failed_stats[stat] = str(exc)
            continue
        try:
            loaded = load_pipeline_config(
                args.config,
                sport_override="mlb",
                stat_override=stat,
            )
        except (
            OSError,
            ConfigValidationError,
            ValueError,
        ) as exc:  # pragma: no cover - exercised in regression test
            failed_stats[stat] = str(exc)
            continue

        snapshot_paths[stat] = snapshot_path
        stat_configs[stat] = replace(
            loaded,
            section={**loaded.section, "lines_path": str(snapshot_path)},
        )

    slate_result = run_mlb_pitcher_prop_slate(
        stat_configs,
        retrain=bool(args.retrain),
        stats=tuple(stat_configs),
    )

    skipped_stats.update(slate_result.skipped_stats)
    failed_stats.update(slate_result.failed_stats)
    if target_date is None:
        target_date = datetime.utcnow().date().isoformat()

    slip_cfg = SlipBuilderConfig(
        top_n=int(args.top_n),
        conservative_count=int(args.conservative_count),
        fullsend_count=int(args.fullsend_count),
    )
    slip_sets = build_slip_sets(slate_result.combined_frame, config=slip_cfg)
    slip_paths = write_slip_artifacts(
        output_dir=args.output_dir,
        prefix=args.prefix,
        target_date=target_date,
        slip_sets=slip_sets,
        payload_common={
            "generated_at": datetime.utcnow().isoformat(timespec="seconds"),
            "config_path": str(args.config),
            "retrain": bool(args.retrain),
        },
    )

    summary = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds"),
        "config_path": str(args.config),
        "target_date": target_date,
        "stat_ids": stat_ids,
        "completed_stats": list(slate_result.completed_stats),
        "skipped_stats": skipped_stats,
        "failed_stats": failed_stats,
        "snapshot_files": {stat: str(path) for stat, path in snapshot_paths.items()},
        "slip_files": {tag: str(path) for tag, path in slip_paths.items()},
        "slip_counts": {tag: len(slips) for tag, slips in slip_sets.items()},
        "combined_rows": int(len(slate_result.combined_frame)),
        "output_dir": str(args.output_dir),
        "snapshot_dir": str(args.snapshot_dir),
    }

    summary_path = args.output_dir / f"{args.prefix}_{target_date}_summary.json"
    summary["summary_file"] = str(summary_path)
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return summary


def main() -> dict[str, Any]:
    """Run the MLB live shadow workflow."""

    args = parse_args()
    summary = run_live_shadow_workflow(args)
    print(
        "MLB live shadow run",
        {
            "target_date": summary["target_date"],
            "completed_stats": summary["completed_stats"],
            "combined_rows": summary["combined_rows"],
            "slips": summary["slip_counts"],
            "output_dir": summary["output_dir"],
        },
    )
    return summary


if __name__ == "__main__":  # pragma: no cover - script entrypoint
    main()
