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

from src.core.config import (
    ConfigValidationError,
    extract_mlb_live_underdog_config,
    load_pipeline_config,
)
from src.core.contracts import PipelineConfig
from src.mlb.data.underdog import import_ud_mlb_lines
from src.mlb.pitcher_props.descriptors import STAT_DESCRIPTORS
from src.mlb.pitcher_props.live_lines import write_live_pitcher_prop_snapshot
from src.mlb.pitcher_props.slate import run_mlb_pitcher_prop_slate
from src.mlb.slips import SlipBuilderConfig, build_slip_sets, prepare_long_df

DEFAULT_CONFIG_PATH = "config/mlb.yaml"
DEFAULT_OUTPUT_DIR = Path("betslips/mlb_live")
DEFAULT_SNAPSHOT_DIR = Path("data/lines")
DEFAULT_PREFIX = "mlb_live"
DEFAULT_MODE = "proof"
STAT_MIX_DOMINANCE_THRESHOLD = 0.70
PROBABILITY_MIN_THRESHOLD = 0.20
PROBABILITY_MAX_THRESHOLD = 0.80
EV_MAX_THRESHOLD = 0.35
STAT_CONFIDENCE_THRESHOLDS: dict[str, dict[str, float]] = {
    "strikeouts": {
        "prob_min": PROBABILITY_MIN_THRESHOLD,
        "prob_max": PROBABILITY_MAX_THRESHOLD,
        "ev_max": EV_MAX_THRESHOLD,
    },
    "outs_recorded": {"prob_min": 0.15, "prob_max": 0.90, "ev_max": 0.60},
    "earned_runs": {"prob_min": 0.15, "prob_max": 0.88, "ev_max": 0.55},
    "hits_allowed": {"prob_min": 0.10, "prob_max": 0.93, "ev_max": 0.85},
    "bb_allowed": {"prob_min": 0.15, "prob_max": 0.88, "ev_max": 0.55},
}


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
        "--mode",
        choices=("proof", "debug"),
        default=DEFAULT_MODE,
        help=(
            "Run mode: proof requires full supported-market coverage; "
            "debug allows subsets."
        ),
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


def _resolve_stat_ids(
    config_path: str, stat_id_args: list[str], mode: str
) -> dict[str, str]:
    """Resolve live stat ids from config defaults plus CLI overrides."""

    cli_stat_ids = _parse_stat_id_pairs(list(stat_id_args))
    loaded = load_pipeline_config(
        config_path,
        sport_override="mlb",
        stat_override="strikeouts",
    )
    live_underdog = extract_mlb_live_underdog_config(loaded)
    stat_ids = {
        stat.strip().lower(): object_id
        for stat, object_id in live_underdog.stat_ids.items()
        if str(object_id).strip()
    }
    stat_ids.update(cli_stat_ids)

    missing_stats = sorted(stat for stat in stat_ids if stat not in STAT_DESCRIPTORS)
    if missing_stats:
        supported = ", ".join(sorted(STAT_DESCRIPTORS))
        raise ValueError(
            "Unsupported MLB stat-id mapping(s): "
            f"{missing_stats}. Supported stats: {supported}"
        )

    if mode == "proof":
        required_stats = tuple(sorted(STAT_DESCRIPTORS))
        missing_required = [stat for stat in required_stats if stat not in stat_ids]
        if missing_required:
            raise ValueError(
                "Missing required MLB stat-id mapping(s) for proof mode: "
                f"{missing_required}"
            )

    if not stat_ids:
        raise ValueError("At least one MLB stat-id mapping is required.")

    return stat_ids


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


def _humanize_stat(stat_id: object) -> str:
    """Return a human-readable stat label."""

    stat_text = str(stat_id or "").strip()
    if not stat_text:
        return "stat"
    return stat_text.replace("_", " ")


def _format_decimal(value: object, digits: int = 2) -> str:
    """Return a compact decimal string for numeric values."""

    numeric = pd.to_numeric([value], errors="coerce")[0]
    if pd.isna(numeric):
        return "n/a"
    text = f"{float(numeric):.{digits}f}"
    return text.rstrip("0").rstrip(".")


def _format_percent(value: object) -> str:
    """Return a percentage string for a probability value."""

    numeric = pd.to_numeric([value], errors="coerce")[0]
    if pd.isna(numeric):
        return "n/a"
    return f"{float(numeric) * 100:.1f}%"


def _format_signed_decimal(value: object, digits: int = 2) -> str:
    """Return a signed decimal string for numeric values."""

    numeric = pd.to_numeric([value], errors="coerce")[0]
    if pd.isna(numeric):
        return "n/a"
    return f"{float(numeric):+.{digits}f}"


def _predicted_value_for_leg(leg: dict[str, Any]) -> object:
    """Return the best available model prediction for a slip leg."""

    prediction_column = (
        f"predicted_{str(leg.get('stat_id', '')).strip().lower()}" if leg else ""
    )
    if prediction_column and prediction_column in leg:
        return leg.get(prediction_column)
    return leg.get("predicted_value")


def _format_leg_line(leg: dict[str, Any], index: int) -> str:
    """Return a single human-readable line for a slip leg."""

    player = str(leg.get("player") or leg.get("pitcher_name") or "Unknown player")
    team = str(leg.get("team") or leg.get("pitcher_team") or "?")
    opponent = str(leg.get("opponent") or leg.get("upcoming_opponent") or "?")
    stat = _humanize_stat(leg.get("stat_id") or leg.get("market"))
    play = str(leg.get("play") or "").lower()
    line = _format_decimal(leg.get("line"), digits=1)
    predicted = _format_decimal(_predicted_value_for_leg(leg))
    prob = _format_percent(leg.get("prob"))
    ev = _format_signed_decimal(leg.get("ev"))
    return (
        f"{index}. {player} ({team}) vs {opponent}: {stat} {play} {line} "
        f"[model {predicted}, win {prob}, ev {ev}]"
    )


def render_slip_report(summary: dict[str, Any]) -> str:
    """Return a human-readable report for generated slip artifacts."""

    lines: list[str] = []
    slip_files = summary.get("slip_files", {})
    for tag in ("conservative", "fullsend"):
        section_title = f"{tag.upper()} SLIPS"
        lines.append(section_title)

        path_text = slip_files.get(tag)
        if not path_text:
            lines.append("No artifact file recorded.")
            lines.append("")
            continue

        path = Path(path_text)
        if not path.exists():
            lines.append(f"Missing artifact: {path}")
            lines.append("")
            continue

        payload = json.loads(path.read_text(encoding="utf-8"))
        slips = payload.get("slips", [])
        if not slips:
            lines.append("None.")
            lines.append("")
            continue

        for slip_index, slip in enumerate(slips, start=1):
            lines.append(
                " | ".join(
                    [
                        f"Slip {slip_index}",
                        f"{int(slip.get('slip_size', len(slip.get('legs', []))))} legs",
                        f"units {slip.get('units', 1)}",
                        f"win {_format_percent(slip.get('p_win'))}",
                        f"total ev {_format_signed_decimal(slip.get('total_ev'))}",
                    ]
                )
            )
            for leg_index, leg in enumerate(slip.get("legs", []), start=1):
                lines.append(_format_leg_line(leg, leg_index))
            lines.append("")

    return "\n".join(lines).rstrip()


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


def _slip_eligible_pool(
    combined_frame: pd.DataFrame, slip_cfg: SlipBuilderConfig
) -> pd.DataFrame:
    """Return the ranked slip-eligible candidate pool."""

    return prepare_long_df(
        combined_frame,
        top_n=int(slip_cfg.top_n),
        min_ev=float(slip_cfg.min_leg_ev),
    )


def _stat_mix(pool: pd.DataFrame) -> dict[str, float]:
    """Return stat share ratios for the slip-eligible pool."""

    if pool.empty or "stat_id" not in pool.columns:
        return {}

    counts = pool["stat_id"].astype(str).value_counts(normalize=True).sort_index()
    return {stat: float(share) for stat, share in counts.items()}


def _probability_extremes(pool: pd.DataFrame) -> dict[str, float | None]:
    """Return min/max probability values for the slip-eligible pool."""

    if pool.empty or "prob" not in pool.columns:
        return {"max": None, "min": None}

    probs = pd.to_numeric(pool["prob"], errors="coerce").dropna()
    if probs.empty:
        return {"max": None, "min": None}
    return {"max": float(probs.max()), "min": float(probs.min())}


def _ev_extreme(pool: pd.DataFrame) -> dict[str, float | None]:
    """Return the maximum EV value for the slip-eligible pool."""

    if pool.empty or "ev" not in pool.columns:
        return {"max": None}

    evs = pd.to_numeric(pool["ev"], errors="coerce").dropna()
    if evs.empty:
        return {"max": None}
    return {"max": float(evs.max())}


def _confidence_thresholds_by_stat() -> dict[str, dict[str, float]]:
    """Return configured confidence thresholds keyed by stat."""

    return {
        stat: dict(thresholds)
        for stat, thresholds in STAT_CONFIDENCE_THRESHOLDS.items()
    }


def _confidence_gate_offenders(pool: pd.DataFrame) -> list[dict[str, Any]]:
    """Return slip-eligible rows that violate stat-specific confidence bounds."""

    if pool.empty:
        return []

    thresholds_by_stat = _confidence_thresholds_by_stat()
    offenders: list[dict[str, Any]] = []
    for row in pool.itertuples(index=False):
        stat_id = str(getattr(row, "stat_id", "")).strip().lower()
        thresholds = thresholds_by_stat.get(
            stat_id,
            {
                "prob_min": PROBABILITY_MIN_THRESHOLD,
                "prob_max": PROBABILITY_MAX_THRESHOLD,
                "ev_max": EV_MAX_THRESHOLD,
            },
        )
        prob = pd.to_numeric([getattr(row, "prob", pd.NA)], errors="coerce")[0]
        ev = pd.to_numeric([getattr(row, "ev", pd.NA)], errors="coerce")[0]

        reasons: list[str] = []
        if pd.notna(prob) and float(prob) <= float(thresholds["prob_min"]):
            reasons.append("prob_below_min")
        if pd.notna(prob) and float(prob) >= float(thresholds["prob_max"]):
            reasons.append("prob_above_max")
        if pd.notna(ev) and float(ev) >= float(thresholds["ev_max"]):
            reasons.append("ev_above_max")
        if not reasons:
            continue

        line = pd.to_numeric([getattr(row, "line", pd.NA)], errors="coerce")[0]
        offenders.append(
            {
                "player": str(getattr(row, "player", "")),
                "stat_id": stat_id,
                "line": float(line) if pd.notna(line) else None,
                "prob": float(prob) if pd.notna(prob) else None,
                "ev": float(ev) if pd.notna(ev) else None,
                "reasons": reasons,
                "thresholds": thresholds,
            }
        )
    return offenders


def _gate_failures(pool: pd.DataFrame) -> tuple[list[str], list[dict[str, Any]]]:
    """Return proof-mode sanity gate failures for the slip-eligible pool."""

    failures: list[str] = []
    mix = _stat_mix(pool)
    if mix and max(mix.values()) > STAT_MIX_DOMINANCE_THRESHOLD:
        failures.append("stat_mix_gate_failed")

    confidence_offenders = _confidence_gate_offenders(pool)
    if confidence_offenders:
        failures.append("confidence_gate_failed")

    return failures, confidence_offenders


def _summary_outcome(
    *,
    mode: str,
    combined_rows: int,
    slip_counts: dict[str, int],
    gate_failures: list[str],
) -> tuple[str, list[str]]:
    """Return outcome tag plus failure reasons for the run summary."""

    has_valid_slips = any(count > 0 for count in slip_counts.values())
    if mode == "proof" and gate_failures:
        return ("failed", gate_failures)
    if combined_rows <= 0:
        return ("failed", ["runtime_failure"])
    if not has_valid_slips:
        return ("no_play_slate", [])
    return ("passed", [])


def run_live_shadow_workflow(args: argparse.Namespace) -> dict[str, Any]:
    """Fetch live lines, score the slate, and emit JSON artifacts."""

    mode = str(getattr(args, "mode", DEFAULT_MODE)).strip().lower() or DEFAULT_MODE
    stat_ids = _resolve_stat_ids(args.config, list(args.stat_ids), mode)

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
    slip_eligible_pool = _slip_eligible_pool(slate_result.combined_frame, slip_cfg)
    slip_sets = build_slip_sets(slate_result.combined_frame, config=slip_cfg)
    slip_counts = {tag: len(slips) for tag, slips in slip_sets.items()}
    gate_failures, confidence_offenders = _gate_failures(slip_eligible_pool)
    outcome, failure_reasons = _summary_outcome(
        mode=mode,
        combined_rows=int(len(slate_result.combined_frame)),
        slip_counts=slip_counts,
        gate_failures=gate_failures,
    )
    warnings = gate_failures if mode == "debug" else []
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
        "mode": mode,
        "config_path": str(args.config),
        "target_date": target_date,
        "stat_ids": stat_ids,
        "outcome": outcome,
        "failure_reasons": failure_reasons,
        "warnings": warnings,
        "completed_stats": list(slate_result.completed_stats),
        "skipped_stats": skipped_stats,
        "failed_stats": failed_stats,
        "snapshot_files": {stat: str(path) for stat, path in snapshot_paths.items()},
        "slip_files": {tag: str(path) for tag, path in slip_paths.items()},
        "slip_counts": slip_counts,
        "combined_rows": int(len(slate_result.combined_frame)),
        "slip_eligible_rows": int(len(slip_eligible_pool)),
        "stat_mix": _stat_mix(slip_eligible_pool),
        "probability_extremes": _probability_extremes(slip_eligible_pool),
        "ev_extreme": _ev_extreme(slip_eligible_pool),
        "confidence_thresholds": _confidence_thresholds_by_stat(),
        "confidence_gate_offenders": confidence_offenders,
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
    print(render_slip_report(summary))
    return summary


if __name__ == "__main__":  # pragma: no cover - script entrypoint
    main()
