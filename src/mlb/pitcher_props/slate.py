"""Unified MLB pitcher-prop slate orchestration."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass

import pandas as pd

from src.core.contracts import PipelineConfig
from src.mlb.pitcher_props.descriptors import STAT_DESCRIPTORS
from src.mlb.pitcher_props.pipeline import run_mlb_pitcher_prop_pipeline

logger = logging.getLogger(__name__)

_EMPTY_COMBINED_COLUMNS: tuple[str, ...] = (
    "stat_id",
    "player",
    "predicted_value",
    "prob_over",
    "prob_under",
    "ev_over",
    "ev_under",
    "run_mode",
    "lines_status",
)


@dataclass(slots=True)
class MlbPitcherPropSlateResult:
    """Aggregated result from a multi-stat MLB pitcher-prop slate run."""

    combined_frame: pd.DataFrame
    completed_stats: tuple[str, ...]
    skipped_stats: dict[str, str]
    failed_stats: dict[str, str]


def _supported_stats(stats: Iterable[str] | None = None) -> tuple[str, ...]:
    """Return the stat order to evaluate for a slate run."""

    if stats is None:
        return tuple(STAT_DESCRIPTORS.keys())
    return tuple(stat.strip().lower() for stat in stats)


def _validate_requested_stats(stats: tuple[str, ...]) -> None:
    """Raise if the requested stat list includes unsupported MLB stats."""

    unsupported = sorted(stat for stat in stats if stat not in STAT_DESCRIPTORS)
    if unsupported:
        supported = ", ".join(sorted(STAT_DESCRIPTORS))
        raise ValueError(
            "Unsupported MLB pitcher-prop stat(s): "
            f"{unsupported}. Supported stats: {supported}"
        )


def _tag_scored_frame(frame: pd.DataFrame, stat: str) -> pd.DataFrame:
    """Return a copy of scored rows annotated with the stat identifier."""

    tagged = frame.copy()
    tagged["stat_id"] = stat
    return tagged


def _empty_combined_frame() -> pd.DataFrame:
    """Return a stable empty frame for downstream slate consumers."""

    return pd.DataFrame(columns=_EMPTY_COMBINED_COLUMNS)


def _is_expected_missing_input_error(exc: FileNotFoundError) -> bool:
    """Return whether a file error represents an expected missing input."""

    message = str(exc).strip().lower()
    return message.startswith("pitcher prop lines file not found:") or any(
        token in message for token in ("missing live lines", "missing input")
    )


def run_mlb_pitcher_prop_slate(
    stat_configs: Mapping[str, PipelineConfig | None],
    *,
    scorer: Callable[[PipelineConfig, bool], pd.DataFrame] = (
        run_mlb_pitcher_prop_pipeline
    ),
    retrain: bool = False,
    stats: Iterable[str] | None = None,
) -> MlbPitcherPropSlateResult:
    """Run a multi-stat MLB pitcher-prop slate and combine scored outputs.

    Args:
        stat_configs: Mapping of stat name to validated config, or ``None`` when
            a stat should be skipped.
        scorer: Callable used to score one stat configuration.
        retrain: Whether each scorer should retrain its underlying model.
        stats: Optional explicit stat order. Defaults to all supported stats.

    Returns:
        Aggregated slate result with combined scored rows and run summary.
    """

    requested_stats = _supported_stats(stats)
    _validate_requested_stats(requested_stats)

    completed_stats: list[str] = []
    skipped_stats: dict[str, str] = {}
    failed_stats: dict[str, str] = {}
    scored_frames: list[pd.DataFrame] = []

    for stat in requested_stats:
        config = stat_configs.get(stat)
        if config is None:
            skipped_stats[stat] = "no config provided"
            logger.debug(
                "Skipping MLB pitcher-prop stat '%s': no config provided", stat
            )
            continue

        if config.stat != stat:
            failed_stats[stat] = f"config stat mismatch: {config.stat}"
            logger.warning(
                "Failed MLB pitcher-prop stat '%s': config stat mismatch (%s)",
                stat,
                config.stat,
            )
            continue

        try:
            scored = scorer(config, retrain)
        except FileNotFoundError as exc:
            if _is_expected_missing_input_error(exc):
                skipped_stats[stat] = str(exc)
                logger.debug("Skipping MLB pitcher-prop stat '%s': %s", stat, exc)
                continue
            failed_stats[stat] = str(exc)
            logger.warning("Failed MLB pitcher-prop stat '%s': %s", stat, exc)
            continue
        except Exception as exc:  # pragma: no cover - exercised via regression test
            failed_stats[stat] = str(exc)
            logger.warning("Failed MLB pitcher-prop stat '%s': %s", stat, exc)
            continue

        if scored is None or scored.empty:
            skipped_stats[stat] = "no scored frame returned"
            logger.debug(
                "Skipping MLB pitcher-prop stat '%s': no scored frame returned",
                stat,
            )
            continue

        completed_stats.append(stat)
        scored_frames.append(_tag_scored_frame(scored, stat))

    combined_frame = (
        pd.concat(scored_frames, ignore_index=True, sort=False)
        if scored_frames
        else _empty_combined_frame()
    )

    return MlbPitcherPropSlateResult(
        combined_frame=combined_frame,
        completed_stats=tuple(completed_stats),
        skipped_stats=skipped_stats,
        failed_stats=failed_stats,
    )
