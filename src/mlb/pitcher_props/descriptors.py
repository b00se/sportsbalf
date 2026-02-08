"""Stat descriptors for MLB pitcher prop pipelines."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class StatDescriptor:
    """Static metadata that drives one pitcher-prop stat pipeline.

    Args:
        stat: Registry stat identifier.
        target_col: Training target column in pitcher-game frames.
        line_col: Market line column in line files.
        prediction_col: Final prediction column in outputs.
        park_factor_col: Park-factor feature column name.
        opponent_feature_col: Opponent tendency feature column name.
    """

    stat: str
    target_col: str
    line_col: str
    prediction_col: str
    park_factor_col: str
    opponent_feature_col: str


STAT_DESCRIPTORS: dict[str, StatDescriptor] = {
    "strikeouts": StatDescriptor(
        stat="strikeouts",
        target_col="strikeouts",
        line_col="k_line",
        prediction_col="predicted_strikeouts",
        park_factor_col="park_factor_K",
        opponent_feature_col="opponent_k_rate",
    ),
    "outs_recorded": StatDescriptor(
        stat="outs_recorded",
        target_col="outs_recorded",
        line_col="outs_line",
        prediction_col="predicted_outs_recorded",
        park_factor_col="park_factor_outs",
        opponent_feature_col="opponent_out_rate",
    ),
    "earned_runs": StatDescriptor(
        stat="earned_runs",
        target_col="earned_runs",
        line_col="er_line",
        prediction_col="predicted_earned_runs",
        park_factor_col="park_factor_runs",
        opponent_feature_col="opponent_run_rate",
    ),
    "hits_allowed": StatDescriptor(
        stat="hits_allowed",
        target_col="hits_allowed",
        line_col="hits_line",
        prediction_col="predicted_hits_allowed",
        park_factor_col="park_factor_hits",
        opponent_feature_col="opponent_hit_rate",
    ),
    "bb_allowed": StatDescriptor(
        stat="bb_allowed",
        target_col="bb_allowed",
        line_col="bb_line",
        prediction_col="predicted_bb_allowed",
        park_factor_col="park_factor_bb",
        opponent_feature_col="opponent_bb_rate",
    ),
}


def get_stat_descriptor(stat: str) -> StatDescriptor:
    """Return a stat descriptor for the provided registry stat name.

    Args:
        stat: Pipeline stat key.

    Returns:
        Resolved stat descriptor.

    Raises:
        KeyError: If the stat is unsupported.
    """

    key = stat.strip().lower()
    if key not in STAT_DESCRIPTORS:
        raise KeyError(f"Unsupported MLB pitcher-prop stat: {stat}")
    return STAT_DESCRIPTORS[key]
