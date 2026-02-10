"""Default sport/stat registration catalog and bootstrap helpers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.core.registry import is_registered, register_pipeline
from src.mlb.pitcher_props.adapter import MlbPitcherPropsPipeline
from src.nfl.pass_attempts.pipeline import NflPassAttemptsPipeline

_REQUIRED_STAGE_METHODS: tuple[str, ...] = (
    "load_inputs",
    "build_training_frame",
    "train_or_load_model",
    "predict_lines",
    "simulate",
)

DEFAULT_PIPELINE_REGISTRATIONS: tuple[tuple[str, str, Callable[[], Any]], ...] = (
    ("mlb", "strikeouts", MlbPitcherPropsPipeline),
    ("mlb", "outs_recorded", MlbPitcherPropsPipeline),
    ("mlb", "earned_runs", MlbPitcherPropsPipeline),
    ("mlb", "hits_allowed", MlbPitcherPropsPipeline),
    ("mlb", "bb_allowed", MlbPitcherPropsPipeline),
    ("nfl", "pass_attempts", NflPassAttemptsPipeline),
)


def validate_registration_declaration(
    sport: str,
    stat: str,
    factory: Callable[[], Any],
) -> None:
    """Validate a single pipeline registration declaration.

    Args:
        sport: Sport key in the declaration.
        stat: Stat key in the declaration.
        factory: Factory expected to return a pipeline-like object.

    Raises:
        TypeError: If the declaration is invalid.
    """

    if not callable(factory):
        raise TypeError(f"Factory for {sport}.{stat} must be callable.")

    instance = factory()
    for method_name in _REQUIRED_STAGE_METHODS:
        method = getattr(instance, method_name, None)
        if not callable(method):
            raise TypeError(
                f"Factory for {sport}.{stat} must provide callable '{method_name}'."
            )


def ensure_default_pipeline_registrations() -> None:
    """Register canonical default sport/stat pipelines.

    The function is idempotent and only registers missing sport/stat pairs.
    """

    for sport, stat, factory in DEFAULT_PIPELINE_REGISTRATIONS:
        validate_registration_declaration(sport, stat, factory)
        if not is_registered(sport, stat):
            register_pipeline(sport, stat, factory)
