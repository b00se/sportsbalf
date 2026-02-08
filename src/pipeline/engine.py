"""Sport/stat orchestration engine."""

from __future__ import annotations

from src.core.config import load_pipeline_config
from src.core.registry import get_pipeline, register_pipeline
from src.mlb.pitcher_props.adapter import MlbPitcherPropsPipeline
from src.mlb.strikeouts.pipeline import MlbStrikeoutsPipeline
from src.nfl.pass_attempts.pipeline import NflPassAttemptsPipeline


def _ensure_default_registrations() -> None:
    register_pipeline("mlb", "strikeouts", MlbStrikeoutsPipeline)
    register_pipeline("mlb", "outs_recorded", MlbPitcherPropsPipeline)
    register_pipeline("mlb", "earned_runs", MlbPitcherPropsPipeline)
    register_pipeline("mlb", "hits_allowed", MlbPitcherPropsPipeline)
    register_pipeline("mlb", "bb_allowed", MlbPitcherPropsPipeline)
    register_pipeline("nfl", "pass_attempts", NflPassAttemptsPipeline)


def run_pipeline(config_path: str, retrain: bool = False):
    """Run a configured sport/stat pipeline and return predictions."""

    _ensure_default_registrations()
    config = load_pipeline_config(config_path)
    pipeline = get_pipeline(config.sport, config.stat)

    inputs = pipeline.load_inputs(config)
    training_frame = pipeline.build_training_frame(inputs, config)
    model_bundle = pipeline.train_or_load_model(training_frame, config, retrain)
    predictions = pipeline.predict_lines(inputs, model_bundle, config)
    return pipeline.simulate(predictions, model_bundle, config)


def run_pipeline_with_overrides(
    config_path: str,
    *,
    sport: str,
    stat: str,
    retrain: bool = False,
):
    """Run a pipeline while overriding sport/stat from CLI args."""

    _ensure_default_registrations()
    config = load_pipeline_config(
        config_path,
        sport_override=sport,
        stat_override=stat,
    )
    pipeline = get_pipeline(config.sport, config.stat)

    inputs = pipeline.load_inputs(config)
    training_frame = pipeline.build_training_frame(inputs, config)
    model_bundle = pipeline.train_or_load_model(training_frame, config, retrain)
    predictions = pipeline.predict_lines(inputs, model_bundle, config)
    return pipeline.simulate(predictions, model_bundle, config)
