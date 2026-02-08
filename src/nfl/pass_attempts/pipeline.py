"""NFL pass attempts pipeline adapter for the core sport/stat contract."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.core.contracts import (
    ModelBundle,
    PipelineConfig,
    PipelineInputs,
    SportStatPipeline,
)
from src.nfl.pipeline import run_pass_attempts_pipeline


@dataclass(slots=True)
class NflPassAttemptsPipeline(SportStatPipeline):
    """Adapter that preserves NFL behavior behind the modular contract."""

    retrain: bool = False

    def load_inputs(self, config: PipelineConfig) -> PipelineInputs:
        return PipelineInputs(payload={"config_path": str(config.config_path)})

    def build_training_frame(
        self,
        inputs: PipelineInputs,
        config: PipelineConfig,
    ) -> pd.DataFrame:
        del inputs, config
        return pd.DataFrame()

    def train_or_load_model(
        self,
        frame: pd.DataFrame,
        config: PipelineConfig,
        retrain: bool,
    ) -> ModelBundle:
        del frame, config
        self.retrain = retrain
        return ModelBundle(payload={})

    def predict_lines(
        self,
        inputs: PipelineInputs,
        model_bundle: ModelBundle,
        config: PipelineConfig,
    ) -> pd.DataFrame:
        del inputs, model_bundle, config
        return pd.DataFrame()

    def simulate(
        self,
        predictions: pd.DataFrame,
        model_bundle: ModelBundle,
        config: PipelineConfig,
    ) -> pd.DataFrame:
        del predictions, model_bundle
        return run_pass_attempts_pipeline(config=config, retrain=self.retrain)
