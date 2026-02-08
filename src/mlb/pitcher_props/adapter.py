"""Contract adapter for MLB non-strikeout pitcher props."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.core.contracts import (
    ModelBundle,
    PipelineConfig,
    PipelineInputs,
    SportStatPipeline,
)
from src.mlb.pitcher_props.pipeline import run_mlb_pitcher_prop_pipeline


@dataclass(slots=True)
class MlbPitcherPropsPipeline(SportStatPipeline):
    """Adapter that delegates to the shared MLB pitcher-prop runner."""

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
        return run_mlb_pitcher_prop_pipeline(config=config, retrain=self.retrain)
