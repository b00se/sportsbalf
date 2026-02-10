"""NHL shots-on-goal adapter for the core sport/stat contract."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.core.contracts import (
    ModelBundle,
    PipelineConfig,
    PipelineInputs,
    SportStatPipeline,
)
from src.nhl.pipeline import run_shots_on_goal_pipeline


@dataclass(slots=True)
class NhlShotsOnGoalPipeline(SportStatPipeline):
    """Adapter that preserves NHL behavior behind the modular contract."""

    retrain: bool = False

    def load_inputs(self, config: PipelineConfig) -> PipelineInputs:
        """Return minimal payload needed by the compatibility adapter flow.

        Args:
            config: Active pipeline configuration.

        Returns:
            Wrapper containing the original config path.
        """

        return PipelineInputs(payload={"config_path": str(config.config_path)})

    def build_training_frame(
        self,
        inputs: PipelineInputs,
        config: PipelineConfig,
    ) -> pd.DataFrame:
        """Return an empty frame to preserve current simulate-only behavior.

        Args:
            inputs: Loaded pipeline inputs.
            config: Active pipeline configuration.

        Returns:
            Empty training frame.
        """

        del inputs, config
        return pd.DataFrame()

    def train_or_load_model(
        self,
        frame: pd.DataFrame,
        config: PipelineConfig,
        retrain: bool,
    ) -> ModelBundle:
        """Capture retrain flag and return an empty model bundle.

        Args:
            frame: Training frame from prior stage.
            config: Active pipeline configuration.
            retrain: Requested retrain behavior from the engine.

        Returns:
            Empty model artifact container.
        """

        del frame, config
        self.retrain = retrain
        return ModelBundle(payload={})

    def predict_lines(
        self,
        inputs: PipelineInputs,
        model_bundle: ModelBundle,
        config: PipelineConfig,
    ) -> pd.DataFrame:
        """Return empty predictions for simulate-only compatibility flow.

        Args:
            inputs: Loaded pipeline inputs.
            model_bundle: Model artifacts from prior stage.
            config: Active pipeline configuration.

        Returns:
            Empty predictions frame.
        """

        del inputs, model_bundle, config
        return pd.DataFrame()

    def simulate(
        self,
        predictions: pd.DataFrame,
        model_bundle: ModelBundle,
        config: PipelineConfig,
    ) -> pd.DataFrame:
        """Delegate NHL execution to the orchestration shim.

        Args:
            predictions: Placeholder predictions frame.
            model_bundle: Placeholder model bundle.
            config: Active pipeline configuration.

        Returns:
            NHL shots-on-goal output DataFrame.
        """

        del predictions, model_bundle
        return run_shots_on_goal_pipeline(config=config, retrain=self.retrain)
