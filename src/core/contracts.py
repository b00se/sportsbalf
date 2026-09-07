"""Core contracts for sport/stat pipeline modularization."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import pandas as pd


@dataclass(slots=True)
class PipelineConfig:
    """Validated pipeline configuration payload.

    Attributes:
        config_path: Path to the YAML config source.
        sport: Sport identifier (for example ``mlb`` or ``nfl``).
        stat: Stat-line identifier (for example ``strikeouts``).
        raw: Original parsed YAML document.
        section: Resolved sport/stat section used by the active pipeline.
    """

    config_path: Path
    sport: str
    stat: str
    raw: dict[str, Any]
    section: dict[str, Any]


@dataclass(slots=True)
class PipelineInputs:
    """Pipeline input container for intermediate orchestration state."""

    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ModelBundle:
    """Model artifact container for intermediate orchestration state."""

    payload: dict[str, Any] = field(default_factory=dict)


class SportStatPipeline(Protocol):
    """Common execution contract for each sport/stat implementation."""

    def load_inputs(self, config: PipelineConfig) -> PipelineInputs:
        """Load raw inputs from disk/providers for the requested sport/stat."""
        ...

    def build_training_frame(
        self,
        inputs: PipelineInputs,
        config: PipelineConfig,
    ) -> pd.DataFrame:
        """Assemble model-ready training data from inputs."""
        ...

    def train_or_load_model(
        self,
        frame: pd.DataFrame,
        config: PipelineConfig,
        retrain: bool,
    ) -> ModelBundle:
        """Train or load model artifacts needed for inference."""
        ...

    def predict_lines(
        self,
        inputs: PipelineInputs,
        model_bundle: ModelBundle,
        config: PipelineConfig,
    ) -> pd.DataFrame:
        """Produce stat-line predictions before Monte Carlo simulation."""
        ...

    def simulate(
        self,
        predictions: pd.DataFrame,
        model_bundle: ModelBundle,
        config: PipelineConfig,
    ) -> pd.DataFrame:
        """Apply sport-specific simulation and return final outputs."""
        ...
