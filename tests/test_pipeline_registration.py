from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from src.core.config import ConfigValidationError, load_pipeline_config
from src.core.contracts import ModelBundle, PipelineConfig, PipelineInputs
from src.core.registry import (
    UnknownPipelineError,
    clear_registry,
    get_pipeline,
    is_registered,
    list_registered_pipelines,
    register_pipeline,
)
from src.pipeline.registration import (
    DEFAULT_PIPELINE_REGISTRATIONS,
    ensure_default_pipeline_registrations,
)


def test_default_pipeline_registration_catalog_is_expected() -> None:
    registered_pairs = {
        (sport, stat) for sport, stat, _factory in DEFAULT_PIPELINE_REGISTRATIONS
    }
    assert registered_pairs == {
        ("mlb", "strikeouts"),
        ("mlb", "outs_recorded"),
        ("mlb", "earned_runs"),
        ("mlb", "hits_allowed"),
        ("mlb", "bb_allowed"),
        ("nfl", "pass_attempts"),
    }


def test_bootstrap_registers_expected_defaults_and_is_idempotent() -> None:
    clear_registry()

    ensure_default_pipeline_registrations()
    first = {(entry.sport, entry.stat) for entry in list_registered_pipelines()}
    ensure_default_pipeline_registrations()
    second = {(entry.sport, entry.stat) for entry in list_registered_pipelines()}

    expected = {
        ("mlb", "strikeouts"),
        ("mlb", "outs_recorded"),
        ("mlb", "earned_runs"),
        ("mlb", "hits_allowed"),
        ("mlb", "bb_allowed"),
        ("nfl", "pass_attempts"),
    }
    assert first == expected
    assert second == expected


def test_discovery_helpers_follow_registry_normalization() -> None:
    clear_registry()
    assert is_registered(" mlb ", " STRIKEOUTS ") is False

    register_pipeline(" mlb ", " STRIKEOUTS ", lambda: object())  # type: ignore[return-value]

    assert is_registered("mlb", "strikeouts") is True
    entries = list_registered_pipelines()
    assert entries[0].sport == "mlb"
    assert entries[0].stat == "strikeouts"


def test_onboarding_dummy_pipeline_minimal_boilerplate(tmp_path: Path) -> None:
    class DummyPipeline:
        def load_inputs(self, config: PipelineConfig) -> PipelineInputs:
            return PipelineInputs(payload={"dummy": True})

        def build_training_frame(
            self,
            inputs: PipelineInputs,
            config: PipelineConfig,
        ) -> pd.DataFrame:
            return pd.DataFrame([{"feature": 1.0}])

        def train_or_load_model(
            self,
            frame: pd.DataFrame,
            config: PipelineConfig,
            retrain: bool,
        ) -> ModelBundle:
            return ModelBundle(payload={"model": "dummy"})

        def predict_lines(
            self,
            inputs: PipelineInputs,
            model_bundle: ModelBundle,
            config: PipelineConfig,
        ) -> pd.DataFrame:
            return pd.DataFrame([{"prediction": 1.0}])

        def simulate(
            self,
            predictions: pd.DataFrame,
            model_bundle: ModelBundle,
            config: PipelineConfig,
        ) -> pd.DataFrame:
            return pd.DataFrame([{"predicted_shots_on_goal": 1.0, "prob_over": 0.5}])

    clear_registry()
    register_pipeline("nhl", "shots_on_goal", DummyPipeline)
    assert is_registered("nhl", "shots_on_goal") is True
    assert get_pipeline("NHL", " SHOTS_ON_GOAL ").simulate(
        pd.DataFrame(),
        ModelBundle(),
        PipelineConfig(
            config_path=Path("config/test.yaml"),
            sport="nhl",
            stat="shots_on_goal",
            raw={},
            section={},
        ),
    ).shape[0] == 1

    config_path = tmp_path / "nhl_dummy.yaml"
    config_path.write_text(
        "\n".join(
            [
                "pipeline:",
                "  sport: nhl",
                "  stat: shots_on_goal",
                "nhl:",
                "  shots_on_goal:",
                "    seed: 7",
            ]
        ),
        encoding="utf-8",
    )
    loaded = load_pipeline_config(str(config_path))
    assert loaded.sport == "nhl"
    assert loaded.stat == "shots_on_goal"
    assert loaded.section == {"seed": 7}


def test_unregistered_sport_stat_still_raises_unknown_pipeline_error() -> None:
    clear_registry()
    with pytest.raises(UnknownPipelineError, match="sport='nhl' stat='shots_on_goal'"):
        get_pipeline("nhl", "shots_on_goal")


def test_sectioned_config_validation_error_is_unchanged_for_missing_section(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "bad_nhl.yaml"
    config_path.write_text(
        "\n".join(
            [
                "pipeline:",
                "  sport: nhl",
                "  stat: shots_on_goal",
                "nhl: {}",
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="nhl.shots_on_goal"):
        load_pipeline_config(str(config_path))
