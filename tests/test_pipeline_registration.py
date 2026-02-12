from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import yaml
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
from src.pipeline import registration as pipeline_registration
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
        ("nhl", "shots_on_goal"),
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
        ("nhl", "shots_on_goal"),
    }
    assert first == expected
    assert second == expected


def test_bootstrap_overwrites_existing_default_registration() -> None:
    clear_registry()

    class ShadowPipeline:
        pass

    register_pipeline("mlb", "strikeouts", ShadowPipeline)  # type: ignore[arg-type]
    ensure_default_pipeline_registrations()

    registrations = {
        (entry.sport, entry.stat): entry.factory
        for entry in list_registered_pipelines()
    }
    assert registrations[("mlb", "strikeouts")] is not ShadowPipeline
    assert (
        registrations[("mlb", "strikeouts")]
        is dict(
            ((sport, stat), factory)
            for sport, stat, factory in DEFAULT_PIPELINE_REGISTRATIONS
        )[("mlb", "strikeouts")]
    )


def test_bootstrap_validation_does_not_reinstantiate_factories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_registry()
    calls = {"count": 0}

    class DummyPipeline:
        def load_inputs(self, config: PipelineConfig) -> PipelineInputs:
            return PipelineInputs()

        def build_training_frame(
            self,
            inputs: PipelineInputs,
            config: PipelineConfig,
        ) -> pd.DataFrame:
            return pd.DataFrame()

        def train_or_load_model(
            self,
            frame: pd.DataFrame,
            config: PipelineConfig,
            retrain: bool,
        ) -> ModelBundle:
            return ModelBundle()

        def predict_lines(
            self,
            inputs: PipelineInputs,
            model_bundle: ModelBundle,
            config: PipelineConfig,
        ) -> pd.DataFrame:
            return pd.DataFrame()

        def simulate(
            self,
            predictions: pd.DataFrame,
            model_bundle: ModelBundle,
            config: PipelineConfig,
        ) -> pd.DataFrame:
            return pd.DataFrame()

    def _counting_factory() -> DummyPipeline:
        calls["count"] += 1
        return DummyPipeline()

    monkeypatch.setattr(
        pipeline_registration,
        "DEFAULT_PIPELINE_REGISTRATIONS",
        (("test", "metric", _counting_factory),),
    )

    ensure_default_pipeline_registrations()
    ensure_default_pipeline_registrations()
    assert calls["count"] == 1


def test_bootstrap_handles_unhashable_callable_factories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_registry()
    calls = {"count": 0}

    class DummyPipeline:
        def load_inputs(self, config: PipelineConfig) -> PipelineInputs:
            return PipelineInputs()

        def build_training_frame(
            self,
            inputs: PipelineInputs,
            config: PipelineConfig,
        ) -> pd.DataFrame:
            return pd.DataFrame()

        def train_or_load_model(
            self,
            frame: pd.DataFrame,
            config: PipelineConfig,
            retrain: bool,
        ) -> ModelBundle:
            return ModelBundle()

        def predict_lines(
            self,
            inputs: PipelineInputs,
            model_bundle: ModelBundle,
            config: PipelineConfig,
        ) -> pd.DataFrame:
            return pd.DataFrame()

        def simulate(
            self,
            predictions: pd.DataFrame,
            model_bundle: ModelBundle,
            config: PipelineConfig,
        ) -> pd.DataFrame:
            return pd.DataFrame()

    class UnhashableFactory:
        def __call__(self) -> DummyPipeline:
            calls["count"] += 1
            return DummyPipeline()

        def __eq__(self, other: object) -> bool:
            return self is other

    monkeypatch.setattr(
        pipeline_registration,
        "_VALIDATED_REGISTRATION_DECLARATIONS",
        set(),
    )
    monkeypatch.setattr(
        pipeline_registration,
        "DEFAULT_PIPELINE_REGISTRATIONS",
        (("test", "metric", UnhashableFactory()),),
    )

    ensure_default_pipeline_registrations()
    ensure_default_pipeline_registrations()
    assert calls["count"] == 1


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
        yaml.safe_dump(
            {
                "pipeline": {"sport": "nhl", "stat": "shots_on_goal"},
                "nhl": {
                    "shots_on_goal": {
                        "provider": "moneypuck_snapshot",
                        "inference_input_path": (
                            "tests/testdata/nhl_shots_on_goal_input.csv"
                        ),
                        "model_path": "models/nhl_shots_on_goal_model.joblib",
                        "provider_seasons": [2024],
                        "moneypuck_skater_games_snapshot_path": (
                            "tests/testdata/nhl/moneypuck/"
                            "skater_games_full_snapshot_sample.csv"
                        ),
                        "moneypuck_skater_games_curated_cache_path": (
                            "/tmp/nhl_curated.parquet"
                        ),
                        "feature_rolling_windows": [5, 10],
                        "auto_refresh_snapshot": False,
                        "fail_on_provider_error": True,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    loaded = load_pipeline_config(str(config_path))
    assert loaded.sport == "nhl"
    assert loaded.stat == "shots_on_goal"
    assert loaded.section["provider"] == "moneypuck_snapshot"


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
