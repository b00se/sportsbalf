from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from src.core.contracts import ModelBundle, PipelineConfig, PipelineInputs
from src.pipeline import engine

SIMULATE_ONLY_ALLOWLIST = {
    "src.mlb.pitcher_props.adapter.MlbPitcherPropsPipeline",
    "src.nfl.pass_attempts.pipeline.NflPassAttemptsPipeline",
}


def _make_config(*, sport: str = "mlb", stat: str = "strikeouts") -> PipelineConfig:
    return PipelineConfig(
        config_path=Path("config/test.yaml"),
        sport=sport,
        stat=stat,
        raw={"pipeline": {"sport": sport, "stat": stat}},
        section={},
    )


def _fqcn(value: Any) -> str:
    return f"{value.__module__}.{value.__name__}"


def _capture_default_registrations() -> list[tuple[str, str, Any]]:
    captured: list[tuple[str, str, Any]] = []

    def _capture_register(sport: str, stat: str, factory: Any) -> None:
        captured.append((sport, stat, factory))

    original = engine.register_pipeline
    try:
        engine.register_pipeline = _capture_register
        engine._ensure_default_registrations()
    finally:
        engine.register_pipeline = original

    return captured


def _is_simulate_only_adapter(factory: Any) -> bool:
    adapter = factory()
    config = _make_config()

    inputs = adapter.load_inputs(config)
    frame = adapter.build_training_frame(inputs, config)
    bundle = adapter.train_or_load_model(frame, config, retrain=False)
    predictions = adapter.predict_lines(inputs, bundle, config)

    return (
        isinstance(frame, pd.DataFrame)
        and frame.empty
        and isinstance(bundle, ModelBundle)
        and bundle.payload == {}
        and isinstance(predictions, pd.DataFrame)
        and predictions.empty
    )


def test_engine_runs_stages_in_strict_order(monkeypatch) -> None:
    stage_calls: list[str] = []
    simulate_output = pd.DataFrame([{"result": 1}])
    config = _make_config()

    class FakePipeline:
        def load_inputs(self, cfg: PipelineConfig) -> PipelineInputs:
            assert cfg is config
            stage_calls.append("load_inputs")
            return PipelineInputs(payload={"inputs": True})

        def build_training_frame(
            self,
            inputs: PipelineInputs,
            cfg: PipelineConfig,
        ) -> pd.DataFrame:
            assert cfg is config
            assert isinstance(inputs, PipelineInputs)
            stage_calls.append("build_training_frame")
            return pd.DataFrame([{"feature": 1.0}])

        def train_or_load_model(
            self,
            frame: pd.DataFrame,
            cfg: PipelineConfig,
            retrain: bool,
        ) -> ModelBundle:
            assert cfg is config
            assert isinstance(frame, pd.DataFrame)
            assert retrain is False
            stage_calls.append("train_or_load_model")
            return ModelBundle(payload={"model": "ok"})

        def predict_lines(
            self,
            inputs: PipelineInputs,
            model_bundle: ModelBundle,
            cfg: PipelineConfig,
        ) -> pd.DataFrame:
            assert cfg is config
            assert isinstance(inputs, PipelineInputs)
            assert isinstance(model_bundle, ModelBundle)
            stage_calls.append("predict_lines")
            return pd.DataFrame([{"prediction": 2.0}])

        def simulate(
            self,
            predictions: pd.DataFrame,
            model_bundle: ModelBundle,
            cfg: PipelineConfig,
        ) -> pd.DataFrame:
            assert cfg is config
            assert isinstance(predictions, pd.DataFrame)
            assert isinstance(model_bundle, ModelBundle)
            stage_calls.append("simulate")
            return simulate_output

    fake_pipeline = FakePipeline()

    monkeypatch.setattr(engine, "_ensure_default_registrations", lambda: None)
    monkeypatch.setattr(engine, "load_pipeline_config", lambda _path: config)
    monkeypatch.setattr(engine, "get_pipeline", lambda sport, stat: fake_pipeline)

    result = engine.run_pipeline("config/test.yaml", retrain=False)

    assert stage_calls == [
        "load_inputs",
        "build_training_frame",
        "train_or_load_model",
        "predict_lines",
        "simulate",
    ]
    assert result is simulate_output


def test_engine_handoff_artifacts_between_stages(monkeypatch) -> None:
    config = _make_config()

    class FakePipeline:
        def __init__(self) -> None:
            self.inputs: PipelineInputs | None = None
            self.frame: pd.DataFrame | None = None
            self.bundle: ModelBundle | None = None
            self.predictions: pd.DataFrame | None = None

        def load_inputs(self, cfg: PipelineConfig) -> PipelineInputs:
            assert cfg is config
            self.inputs = PipelineInputs(payload={"loaded": True})
            return self.inputs

        def build_training_frame(
            self,
            inputs: PipelineInputs,
            cfg: PipelineConfig,
        ) -> pd.DataFrame:
            assert cfg is config
            assert isinstance(inputs, PipelineInputs)
            assert inputs is self.inputs
            self.frame = pd.DataFrame([{"feature": 1.0}])
            return self.frame

        def train_or_load_model(
            self,
            frame: pd.DataFrame,
            cfg: PipelineConfig,
            retrain: bool,
        ) -> ModelBundle:
            assert cfg is config
            assert retrain is True
            assert isinstance(frame, pd.DataFrame)
            assert frame is self.frame
            self.bundle = ModelBundle(payload={"trained": True})
            return self.bundle

        def predict_lines(
            self,
            inputs: PipelineInputs,
            model_bundle: ModelBundle,
            cfg: PipelineConfig,
        ) -> pd.DataFrame:
            assert cfg is config
            assert isinstance(inputs, PipelineInputs)
            assert isinstance(model_bundle, ModelBundle)
            assert inputs is self.inputs
            assert model_bundle is self.bundle
            self.predictions = pd.DataFrame([{"prediction": 2.0}])
            return self.predictions

        def simulate(
            self,
            predictions: pd.DataFrame,
            model_bundle: ModelBundle,
            cfg: PipelineConfig,
        ) -> pd.DataFrame:
            assert cfg is config
            assert isinstance(predictions, pd.DataFrame)
            assert isinstance(model_bundle, ModelBundle)
            assert predictions is self.predictions
            assert model_bundle is self.bundle
            return pd.DataFrame([{"final": 3.0}])

    fake_pipeline = FakePipeline()

    monkeypatch.setattr(engine, "_ensure_default_registrations", lambda: None)
    monkeypatch.setattr(engine, "load_pipeline_config", lambda _path: config)
    monkeypatch.setattr(engine, "get_pipeline", lambda sport, stat: fake_pipeline)

    result = engine.run_pipeline("config/test.yaml", retrain=True)

    assert list(result.columns) == ["final"]


def test_run_pipeline_with_overrides_passes_cli_overrides(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    config = _make_config(sport="nfl", stat="pass_attempts")

    class FakePipeline:
        def load_inputs(self, cfg: PipelineConfig) -> PipelineInputs:
            return PipelineInputs(payload={})

        def build_training_frame(
            self,
            inputs: PipelineInputs,
            cfg: PipelineConfig,
        ) -> pd.DataFrame:
            return pd.DataFrame()

        def train_or_load_model(
            self,
            frame: pd.DataFrame,
            cfg: PipelineConfig,
            retrain: bool,
        ) -> ModelBundle:
            return ModelBundle(payload={})

        def predict_lines(
            self,
            inputs: PipelineInputs,
            model_bundle: ModelBundle,
            cfg: PipelineConfig,
        ) -> pd.DataFrame:
            return pd.DataFrame()

        def simulate(
            self,
            predictions: pd.DataFrame,
            model_bundle: ModelBundle,
            cfg: PipelineConfig,
        ) -> pd.DataFrame:
            return pd.DataFrame([{"ok": True}])

    def _capture_load(
        config_path: str,
        *,
        sport_override: str | None = None,
        stat_override: str | None = None,
    ) -> PipelineConfig:
        captured["config_path"] = config_path
        captured["sport_override"] = sport_override
        captured["stat_override"] = stat_override
        return config

    monkeypatch.setattr(engine, "_ensure_default_registrations", lambda: None)
    monkeypatch.setattr(engine, "load_pipeline_config", _capture_load)
    monkeypatch.setattr(engine, "get_pipeline", lambda sport, stat: FakePipeline())

    result = engine.run_pipeline_with_overrides(
        "config/test.yaml",
        sport="nfl",
        stat="pass_attempts",
        retrain=False,
    )

    assert captured == {
        "config_path": "config/test.yaml",
        "sport_override": "nfl",
        "stat_override": "pass_attempts",
    }
    assert list(result.columns) == ["ok"]


def test_default_registrations_match_expected_pairs(monkeypatch) -> None:
    calls: list[tuple[str, str, Any]] = []

    def _capture_register(sport: str, stat: str, factory: Any) -> None:
        calls.append((sport, stat, factory))

    monkeypatch.setattr(engine, "register_pipeline", _capture_register)

    engine._ensure_default_registrations()

    assert {(sport, stat) for sport, stat, _factory in calls} == {
        ("mlb", "strikeouts"),
        ("mlb", "outs_recorded"),
        ("mlb", "earned_runs"),
        ("mlb", "hits_allowed"),
        ("mlb", "bb_allowed"),
        ("nfl", "pass_attempts"),
    }


def test_simulate_only_adapters_must_be_explicitly_allowlisted() -> None:
    registrations = _capture_default_registrations()

    for _sport, _stat, factory in registrations:
        if _is_simulate_only_adapter(factory):
            assert _fqcn(factory) in SIMULATE_ONLY_ALLOWLIST


def test_allowlist_entries_must_exist_in_default_registrations() -> None:
    registrations = _capture_default_registrations()
    registered_classes = {_fqcn(factory) for _sport, _stat, factory in registrations}

    assert SIMULATE_ONLY_ALLOWLIST.issubset(registered_classes)
