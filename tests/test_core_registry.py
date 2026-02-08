from __future__ import annotations

import pytest
import yaml
from src.core.config import ConfigValidationError, load_pipeline_config
from src.core.registry import UnknownPipelineError, clear_registry, get_pipeline


def test_unknown_sport_stat_returns_clear_error() -> None:
    clear_registry()
    with pytest.raises(UnknownPipelineError, match="sport='mlb' stat='strikeouts'"):
        get_pipeline("mlb", "strikeouts")


def test_config_validation_rejects_missing_pipeline_identity(tmp_path) -> None:
    config_path = tmp_path / "bad.yaml"
    config_path.write_text(yaml.safe_dump({"pipeline": {}}), encoding="utf-8")

    with pytest.raises(ConfigValidationError, match="pipeline.sport"):
        load_pipeline_config(str(config_path))
