from __future__ import annotations

from pathlib import Path

import pandas as pd
from src.core.contracts import PipelineConfig
from src.mlb.pipeline import run_strikeouts_pipeline


def test_run_strikeouts_pipeline_delegates_to_shared_core(monkeypatch) -> None:
    cfg = PipelineConfig(
        config_path=Path("config/mlb.yaml"),
        sport="mlb",
        stat="strikeouts",
        raw={"pipeline": {"sport": "mlb", "stat": "strikeouts"}},
        section={},
    )
    fixture = pd.DataFrame([{"predicted_strikeouts": 7.0, "k_line": 5.5}])

    monkeypatch.setattr(
        "src.mlb.pipeline.run_mlb_pitcher_prop_pipeline",
        lambda **_: fixture.copy(),
    )

    result = run_strikeouts_pipeline(cfg, retrain=False)
    assert result.equals(fixture)
