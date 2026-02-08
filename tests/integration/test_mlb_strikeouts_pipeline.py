from __future__ import annotations

from pathlib import Path

import pandas as pd
from src.pipeline.engine import run_pipeline_with_overrides

REQUIRED_COLUMNS = {
    "predicted_strikeouts",
    "k_line",
    "prob_over",
    "prob_under",
    "prob_push",
    "ev_over",
    "ev_under",
    "edge_over",
    "edge_under",
}


def test_engine_run_mlb_strikeouts_schema(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "mlb.yaml"
    config_path.write_text(
        Path("config/mlb.yaml").read_text(encoding="utf-8"), encoding="utf-8"
    )

    fixture = pd.DataFrame(
        [
            {
                "predicted_strikeouts": 6.1,
                "k_line": 5.5,
                "prob_over": 0.57,
                "prob_under": 0.41,
                "prob_push": 0.02,
                "ev_over": 0.03,
                "ev_under": -0.01,
                "edge_over": 0.04,
                "edge_under": -0.02,
            }
        ]
    )

    from src.mlb.pitcher_props import adapter as mlb_adapter

    monkeypatch.setattr(
        mlb_adapter,
        "run_mlb_pitcher_prop_pipeline",
        lambda **_: fixture.copy(),
    )

    result = run_pipeline_with_overrides(
        str(config_path),
        sport="mlb",
        stat="strikeouts",
        retrain=False,
    )

    assert isinstance(result, pd.DataFrame)
    assert REQUIRED_COLUMNS.issubset(result.columns)
