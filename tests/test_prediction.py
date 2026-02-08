from pathlib import Path

import pandas as pd
import yaml
from pytest import MonkeyPatch
from src.mlb.pipeline import run


def _config_with_existing_lines(tmp_path: Path) -> str:
    base = Path("config/mlb.yaml")
    config = yaml.safe_load(base.read_text(encoding="utf-8"))
    mlb_section = config["mlb"]["strikeouts"]

    lines_path = Path(mlb_section["lines_path"])
    if not lines_path.exists():
        dated_files = sorted(Path("data/lines").glob("strikeouts_*.csv"))
        mlb_section["lines_path"] = (
            str(dated_files[-1])
            if dated_files
            else "tests/testdata/lines_with_odds.csv"
        )
    if not Path(mlb_section["pitch_data_path"]).exists():
        mlb_section["pitch_data_path"] = "tests/testdata/pitches.csv"
        mlb_section["training_data_paths"] = ["tests/testdata/pitches.csv"]
    if not Path(mlb_section["park_factors_path"]).exists():
        mlb_section["park_factors_path"] = "tests/testdata/park.csv"

    out_path = tmp_path / "mlb_test.yaml"
    out_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return str(out_path)


def test_prediction_flow(tmp_path: Path):
    fixture = pd.DataFrame(
        [
            {
                "predicted_strikeouts": 6.1,
                "prob_over": 0.57,
                "prob_under": 0.41,
                "ev_over": 0.03,
                "ev_under": -0.01,
                "model_residual_std": 1.2,
                "simulated_median": 6.0,
                "upcoming_game_date": "2025-09-22",
                "upcoming_opponent": "NYY",
                "upcoming_rest_days": 5,
                "upcoming_park_factor_K": 1.01,
            }
        ]
    )
    monkeypatch = MonkeyPatch()
    monkeypatch.setattr(
        "src.mlb.pipeline.run_strikeouts_pipeline",
        lambda **_: fixture.copy(),
    )

    try:
        result = run(_config_with_existing_lines(tmp_path))
    finally:
        monkeypatch.undo()

    required_cols = {
        "predicted_strikeouts",
        "prob_over",
        "prob_under",
        "ev_over",
        "ev_under",
        "model_residual_std",
        "simulated_median",
        "upcoming_game_date",
        "upcoming_opponent",
        "upcoming_rest_days",
        "upcoming_park_factor_K",
    }

    assert required_cols.issubset(result.columns)
    assert result["predicted_strikeouts"].notna().any()
    assert result["simulated_median"].notna().any()
