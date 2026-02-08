from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import yaml
from src.pipeline.engine import run_pipeline_with_overrides


@pytest.mark.parametrize(
    "stat,line_col,pred_col,lines_fixture",
    [
        (
            "earned_runs",
            "er_line",
            "predicted_earned_runs",
            "tests/testdata/earned_runs_lines.csv",
        ),
        (
            "hits_allowed",
            "hits_line",
            "predicted_hits_allowed",
            "tests/testdata/hits_allowed_lines.csv",
        ),
        (
            "bb_allowed",
            "bb_line",
            "predicted_bb_allowed",
            "tests/testdata/bb_allowed_lines.csv",
        ),
    ],
)
def test_multi_stat_pipeline_lines_present_prediction_mode(
    tmp_path: Path,
    stat: str,
    line_col: str,
    pred_col: str,
    lines_fixture: str,
) -> None:
    config = yaml.safe_load(Path("config/mlb.yaml").read_text(encoding="utf-8"))
    config["pipeline"]["sport"] = "mlb"
    config["pipeline"]["stat"] = stat

    section = config["mlb"][stat]
    section["pitch_data_path"] = "tests/testdata/mlb_multi_stat_pitches.csv"
    section["training_data_paths"] = ["tests/testdata/mlb_multi_stat_pitches.csv"]
    section["lines_path"] = lines_fixture
    section["allow_missing_lines"] = True
    section["model_path"] = str(tmp_path / f"{stat}_model.joblib")
    section["pitcher_dataset_output_path"] = str(
        tmp_path / f"{stat}_pitcher_games.parquet"
    )
    section["batter_dataset_output_path"] = str(
        tmp_path / f"{stat}_batter_games.parquet"
    )

    config_path = tmp_path / f"mlb_{stat}.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    result = run_pipeline_with_overrides(
        str(config_path),
        sport="mlb",
        stat=stat,
        retrain=True,
    )

    assert isinstance(result, pd.DataFrame)
    required = {
        pred_col,
        line_col,
        "prob_over",
        "prob_under",
        "prob_push",
        "ev_over",
        "ev_under",
        "edge_over",
        "edge_under",
        "run_mode",
        "lines_status",
    }
    assert required.issubset(result.columns)
    assert set(result["run_mode"].astype(str).unique()) == {"prediction"}
    assert set(result["lines_status"].astype(str).unique()) == {"present"}


@pytest.mark.parametrize(
    "stat,pred_col",
    [
        ("earned_runs", "predicted_earned_runs"),
        ("hits_allowed", "predicted_hits_allowed"),
        ("bb_allowed", "predicted_bb_allowed"),
    ],
)
def test_multi_stat_pipeline_missing_lines_train_backtest_only(
    tmp_path: Path,
    stat: str,
    pred_col: str,
) -> None:
    config = yaml.safe_load(Path("config/mlb.yaml").read_text(encoding="utf-8"))
    config["pipeline"]["sport"] = "mlb"
    config["pipeline"]["stat"] = stat

    section = config["mlb"][stat]
    section["pitch_data_path"] = "tests/testdata/mlb_multi_stat_pitches.csv"
    section["training_data_paths"] = ["tests/testdata/mlb_multi_stat_pitches.csv"]
    section["lines_path"] = str(tmp_path / f"missing_{stat}_lines.csv")
    section["allow_missing_lines"] = True
    section["model_path"] = str(tmp_path / f"{stat}_model.joblib")
    section["pitcher_dataset_output_path"] = str(
        tmp_path / f"{stat}_pitcher_games.parquet"
    )
    section["batter_dataset_output_path"] = str(
        tmp_path / f"{stat}_batter_games.parquet"
    )

    config_path = tmp_path / f"mlb_{stat}_missing.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    result = run_pipeline_with_overrides(
        str(config_path),
        sport="mlb",
        stat=stat,
        retrain=True,
    )

    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert pred_col in result.columns
    assert "run_mode" in result.columns
    assert "lines_status" in result.columns
