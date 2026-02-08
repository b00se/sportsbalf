from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml
from src.pipeline.engine import run_pipeline_with_overrides

REQUIRED_PRESENT_COLUMNS = {
    "predicted_outs_recorded",
    "outs_line",
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


def _write_outs_config(
    tmp_path: Path, *, lines_path: str, allow_missing_lines: bool
) -> Path:
    config = yaml.safe_load(Path("config/mlb.yaml").read_text(encoding="utf-8"))
    config["pipeline"]["sport"] = "mlb"
    config["pipeline"]["stat"] = "outs_recorded"

    section = config["mlb"]["outs_recorded"]
    section["pitch_data_path"] = "tests/testdata/mlb_multi_stat_pitches.csv"
    section["training_data_paths"] = ["tests/testdata/mlb_multi_stat_pitches.csv"]
    section["lines_path"] = lines_path
    section["allow_missing_lines"] = allow_missing_lines
    section["model_path"] = str(tmp_path / "outs_model.joblib")
    section["pitcher_dataset_output_path"] = str(tmp_path / "pitcher_games.parquet")
    section["batter_dataset_output_path"] = str(tmp_path / "batter_games.parquet")

    config_path = tmp_path / "mlb_outs.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return config_path


def test_outs_pipeline_lines_missing_runs_train_backtest_only(tmp_path: Path) -> None:
    config_path = _write_outs_config(
        tmp_path,
        lines_path=str(tmp_path / "missing_outs_lines.csv"),
        allow_missing_lines=True,
    )

    result = run_pipeline_with_overrides(
        str(config_path),
        sport="mlb",
        stat="outs_recorded",
        retrain=True,
    )

    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert "run_mode" in result.columns
    assert "lines_status" in result.columns


def test_outs_pipeline_lines_present_runs_prediction_mode(tmp_path: Path) -> None:
    config_path = _write_outs_config(
        tmp_path,
        lines_path="tests/testdata/outs_lines.csv",
        allow_missing_lines=True,
    )

    result = run_pipeline_with_overrides(
        str(config_path),
        sport="mlb",
        stat="outs_recorded",
        retrain=True,
    )

    assert isinstance(result, pd.DataFrame)
    assert REQUIRED_PRESENT_COLUMNS.issubset(result.columns)
    assert set(result["run_mode"].astype(str).unique()) == {"prediction"}
    assert set(result["lines_status"].astype(str).unique()) == {"present"}


def test_outs_pipeline_no_name_match_still_returns_simulation_schema(
    tmp_path: Path,
) -> None:
    lines_path = tmp_path / "outs_lines_2024-04-12.csv"
    lines_path.write_text(
        (
            "player,outs_line,over_decimal_price,under_decimal_price\n"
            "Unknown Pitcher,6.5,1.9,1.9\n"
        ),
        encoding="utf-8",
    )

    config_path = _write_outs_config(
        tmp_path,
        lines_path=str(lines_path),
        allow_missing_lines=True,
    )

    result = run_pipeline_with_overrides(
        str(config_path),
        sport="mlb",
        stat="outs_recorded",
        retrain=True,
    )

    assert REQUIRED_PRESENT_COLUMNS.issubset(result.columns)
    assert len(result) == 1
    assert result["predicted_outs_recorded"].isna().all()
    assert result["prob_over"].isna().all()


def test_outs_pipeline_rest_days_uses_slate_date_from_lines_path(
    tmp_path: Path,
) -> None:
    lines_path = tmp_path / "outs_lines_2024-04-12.csv"
    lines_path.write_text(
        "player,outs_line,over_decimal_price,under_decimal_price\n10,6.5,1.9,1.9\n",
        encoding="utf-8",
    )

    config_path = _write_outs_config(
        tmp_path,
        lines_path=str(lines_path),
        allow_missing_lines=True,
    )

    result = run_pipeline_with_overrides(
        str(config_path),
        sport="mlb",
        stat="outs_recorded",
        retrain=True,
    )

    assert len(result) == 1
    assert float(result.loc[0, "upcoming_rest_days"]) == 2.0
