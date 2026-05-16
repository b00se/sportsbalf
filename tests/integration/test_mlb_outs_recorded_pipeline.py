from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml
from src.pipeline.engine import run_pipeline_with_overrides
from tests.helpers.assertions import (
    assert_probability_columns_valid,
    assert_simulation_contract,
)

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
    section["calibration_report_path"] = str(tmp_path / "outs_calibration.csv")
    section["model_selection"]["champion_model_path"] = str(
        tmp_path / "outs_champion.joblib"
    )
    section["model_selection"]["champion_metadata_path"] = str(
        tmp_path / "outs_champion.json"
    )
    section["model_selection"]["leaderboard_path"] = str(
        tmp_path / "outs_leaderboard.csv"
    )

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
    assert_simulation_contract(result)
    assert_probability_columns_valid(result)


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


def test_outs_pipeline_writes_calibration_report(tmp_path: Path) -> None:
    calibration_path = tmp_path / "outs_calibration.csv"
    calibration_rows = pd.DataFrame(
        [
            {
                "pitcher_id": 101,
                "pitcher_name": "Pitcher One",
                "game_date": "2024-04-01",
                "rolling_K_avg_3": 5.1,
                "rolling_K_avg_5": 5.0,
                "rolling_pitch_count_5": 82.0,
                "rolling_K_rate": 0.21,
                "rest_days": 4,
                "rolling_on_base_events_allowed_5": 0.24,
                "rolling_hard_contact_allowed_5": 0.34,
                "opponent_out_rate": 0.63,
                "park_factor_outs": 0.98,
                "outs_recorded": 18,
                "outs_line": 17.5,
                "over_decimal_price": 1.91,
                "under_decimal_price": 1.91,
            },
            {
                "pitcher_id": 102,
                "pitcher_name": "Pitcher Two",
                "game_date": "2024-04-02",
                "rolling_K_avg_3": 4.8,
                "rolling_K_avg_5": 4.9,
                "rolling_pitch_count_5": 84.0,
                "rolling_K_rate": 0.19,
                "rest_days": 5,
                "rolling_on_base_events_allowed_5": 0.26,
                "rolling_hard_contact_allowed_5": 0.32,
                "opponent_out_rate": 0.61,
                "park_factor_outs": 1.01,
                "outs_recorded": 15,
                "outs_line": 15.5,
                "over_decimal_price": 2.05,
                "under_decimal_price": 1.75,
            },
            {
                "pitcher_id": 103,
                "pitcher_name": "Pitcher Three",
                "game_date": "2024-04-03",
                "rolling_K_avg_3": 6.0,
                "rolling_K_avg_5": 5.8,
                "rolling_pitch_count_5": 88.0,
                "rolling_K_rate": 0.23,
                "rest_days": 3,
                "rolling_on_base_events_allowed_5": 0.22,
                "rolling_hard_contact_allowed_5": 0.31,
                "opponent_out_rate": 0.66,
                "park_factor_outs": 0.99,
                "outs_recorded": 21,
                "outs_line": 20.5,
                "over_decimal_price": 1.87,
                "under_decimal_price": 1.97,
            },
        ]
    )
    calibration_data_path = tmp_path / "outs_calibration_data.csv"
    calibration_rows.to_csv(calibration_data_path, index=False)

    config_path = _write_outs_config(
        tmp_path,
        lines_path="tests/testdata/outs_lines.csv",
        allow_missing_lines=True,
    )
    config = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    config["mlb"]["outs_recorded"]["calibration_data_path"] = str(
        calibration_data_path
    )
    config["mlb"]["outs_recorded"]["calibration_report_path"] = str(
        calibration_path
    )
    Path(config_path).write_text(
        yaml.safe_dump(config, sort_keys=False), encoding="utf-8"
    )

    result = run_pipeline_with_overrides(
        str(config_path),
        sport="mlb",
        stat="outs_recorded",
        retrain=True,
    )

    assert isinstance(result, pd.DataFrame)
    assert calibration_path.exists()
    report = pd.read_csv(calibration_path)
    assert {
        "prob_bin",
        "count",
        "mean_predicted_over",
        "observed_over_rate",
        "abs_gap",
        "brier_score",
        "log_loss",
        "ece",
    }.issubset(report.columns)
