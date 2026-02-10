from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml
from src.pipeline.engine import run_pipeline_with_overrides

REQUIRED_COLUMNS = [
    "player_id",
    "player_name",
    "team",
    "opponent",
    "game_id",
    "sog_line",
    "predicted_shots_on_goal",
    "prob_over",
    "prob_under",
    "prob_push",
    "ev_over",
    "ev_under",
    "edge_over",
    "edge_under",
    "run_mode",
    "lines_status",
]


def _write_nhl_config(tmp_path: Path, *, inference_input_path: str) -> Path:
    config = {
        "pipeline": {"sport": "nhl", "stat": "shots_on_goal"},
        "nhl": {
            "shots_on_goal": {
                "inference_input_path": inference_input_path,
                "model_path": str(tmp_path / "nhl_sog_model.joblib"),
                "monte_carlo_simulations": 400,
                "monte_carlo_seed": 7,
                "fallback_std": 0.9,
                "fallback_prediction": 2.7,
                "default_over_decimal_price": 1.91,
                "default_under_decimal_price": 1.91,
            }
        },
    }
    config_path = tmp_path / "nhl.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return config_path


def test_engine_run_nhl_shots_on_goal_offline_deterministic(tmp_path: Path) -> None:
    input_path = tmp_path / "nhl_sog_input.csv"
    pd.DataFrame(
        [
            {
                "player_id": "8478402",
                "player_name": "Player One",
                "team": "NYR",
                "opponent": "BOS",
                "game_id": "2026-02-10-NYR-BOS",
                "sog_line": 2.5,
            },
            {
                "player_id": "8471214",
                "player_name": "Player Two",
                "team": "TOR",
                "opponent": "MTL",
                "game_id": "2026-02-10-TOR-MTL",
                "sog_line": 3.5,
                "over_decimal_price": 1.87,
                "under_decimal_price": 1.95,
                "predicted_shots_on_goal": 3.1,
            },
        ]
    ).to_csv(input_path, index=False)

    config_path = _write_nhl_config(tmp_path, inference_input_path=str(input_path))

    first = run_pipeline_with_overrides(
        str(config_path),
        sport="nhl",
        stat="shots_on_goal",
        retrain=False,
    )
    second = run_pipeline_with_overrides(
        str(config_path),
        sport="nhl",
        stat="shots_on_goal",
        retrain=False,
    )

    assert not first.empty
    assert first.columns.tolist() == REQUIRED_COLUMNS
    assert first.equals(second)
    assert set(first["run_mode"].astype(str).unique()) == {"prediction"}
    assert set(first["lines_status"].astype(str).unique()) == {"present"}


def test_engine_run_nhl_shots_on_goal_missing_input_fallback(tmp_path: Path) -> None:
    config_path = _write_nhl_config(
        tmp_path,
        inference_input_path=str(tmp_path / "missing_input.csv"),
    )

    result = run_pipeline_with_overrides(
        str(config_path),
        sport="nhl",
        stat="shots_on_goal",
        retrain=False,
    )

    assert result.empty
    assert result.columns.tolist() == REQUIRED_COLUMNS


def test_engine_run_nhl_shots_on_goal_defaults_missing_optional_columns(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "nhl_required_only.csv"
    pd.DataFrame(
        [
            {
                "player_id": "8478402",
                "player_name": "Player One",
                "team": "NYR",
                "opponent": "BOS",
                "game_id": "2026-02-10-NYR-BOS",
                "sog_line": 2.5,
            },
            {
                "player_id": "8471214",
                "player_name": "Player Two",
                "team": "TOR",
                "opponent": "MTL",
                "game_id": "2026-02-10-TOR-MTL",
                "sog_line": 3.5,
            },
        ]
    ).to_csv(input_path, index=False)

    config_path = _write_nhl_config(tmp_path, inference_input_path=str(input_path))
    result = run_pipeline_with_overrides(
        str(config_path),
        sport="nhl",
        stat="shots_on_goal",
        retrain=False,
    )

    assert not result.empty
    assert result.columns.tolist() == REQUIRED_COLUMNS
    assert result["predicted_shots_on_goal"].notna().all()
    assert set(result["run_mode"].astype(str).unique()) == {"prediction"}
    assert set(result["lines_status"].astype(str).unique()) == {"present"}
