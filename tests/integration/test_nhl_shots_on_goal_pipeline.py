from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import yaml
from src.pipeline.engine import run_pipeline_with_overrides
from tests.helpers.assertions import (
    assert_probability_columns_valid,
    assert_simulation_contract,
)

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
ADDITIVE_COLUMNS = [
    "baseline_predicted_shots_on_goal",
    "model_residual_std",
    "training_rmse",
    "training_mae",
    "training_r2",
    "model_name",
]


def _write_nhl_config(
    tmp_path: Path,
    *,
    inference_input_path: str,
    snapshot_path: str,
    curated_cache_path: str,
    auto_refresh_snapshot: bool = True,
    fail_on_provider_error: bool = True,
) -> Path:
    config = {
        "pipeline": {"sport": "nhl", "stat": "shots_on_goal"},
        "nhl": {
            "shots_on_goal": {
                "provider": "moneypuck_snapshot",
                "inference_input_path": inference_input_path,
                "model_path": str(tmp_path / "nhl_sog_model.joblib"),
                "provider_seasons": [2024],
                "moneypuck_skater_games_snapshot_path": snapshot_path,
                "moneypuck_skater_games_curated_cache_path": curated_cache_path,
                "feature_rolling_windows": [5, 10],
                "auto_refresh_snapshot": auto_refresh_snapshot,
                "fail_on_provider_error": fail_on_provider_error,
                "training_seasons": [2024],
                "monte_carlo_simulations": 400,
                "monte_carlo_seed": 7,
                "fallback_std": 0.9,
                "fallback_prediction": 2.7,
                "default_over_decimal_price": 1.91,
                "default_under_decimal_price": 1.91,
                "bootstrap_enabled": True,
            }
        },
    }
    config_path = tmp_path / "nhl.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return config_path


def test_engine_run_nhl_shots_on_goal_offline_deterministic(tmp_path: Path) -> None:
    snapshot_path = "tests/testdata/nhl/moneypuck/skater_games_full_snapshot_sample.csv"
    curated_cache_path = str(tmp_path / "curated.parquet")
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

    config_path = _write_nhl_config(
        tmp_path,
        inference_input_path=str(input_path),
        snapshot_path=snapshot_path,
        curated_cache_path=curated_cache_path,
        auto_refresh_snapshot=True,
        fail_on_provider_error=True,
    )

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
    assert first.columns.tolist() == REQUIRED_COLUMNS + ADDITIVE_COLUMNS
    assert first.equals(second)
    assert set(first["run_mode"].astype(str).unique()) == {"prediction"}
    assert set(first["lines_status"].astype(str).unique()) == {"present"}
    assert_simulation_contract(first)
    assert_probability_columns_valid(first)


def test_engine_run_nhl_shots_on_goal_missing_input_fallback(tmp_path: Path) -> None:
    snapshot_path = "tests/testdata/nhl/moneypuck/skater_games_full_snapshot_sample.csv"
    curated_cache_path = str(tmp_path / "curated.parquet")
    config_path = _write_nhl_config(
        tmp_path,
        inference_input_path=str(tmp_path / "missing_input.csv"),
        snapshot_path=snapshot_path,
        curated_cache_path=curated_cache_path,
        auto_refresh_snapshot=True,
        fail_on_provider_error=True,
    )

    result = run_pipeline_with_overrides(
        str(config_path),
        sport="nhl",
        stat="shots_on_goal",
        retrain=False,
    )

    assert result.empty
    assert result.columns.tolist() == REQUIRED_COLUMNS + ADDITIVE_COLUMNS


def test_engine_run_nhl_shots_on_goal_defaults_missing_optional_columns(
    tmp_path: Path,
) -> None:
    snapshot_path = "tests/testdata/nhl/moneypuck/skater_games_full_snapshot_sample.csv"
    curated_cache_path = str(tmp_path / "curated.parquet")
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

    config_path = _write_nhl_config(
        tmp_path,
        inference_input_path=str(input_path),
        snapshot_path=snapshot_path,
        curated_cache_path=curated_cache_path,
        auto_refresh_snapshot=True,
        fail_on_provider_error=True,
    )
    result = run_pipeline_with_overrides(
        str(config_path),
        sport="nhl",
        stat="shots_on_goal",
        retrain=False,
    )

    assert not result.empty
    assert result.columns.tolist() == REQUIRED_COLUMNS + ADDITIVE_COLUMNS
    assert result["predicted_shots_on_goal"].notna().all()
    assert set(result["run_mode"].astype(str).unique()) == {"prediction"}
    assert set(result["lines_status"].astype(str).unique()) == {"present"}


def test_engine_run_nhl_shots_on_goal_raises_on_provider_failure(
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
            }
        ]
    ).to_csv(input_path, index=False)

    config_path = _write_nhl_config(
        tmp_path,
        inference_input_path=str(input_path),
        snapshot_path=str(tmp_path / "missing_snapshot.csv"),
        curated_cache_path=str(tmp_path / "missing_curated.parquet"),
        auto_refresh_snapshot=True,
        fail_on_provider_error=True,
    )

    with pytest.raises(RuntimeError, match="provider"):
        run_pipeline_with_overrides(
            str(config_path),
            sport="nhl",
            stat="shots_on_goal",
            retrain=False,
        )


def test_engine_run_nhl_shots_on_goal_incompatible_artifact_retrains(
    tmp_path: Path,
) -> None:
    from src.nhl.models.predict import NHL_FEATURES, load_model, save_model, train_model

    snapshot_path = "tests/testdata/nhl/moneypuck/skater_games_full_snapshot_sample.csv"
    curated_cache_path = str(tmp_path / "curated.parquet")
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
            }
        ]
    ).to_csv(input_path, index=False)
    config_path = _write_nhl_config(
        tmp_path,
        inference_input_path=str(input_path),
        snapshot_path=snapshot_path,
        curated_cache_path=curated_cache_path,
    )
    model_path = tmp_path / "nhl_sog_model.joblib"

    train_df = pd.DataFrame(
        [
            {
                **{feature: 1.0 for feature in NHL_FEATURES},
                "shots_on_goal": 2.0,
            },
            {
                **{feature: 2.0 for feature in NHL_FEATURES},
                "shots_on_goal": 3.0,
            },
            {
                **{feature: 3.0 for feature in NHL_FEATURES},
                "shots_on_goal": 4.0,
            },
        ]
    )
    legacy_model = train_model(train_df)
    save_model(
        legacy_model,
        model_path,
        feature_columns=NHL_FEATURES + ["legacy_only_feature"],
        model_name="xgboost",
    )
    result = run_pipeline_with_overrides(
        str(config_path),
        sport="nhl",
        stat="shots_on_goal",
        retrain=False,
    )

    assert not result.empty
    assert model_path.exists()
    _, metadata = load_model(model_path, expected_feature_columns=NHL_FEATURES)
    assert metadata["model_name"] == "xgboost"
    assert result.columns.tolist() == REQUIRED_COLUMNS + ADDITIVE_COLUMNS


def test_engine_run_nhl_shots_on_goal_retrain_false_loads_compatible_artifact(
    tmp_path: Path,
) -> None:
    snapshot_path = "tests/testdata/nhl/moneypuck/skater_games_full_snapshot_sample.csv"
    curated_cache_path = str(tmp_path / "curated.parquet")
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
            }
        ]
    ).to_csv(input_path, index=False)
    config_path = _write_nhl_config(
        tmp_path,
        inference_input_path=str(input_path),
        snapshot_path=snapshot_path,
        curated_cache_path=curated_cache_path,
        auto_refresh_snapshot=True,
        fail_on_provider_error=True,
    )
    model_path = tmp_path / "nhl_sog_model.joblib"

    first = run_pipeline_with_overrides(
        str(config_path),
        sport="nhl",
        stat="shots_on_goal",
        retrain=True,
    )
    mtime_after_train = model_path.stat().st_mtime_ns
    second = run_pipeline_with_overrides(
        str(config_path),
        sport="nhl",
        stat="shots_on_goal",
        retrain=False,
    )
    mtime_after_load = model_path.stat().st_mtime_ns

    assert not first.empty
    assert first.columns.tolist() == REQUIRED_COLUMNS + ADDITIVE_COLUMNS
    assert second.columns.tolist() == REQUIRED_COLUMNS + ADDITIVE_COLUMNS
    assert mtime_after_load == mtime_after_train


def test_engine_run_nhl_shots_on_goal_falls_back_when_training_frame_empty(
    tmp_path: Path,
) -> None:
    snapshot_path = "tests/testdata/nhl/moneypuck/skater_games_full_snapshot_sample.csv"
    curated_cache_path = str(tmp_path / "curated.parquet")
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
            }
        ]
    ).to_csv(input_path, index=False)

    config_path = _write_nhl_config(
        tmp_path,
        inference_input_path=str(input_path),
        snapshot_path=snapshot_path,
        curated_cache_path=curated_cache_path,
        auto_refresh_snapshot=True,
        fail_on_provider_error=True,
    )
    config_payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config_payload["nhl"]["shots_on_goal"]["training_seasons"] = [1999]
    config_path.write_text(
        yaml.safe_dump(config_payload, sort_keys=False),
        encoding="utf-8",
    )

    result = run_pipeline_with_overrides(
        str(config_path),
        sport="nhl",
        stat="shots_on_goal",
        retrain=False,
    )

    assert not result.empty
    assert result.columns.tolist() == REQUIRED_COLUMNS + ADDITIVE_COLUMNS
    assert result["predicted_shots_on_goal"].notna().all()
    assert result["baseline_predicted_shots_on_goal"].notna().all()
