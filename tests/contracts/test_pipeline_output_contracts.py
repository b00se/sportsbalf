from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml
from src.fantasy.adapters.mlb.projection_adapter import (
    MlbProjectionAdapterConfig,
    MlbSeasonProjectionAdapter,
)
from src.fantasy.core.contracts import ContestConfig, MarketDefinition
from src.pipeline.engine import run_pipeline_with_overrides
from tests.helpers.assertions import (
    assert_horizon_semantics,
    assert_probability_columns_valid,
    assert_simulation_contract,
)


def _run_mlb_outs(tmp_path: Path) -> pd.DataFrame:
    tmp_path.mkdir(parents=True, exist_ok=True)
    config = yaml.safe_load(Path("config/mlb.yaml").read_text(encoding="utf-8"))
    config["pipeline"]["sport"] = "mlb"
    config["pipeline"]["stat"] = "outs_recorded"
    section = config["mlb"]["outs_recorded"]
    section["pitch_data_path"] = "tests/testdata/mlb_multi_stat_pitches.csv"
    section["training_data_paths"] = ["tests/testdata/mlb_multi_stat_pitches.csv"]
    section["lines_path"] = "tests/testdata/outs_lines.csv"
    section["allow_missing_lines"] = True
    section["model_path"] = str(tmp_path / "outs_model.joblib")
    section["pitcher_dataset_output_path"] = str(tmp_path / "pitcher_games.parquet")
    section["batter_dataset_output_path"] = str(tmp_path / "batter_games.parquet")
    config_path = tmp_path / "mlb.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    return run_pipeline_with_overrides(
        str(config_path),
        sport="mlb",
        stat="outs_recorded",
        retrain=True,
    )


def _run_nfl(tmp_path: Path, monkeypatch) -> pd.DataFrame:
    tmp_path.mkdir(parents=True, exist_ok=True)
    rows = []
    for week in (1, 2, 3):
        rows.append(
            {
                "season": 2023,
                "week": week,
                "game_id": f"2023_{week:02d}_AAA",
                "qb_name": f"QB {week}",
                "qb_id": f"QB{week}",
                "team": "AAA",
                "opponent": "BBB",
                "home": week % 2 == 0,
                "spread": -2 + week,
                "total": 45.0 + week,
                "pass_attempts": 30 + week,
                "ud_line": 31.5,
                "prev_attempts": 28 + week,
                "rolling3_attempts": 29 + week,
                "season_avg_attempts": 30 + week,
                "career_avg_attempts": 31 + week,
                "season_attempts_to_date": 25 + week,
                "season_games_played": week - 1,
                "season_avg_attempts_to_date": 27 + week,
                "plays_per_game": 62 + week,
                "pass_rate": 0.56,
                "neutral_pass_rate": 0.54,
                "pass_rate_over_expected": 0.02,
                "plays_faced": 61 + week,
                "opponent_pass_rate_allowed": 0.55,
                "opponent_neutral_pass_rate": 0.52,
                "qb_dropbacks": 34 + week,
                "avg_cpoe": 0.5,
                "epa_per_dropback": 0.1,
                "air_yards_per_attempt": 6.5,
                "qb_rush_attempts": 4,
                "ngs_avg_time_to_throw": 2.5,
                "ngs_avg_air_yards": 7.5,
                "ngs_cpoe": 0.01,
                "rest_days": 7,
                "short_week": False,
                "is_divisional": False,
            }
        )
    dataset = pd.DataFrame(rows)
    dataset_path = tmp_path / "qb_dataset.parquet"
    dataset.to_parquet(dataset_path, index=False)
    config_path = tmp_path / "nfl.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "pipeline": {"sport": "nfl", "stat": "pass_attempts"},
                "nfl": {
                    "pass_attempts": {
                        "dataset_path": str(dataset_path),
                        "model_path": str(tmp_path / "model.joblib"),
                        "rebuild_dataset": False,
                        "training_years": [2023],
                        "inference_years": [2023],
                        "monte_carlo_simulations": 250,
                        "fallback_std": 1.0,
                        "monte_carlo_seed": 42,
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    from src.nfl import pipeline as nfl_pipeline

    monkeypatch.setattr(
        nfl_pipeline, "import_ud_pass_attempt_lines", lambda **_: pd.DataFrame()
    )

    return run_pipeline_with_overrides(
        str(config_path),
        sport="nfl",
        stat="pass_attempts",
        retrain=True,
    )


def _run_nhl(tmp_path: Path) -> pd.DataFrame:
    tmp_path.mkdir(parents=True, exist_ok=True)
    input_path = tmp_path / "input.csv"
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

    config = {
        "pipeline": {"sport": "nhl", "stat": "shots_on_goal"},
        "nhl": {
            "shots_on_goal": {
                "provider": "moneypuck_snapshot",
                "inference_input_path": str(input_path),
                "model_path": str(tmp_path / "nhl_sog_model.joblib"),
                "provider_seasons": [2024],
                "moneypuck_skater_games_snapshot_path": (
                    "tests/testdata/nhl/moneypuck/skater_games_full_snapshot_sample.csv"
                ),
                "moneypuck_skater_games_curated_cache_path": str(
                    tmp_path / "curated.parquet"
                ),
                "feature_rolling_windows": [5, 10],
                "auto_refresh_snapshot": True,
                "fail_on_provider_error": True,
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

    return run_pipeline_with_overrides(
        str(config_path),
        sport="nhl",
        stat="shots_on_goal",
        retrain=False,
    )


def test_pipeline_output_contract_parity_across_sports(
    tmp_path: Path, monkeypatch
) -> None:
    frames = {
        ("mlb", "outs_recorded"): _run_mlb_outs(tmp_path / "mlb"),
        ("nfl", "pass_attempts"): _run_nfl(tmp_path / "nfl", monkeypatch),
        ("nhl", "shots_on_goal"): _run_nhl(tmp_path / "nhl"),
    }

    for (sport, stat), frame in frames.items():
        assert not frame.empty, f"{sport}.{stat} produced empty output in parity test."
        assert_simulation_contract(frame)
        assert_probability_columns_valid(frame)
        if {"run_mode", "lines_status"}.issubset(frame.columns):
            assert set(frame["run_mode"].astype(str).unique()) == {"prediction"}
            assert set(frame["lines_status"].astype(str).unique()) == {"present"}


def test_fantasy_adapter_season_horizon_scaling_contract() -> None:
    adapter = MlbSeasonProjectionAdapter(
        metric_id="hits",
        adapter_config=MlbProjectionAdapterConfig(
            input_dataset_path="tests/testdata/fantasy/mlb_batter_games_phase1.csv",
            entity_id_col="batter",
            date_col="game_date",
            seed=2026,
            min_history_games=2,
            model_name="poisson",
            train_end_date="2025-12-31",
            inference_anchor_date="2026-04-01",
            uncertainty_method="empirical_quantiles",
            source_snapshot_id="fixture-snapshot",
        ),
    )
    market = MarketDefinition(
        market_id="season_hits",
        provider="underdog",
        sport="mlb",
        mode="season_long_tournament",
        metric_id="hits",
        horizon="season",
        operator=None,
        line_value=None,
        window_start="2026-03-01",
        window_end="2026-06-01",
        game_id=None,
    )
    contest = ContestConfig(
        contest_id="season-mlb",
        provider="underdog",
        sport="mlb",
        mode="season_long_tournament",
        scoring_ruleset_id="rules",
        market_definitions=(market,),
        mode_config={},
        metadata={},
    )

    projected = adapter.project(contest)
    assert not projected.empty
    assert_horizon_semantics(
        projected,
        horizon="season",
        prediction_col="mean",
    )
