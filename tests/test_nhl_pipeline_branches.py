from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from src.core.contracts import PipelineConfig
from src.nhl.models.predict import NHL_FEATURES
from src.nhl.pipeline import (
    _prepare_training_frame,
    _resolve_sigma_series,
    _safe_read_inference_input,
    run_shots_on_goal_pipeline,
)


def test_resolve_sigma_series_fallback_and_clipping() -> None:
    inference = pd.DataFrame({"player_id": ["p1", "p2", "p3"]})
    sigma_by_player = pd.Series({"p1": 0.1, "p2": 5.0}, dtype="float64")

    sigma = _resolve_sigma_series(
        inference,
        sigma_by_player=sigma_by_player,
        global_sigma=np.nan,
        section={"fallback_std": 1.2, "min_sigma": 0.5, "max_sigma": 2.0},
    )

    assert sigma.tolist() == [0.5, 2.0, 1.2]


def test_safe_read_inference_input_handles_read_errors(monkeypatch) -> None:
    monkeypatch.setattr(
        "src.nhl.pipeline.read_csv",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("boom")),
    )

    frame = _safe_read_inference_input("missing.csv")

    assert frame.empty


def test_prepare_training_frame_filters_season_and_min_games(monkeypatch) -> None:
    skater_games = pd.DataFrame(
        [
            {"season": 2024, "player_id": "p1"},
            {"season": 2024, "player_id": "p1"},
            {"season": 2025, "player_id": "p2"},
            {"season": 2025, "player_id": "p2"},
        ]
    )

    def _fake_features(
        skater_games: pd.DataFrame, rolling_windows: list[int]
    ) -> pd.DataFrame:
        assert rolling_windows == [5]
        rows: list[dict[str, float | str]] = []
        for row in skater_games.itertuples(index=False):
            base = {feature: 1.0 for feature in NHL_FEATURES}
            rows.append(
                {
                    **base,
                    "player_id": row.player_id,
                    "shots_on_goal": 2.0,
                }
            )
        return pd.DataFrame(rows)

    monkeypatch.setattr("src.nhl.pipeline.build_sog_training_features", _fake_features)

    training = _prepare_training_frame(
        skater_games,
        {
            "training_seasons": [2025],
            "feature_rolling_windows": [5],
            "min_training_games_per_player": 2,
        },
    )

    assert not training.empty
    assert set(training["player_id"]) == {"p2"}


def test_run_pipeline_toggles_bootstrap_sampler(monkeypatch, tmp_path: Path) -> None:
    inference_input = tmp_path / "inference.csv"
    pd.DataFrame(
        [
            {
                "player_id": "p1",
                "player_name": "Player One",
                "team": "NYR",
                "opponent": "BOS",
                "game_id": "g1",
                "sog_line": 2.5,
            }
        ]
    ).to_csv(inference_input, index=False)

    config = PipelineConfig(
        config_path=tmp_path / "nhl.yaml",
        sport="nhl",
        stat="shots_on_goal",
        raw={},
        section={
            "inference_input_path": str(inference_input),
            "provider": "moneypuck_snapshot",
            "provider_seasons": [2024],
            "model_name": "xgboost",
            "fallback_std": 1.0,
            "monte_carlo_seed": 7,
            "monte_carlo_simulations": 10,
            "default_over_decimal_price": 1.9,
            "default_under_decimal_price": 1.9,
            "moneypuck_skater_games_curated_cache_path": str(
                tmp_path / "cache.parquet"
            ),
            "moneypuck_skater_games_snapshot_path": str(tmp_path / "snapshot.csv"),
            "auto_refresh_snapshot": False,
            "fail_on_provider_error": True,
            "bootstrap_enabled": True,
        },
    )

    provider_data = pd.DataFrame([{"season": 2024, "player_id": "p1"}])
    provider_result = type("ProviderResult", (), {"data": provider_data})()
    provider = type(
        "Provider", (), {"load_skater_games": lambda self, seasons: provider_result}
    )()

    monkeypatch.setattr(
        "src.nhl.pipeline.get_provider", lambda *args, **kwargs: provider
    )
    monkeypatch.setattr(
        "src.nhl.pipeline.build_sog_inference_features",
        lambda **kwargs: pd.DataFrame(
            [
                {
                    "player_id": "p1",
                    "player_name": "Player One",
                    "team": "NYR",
                    "opponent": "BOS",
                    "game_id": "g1",
                    "sog_line": 2.5,
                    "baseline_predicted_shots_on_goal": 2.6,
                    "predicted_shots_on_goal": 2.6,
                    "over_decimal_price": 1.9,
                    "under_decimal_price": 1.9,
                    **{feature: 1.0 for feature in NHL_FEATURES},
                }
            ]
        ),
    )
    monkeypatch.setattr(
        "src.nhl.pipeline._prepare_training_frame",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {
                    "player_id": "p1",
                    "shots_on_goal": 2.0,
                    **{feature: 1.0 for feature in NHL_FEATURES},
                }
            ]
        ),
    )
    monkeypatch.setattr(
        "src.nhl.pipeline._train_or_load_model",
        lambda **kwargs: (
            object(),
            {"training_rmse": 1.0, "training_mae": 0.8, "training_r2": 0.1},
            pd.Series({"p1": 1.0}, dtype="float64"),
            1.0,
            object(),
        ),
    )
    monkeypatch.setattr(
        "src.nhl.pipeline.predict_sog",
        lambda frame, model: pd.Series([2.8] * len(frame), index=frame.index),
    )

    captured: list[object | None] = []

    def _fake_apply_simulations(
        lines: pd.DataFrame,
        mean_col: str,
        std_dev: str,
        config,
        sampler=None,
        line_col: str = "sog_line",
        id_col: str = "player_id",
    ) -> pd.DataFrame:
        captured.append(sampler)
        output = lines.copy()
        output["prob_over"] = 0.55
        output["prob_under"] = 0.40
        output["prob_push"] = 0.05
        output["ev_over"] = 0.02
        output["ev_under"] = -0.01
        output["edge_over"] = 0.01
        output["edge_under"] = -0.01
        return output

    monkeypatch.setattr("src.nhl.pipeline.apply_simulations", _fake_apply_simulations)

    _ = run_shots_on_goal_pipeline(config=config, retrain=False)
    assert captured[-1] is not None

    config.section["bootstrap_enabled"] = False
    _ = run_shots_on_goal_pipeline(config=config, retrain=False)
    assert captured[-1] is None
