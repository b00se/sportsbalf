import pandas as pd
import yaml
from src.nfl import pipeline


def _mini_dataset() -> pd.DataFrame:
    data = []
    for week, qb_id in enumerate(["QB1", "QB2", "QB3"], start=1):
        data.append(
            {
                "season": 2022,
                "week": week,
                "game_id": f"2022_{week:02d}_AAA",
                "qb_name": f"QB {week}",
                "qb_id": qb_id,
                "team": "AAA",
                "opponent": "BBB",
                "home": week % 2 == 0,
                "spread": -1.5 + week,
                "total": 45.0 + week,
                "pass_attempts": 30 + week,
                "ud_line": 31.5 + week,
                "prev_attempts": 28 + week,
                "rolling3_attempts": 29 + week,
                "season_avg_attempts": 30 + week,
                "career_avg_attempts": 31 + week,
                "season_attempts_to_date": 25 + week,
                "season_games_played": week - 1,
                "season_avg_attempts_to_date": 27 + week,
                "plays_per_game": 62 + week,
                "pass_rate": 0.56 + week * 0.01,
                "neutral_pass_rate": 0.54 + week * 0.01,
                "pass_rate_over_expected": 0.02,
                "plays_faced": 61 + week,
                "opponent_pass_rate_allowed": 0.55,
                "opponent_neutral_pass_rate": 0.52,
                "qb_dropbacks": 34 + week,
                "avg_cpoe": 0.5,
                "epa_per_dropback": 0.1 * week,
                "air_yards_per_attempt": 6.5 + week,
                "qb_rush_attempts": 4 + week,
                "ngs_avg_time_to_throw": 2.5,
                "ngs_avg_air_yards": 7.5,
                "ngs_cpoe": 0.01,
                "rest_days": 7,
                "short_week": False,
                "is_divisional": False,
            }
        )
    return pd.DataFrame(data)


def test_pipeline_run_returns_enriched_frame(tmp_path, monkeypatch):
    dataset = _mini_dataset()
    dataset_path = tmp_path / "qb_dataset.parquet"
    dataset.to_parquet(dataset_path, index=False)

    cfg = {
        "pipeline": {"sport": "nfl", "stat": "pass_attempts"},
        "nfl": {
            "pass_attempts": {
                "dataset_path": str(dataset_path),
                "model_path": str(tmp_path / "model.joblib"),
                "rebuild_dataset": False,
                "training_years": [2022],
                "inference_years": [2022],
                "monte_carlo_simulations": 1000,
                "fallback_std": 1.0,
            }
        },
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")

    monkeypatch.setattr(
        pipeline, "import_ud_pass_attempt_lines", lambda **_: pd.DataFrame()
    )

    result = pipeline.run(config_path=config_path, retrain=True)
    assert not result.empty
    assert {
        "prob_over",
        "prob_under",
        "predicted_pass_attempts",
        "attempts_line",
    }.issubset(result.columns)
