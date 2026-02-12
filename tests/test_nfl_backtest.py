import pandas as pd
from src.nfl.models import WalkForwardConfig, run_walk_forward_backtest


def _feature_cols(frame: pd.DataFrame) -> list[str]:
    return [
        c
        for c in frame.columns
        if c not in {"season", "week", "qb_id", "pass_attempts"}
    ]


def _toy_frame() -> pd.DataFrame:
    rows = []
    nfl_features = [
        "prev_attempts",
        "rolling3_attempts",
        "season_avg_attempts",
        "season_avg_attempts_to_date",
        "career_avg_attempts",
        "plays_per_game",
        "pass_rate",
        "neutral_pass_rate",
        "pass_rate_over_expected",
        "plays_faced",
        "opponent_pass_rate_allowed",
        "opponent_neutral_pass_rate",
        "qb_dropbacks",
        "avg_cpoe",
        "epa_per_dropback",
        "air_yards_per_attempt",
        "qb_rush_attempts",
        "ngs_avg_time_to_throw",
        "ngs_avg_air_yards",
        "ngs_cpoe",
        "spread",
        "total",
        "rest_days",
        "short_week",
        "is_divisional",
        "home",
    ]
    for week in range(1, 11):
        for qb_idx, qb_id in enumerate(["QB1", "QB2"], start=1):
            base = 20 + week + qb_idx
            row = {
                "season": 2023,
                "week": week,
                "qb_id": qb_id,
                "pass_attempts": float(base),
            }
            for i, col in enumerate(nfl_features, start=1):
                row[col] = float(base + i * 0.01)
            rows.append(row)
    return pd.DataFrame(rows)


def test_walk_forward_respects_temporal_order() -> None:
    frame = _toy_frame()
    features = _feature_cols(frame)
    folds = run_walk_forward_backtest(
        frame,
        features=features,
        config=WalkForwardConfig(min_train_weeks=4, step_weeks=2, max_folds=3),
    )
    assert not folds.empty
    assert len(folds) == 3
    assert (folds["train_end_week"] < folds["test_start_week"]).all()


def test_walk_forward_returns_metric_columns() -> None:
    frame = _toy_frame()
    features = _feature_cols(frame)
    folds = run_walk_forward_backtest(
        frame,
        features=features,
        config=WalkForwardConfig(min_train_weeks=5, step_weeks=1, max_folds=1),
    )
    assert len(folds) == 1
    expected = {
        "rmse",
        "mae",
        "r2",
        "baseline_rmse",
        "baseline_mae",
        "baseline_r2",
        "rows_train",
        "rows_test",
    }
    assert expected.issubset(set(folds.columns))
