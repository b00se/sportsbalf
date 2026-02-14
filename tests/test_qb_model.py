import pandas as pd
from src.nfl.models.predict import (
    predict_attempts,
    residual_std,
    train_model,
)


def _sample_training_frame() -> pd.DataFrame:
    rows = []
    for week in (1, 2, 3):
        rows.append(
            {
                "season": 2022,
                "week": week,
                "game_id": f"2022_{week:02d}_TEAM",
                "qb_id": f"QB{week}",
                "pass_attempts": 30 + week,
                "prev_attempts": 28 + week,
                "rolling3_attempts": 29 + week,
                "season_avg_attempts": 30 + week,
                "season_avg_attempts_to_date": 29 + week,
                "career_avg_attempts": 31 + week,
                "plays_per_game": 62 + week,
                "pass_rate": 0.56 + week * 0.01,
                "neutral_pass_rate": 0.54 + week * 0.01,
                "pass_rate_over_expected": 0.03,
                "plays_faced": 60 + week,
                "opponent_pass_rate_allowed": 0.55,
                "opponent_neutral_pass_rate": 0.53,
                "qb_dropbacks": 35 + week,
                "avg_cpoe": 0.5,
                "epa_per_dropback": 0.1 * week,
                "air_yards_per_attempt": 7.0 + week,
                "qb_rush_attempts": 3 + week,
                "ngs_avg_time_to_throw": 2.5 + week * 0.01,
                "ngs_avg_air_yards": 7.2 + week * 0.1,
                "ngs_cpoe": 0.02,
                "spread": -2.5 + week,
                "total": 45.5,
                "rest_days": 7,
                "short_week": False,
                "is_divisional": week % 2 == 0,
                "home": week % 2 == 1,
            }
        )
    df = pd.DataFrame(rows)
    return df


def test_train_and_predict_model(tmp_path):
    frame = _sample_training_frame()
    model_one = train_model(frame)
    model_two = train_model(frame)
    predictions_one = predict_attempts(frame, model_one)
    predictions_two = predict_attempts(frame, model_two)

    assert len(predictions_one) == len(frame)
    assert len(predictions_two) == len(frame)
    assert predictions_one.notna().all()
    assert predictions_two.notna().all()
    assert (predictions_one >= 0.0).all()
    assert (predictions_two >= 0.0).all()
    pd.testing.assert_series_equal(
        predictions_one.reset_index(drop=True),
        predictions_two.reset_index(drop=True),
        check_dtype=False,
    )

    sigma = residual_std(frame["pass_attempts"], predictions_one)
    assert sigma >= 0
