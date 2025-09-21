from src.mlb.pipeline import run


def test_prediction_flow():
    result = run("config/mlb.yaml")
    required_cols = {
        "predicted_strikeouts",
        "prob_over",
        "prob_under",
        "ev_over",
        "ev_under",
        "model_residual_std",
        "simulated_median",
        "upcoming_game_date",
        "upcoming_opponent",
        "upcoming_rest_days",
        "upcoming_park_factor_K",
    }

    assert required_cols.issubset(result.columns)
    assert result["predicted_strikeouts"].notna().any()
    assert result["simulated_median"].notna().any()
