from src.utils.io import load_config, read_csv
from src.data.load_props import load_strikeout_lines
from src.data.load_pitcher_stats import load_pitcher_game_logs
from src.features.opponent_k import add_opponent_k_rate
from src.features.pitcher_games import (
    aggregate_pitcher_games,
    add_rolling_features,
    add_park_factor,
)
from src.models.predict import train_model, predict_strikeouts


def test_prediction_flow():
    config = load_config("config/config.yaml")
    lines = load_strikeout_lines(config["lines_path"])
    pitch_df = read_csv(config["pitch_data_path"])
    park_df = read_csv(config["park_factors_path"])

    games = aggregate_pitcher_games(pitch_df)
    games = add_rolling_features(games)
    games = add_park_factor(games, park_df)
    games = add_opponent_k_rate(games)

    model = train_model(games)
    preds = predict_strikeouts(games, model)

    assert len(preds) == len(games)
    lines["prediction"] = preds.head(len(lines))
    assert "prediction" in lines.columns
