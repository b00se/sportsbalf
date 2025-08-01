from src.utils.io import load_config, read_csv
from src.data.load_props import load_strikeout_lines
from src.data.load_pitcher_stats import load_pitcher_game_logs
from src.features.pitcher_games import aggregate_pitcher_games


def test_loaders():
    config = load_config("config/config.yaml")
    lines = load_strikeout_lines(config["lines_path"])
    games = load_pitcher_game_logs(config["game_logs_path"])
    pitch_df = read_csv(config["pitch_data_path"])
    agg = aggregate_pitcher_games(pitch_df)

    assert not lines.empty
    assert not games.empty
    assert not agg.empty

    park = read_csv(config["park_factors_path"])
    assert not park.empty
