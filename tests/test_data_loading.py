from pathlib import Path

from src.mlb.data.load_pitcher_stats import load_pitcher_game_logs
from src.mlb.data.load_props import load_strikeout_lines
from src.mlb.features.pitcher_games import aggregate_pitcher_games
from src.utils.io import load_config, read_csv


def _resolve_existing_lines_path(config_lines_path: str) -> str:
    candidate = Path(config_lines_path)
    if candidate.exists():
        return str(candidate)

    dated_files = sorted(Path("data/lines").glob("strikeouts_*.csv"))
    if dated_files:
        return str(dated_files[-1])
    return "tests/testdata/lines_with_odds.csv"


def test_loaders():
    config = load_config("config/mlb.yaml")
    lines = load_strikeout_lines(_resolve_existing_lines_path(config["lines_path"]))
    games = load_pitcher_game_logs(config["game_logs_path"])
    pitch_df = read_csv(config["pitch_data_path"])
    agg = aggregate_pitcher_games(pitch_df)

    assert not lines.empty
    assert not games.empty
    assert not agg.empty

    park = read_csv(config["park_factors_path"])
    assert not park.empty
