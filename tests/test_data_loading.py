from pathlib import Path

import pandas as pd
from src.core.config import extract_stat_section
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
    raw_config = load_config("config/mlb.yaml")
    config = extract_stat_section(raw_config, sport="mlb", stat="strikeouts")
    lines = load_strikeout_lines(_resolve_existing_lines_path(config["lines_path"]))
    games = load_pitcher_game_logs(config["game_logs_path"])

    pitch_data_path = Path(config["pitch_data_path"])
    if not pitch_data_path.exists():
        pitch_data_path = Path("tests/testdata/pitches.csv")
    pitch_df = read_csv(str(pitch_data_path))
    agg = aggregate_pitcher_games(pitch_df)

    assert not lines.empty
    assert {"player", "k_line", "over_decimal_price", "under_decimal_price"}.issubset(
        lines.columns
    )
    assert lines["k_line"].notna().all()
    assert (lines["over_decimal_price"] > 1.0).all()
    assert (lines["under_decimal_price"] > 1.0).all()

    assert not games.empty
    assert pd.api.types.is_datetime64_any_dtype(games["game_date"])
    assert games["game_date"].notna().all()

    assert not agg.empty
    assert {"pitcher", "game_date", "strikeouts"}.issubset(agg.columns)
    assert (pd.to_numeric(agg["strikeouts"], errors="coerce") >= 0).all()

    park_path = Path(config["park_factors_path"])
    if not park_path.exists():
        park_path = Path("tests/testdata/park.csv")
    park = read_csv(str(park_path))
    assert not park.empty
    assert {"Team_abbr", "K_park_factor"}.issubset(park.columns)
