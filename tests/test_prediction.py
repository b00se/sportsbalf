from pathlib import Path

import yaml

from src.mlb.pipeline import run


def _config_with_existing_lines(tmp_path: Path) -> str:
    base = Path("config/mlb.yaml")
    config = yaml.safe_load(base.read_text(encoding="utf-8"))

    lines_path = Path(config["lines_path"])
    if not lines_path.exists():
        dated_files = sorted(Path("data/lines").glob("strikeouts_*.csv"))
        config["lines_path"] = (
            str(dated_files[-1]) if dated_files else "tests/testdata/lines_with_odds.csv"
        )

    out_path = tmp_path / "mlb_test.yaml"
    out_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return str(out_path)


def test_prediction_flow(tmp_path: Path):
    result = run(_config_with_existing_lines(tmp_path))
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
