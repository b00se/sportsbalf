"""Run a walk-forward model tournament for MLB strikeout prediction."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import pandas as pd
from src.core.config import load_pipeline_config
from src.mlb.features import (
    add_opponent_k_rate,
    add_park_factor,
    add_rolling_features,
    aggregate_pitcher_games,
    build_historical_live_features,
)
from src.mlb.models.buckets import segmentation_config_from_model_selection
from src.mlb.models.evaluation import run_walk_forward_tournament, select_champion
from src.mlb.models.predict import FEATURES
from src.mlb.models.registry import SIMPLE_MODEL_PREFERENCE, resolve_model_specs
from src.mlb.models.strategy import strategy_candidates_from_config
from src.mlb.pipeline import (
    _clean_for_model,
    _load_or_create_park_factors,
    _normalize_opponent_feature_columns,
)
from src.utils.io import read_csv

logger = logging.getLogger(__name__)


def _prepare_training_frame(section: dict[str, object]) -> pd.DataFrame:
    """Build full historical training frame from configured paths."""

    pitch_path = str(section["pitch_data_path"])
    park_path = str(section["park_factors_path"])

    pitch_df = read_csv(pitch_path)
    park_df = _load_or_create_park_factors(pitch_df, park_path, retrain=False)

    current_games = aggregate_pitcher_games(pitch_df)
    current_games = add_rolling_features(current_games)
    current_games = add_park_factor(current_games, park_df)
    current_games = add_opponent_k_rate(current_games)
    current_games = _normalize_opponent_feature_columns(current_games)
    current_games = build_historical_live_features(current_games)

    training_paths = section.get("training_data_paths") or [pitch_path]
    frames: list[pd.DataFrame] = []
    for path in training_paths:
        if Path(path).resolve() == Path(pitch_path).resolve():
            frames.append(current_games)
            continue
        hist_df = read_csv(str(path))
        hist_games = aggregate_pitcher_games(hist_df)
        hist_games = add_rolling_features(hist_games)
        hist_games = add_park_factor(hist_games, park_df)
        hist_games = add_opponent_k_rate(hist_games)
        hist_games = _normalize_opponent_feature_columns(hist_games)
        hist_games = build_historical_live_features(hist_games)
        frames.append(hist_games)

    full = pd.concat(frames, ignore_index=True)
    if "pitcher_id" not in full.columns and "pitcher" in full.columns:
        full["pitcher_id"] = full["pitcher"]
    full.sort_values(["pitcher_id", "game_date"], inplace=True)
    full = full.drop_duplicates(subset=["pitcher_id", "game_date"], keep="last")
    return _clean_for_model(full)


def run_backtest(
    config_path: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    """Execute tournament and return leaderboard + champion metadata payload."""

    config = load_pipeline_config(
        config_path,
        sport_override="mlb",
        stat_override="strikeouts",
    )
    section = config.section

    selection_cfg = section.get("model_selection") or {}
    candidates = selection_cfg.get("candidates")
    primary_metric = str(selection_cfg.get("primary_metric", "mae"))
    tie_breakers = list(selection_cfg.get("tie_breakers", ["rmse", "r2"]))
    tie_epsilon = float(selection_cfg.get("tie_epsilon", 1e-6))
    tuning_cfg = selection_cfg.get("tuning") or {}
    tuning_enabled = bool(tuning_cfg.get("enabled", False))
    max_trials = int(tuning_cfg.get("max_trials_per_model", 1)) if tuning_enabled else 1
    segmentation = segmentation_config_from_model_selection(selection_cfg)
    strategies = strategy_candidates_from_config(segmentation)

    frame = _prepare_training_frame(section)
    specs = resolve_model_specs(candidates)
    fold_metrics, leaderboard = run_walk_forward_tournament(
        frame,
        specs=specs,
        features=FEATURES,
        strategies=strategies,
        segmentation=segmentation,
        max_trials_per_model=max_trials,
    )
    champion = select_champion(
        leaderboard,
        primary_metric=primary_metric,
        tie_breakers=tie_breakers,
        epsilon=tie_epsilon,
        simplicity_order=SIMPLE_MODEL_PREFERENCE,
    )

    metadata: dict[str, object] = {
        "config_path": config_path,
        "champion_strategy": champion.strategy_name,
        "champion_model": champion.model_name,
        "mean_mae": champion.mean_mae,
        "mean_rmse": champion.mean_rmse,
        "mean_r2": champion.mean_r2,
        "trial_id": champion.trial_id,
        "trial_params": champion.params or {},
        "features": FEATURES,
        "fold_metrics": fold_metrics.to_dict(orient="records"),
    }
    return fold_metrics, leaderboard, metadata


def main() -> None:
    """CLI entrypoint."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/mlb.yaml", help="Config YAML path")
    parser.add_argument(
        "--leaderboard-out",
        default="runtime/mlb_strikeouts_leaderboard.csv",
        help="CSV output path for aggregated leaderboard",
    )
    parser.add_argument(
        "--champion-out",
        default="runtime/mlb_strikeouts_champion.json",
        help="JSON output path for champion metadata",
    )
    parser.add_argument(
        "--fold-metrics-out",
        default="runtime/mlb_strikeouts_fold_metrics.csv",
        help="CSV output path for fold-level metrics",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s - %(message)s",
    )

    fold_metrics, leaderboard, metadata = run_backtest(args.config)

    fold_metrics_out = Path(args.fold_metrics_out)
    leaderboard_out = Path(args.leaderboard_out)
    champion_out = Path(args.champion_out)
    fold_metrics_out.parent.mkdir(parents=True, exist_ok=True)
    leaderboard_out.parent.mkdir(parents=True, exist_ok=True)
    champion_out.parent.mkdir(parents=True, exist_ok=True)

    fold_metrics.to_csv(fold_metrics_out, index=False)
    leaderboard.to_csv(leaderboard_out, index=False)
    champion_out.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    print("MLB Strikeouts Model Tournament")
    print(leaderboard.to_string(index=False))
    print(
        f"\nChampion: strategy={metadata['champion_strategy']} "
        f"model={metadata['champion_model']}"
    )
    print(f"Fold Metrics CSV: {fold_metrics_out}")
    print(f"Leaderboard CSV: {leaderboard_out}")
    print(f"Champion JSON: {champion_out}")


if __name__ == "__main__":
    main()
