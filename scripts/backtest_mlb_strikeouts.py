"""Run a walk-forward model tournament for MLB strikeout prediction."""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

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
from src.mlb.models.evaluation import (
    ChampionSelection,
    run_walk_forward_tournament,
    select_champion,
)
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

BASELINE_FEATURES: list[str] = [
    "rolling_K_avg_3",
    "rolling_K_avg_5",
    "rolling_pitch_count_5",
    "rolling_K_rate",
    "opponent_k_pct",
    "opponent_k_rate",
    "park_factor_K",
    "rest_days",
]


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

    training_paths = cast(
        Sequence[str], section.get("training_data_paths") or [pitch_path]
    )
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
    full = _normalize_opponent_feature_columns(full)
    full = build_historical_live_features(full)
    if "pitcher_id" not in full.columns and "pitcher" in full.columns:
        full["pitcher_id"] = full["pitcher"]
    full.sort_values(["pitcher_id", "game_date"], inplace=True)
    full = full.drop_duplicates(subset=["pitcher_id", "game_date"], keep="last")
    return _clean_for_model(full)


def _run_tournament(
    frame: pd.DataFrame,
    *,
    selection_cfg: dict[str, object],
    features: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, ChampionSelection]:
    """Run tournament for a feature set and return fold metrics, leaderboard, winner."""

    candidates = cast(Sequence[str] | None, selection_cfg.get("candidates"))
    primary_metric = str(selection_cfg.get("primary_metric", "mae"))
    tie_breakers = list(
        cast(Sequence[str], selection_cfg.get("tie_breakers", ["rmse", "r2"]))
    )
    tie_epsilon = float(cast(float, selection_cfg.get("tie_epsilon", 1e-6)))
    tuning_cfg = cast(Mapping[str, object], selection_cfg.get("tuning") or {})
    tuning_enabled = bool(tuning_cfg.get("enabled", False))
    max_trials = (
        int(cast(int, tuning_cfg.get("max_trials_per_model", 1)))
        if tuning_enabled
        else 1
    )
    segmentation = segmentation_config_from_model_selection(selection_cfg)
    strategies = strategy_candidates_from_config(segmentation)

    specs = resolve_model_specs(candidates)
    fold_metrics, leaderboard = run_walk_forward_tournament(
        frame,
        specs=specs,
        features=features,
        strategies=strategies,
        segmentation=segmentation,
        max_trials_per_model=max_trials,
    )
    winner = select_champion(
        leaderboard,
        primary_metric=primary_metric,
        tie_breakers=tie_breakers,
        epsilon=tie_epsilon,
        simplicity_order=SIMPLE_MODEL_PREFERENCE,
    )
    return fold_metrics, leaderboard, winner


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
    frame = _prepare_training_frame(section)
    fold_metrics, leaderboard, champion = _run_tournament(
        frame,
        selection_cfg=selection_cfg,
        features=FEATURES,
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


def run_feature_set_comparison(
    config_path: str,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Run baseline-vs-enriched comparison and return summary artifacts."""

    config = load_pipeline_config(
        config_path,
        sport_override="mlb",
        stat_override="strikeouts",
    )
    section = config.section
    selection_cfg = section.get("model_selection") or {}
    frame = _prepare_training_frame(section)

    rows: list[dict[str, object]] = []
    details: dict[str, object] = {}
    for label, feature_set in [
        ("baseline", BASELINE_FEATURES),
        ("enriched", FEATURES),
    ]:
        fold_metrics, leaderboard, champion = _run_tournament(
            frame,
            selection_cfg=selection_cfg,
            features=feature_set,
        )
        rows.append(
            {
                "variant": label,
                "feature_count": len(feature_set),
                "champion_model": champion.model_name,
                "champion_strategy": champion.strategy_name,
                "mean_mae": float(champion.mean_mae),
                "mean_rmse": float(champion.mean_rmse),
                "mean_r2": float(champion.mean_r2),
            }
        )
        details[label] = {
            "features": feature_set,
            "fold_metrics": fold_metrics.to_dict(orient="records"),
            "leaderboard_rows": int(len(leaderboard)),
        }

    comparison = pd.DataFrame(rows)
    baseline_mae = float(
        comparison.loc[comparison["variant"] == "baseline", "mean_mae"].iloc[0]
    )
    enriched_mae = float(
        comparison.loc[comparison["variant"] == "enriched", "mean_mae"].iloc[0]
    )
    mae_improvement = baseline_mae - enriched_mae
    live_cov = {
        "weather_known_pct": float(
            pd.to_numeric(frame.get("weather_known_flag"), errors="coerce")
            .fillna(0.0)
            .mean()
        ),
        "roof_known_pct": float(
            (
                ~frame.get("roof_state", pd.Series("unknown", index=frame.index))
                .astype(str)
                .str.lower()
                .eq("unknown")
            ).mean()
        ),
        "umpire_known_pct": float(
            pd.to_numeric(frame.get("umpire_known_flag"), errors="coerce")
            .fillna(0.0)
            .mean()
        ),
    }
    summary: dict[str, object] = {
        "config_path": config_path,
        "baseline_mae": baseline_mae,
        "enriched_mae": enriched_mae,
        "mae_improvement_vs_baseline": mae_improvement,
        "mae_gate_passed": bool(mae_improvement > 0),
        "coverage": live_cov,
        "details": details,
    }
    return comparison, summary


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
    parser.add_argument(
        "--compare-feature-sets",
        action="store_true",
        help="Run baseline-vs-enriched feature MAE comparison.",
    )
    parser.add_argument(
        "--comparison-out",
        default="runtime/mlb_strikeouts_feature_comparison.csv",
        help="CSV output path for feature-set comparison rows.",
    )
    parser.add_argument(
        "--require-mae-lift",
        action="store_true",
        help="Exit non-zero when enriched feature set does not improve MAE.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s - %(message)s",
    )

    if args.compare_feature_sets:
        comparison, summary = run_feature_set_comparison(args.config)
        comparison_out = Path(args.comparison_out)
        champion_out = Path(args.champion_out)
        comparison_out.parent.mkdir(parents=True, exist_ok=True)
        champion_out.parent.mkdir(parents=True, exist_ok=True)
        comparison.to_csv(comparison_out, index=False)
        champion_out.write_text(json.dumps(summary, indent=2), encoding="utf-8")

        print("MLB Strikeouts Feature-Set Comparison")
        print(comparison.to_string(index=False))
        print(
            f"\nMAE improvement vs baseline: "
            f"{summary['mae_improvement_vs_baseline']:.6f}"
        )
        print(f"MAE gate passed: {summary['mae_gate_passed']}")
        print(f"Comparison CSV: {comparison_out}")
        print(f"Summary JSON: {champion_out}")

        if args.require_mae_lift and not bool(summary["mae_gate_passed"]):
            raise SystemExit(2)
        return

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
