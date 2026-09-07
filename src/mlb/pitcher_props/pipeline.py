"""Shared MLB pitcher-prop orchestration for multi-stat execution."""

from __future__ import annotations

import json
import logging
import re
from hashlib import sha256
from pathlib import Path
from collections.abc import Sequence
from typing import cast

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from src.core.contracts import PipelineConfig
from src.mlb.data.load_props import load_pitcher_prop_lines
from src.mlb.features.feature_store import (
    LIVE_CONTEXT_FEATURE_COLUMNS,
    build_historical_live_features,
    coverage_metrics,
    ensure_live_feature_defaults,
    merge_live_feature_frame,
)
from src.mlb.features.live_context import LiveContextService
from src.mlb.features.rolling import add_rolling_features
from src.mlb.models.buckets import (
    SegmentationConfig,
    segmentation_config_from_model_selection,
)
from src.mlb.models.distributions import ResidualBootstrapper
from src.mlb.models.evaluation import run_walk_forward_tournament, select_champion
from src.mlb.models.monte_carlo import MonteCarloConfig, apply_simulations
from src.mlb.models.registry import (
    SIMPLE_MODEL_PREFERENCE,
    get_model_spec,
    resolve_model_specs,
)
from src.mlb.models.strategy import (
    predict_with_strategy_artifact,
    strategy_candidates_from_config,
    strategy_metadata,
    train_strategy_artifact,
)
from src.mlb.models.trainers import fit_estimator
from src.mlb.pitcher_props.data import persist_reusable_tables
from src.mlb.pitcher_props.descriptors import StatDescriptor, get_stat_descriptor
from src.mlb.pitcher_props.park_factors import (
    add_rolling_park_factor,
    park_factor_lookup,
)
from src.utils.io import read_csv
from src.utils.names import normalize_person_name, resolve_unique_name_match

logger = logging.getLogger(__name__)

BASE_FEATURES: list[str] = [
    "rolling_K_avg_3",
    "rolling_K_avg_5",
    "rolling_pitch_count_5",
    "rolling_K_rate",
    "rest_days",
    "rolling_on_base_events_allowed_5",
    "rolling_hard_contact_allowed_5",
] + [col for col in LIVE_CONTEXT_FEATURE_COLUMNS if col != "roof_state"]


SIMULATION_COLUMNS: tuple[str, ...] = (
    "prob_over",
    "prob_under",
    "prob_push",
    "ev_over",
    "ev_under",
    "edge_over",
    "edge_under",
    "simulated_mean",
    "simulated_std",
    "simulated_median",
)


def _model_features(descriptor: StatDescriptor) -> list[str]:
    """Return ordered feature list for the provided stat descriptor.

    Args:
        descriptor: Stat descriptor payload.

    Returns:
        Ordered model feature list.
    """

    return BASE_FEATURES + [descriptor.opponent_feature_col, descriptor.park_factor_col]


def _person_lookup_key(name: object, *, fallback_id: object | None = None) -> str:
    """Build a stable lookup key from person-name text or a numeric ID."""

    normalized = normalize_person_name(str(name))
    if normalized:
        return normalized

    null_tokens = {"", "nan", "none", "nat", "<na>", "null"}
    for candidate in (fallback_id, name):
        raw = str(candidate).strip().lower()
        if raw not in null_tokens:
            return f"id:{raw}"
    return ""


def _feature_schema_hash(features: list[str]) -> str:
    """Return deterministic hash for a feature schema."""

    payload = ",".join(features).encode("utf-8")
    return sha256(payload).hexdigest()


def _model_selection_config(
    section: dict[str, object],
    descriptor: StatDescriptor,
) -> dict[str, object]:
    """Return normalized model-selection configuration with defaults."""

    raw = section.get("model_selection")
    if not isinstance(raw, dict):
        raw = {}

    stat_tag = descriptor.stat
    holdout = raw.get("final_holdout")
    if not isinstance(holdout, dict):
        holdout = {}

    return {
        "enabled": bool(raw.get("enabled", False)),
        "candidates": raw.get("candidates"),
        "primary_metric": raw.get("primary_metric", "mae"),
        "tie_breakers": raw.get("tie_breakers", ["rmse", "r2"]),
        "tie_epsilon": float(raw.get("tie_epsilon", 1e-6)),
        "runtime_budget_minutes": int(raw.get("runtime_budget_minutes", 30)),
        "tuning": raw.get("tuning", {}),
        "segmentation": raw.get("segmentation", {}),
        "champion_model_path": raw.get(
            "champion_model_path",
            f"models/mlb_{stat_tag}_champion.joblib",
        ),
        "champion_metadata_path": raw.get(
            "champion_metadata_path",
            f"models/mlb_{stat_tag}_champion_metadata.json",
        ),
        "leaderboard_path": raw.get(
            "leaderboard_path",
            f"models/mlb_{stat_tag}_leaderboard.csv",
        ),
        "final_holdout": {
            "enabled": bool(holdout.get("enabled", False)),
            "seasons": max(1, int(holdout.get("seasons", 1))),
            "baseline_model": str(holdout.get("baseline_model", "xgboost")),
            "report_path": str(
                holdout.get(
                    "report_path",
                    f"models/mlb_{stat_tag}_final_holdout_report.csv",
                )
            ),
        },
    }


def _persist_champion_metadata(metadata_path: Path, payload: dict[str, object]) -> None:
    """Persist champion metadata JSON to disk."""

    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _add_rolling_pressure_features(games: pd.DataFrame) -> pd.DataFrame:
    """Add rolling baserunner pressure / contact quality context features."""

    enriched = games.sort_values(["pitcher", "game_date"]).copy()
    for source, target, default in [
        ("on_base_events_allowed", "rolling_on_base_events_allowed_5", 0.25),
        ("hard_contact_rate_allowed", "rolling_hard_contact_allowed_5", 0.35),
    ]:
        if source not in enriched.columns:
            enriched[target] = default
            continue

        rolled = (
            enriched.groupby("pitcher")[source]
            .rolling(window=5, min_periods=1)
            .mean()
            .groupby(level=0)
            .shift(1)
            .droplevel(0)
        )
        enriched[target] = pd.to_numeric(rolled, errors="coerce").fillna(default)

    return enriched


def _add_opponent_tendency(
    games: pd.DataFrame,
    *,
    target_col: str,
    feature_col: str,
) -> pd.DataFrame:
    """Attach opponent stat tendency using leakage-safe historical means.

    Args:
        games: Game-level frame.
        target_col: Target stat column to average by opponent.
        feature_col: Output feature name.

    Returns:
        Enriched frame with opponent tendency feature.
    """

    enriched = games.copy()
    enriched["game_date"] = pd.to_datetime(enriched["game_date"], errors="coerce")
    enriched = enriched.dropna(subset=["game_date"]).copy()
    if enriched.empty:
        enriched[feature_col] = 0.0
        return enriched

    sort_cols = ["game_date", "opponent_team"]
    for candidate in ["game_pk", "pitcher_id", "pitcher"]:
        if candidate in enriched.columns:
            sort_cols.append(candidate)
    enriched = enriched.sort_values(sort_cols, kind="stable").reset_index(drop=True)

    enriched["_target_value"] = pd.to_numeric(enriched[target_col], errors="coerce")
    enriched["_target_valid"] = enriched["_target_value"].notna().astype(int)

    opponent_daily = (
        enriched.groupby(["opponent_team", "game_date"], as_index=False)
        .agg(
            day_target_sum=("_target_value", "sum"),
            day_target_count=("_target_valid", "sum"),
        )
        .sort_values(["opponent_team", "game_date"], kind="stable")
    )
    opponent_daily["prior_sum"] = (
        opponent_daily.groupby("opponent_team", sort=False)["day_target_sum"]
        .cumsum()
        .shift(1)
    )
    opponent_daily["prior_count"] = (
        opponent_daily.groupby("opponent_team", sort=False)["day_target_count"]
        .cumsum()
        .shift(1)
    )
    opponent_daily["_opponent_prior"] = opponent_daily["prior_sum"] / opponent_daily[
        "prior_count"
    ].replace(0.0, np.nan)

    global_daily = (
        enriched.groupby("game_date", as_index=False)
        .agg(
            day_target_sum=("_target_value", "sum"),
            day_target_count=("_target_valid", "sum"),
        )
        .sort_values("game_date", kind="stable")
    )
    global_daily["global_prior"] = global_daily["day_target_sum"].cumsum().shift(
        1
    ) / global_daily["day_target_count"].cumsum().shift(1).replace(0.0, np.nan)

    merged = enriched.merge(
        opponent_daily[["opponent_team", "game_date", "_opponent_prior"]],
        on=["opponent_team", "game_date"],
        how="left",
    ).merge(
        global_daily[["game_date", "global_prior"]],
        on="game_date",
        how="left",
    )
    merged[feature_col] = (
        pd.to_numeric(merged["_opponent_prior"], errors="coerce")
        .fillna(pd.to_numeric(merged["global_prior"], errors="coerce"))
        .fillna(0.0)
    )
    return merged.drop(
        columns=[
            "_target_value",
            "_target_valid",
            "_opponent_prior",
            "global_prior",
        ]
    )


def _build_training_games(
    section: dict[str, object],
    descriptor: StatDescriptor,
) -> pd.DataFrame:
    """Build concatenated training games across configured training paths."""

    pitch_data_path = str(section["pitch_data_path"])
    training_paths = [
        str(path)
        for path in cast(
            Sequence[object], section.get("training_data_paths") or [pitch_data_path]
        )
    ]

    earned_runs_source: pd.DataFrame | None = None
    earned_runs_labels_path = section.get("earned_runs_labels_path")
    if descriptor.stat == "earned_runs" and earned_runs_labels_path:
        try:
            earned_runs_source = read_csv(str(earned_runs_labels_path))
        except FileNotFoundError:
            logger.warning(
                "Configured earned-runs labels path '%s' was not found; "
                "falling back to score-delta/fallback-column labels.",
                earned_runs_labels_path,
            )
            earned_runs_source = None

    frames: list[pd.DataFrame] = []
    for path in training_paths:
        pitch_df = read_csv(path)
        pitcher_out = (
            section.get("pitcher_dataset_output_path")
            if path == pitch_data_path
            else None
        )
        batter_out = (
            section.get("batter_dataset_output_path")
            if path == pitch_data_path
            else None
        )

        games, _batter_games = persist_reusable_tables(
            pitch_df,
            pitcher_output_path=str(pitcher_out) if pitcher_out else None,
            batter_output_path=str(batter_out) if batter_out else None,
            earned_runs_source=earned_runs_source,
        )
        if "is_starter" in games.columns:
            games = games.loc[
                pd.to_numeric(games["is_starter"], errors="coerce")
                .fillna(0)
                .astype(int)
                .eq(1)
            ].copy()
        games = add_rolling_features(games)
        games = _add_rolling_pressure_features(games)
        games = _add_opponent_tendency(
            games,
            target_col=descriptor.target_col,
            feature_col=descriptor.opponent_feature_col,
        )
        games = add_rolling_park_factor(
            games,
            target_col=descriptor.target_col,
            park_col=descriptor.park_factor_col,
            min_samples=int(cast(int, section.get("park_factor_min_samples", 20))),
            half_life_games=int(
                cast(int, section.get("park_factor_half_life_games", 60))
            ),
        )
        games = build_historical_live_features(games)
        games = ensure_live_feature_defaults(games)

        if descriptor.stat == "strikeouts":
            # Retain compatibility for strikeout-specific downstream expectations.
            games["opponent_k_rate"] = games[descriptor.opponent_feature_col]
            games["opponent_k_pct"] = games[descriptor.opponent_feature_col]

        if "pitcher_id" not in games.columns and "pitcher" in games.columns:
            games["pitcher_id"] = games["pitcher"]
        if "pitcher_name" not in games.columns:
            games["pitcher_name"] = games["pitcher_id"].astype(str)

        frames.append(games)

    training_games = pd.concat(frames, ignore_index=True)
    training_games["game_date"] = pd.to_datetime(
        training_games["game_date"], errors="coerce"
    )
    training_games = training_games.dropna(subset=["game_date"]).copy()
    training_games.sort_values(["pitcher_id", "game_date"], inplace=True)
    training_games = training_games.drop_duplicates(
        subset=["pitcher_id", "game_date"], keep="last"
    )
    return training_games


def _clean_for_model(games: pd.DataFrame, descriptor: StatDescriptor) -> pd.DataFrame:
    """Return model-clean frame for a given stat target."""

    features = _model_features(descriptor)
    frame = ensure_live_feature_defaults(games.replace([np.inf, -np.inf], np.nan))
    required = [descriptor.target_col] + features
    return frame.dropna(subset=required).copy()


def _persist_label_quality_report(
    games: pd.DataFrame,
    *,
    descriptor: StatDescriptor,
    report_path: str,
) -> None:
    """Persist per-season label fallback share diagnostics."""

    if games.empty or "game_date" not in games.columns:
        return

    report = games.copy()
    report["game_date"] = pd.to_datetime(report["game_date"], errors="coerce")
    report = report.dropna(subset=["game_date"]).copy()
    if report.empty:
        return

    report["season"] = report["game_date"].dt.year.astype(int)
    report["target_non_null"] = pd.to_numeric(
        report[descriptor.target_col], errors="coerce"
    ).notna()

    if (
        descriptor.stat == "earned_runs"
        and "earned_runs_fallback_used" in report.columns
    ):
        report["fallback_used"] = pd.to_numeric(
            report["earned_runs_fallback_used"], errors="coerce"
        ).fillna(0.0)
    else:
        report["fallback_used"] = 0.0
    if (
        descriptor.stat == "earned_runs"
        and "earned_runs_high_fidelity_used" in report.columns
    ):
        report["high_fidelity_used"] = pd.to_numeric(
            report["earned_runs_high_fidelity_used"], errors="coerce"
        ).fillna(0.0)
    else:
        report["high_fidelity_used"] = 0.0

    grouped = (
        report.groupby("season", as_index=False)
        .agg(
            rows=("target_non_null", "sum"),
            fallback_rows=("fallback_used", "sum"),
            high_fidelity_rows=("high_fidelity_used", "sum"),
        )
        .sort_values("season", kind="stable")
    )
    grouped["fallback_rows"] = grouped["fallback_rows"].astype(int)
    grouped["high_fidelity_rows"] = grouped["high_fidelity_rows"].astype(int)
    grouped["fallback_share"] = np.where(
        grouped["rows"] > 0,
        grouped["fallback_rows"] / grouped["rows"],
        0.0,
    )
    grouped["high_fidelity_share"] = np.where(
        grouped["rows"] > 0,
        grouped["high_fidelity_rows"] / grouped["rows"],
        0.0,
    )
    grouped["stat"] = descriptor.stat
    grouped = grouped[
        [
            "stat",
            "season",
            "rows",
            "fallback_rows",
            "fallback_share",
            "high_fidelity_rows",
            "high_fidelity_share",
        ]
    ].reset_index(drop=True)

    output = Path(report_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    grouped.to_csv(output, index=False)


def _score_regression(actual: pd.Series, predicted: pd.Series) -> dict[str, float]:
    """Compute regression metrics for holdout comparisons."""

    mae = float(mean_absolute_error(actual, predicted))
    rmse = float(np.sqrt(mean_squared_error(actual, predicted)))
    r2 = float(r2_score(actual, predicted))
    return {"mae": mae, "rmse": rmse, "r2": r2}


def _persist_final_holdout_report(
    frame: pd.DataFrame,
    *,
    descriptor: StatDescriptor,
    winner_model_name: str,
    winner_strategy_name: str,
    winner_params: dict[str, float | int] | None,
    segmentation: SegmentationConfig,
    features: list[str],
    holdout_cfg: dict[str, object],
) -> None:
    """Persist champion-vs-baseline metrics on an untouched final-season slice."""

    if not bool(holdout_cfg.get("enabled", False)):
        return

    dated = frame.copy()
    dated["game_date"] = pd.to_datetime(dated["game_date"], errors="coerce")
    dated = dated.dropna(subset=["game_date"]).copy()
    if dated.empty:
        return
    dated["season"] = dated["game_date"].dt.year.astype(int)
    seasons = sorted(int(cast(float, x)) for x in dated["season"].unique())

    holdout_seasons = max(
        1, int(cast(int, holdout_cfg.get("seasons", 1)))
    )
    if len(seasons) <= holdout_seasons:
        logger.warning(
            "Skipping final holdout report for '%s': only %d season(s) available.",
            descriptor.stat,
            len(seasons),
        )
        return

    locked = seasons[-holdout_seasons:]
    train_df = dated[~dated["season"].isin(locked)].copy()
    holdout_df = dated[dated["season"].isin(locked)].copy()
    if train_df.empty or holdout_df.empty:
        return

    baseline_name = str(holdout_cfg.get("baseline_model", "xgboost"))
    baseline_spec = get_model_spec(baseline_name)
    baseline_artifact = train_strategy_artifact(
        train_df,
        spec=baseline_spec,
        features=features,
        target_col=descriptor.target_col,
        strategy_name="global",
        segmentation=segmentation,
    )
    champion_spec = get_model_spec(winner_model_name)
    champion_artifact = train_strategy_artifact(
        train_df,
        spec=champion_spec,
        features=features,
        target_col=descriptor.target_col,
        strategy_name=winner_strategy_name,
        segmentation=segmentation,
        model_params=winner_params,
    )

    actual = pd.to_numeric(holdout_df[descriptor.target_col], errors="coerce")
    baseline_preds = predict_with_strategy_artifact(
        holdout_df,
        features=features,
        name="prediction",
        artifact=baseline_artifact,
    )
    champion_preds = predict_with_strategy_artifact(
        holdout_df,
        features=features,
        name="prediction",
        artifact=champion_artifact,
    )

    baseline_scores = _score_regression(actual, baseline_preds)
    champion_scores = _score_regression(actual, champion_preds)
    report_rows = [
        {
            "stat": descriptor.stat,
            "model_role": "baseline",
            "model_name": baseline_name,
            "strategy_name": "global",
            "holdout_seasons": ",".join(str(x) for x in locked),
            "train_rows": int(len(train_df)),
            "holdout_rows": int(len(holdout_df)),
            "mae": baseline_scores["mae"],
            "rmse": baseline_scores["rmse"],
            "r2": baseline_scores["r2"],
        },
        {
            "stat": descriptor.stat,
            "model_role": "champion",
            "model_name": winner_model_name,
            "strategy_name": winner_strategy_name,
            "holdout_seasons": ",".join(str(x) for x in locked),
            "train_rows": int(len(train_df)),
            "holdout_rows": int(len(holdout_df)),
            "mae": champion_scores["mae"],
            "rmse": champion_scores["rmse"],
            "r2": champion_scores["r2"],
        },
    ]
    report = pd.DataFrame(report_rows)
    report["mae_delta_vs_baseline"] = report["mae"] - baseline_scores["mae"]
    report["rmse_delta_vs_baseline"] = report["rmse"] - baseline_scores["rmse"]
    report["r2_delta_vs_baseline"] = report["r2"] - baseline_scores["r2"]

    output = Path(str(holdout_cfg["report_path"]))
    output.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(output, index=False)


def _train_or_load(
    frame: pd.DataFrame,
    *,
    section: dict[str, object],
    descriptor: StatDescriptor,
    retrain: bool,
) -> tuple[object, str, str]:
    """Train or load a model artifact for the supplied stat."""

    model_path = Path(str(section["model_path"]))
    features = _model_features(descriptor)
    selection = _model_selection_config(section, descriptor)

    if not bool(selection["enabled"]):
        if model_path.exists() and not retrain:
            try:
                return joblib.load(model_path), "xgboost", "global"
            except Exception as exc:  # pragma: no cover - defensive fallback
                logger.warning(
                    "Failed to load model '%s': %s. Retraining.", model_path, exc
                )

        spec = get_model_spec("xgboost")
        model = fit_estimator(
            frame,
            spec=spec,
            features=features,
            target_col=descriptor.target_col,
        )
        model_path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(model, model_path)
        return model, "xgboost", "global"

    champion_model_path = Path(str(selection["champion_model_path"]))
    champion_metadata_path = Path(str(selection["champion_metadata_path"]))
    leaderboard_path = Path(str(selection["leaderboard_path"]))
    candidates = selection["candidates"]
    tuning_cfg = selection["tuning"] if isinstance(selection["tuning"], dict) else {}
    tuning_enabled = bool(tuning_cfg.get("enabled", False))
    max_trials = int(tuning_cfg.get("max_trials_per_model", 1)) if tuning_enabled else 1
    segmentation = segmentation_config_from_model_selection(selection)
    strategies = strategy_candidates_from_config(segmentation)

    should_retrain_champion = (
        retrain
        or not champion_model_path.exists()
        or not champion_metadata_path.exists()
    )
    if should_retrain_champion:
        try:
            specs = resolve_model_specs(cast(Sequence[str] | None, candidates))
            fold_metrics, leaderboard = run_walk_forward_tournament(
                frame,
                specs=specs,
                features=features,
                target_col=descriptor.target_col,
                date_col="game_date",
                strategies=strategies,
                segmentation=segmentation,
                max_trials_per_model=max_trials,
            )
            winner = select_champion(
                leaderboard,
                primary_metric=str(selection["primary_metric"]),
                tie_breakers=list(cast(Sequence[str], selection["tie_breakers"])),
                epsilon=float(cast(float, selection["tie_epsilon"])),
                simplicity_order=SIMPLE_MODEL_PREFERENCE,
            )
            winner_spec = get_model_spec(winner.model_name)
            artifact = train_strategy_artifact(
                frame,
                spec=winner_spec,
                features=features,
                target_col=descriptor.target_col,
                strategy_name=winner.strategy_name,
                segmentation=segmentation,
                model_params=winner.params,
            )
            champion_model_path.parent.mkdir(parents=True, exist_ok=True)
            joblib.dump(artifact, champion_model_path)

            leaderboard_path.parent.mkdir(parents=True, exist_ok=True)
            leaderboard.to_csv(leaderboard_path, index=False)

            dated = pd.to_datetime(frame["game_date"], errors="coerce")
            metadata_payload: dict[str, object] = {
                "model_name": winner.model_name,
                "strategy_name": winner.strategy_name,
                "stat": descriptor.stat,
                "target_col": descriptor.target_col,
                "training_window": {
                    "start": str(dated.min().date()) if dated.notna().any() else None,
                    "end": str(dated.max().date()) if dated.notna().any() else None,
                    "rows": int(len(frame)),
                },
                "metrics_snapshot": {
                    "primary_metric": selection["primary_metric"],
                    "tie_breakers": selection["tie_breakers"],
                    "trial_id": winner.trial_id,
                    "trial_params": winner.params or {},
                    "mean_mae": winner.mean_mae,
                    "mean_rmse": winner.mean_rmse,
                    "mean_r2": winner.mean_r2,
                },
                "selection_runtime_budget_minutes": selection["runtime_budget_minutes"],
                "segmentation": strategy_metadata(artifact),
                "feature_schema_hash": _feature_schema_hash(features),
                "leaderboard_path": str(leaderboard_path),
                "fold_metrics": fold_metrics.to_dict(orient="records"),
            }
            _persist_champion_metadata(champion_metadata_path, metadata_payload)
            try:
                _persist_final_holdout_report(
                    frame,
                    descriptor=descriptor,
                    winner_model_name=winner.model_name,
                    winner_strategy_name=winner.strategy_name,
                    winner_params=winner.params,
                    segmentation=segmentation,
                    features=features,
                    holdout_cfg=cast(dict[str, object], selection["final_holdout"]),
                )
            except Exception as exc:
                logger.warning(
                    "Failed to write final holdout report for '%s': %s",
                    descriptor.stat,
                    exc,
                )
            logger.info(
                "Selected %s champion strategy=%s model=%s with mean_mae=%.4f",
                descriptor.stat,
                winner.strategy_name,
                winner.model_name,
                winner.mean_mae,
            )
            return artifact, winner.model_name, winner.strategy_name
        except Exception as exc:
            logger.warning(
                "Model tournament failed for '%s' (%s); training baseline model.",
                descriptor.stat,
                exc,
            )

    try:
        metadata = json.loads(champion_metadata_path.read_text(encoding="utf-8"))
        champion_name = str(metadata.get("model_name", "xgboost"))
        champion_strategy = str(metadata.get("strategy_name", "global"))
        model = joblib.load(champion_model_path)
        return model, champion_name, champion_strategy
    except Exception as exc:
        logger.warning(
            "Champion artifact missing/incompatible for '%s' (%s); "
            "training baseline model.",
            descriptor.stat,
            exc,
        )
        spec = get_model_spec("xgboost")
        model = fit_estimator(
            frame,
            spec=spec,
            features=features,
            target_col=descriptor.target_col,
        )
        model_path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(model, model_path)
        return model, "xgboost", "global"


def _build_prediction_rows(
    lines: pd.DataFrame,
    games: pd.DataFrame,
    descriptor: StatDescriptor,
    target_date: pd.Timestamp,
) -> pd.DataFrame:
    """Build inference rows by resolving each line to a latest pitcher row."""

    if lines.empty:
        return pd.DataFrame()

    latest = (
        games.sort_values(["pitcher_id", "game_date"])
        .drop_duplicates(subset=["pitcher_id"], keep="last")
        .copy()
    )
    latest["name_key"] = latest.apply(
        lambda row: _person_lookup_key(
            row.get("pitcher_name"),
            fallback_id=row.get("pitcher_id"),
        ),
        axis=1,
    )

    index = latest.set_index("name_key", drop=False)
    lookup = {
        str(row.name_key): pd.Series(row._asdict())
        for row in latest.itertuples(index=False)
        if str(row.name_key)
    }

    park_lookup = park_factor_lookup(games, descriptor.park_factor_col)
    rows: list[pd.Series] = []
    for line in lines.itertuples(index=False):
        name_key = _person_lookup_key(getattr(line, "player"))
        try:
            row = index.loc[name_key]
        except KeyError:
            row = resolve_unique_name_match(name_key, lookup)
            if row is None:
                continue

        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]

        record = row.copy()
        record["player"] = getattr(line, "player")
        record[descriptor.line_col] = getattr(line, descriptor.line_col)
        record["over_decimal_price"] = getattr(line, "over_decimal_price", np.nan)
        record["under_decimal_price"] = getattr(line, "under_decimal_price", np.nan)
        if pd.notna(record.get("game_date")):
            rest_days = (
                target_date.normalize() - pd.Timestamp(record["game_date"]).normalize()
            ).days
            record["rest_days"] = max(rest_days, 0)
        if pd.notna(record.get("home_team")):
            record[descriptor.park_factor_col] = park_lookup.get(
                str(record["home_team"]),
                float(record.get(descriptor.park_factor_col, 1.0)),
            )
        rows.append(record)

    if not rows:
        return pd.DataFrame()

    prediction_rows = pd.DataFrame(rows)
    prediction_rows = ensure_live_feature_defaults(prediction_rows)
    return prediction_rows


def _infer_target_date(lines_path: str, fallback: pd.Timestamp) -> pd.Timestamp:
    """Infer a prediction target date from a dated lines path.

    Args:
        lines_path: Configured lines file path.
        fallback: Default date when path does not include an ISO date.

    Returns:
        Target date used for slate-relative feature engineering.
    """

    match = re.search(r"(20\d{2}-\d{2}-\d{2})", lines_path)
    if match:
        parsed = pd.to_datetime(match.group(1), errors="coerce")
        if pd.notna(parsed):
            return pd.Timestamp(parsed)
    return pd.Timestamp(fallback)


def _resolve_live_features_config(section: dict[str, object]) -> dict[str, object]:
    """Return non-breaking live-feature config with strikeouts defaults."""

    raw = section.get("live_features")
    if not isinstance(raw, dict):
        raw = {}

    weather_raw = raw.get("weather")
    if not isinstance(weather_raw, dict):
        weather_raw = {}

    return {
        "enabled": bool(raw.get("enabled", True)),
        "source_policy": str(raw.get("source_policy", "pybaseball_first")),
        "fallback_policy": str(raw.get("fallback_policy", "stale_cache")),
        "cache_path": str(
            raw.get("cache_path", "data/cache/mlb_live_features.parquet")
        ),
        "cache_ttl_hours": int(raw.get("cache_ttl_hours", 24)),
        "weather": {
            "enabled": bool(weather_raw.get("enabled", True)),
            "primary_source": str(
                weather_raw.get("primary_source", "pybaseball_team_game_logs")
            ),
            "secondary_source": str(
                weather_raw.get("secondary_source", "statsapi_game_feed")
            ),
        },
    }


def _enrich_live_context_for_strikeouts(
    prediction_rows: pd.DataFrame,
    *,
    section: dict[str, object],
    target_date: pd.Timestamp,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Apply live-context enrichment for strikeouts inference rows."""

    live_cfg = _resolve_live_features_config(section)
    service = LiveContextService(config=live_cfg)
    live_result = service.fetch(prediction_rows, target_date.to_pydatetime())
    enriched = merge_live_feature_frame(
        prediction_rows,
        live_result.frame,
        join_keys=("pitcher_id", "opponent_team"),
    )

    coverage = coverage_metrics(enriched)
    logger.info(
        (
            "Strikeouts live-feature coverage weather=%.1f%% roof=%.1f%% "
            "umpire=%.1f%% handedness=%.1f%% cache=%s"
        ),
        100.0 * coverage["weather_known_pct"],
        100.0 * coverage["roof_known_pct"],
        100.0 * coverage["umpire_known_pct"],
        100.0 * coverage["handedness_known_pct"],
        live_result.metadata.get("cache_status"),
    )

    metadata: dict[str, object] = {
        "live_feature_set_version": live_result.metadata.get(
            "live_feature_set_version"
        ),
        "live_feature_sources": ",".join(
            str(source)
            for source in (live_result.metadata.get("live_feature_sources") or [])
        ),
        "live_fetch_timestamp": live_result.metadata.get("live_fetch_timestamp"),
        "cache_age_hours": live_result.metadata.get("cache_age_hours"),
        "stale_cache_usage_pct": live_result.metadata.get("stale_cache_usage_pct"),
    }
    return enriched, metadata


def _empty_result(
    descriptor: StatDescriptor, *, run_mode: str, lines_status: str
) -> pd.DataFrame:
    """Create a typed empty output frame with stable simulation schema."""

    return pd.DataFrame(
        columns=[
            "player",
            descriptor.line_col,
            descriptor.prediction_col,
            *SIMULATION_COLUMNS,
            "model_residual_std",
            "run_mode",
            "lines_status",
        ]
    ).assign(run_mode=run_mode, lines_status=lines_status)


def run_mlb_pitcher_prop_pipeline(
    config: PipelineConfig,
    retrain: bool = False,
) -> pd.DataFrame:
    """Execute the shared MLB pitcher-prop pipeline for the configured stat.

    Args:
        config: Loaded pipeline config.
        retrain: Whether to force model retraining.

    Returns:
        Prediction/simulation output frame for the configured stat.
    """

    descriptor = get_stat_descriptor(config.stat)
    section = config.section

    training_games = _build_training_games(section, descriptor)
    label_quality_path = str(
        section.get(
            "label_quality_report_path",
            f"models/mlb_{descriptor.stat}_label_quality.csv",
        )
    )
    _persist_label_quality_report(
        training_games,
        descriptor=descriptor,
        report_path=label_quality_path,
    )
    model_frame = _clean_for_model(training_games, descriptor)
    if model_frame.empty:
        raise ValueError(f"No model-ready rows for stat '{descriptor.stat}'.")

    model, model_name, strategy_name = _train_or_load(
        model_frame,
        section=section,
        descriptor=descriptor,
        retrain=retrain,
    )
    logger.info(
        "Using %s model artifact for stat '%s' with strategy '%s'.",
        model_name,
        descriptor.stat,
        strategy_name,
    )

    try:
        train_preds = predict_with_strategy_artifact(
            model_frame,
            features=_model_features(descriptor),
            name="prediction",
            artifact=model,
        )
    except ValueError:
        logger.warning(
            "Loaded model artifact incompatible with current features for '%s'; "
            "retraining baseline artifact.",
            descriptor.stat,
        )
        model, model_name, strategy_name = _train_or_load(
            model_frame,
            section=section,
            descriptor=descriptor,
            retrain=True,
        )
        train_preds = predict_with_strategy_artifact(
            model_frame,
            features=_model_features(descriptor),
            name="prediction",
            artifact=model,
        )
    model_frame = model_frame.copy()
    model_frame["prediction"] = pd.to_numeric(train_preds, errors="coerce")

    residuals = pd.to_numeric(
        model_frame[descriptor.target_col], errors="coerce"
    ) - pd.to_numeric(train_preds, errors="coerce")
    sigma = (
        float(np.std(residuals.dropna().to_numpy(dtype=float), ddof=1))
        if residuals.notna().sum() > 1
        else np.nan
    )
    if not np.isfinite(sigma) or sigma <= 0:
        sigma = float(section.get("fallback_std", 1.0))

    try:
        lines_path = str(section["lines_path"])
        lines = load_pitcher_prop_lines(lines_path, descriptor.line_col)
        lines_status = "present"
    except FileNotFoundError:
        if not bool(section.get("allow_missing_lines", False)):
            raise
        logger.info(
            "Lines are missing for stat '%s'; running in train_backtest_only mode.",
            descriptor.stat,
        )
        return _empty_result(
            descriptor,
            run_mode="train_backtest_only",
            lines_status="missing",
        )

    target_date = _infer_target_date(
        lines_path,
        fallback=pd.Timestamp(training_games["game_date"].max()),
    )
    prediction_rows = _build_prediction_rows(
        lines,
        training_games,
        descriptor,
        target_date=target_date,
    )
    if prediction_rows.empty:
        result = lines.copy()
        result[descriptor.prediction_col] = np.nan
        for col in SIMULATION_COLUMNS:
            result[col] = np.nan
        result["model_residual_std"] = sigma
        result["run_mode"] = "prediction"
        result["lines_status"] = lines_status
        return result

    live_metadata: dict[str, object] = {}
    if descriptor.stat == "strikeouts":
        prediction_rows, live_metadata = _enrich_live_context_for_strikeouts(
            prediction_rows,
            section=section,
            target_date=target_date,
        )

    prediction_rows["prediction"] = predict_with_strategy_artifact(
        prediction_rows,
        features=_model_features(descriptor),
        name="prediction",
        artifact=model,
    )

    sim_cfg = MonteCarloConfig(
        simulations=int(section.get("monte_carlo_simulations", 10_000)),
        random_seed=section.get("monte_carlo_seed"),
    )

    sampler = None
    try:
        sampler = ResidualBootstrapper.from_games(
            model_frame.rename(columns={descriptor.target_col: "strikeouts"})
        )
    except Exception:
        sampler = None

    simulated = apply_simulations(
        prediction_rows,
        mean_col="prediction",
        std_dev=sigma,
        config=sim_cfg,
        sampler=sampler,
        line_col=descriptor.line_col,
        id_col="pitcher_id",
    )
    simulated["model_residual_std"] = sigma
    simulated["run_mode"] = "prediction"
    simulated["lines_status"] = lines_status
    for key, value in live_metadata.items():
        simulated[key] = value

    simulated.rename(
        columns={
            "prediction": descriptor.prediction_col,
            "opponent_team": "upcoming_opponent",
            "rest_days": "upcoming_rest_days",
            descriptor.park_factor_col: f"upcoming_{descriptor.park_factor_col}",
        },
        inplace=True,
    )
    return simulated
