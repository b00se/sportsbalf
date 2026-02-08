"""Shared MLB pitcher-prop orchestration for multi-stat execution."""

from __future__ import annotations

import logging
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from src.core.contracts import PipelineConfig
from src.mlb.data.load_props import load_pitcher_prop_lines
from src.mlb.features.feature_store import (
    LIVE_CONTEXT_FEATURE_COLUMNS,
    build_historical_live_features,
    ensure_live_feature_defaults,
)
from src.mlb.features.rolling import add_rolling_features
from src.mlb.models.distributions import ResidualBootstrapper
from src.mlb.models.monte_carlo import MonteCarloConfig, apply_simulations
from src.mlb.models.registry import get_model_spec
from src.mlb.models.trainers import fit_estimator, predict_estimator
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


def _model_features(descriptor: StatDescriptor) -> list[str]:
    """Return ordered feature list for the provided stat descriptor.

    Args:
        descriptor: Stat descriptor payload.

    Returns:
        Ordered model feature list.
    """

    return BASE_FEATURES + [descriptor.opponent_feature_col, descriptor.park_factor_col]


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

    enriched = games.sort_values(["opponent_team", "game_date"]).copy()
    grouped = enriched.groupby("opponent_team", sort=False)
    csum = grouped[target_col].cumsum().groupby(enriched["opponent_team"]).shift(1)
    ccnt = grouped.cumcount().astype(float)
    tendency = csum / ccnt.replace(0.0, np.nan)
    fallback = float(pd.to_numeric(enriched[target_col], errors="coerce").mean())
    if np.isnan(fallback):
        fallback = 0.0
    enriched[feature_col] = pd.to_numeric(tendency, errors="coerce").fillna(fallback)
    return enriched


def _build_training_games(
    section: dict[str, object],
    descriptor: StatDescriptor,
) -> pd.DataFrame:
    """Build concatenated training games across configured training paths."""

    pitch_data_path = str(section["pitch_data_path"])
    training_paths = [
        str(path) for path in (section.get("training_data_paths") or [pitch_data_path])
    ]

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
        )
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
            min_samples=int(section.get("park_factor_min_samples", 20)),
            half_life_games=int(section.get("park_factor_half_life_games", 60)),
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


def _train_or_load(
    frame: pd.DataFrame,
    *,
    section: dict[str, object],
    descriptor: StatDescriptor,
    retrain: bool,
):
    """Train or load a model artifact for the supplied stat."""

    model_path = Path(str(section["model_path"]))
    features = _model_features(descriptor)

    if model_path.exists() and not retrain:
        try:
            return joblib.load(model_path)
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
    return model


def _build_prediction_rows(
    lines: pd.DataFrame,
    games: pd.DataFrame,
    descriptor: StatDescriptor,
) -> pd.DataFrame:
    """Build inference rows by resolving each line to a latest pitcher row."""

    if lines.empty:
        return pd.DataFrame()

    latest = (
        games.sort_values(["pitcher_id", "game_date"])
        .drop_duplicates(subset=["pitcher_id"], keep="last")
        .copy()
    )
    latest["name_key"] = latest["pitcher_name"].map(normalize_person_name)

    index = latest.set_index("name_key", drop=False)
    lookup = {
        str(row.name_key): pd.Series(row._asdict())
        for row in latest.itertuples(index=False)
        if str(row.name_key)
    }

    park_lookup = park_factor_lookup(games, descriptor.park_factor_col)
    rows: list[pd.Series] = []
    for line in lines.itertuples(index=False):
        name_key = normalize_person_name(getattr(line, "player"))
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
                pd.Timestamp.now().normalize()
                - pd.Timestamp(record["game_date"]).normalize()
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


def _empty_result(
    descriptor: StatDescriptor, *, run_mode: str, lines_status: str
) -> pd.DataFrame:
    """Create a typed empty output frame with stable simulation schema."""

    return pd.DataFrame(
        columns=[
            "player",
            descriptor.line_col,
            descriptor.prediction_col,
            "prob_over",
            "prob_under",
            "prob_push",
            "ev_over",
            "ev_under",
            "edge_over",
            "edge_under",
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
    model_frame = _clean_for_model(training_games, descriptor)
    if model_frame.empty:
        raise ValueError(f"No model-ready rows for stat '{descriptor.stat}'.")

    model = _train_or_load(
        model_frame,
        section=section,
        descriptor=descriptor,
        retrain=retrain,
    )

    train_preds = predict_estimator(
        model_frame,
        model=model,
        features=_model_features(descriptor),
        name="prediction",
    )

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
        lines = load_pitcher_prop_lines(str(section["lines_path"]), descriptor.line_col)
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

    prediction_rows = _build_prediction_rows(lines, training_games, descriptor)
    if prediction_rows.empty:
        result = lines.copy()
        result[descriptor.prediction_col] = np.nan
        result["model_residual_std"] = sigma
        result["run_mode"] = "prediction"
        result["lines_status"] = lines_status
        return result

    prediction_rows["prediction"] = predict_estimator(
        prediction_rows,
        model=model,
        features=_model_features(descriptor),
        name="prediction",
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
    )
    simulated["model_residual_std"] = sigma
    simulated["run_mode"] = "prediction"
    simulated["lines_status"] = lines_status

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
