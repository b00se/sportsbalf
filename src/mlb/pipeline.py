"""Primary MLB prediction pipeline."""

from __future__ import annotations

import json
import logging
import re
import time
import warnings
from datetime import datetime
from functools import lru_cache
from hashlib import sha256
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:  # pragma: no cover - optional network dependency
    from pybaseball import schedule_and_record
except Exception:  # pragma: no cover - optional dependency missing
    schedule_and_record = None

from src.core.config import load_pipeline_config
from src.core.contracts import PipelineConfig
from src.mlb.data.load_props import load_strikeout_lines
from src.mlb.features import (
    add_opponent_k_rate,
    add_park_factor,
    add_rolling_features,
    aggregate_pitcher_games,
)
from src.mlb.features.park_factors import (
    compute_k_park_factors,
    derive_park_factors_from_games,
)
from src.mlb.models.distributions import ResidualBootstrapper
from src.mlb.models.evaluation import run_walk_forward_tournament, select_champion
from src.mlb.models.monte_carlo import MonteCarloConfig, apply_simulations
from src.mlb.models.predict import (
    DEFAULT_MODEL_PATH,
    FEATURES,
    load_model,
    predict_strikeouts,
    residual_std,
    save_model,
    train_model,
)
from src.mlb.models.registry import (
    SIMPLE_MODEL_PREFERENCE,
    get_model_spec,
    resolve_model_specs,
)
from src.utils.io import read_csv
from src.utils.names import normalize_person_name, resolve_unique_name_match

logger = logging.getLogger(__name__)


def _normalize_name(name: str) -> str:
    return normalize_person_name(name)


def _latest_games(games: pd.DataFrame) -> pd.DataFrame:
    normalized = games.copy()
    if "pitcher_id" not in normalized.columns and "pitcher" in normalized.columns:
        normalized["pitcher_id"] = normalized["pitcher"]
    if "pitcher_name" not in normalized.columns:
        normalized["pitcher_name"] = normalized["pitcher_id"].astype(str)

    latest = (
        normalized.sort_values(["pitcher_id", "game_date"])
        .drop_duplicates("pitcher_id", keep="last")
        .copy()
    )
    latest["name_key"] = latest["pitcher_name"].map(_normalize_name)
    latest["most_recent_game"] = latest["game_date"]
    return latest


def _infer_target_date(lines_path: str, default: datetime | None = None) -> datetime:
    match = re.search(r"(20\d{2}-\d{2}-\d{2})", lines_path)
    if match:
        try:
            return datetime.fromisoformat(match.group(1))
        except ValueError:
            pass
    return default or datetime.utcnow()


@lru_cache(maxsize=32)
def _team_schedule(team_abbr: str, year: int) -> pd.DataFrame | None:
    if schedule_and_record is None:
        return None
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="A value is trying to be set on a copy of a DataFrame",
                category=FutureWarning,
            )
            df = schedule_and_record(year, team_abbr)
    except Exception:
        return None
    if df is None or df.empty:
        return None
    df = df.copy()
    if "Date" not in df.columns:
        return None
    # Schedule uses strings like "Sun, Apr 07"; prepend the year and coerce.
    df["game_date"] = pd.to_datetime(
        df["Date"].astype(str) + f" {year}", errors="coerce"
    )
    df.dropna(subset=["game_date"], inplace=True)
    return df


def _next_opponent(team_abbr: str, target_date: datetime) -> dict | None:
    schedule = _team_schedule(team_abbr, target_date.year)
    if schedule is None:
        return None
    future = schedule[schedule["game_date"] >= target_date].sort_values("game_date")
    if future.empty:
        return None
    row = future.iloc[0]
    opponent = row.get("Opp") or row.get("Opponent")
    if not isinstance(opponent, str) or not opponent:
        return None

    # Determine home/away. Many schedules place '@' in a dedicated column.
    is_away = False
    for col in ("Home", "Home/Away", "Venue", "Unnamed: 5", "Unnamed: 6"):
        value = row.get(col)
        if isinstance(value, str) and value.strip() == "@":
            is_away = True
        if isinstance(value, str) and value.lower() in {"away", "road"}:
            is_away = True
        if isinstance(value, str) and value.lower() in {"home"}:
            is_away = False

    return {
        "game_date": row["game_date"],
        "opponent": opponent.strip(),
        "home_team": opponent.strip() if is_away else team_abbr,
        "is_home": not is_away,
    }


def _opponent_metrics(frame: pd.DataFrame, target_date: datetime) -> tuple[dict, dict]:
    filtered = frame[frame["game_date"] <= target_date].copy()
    metrics = (
        filtered.sort_values("game_date")
        .groupby("opponent_team")[["opponent_k_pct", "opponent_k_rate"]]
        .last()
    )
    team_lookup = metrics.to_dict("index")
    fallback = {
        "opponent_k_pct": (
            float(filtered["opponent_k_pct"].mean()) if not filtered.empty else 0.0
        ),
        "opponent_k_rate": (
            float(filtered["opponent_k_rate"].mean()) if not filtered.empty else 0.0
        ),
    }
    return team_lookup, fallback


def _park_lookup(park_df: pd.DataFrame) -> dict:
    df = park_df.copy()
    if "Team_abbr" not in df.columns:
        return {}
    return (
        df.dropna(subset=["Team_abbr", "K_park_factor"])
        .drop_duplicates("Team_abbr", keep="last")
        .set_index("Team_abbr")["K_park_factor"]
        .to_dict()
    )


def _build_park_factors_from_pitch_data(pitch_df: pd.DataFrame) -> pd.DataFrame:
    """Construct park factors from either aggregated or pitch-level data."""

    if {"home_team", "park_factor_K"}.issubset(pitch_df.columns):
        park_df = derive_park_factors_from_games(pitch_df)
        if not park_df.empty:
            return park_df

    pitch_level_cols = {"pitch_type", "events", "home_team", "batter"}
    if pitch_level_cols.issubset(pitch_df.columns):
        frame = pitch_df.copy()
        if "game_date" in frame.columns:
            frame["game_date"] = pd.to_datetime(frame["game_date"], errors="coerce")
            start = frame["game_date"].min()
            end = frame["game_date"].max()
            if pd.notna(start) and pd.notna(end):
                return compute_k_park_factors(
                    start.strftime("%Y-%m-%d"),
                    end.strftime("%Y-%m-%d"),
                    source_df=frame,
                )

    return pd.DataFrame(columns=["Team_abbr", "K_park_factor"])


def _load_or_create_park_factors(
    pitch_df: pd.DataFrame, park_path: str, retrain: bool
) -> pd.DataFrame:
    """Load park factors from disk, regenerating if missing or retraining."""

    park_file = Path(park_path)

    if park_file.exists() and not retrain:
        return read_csv(str(park_file))

    park_df = _build_park_factors_from_pitch_data(pitch_df)

    if park_df.empty:
        teams_series = (
            pitch_df["home_team"].dropna()
            if "home_team" in pitch_df.columns
            else pd.Series(dtype="object")
        )
        unique_teams = sorted(teams_series.astype(str).unique().tolist())

        if not unique_teams:
            raise FileNotFoundError(
                "Unable to derive park factors from pitch data "
                "and no cached file present."
            )

        logger.warning(
            "Using neutral park factors because no park factor data could be derived."
        )
        park_df = pd.DataFrame(
            {
                "Team_abbr": unique_teams,
                "K_park_factor": 1.0,
            }
        )

    park_file.parent.mkdir(parents=True, exist_ok=True)
    park_df.to_csv(park_file, index=False)
    return park_df


def _build_prediction_rows(
    lines: pd.DataFrame,
    latest_games: pd.DataFrame,
    target_date: datetime,
    park_lookup: dict,
    opponent_lookup: dict,
    fallback_opponent: dict,
) -> pd.DataFrame:
    latest_indexed = latest_games.set_index("name_key", drop=False)
    latest_by_name: dict[str, pd.Series] = {}
    duplicate_names: set[str] = set()
    for row in latest_games.itertuples(index=False):
        key = str(getattr(row, "name_key", ""))
        if not key:
            continue
        if key in latest_by_name:
            duplicate_names.add(key)
            continue
        latest_by_name[key] = pd.Series(row._asdict())
    for key in duplicate_names:
        latest_by_name.pop(key, None)

    rows = []
    missing_players = []

    for line in lines.itertuples():
        try:
            latest = latest_indexed.loc[line.name_key]
        except KeyError:
            latest = resolve_unique_name_match(line.name_key, latest_by_name)
            if latest is None:
                missing_players.append(line.player)
                continue

        if isinstance(latest, pd.DataFrame):
            latest = latest.iloc[0]

        latest = latest.copy()
        latest["player"] = line.player
        latest["player_key"] = line.name_key
        latest["target_date"] = target_date

        rest_days = (target_date.date() - latest["most_recent_game"].date()).days
        latest["rest_days"] = max(rest_days, 0)

        next_game = _next_opponent(latest["pitcher_team"], target_date)
        if next_game:
            latest["upcoming_game_date"] = next_game["game_date"]
            latest["opponent_team"] = next_game["opponent"]
            home_team = next_game["home_team"]
        else:
            latest["upcoming_game_date"] = pd.Timestamp(target_date)
            home_team = latest.get("pitcher_team")

        metrics = opponent_lookup.get(latest.get("opponent_team")) or fallback_opponent
        latest["opponent_k_pct"] = metrics.get("opponent_k_pct")
        latest["opponent_k_rate"] = metrics.get("opponent_k_rate")

        if isinstance(home_team, str):
            latest["park_factor_K"] = park_lookup.get(
                home_team, latest.get("park_factor_K")
            )

        rows.append(latest)

    if missing_players:
        missing_list = ", ".join(sorted(set(missing_players)))
        logger.warning("Missing historical data for players: %s", missing_list)

    return pd.DataFrame(rows)


def _clean_for_model(games: pd.DataFrame) -> pd.DataFrame:
    filtered = games.replace([np.inf, -np.inf], np.nan)
    required = FEATURES + ["strikeouts"]
    filtered = filtered.dropna(subset=required)
    return filtered


def _normalize_opponent_feature_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Ensure both opponent K feature columns are present for model parity."""

    normalized = frame.copy()
    if "opponent_k_pct" not in normalized.columns:
        if "opponent_k_rate" in normalized.columns:
            normalized["opponent_k_pct"] = normalized["opponent_k_rate"]
        else:
            normalized["opponent_k_pct"] = np.nan
    if "opponent_k_rate" not in normalized.columns:
        normalized["opponent_k_rate"] = normalized["opponent_k_pct"]
    return normalized


def _log_strikeout_scale(frame: pd.DataFrame, *, label: str) -> None:
    """Emit strikeout scale diagnostics for quick target-integrity checks."""

    if "strikeouts" not in frame.columns or frame.empty:
        return

    strikeouts = pd.to_numeric(frame["strikeouts"], errors="coerce")
    pitch_count = pd.to_numeric(frame.get("pitch_count"), errors="coerce")
    summary = {
        "rows": int(len(frame)),
        "mean": float(strikeouts.mean(skipna=True)),
        "p95": float(strikeouts.quantile(0.95)),
        "max": float(strikeouts.max(skipna=True)),
    }
    logger.info("Strikeout scale for %s: %s", label, summary)

    invalid = strikeouts < 0
    if invalid.any():
        logger.warning(
            "Detected %d rows with negative strikeouts in %s.",
            int(invalid.sum()),
            label,
        )

    if pitch_count.notna().any():
        impossible = (strikeouts > pitch_count).fillna(False)
        if impossible.any():
            logger.warning(
                "Detected %d rows where strikeouts exceed pitch_count in %s.",
                int(impossible.sum()),
                label,
            )


def _feature_schema_hash(features: list[str]) -> str:
    """Return deterministic hash for the active model feature schema."""

    payload = ",".join(features).encode("utf-8")
    return sha256(payload).hexdigest()


def _model_selection_config(section: dict[str, Any]) -> dict[str, Any]:
    """Return normalized model-selection configuration with defaults."""

    raw = section.get("model_selection")
    if not isinstance(raw, dict):
        raw = {}

    return {
        "enabled": bool(raw.get("enabled", False)),
        "candidates": raw.get("candidates"),
        "primary_metric": raw.get("primary_metric", "mae"),
        "tie_breakers": raw.get("tie_breakers", ["rmse", "r2"]),
        "tie_epsilon": float(raw.get("tie_epsilon", 1e-6)),
        "champion_model_path": raw.get(
            "champion_model_path", "models/mlb_strikeouts_champion.joblib"
        ),
        "champion_metadata_path": raw.get(
            "champion_metadata_path", "models/mlb_strikeouts_champion_metadata.json"
        ),
        "leaderboard_path": raw.get(
            "leaderboard_path", "models/mlb_strikeouts_leaderboard.csv"
        ),
    }


def _persist_champion_metadata(metadata_path: Path, payload: dict[str, Any]) -> None:
    """Persist champion metadata JSON to disk."""

    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _train_or_load_serving_model(
    model_frame: pd.DataFrame,
    *,
    section: dict[str, Any],
    retrain: bool,
) -> tuple[Any, str]:
    """Resolve serving model, optionally running model selection tournament."""

    selection = _model_selection_config(section)
    model_path = Path(section.get("model_path") or str(DEFAULT_MODEL_PATH))

    if not selection["enabled"]:
        if retrain or not model_path.exists():
            params = section.get("xgb_params")
            logger.info(
                "Training baseline XGBoost model on %d rows (path=%s).",
                len(model_frame),
                model_path,
            )
            model = train_model(model_frame, params=params, model_name="xgboost")
            save_model(model, model_path)
            return model, "xgboost"
        logger.info("Loading existing baseline model from %s", model_path)
        return load_model(model_path), "xgboost"

    champion_model_path = Path(selection["champion_model_path"])
    champion_metadata_path = Path(selection["champion_metadata_path"])
    leaderboard_path = Path(selection["leaderboard_path"])
    candidates = selection["candidates"]

    should_retrain_champion = (
        retrain
        or not champion_model_path.exists()
        or not champion_metadata_path.exists()
    )

    if should_retrain_champion:
        try:
            specs = resolve_model_specs(candidates)
            fold_metrics, leaderboard = run_walk_forward_tournament(
                model_frame,
                specs=specs,
                features=FEATURES,
                target_col="strikeouts",
                date_col="game_date",
            )
            winner = select_champion(
                leaderboard,
                primary_metric=str(selection["primary_metric"]),
                tie_breakers=list(selection["tie_breakers"]),
                epsilon=selection["tie_epsilon"],
                simplicity_order=SIMPLE_MODEL_PREFERENCE,
            )
            winner_spec = get_model_spec(winner.model_name)
            model = train_model(model_frame, model_name=winner_spec.name)
            save_model(model, champion_model_path)

            leaderboard_path.parent.mkdir(parents=True, exist_ok=True)
            leaderboard.to_csv(leaderboard_path, index=False)

            dated = pd.to_datetime(model_frame["game_date"], errors="coerce")
            metadata_payload = {
                "model_name": winner.model_name,
                "training_window": {
                    "start": str(dated.min().date()) if dated.notna().any() else None,
                    "end": str(dated.max().date()) if dated.notna().any() else None,
                    "rows": int(len(model_frame)),
                },
                "metrics_snapshot": {
                    "primary_metric": selection["primary_metric"],
                    "tie_breakers": selection["tie_breakers"],
                    "mean_mae": winner.mean_mae,
                    "mean_rmse": winner.mean_rmse,
                    "mean_r2": winner.mean_r2,
                },
                "feature_schema_hash": _feature_schema_hash(FEATURES),
                "leaderboard_path": str(leaderboard_path),
                "fold_metrics": fold_metrics.to_dict(orient="records"),
            }
            _persist_champion_metadata(champion_metadata_path, metadata_payload)
            logger.info(
                "Selected champion model=%s with mean_mae=%.4f",
                winner.model_name,
                winner.mean_mae,
            )
            return model, winner.model_name
        except Exception as exc:
            logger.warning(
                "Model tournament failed (%s); training baseline model instead.",
                exc,
            )
            params = section.get("xgb_params")
            model = train_model(model_frame, params=params, model_name="xgboost")
            save_model(model, model_path)
            return model, "xgboost"

    try:
        metadata = json.loads(champion_metadata_path.read_text(encoding="utf-8"))
        champion_name = str(metadata.get("model_name", "xgboost"))
        model = load_model(champion_model_path)
        return model, champion_name
    except Exception as exc:
        logger.warning(
            "Champion artifact missing/incompatible (%s); training baseline model.",
            exc,
        )
        params = section.get("xgb_params")
        model = train_model(model_frame, params=params, model_name="xgboost")
        save_model(model, model_path)
        return model, "xgboost"


def run_strikeouts_pipeline(
    config: PipelineConfig, retrain: bool = False
) -> pd.DataFrame:
    """Execute the MLB strikeout workflow and return lines with probabilities."""

    section = config.section
    lines = load_strikeout_lines(section["lines_path"])
    lines = lines.copy()
    lines["name_key"] = lines["player"].map(_normalize_name)
    pitch_df = read_csv(section["pitch_data_path"])
    park_df = _load_or_create_park_factors(
        pitch_df, section["park_factors_path"], retrain
    )

    games = aggregate_pitcher_games(pitch_df)
    if "pitcher_id" not in games.columns and "pitcher" in games.columns:
        games["pitcher_id"] = games["pitcher"]
    if "pitcher_name" not in games.columns:
        games["pitcher_name"] = games["pitcher_id"].astype(str)
    games = add_rolling_features(games)
    games = add_park_factor(games, park_df)
    games = add_opponent_k_rate(games)
    games = _normalize_opponent_feature_columns(games)

    training_games_list = []
    training_paths = section.get("training_data_paths") or [section["pitch_data_path"]]
    for path in training_paths:
        if Path(path).resolve() == Path(section["pitch_data_path"]).resolve():
            training_games_list.append(games)
            continue

        hist_df = read_csv(path)
        hist_games = aggregate_pitcher_games(hist_df)
        hist_games = add_rolling_features(hist_games)
        hist_games = add_park_factor(hist_games, park_df)
        hist_games = add_opponent_k_rate(hist_games)
        hist_games = _normalize_opponent_feature_columns(hist_games)
        training_games_list.append(hist_games)

    training_games = pd.concat(training_games_list, ignore_index=True)
    training_games = _normalize_opponent_feature_columns(training_games)
    if (
        "pitcher_id" not in training_games.columns
        and "pitcher" in training_games.columns
    ):
        training_games["pitcher_id"] = training_games["pitcher"]

    training_games.sort_values(["pitcher_id", "game_date"], inplace=True)
    training_games = training_games.drop_duplicates(
        subset=["pitcher_id", "game_date"], keep="last"
    )
    _log_strikeout_scale(training_games, label="training_games")

    target_date = _infer_target_date(
        section["lines_path"], default=games["game_date"].max()
    )

    model_frame = _clean_for_model(training_games)
    start = time.time()
    model, model_name = _train_or_load_serving_model(
        model_frame,
        section=section,
        retrain=retrain,
    )
    logger.info("Resolved serving model=%s in %.2fs", model_name, time.time() - start)

    try:
        train_preds = predict_strikeouts(model_frame, model)
    except ValueError:
        logger.warning(
            "Loaded model incompatible with current features; "
            "retraining XGBoost baseline."
        )
        params = section.get("xgb_params")
        fallback_model_path = Path(section.get("model_path") or str(DEFAULT_MODEL_PATH))
        model = train_model(model_frame, params=params, model_name="xgboost")
        save_model(model, fallback_model_path)
        train_preds = predict_strikeouts(model_frame, model)
    sigma = residual_std(model_frame["strikeouts"], train_preds)
    model_frame = model_frame.copy()
    model_frame["prediction"] = train_preds

    park_lookup = _park_lookup(park_df)
    opponent_lookup, opponent_fallback = _opponent_metrics(model_frame, target_date)
    latest_games = _latest_games(games)

    prediction_rows = _build_prediction_rows(
        lines,
        latest_games,
        target_date,
        park_lookup,
        opponent_lookup,
        opponent_fallback,
    )

    if prediction_rows.empty:
        lines_enriched = lines.copy()
        lines_enriched["prediction"] = np.nan
        lines_enriched["pitcher_id"] = np.nan
        lines_enriched["pitcher_team"] = np.nan
        lines_enriched["most_recent_game"] = np.nan
        lines_enriched["upcoming_game_date"] = pd.Timestamp(target_date)
        lines_enriched["opponent_team"] = np.nan
        lines_enriched["park_factor_K"] = np.nan
        lines_enriched["rest_days"] = np.nan
    else:
        prediction_rows = prediction_rows.reset_index(drop=True)
        prediction_rows = _normalize_opponent_feature_columns(prediction_rows)
        prediction_rows["prediction"] = np.nan
        valid_mask = prediction_rows[FEATURES].notna().all(axis=1)
        if valid_mask.any():
            preds = predict_strikeouts(prediction_rows.loc[valid_mask], model)
            prediction_rows.loc[valid_mask, "prediction"] = preds.values

        lines_enriched = lines.merge(
            prediction_rows[
                [
                    "player_key",
                    "pitcher_id",
                    "pitcher_team",
                    "prediction",
                    "most_recent_game",
                    "upcoming_game_date",
                    "opponent_team",
                    "park_factor_K",
                    "rest_days",
                ]
            ],
            left_on="name_key",
            right_on="player_key",
            how="left",
        )
        lines_enriched.drop(columns=["player_key"], inplace=True, errors="ignore")

    if not sigma or pd.isna(sigma) or sigma <= 0:
        sigma = float(section.get("fallback_std", 1.0))
    bootstrapper = None
    try:
        bootstrapper = ResidualBootstrapper.from_games(model_frame)
    except ValueError:
        bootstrapper = None

    sim_count = section.get("monte_carlo_simulations", 10_000) or 10_000
    mc_config = MonteCarloConfig(
        simulations=int(sim_count),
        random_seed=section.get("monte_carlo_seed"),
    )

    enriched = apply_simulations(
        lines_enriched,
        mean_col="prediction",
        std_dev=sigma,
        config=mc_config,
        sampler=bootstrapper,
    )
    enriched["model_residual_std"] = sigma
    enriched.rename(
        columns={
            "prediction": "predicted_strikeouts",
            "opponent_team": "upcoming_opponent",
            "rest_days": "upcoming_rest_days",
            "park_factor_K": "upcoming_park_factor_K",
        },
        inplace=True,
    )
    enriched.drop(columns=["name_key"], inplace=True, errors="ignore")

    return enriched


def run(config_path: str | None = None, retrain: bool = False) -> pd.DataFrame:
    """Compatibility shim for callers still importing ``src.mlb.pipeline.run``."""

    config = load_pipeline_config(
        config_path or "config/mlb.yaml",
        sport_override="mlb",
        stat_override="strikeouts",
    )
    return run_strikeouts_pipeline(config=config, retrain=retrain)
