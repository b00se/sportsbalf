"""Phase 1 MLB season-horizon projection adapter."""

from __future__ import annotations

import hashlib
import inspect
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from src.fantasy.adapters.mlb.datasets import (
    build_hits_pa_training_view,
    build_player_season_snapshots,
)
from src.fantasy.adapters.mlb.features import (
    is_derived_rate_metric,
    model_feature_columns_for_metric,
    prepare_mlb_projection_frame,
    rate_metric_inputs,
)
from src.fantasy.adapters.mlb.uncertainty import (
    availability_confidence_by_entity,
    summarize_empirical_uncertainty,
    summarize_hit_rate_uncertainty_from_counts,
)
from src.fantasy.core.contracts import ContestConfig
from src.mlb.models.registry import get_model_spec
from src.mlb.models.trainers import fit_estimator, predict_estimator

NEUTRAL_OUTPUT_COLUMNS: tuple[str, ...] = (
    "entity_id",
    "sport",
    "metric_id",
    "horizon",
    "window_start",
    "window_end",
    "game_id",
    "mean",
    "p10",
    "p50",
    "p90",
    "stddev",
    "availability_confidence",
    "source_model_version",
    "source_snapshot_id",
)

SNAPSHOT_COUNT_TARGETS: frozenset[str] = frozenset({"hits", "plate_appearances"})
PA_SNAPSHOT_FEATURES: tuple[str, ...] = (
    "roll_7_plate_appearances",
    "roll_14_plate_appearances",
    "roll_30_plate_appearances",
    "games_played_last_14",
    "games_played_last_30",
    "pa_per_game_last_14",
    "pa_per_game_last_30",
    "days_since_last_game",
    "team_games_seen_last_30",
    "player_game_share_last_30",
    "recent_consecutive_games_played",
    "roll_30_pa_vs_lhp_share",
    "roll_30_pa_vs_rhp_share",
    "season_to_date_pa",
    "snapshot_games_played",
)
HITS_SNAPSHOT_FEATURES: tuple[str, ...] = PA_SNAPSHOT_FEATURES + (
    "roll_7_hits",
    "roll_14_hits",
    "roll_30_hits",
    "roll_7_hit_rate",
    "roll_14_hit_rate",
    "roll_30_hit_rate",
    "roll_7_hard_hit_rate",
    "roll_14_hard_hit_rate",
    "roll_30_hard_hit_rate",
    "roll_7_total_bases",
    "roll_14_total_bases",
    "roll_30_total_bases",
    "roll_7_slugging_proxy",
    "roll_14_slugging_proxy",
    "roll_30_slugging_proxy",
    "smoothed_hit_rate_rolling_30",
    "smoothed_hit_rate_season_to_date",
    "season_to_date_hits",
)


@dataclass(frozen=True, slots=True)
class MlbProjectionAdapterConfig:
    """Runtime settings for the Phase 1.5 MLB projection adapter."""

    input_dataset_path: str
    training_data_paths: tuple[str, ...] = ()
    entity_id_col: str = "batter"
    date_col: str = "game_date"
    seed: int = 2026
    min_history_games: int = 20
    model_name: str = "xgboost"
    train_end_date: str | None = None
    inference_anchor_date: str | None = None
    uncertainty_method: str = "empirical_quantiles"
    snapshot_anchor_frequency: str = "weekly"
    snapshot_min_games: int = 5
    model_selection_enabled: bool = False
    model_selection_candidates: tuple[str, ...] = (
        "poisson",
        "elastic_net",
        "hist_gradient_boosting",
        "xgboost",
    )
    model_selection_primary_metric: str = "mae"
    model_selection_max_trials_per_model: int = 1
    selection_min_delta_mae: float = 0.0
    pybaseball_priors_enabled: bool = False
    pybaseball_priors_cache_path: str = ""
    pybaseball_priors_seasons: tuple[int, ...] = ()
    pybaseball_priors_refresh: bool = False
    uncertainty_residual_bucket_col: str = "season_to_date_pa"
    uncertainty_bucket_edges: tuple[float, ...] = (0.0, 100.0, 250.0, 450.0, 700.0)
    hit_rate_residual_scale_global: float = 1.0
    hit_rate_residual_scale_by_bucket: dict[str, float] | None = None
    coverage_target: float = 0.80
    calibration_objective: str = "coverage_width_tradeoff"
    min_bucket_residual_count: int = 100
    regular_season_only: bool = True
    require_batter_pa_dedup: bool = True
    count_nonnegative_constraints: bool = True
    hits_leq_pa_constraint: bool = True
    hit_rate_derivation_source: str = "counts_only"
    evaluation_primary_metric_focus: str = "hit_rate"
    hit_rate_uncertainty_draws: int = 500
    source_snapshot_id: str | None = None

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> MlbProjectionAdapterConfig:
        """Construct config from a plain mapping payload."""

        phase15 = payload.get("mlb_projection_phase15")
        if isinstance(phase15, dict):
            effective = dict(payload)
            effective.update(phase15)
        else:
            effective = dict(payload)
        cleaning = effective.get("data_cleaning", {})
        modeling = effective.get("modeling", {})
        evaluation = effective.get("evaluation", {})
        uncertainty = effective.get("uncertainty", {})
        if not isinstance(cleaning, dict):
            cleaning = {}
        if not isinstance(modeling, dict):
            modeling = {}
        if not isinstance(evaluation, dict):
            evaluation = {}
        if not isinstance(uncertainty, dict):
            uncertainty = {}

        candidates_raw = effective.get("model_selection_candidates")
        if isinstance(candidates_raw, list):
            candidates = tuple(str(value).strip().lower() for value in candidates_raw)
        else:
            candidates = (
                "poisson",
                "elastic_net",
                "hist_gradient_boosting",
                "xgboost",
            )
        prior_seasons_raw = effective.get("pybaseball_priors_seasons")
        if isinstance(prior_seasons_raw, list):
            prior_seasons = tuple(int(value) for value in prior_seasons_raw)
        else:
            prior_seasons = ()
        edges_raw = effective.get("uncertainty_bucket_edges")
        if isinstance(edges_raw, list):
            bucket_edges = tuple(float(value) for value in edges_raw)
        else:
            bucket_edges = (0.0, 100.0, 250.0, 450.0, 700.0)
        scale_by_bucket_raw = uncertainty.get(
            "hit_rate_residual_scale_by_bucket",
            effective.get("hit_rate_residual_scale_by_bucket", {}),
        )
        if isinstance(scale_by_bucket_raw, dict):
            scale_by_bucket = {
                str(bucket): float(scale)
                for bucket, scale in scale_by_bucket_raw.items()
            }
        else:
            scale_by_bucket = {}
        training_paths_raw = effective.get("training_data_paths")
        if isinstance(training_paths_raw, list):
            training_data_paths = tuple(str(path) for path in training_paths_raw)
        else:
            training_data_paths = ()

        return cls(
            input_dataset_path=str(effective["input_dataset_path"]),
            training_data_paths=training_data_paths,
            entity_id_col=str(effective.get("entity_id_col", "batter")),
            date_col=str(effective.get("date_col", "game_date")),
            seed=int(effective.get("seed", 2026)),
            min_history_games=int(effective.get("min_history_games", 20)),
            model_name=str(effective.get("model_name", "xgboost")),
            train_end_date=(
                str(effective["train_end_date"])
                if effective.get("train_end_date") is not None
                else None
            ),
            inference_anchor_date=(
                str(effective["inference_anchor_date"])
                if effective.get("inference_anchor_date") is not None
                else None
            ),
            uncertainty_method=str(
                effective.get("uncertainty_method", "empirical_quantiles")
            ),
            snapshot_anchor_frequency=str(
                effective.get("snapshot_anchor_frequency", "weekly")
            ),
            snapshot_min_games=int(effective.get("snapshot_min_games", 5)),
            model_selection_enabled=bool(
                effective.get("model_selection_enabled", False)
            ),
            model_selection_candidates=candidates,
            model_selection_primary_metric=str(
                effective.get("model_selection_primary_metric", "mae")
            ),
            model_selection_max_trials_per_model=int(
                effective.get("model_selection_max_trials_per_model", 1)
            ),
            selection_min_delta_mae=float(
                modeling.get(
                    "selection_min_delta_mae",
                    effective.get("selection_min_delta_mae", 0.0),
                )
            ),
            pybaseball_priors_enabled=bool(
                effective.get("pybaseball_priors_enabled", False)
            ),
            pybaseball_priors_cache_path=str(
                effective.get("pybaseball_priors_cache_path", "")
            ),
            pybaseball_priors_seasons=prior_seasons,
            pybaseball_priors_refresh=bool(
                effective.get("pybaseball_priors_refresh", False)
            ),
            uncertainty_residual_bucket_col=str(
                effective.get("uncertainty_residual_bucket_col", "season_to_date_pa")
            ),
            uncertainty_bucket_edges=bucket_edges,
            hit_rate_residual_scale_global=float(
                uncertainty.get(
                    "hit_rate_residual_scale_global",
                    effective.get("hit_rate_residual_scale_global", 1.0),
                )
            ),
            hit_rate_residual_scale_by_bucket=scale_by_bucket,
            coverage_target=float(
                uncertainty.get(
                    "coverage_target",
                    effective.get("coverage_target", 0.80),
                )
            ),
            calibration_objective=str(
                uncertainty.get(
                    "calibration_objective",
                    effective.get("calibration_objective", "coverage_width_tradeoff"),
                )
            ),
            min_bucket_residual_count=int(
                uncertainty.get(
                    "min_bucket_residual_count",
                    effective.get("min_bucket_residual_count", 100),
                )
            ),
            regular_season_only=bool(
                cleaning.get(
                    "regular_season_only",
                    effective.get("regular_season_only", True),
                )
            ),
            require_batter_pa_dedup=bool(
                cleaning.get(
                    "require_batter_pa_dedup",
                    effective.get("require_batter_pa_dedup", True),
                )
            ),
            count_nonnegative_constraints=bool(
                modeling.get(
                    "count_nonnegative_constraints",
                    effective.get("count_nonnegative_constraints", True),
                )
            ),
            hits_leq_pa_constraint=bool(
                modeling.get(
                    "hits_leq_pa_constraint",
                    effective.get("hits_leq_pa_constraint", True),
                )
            ),
            hit_rate_derivation_source=str(
                modeling.get(
                    "hit_rate_derivation_source",
                    effective.get("hit_rate_derivation_source", "counts_only"),
                )
            ),
            evaluation_primary_metric_focus=str(
                evaluation.get(
                    "primary_metric_focus",
                    effective.get("evaluation_primary_metric_focus", "hit_rate"),
                )
            ),
            hit_rate_uncertainty_draws=int(
                modeling.get(
                    "hit_rate_uncertainty_draws",
                    effective.get("hit_rate_uncertainty_draws", 500),
                )
            ),
            source_snapshot_id=(
                str(effective["source_snapshot_id"])
                if effective.get("source_snapshot_id") is not None
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class CountProjectionDetails:
    """Count projection payload with uncertainty calibration metadata."""

    mean_by_entity: pd.Series
    residuals: pd.Series
    model_name: str
    bucket_by_entity: pd.Series
    residuals_by_bucket: dict[str, pd.Series]


class MlbSeasonProjectionAdapter:
    """Generate neutral MLB season projection rows for a single base metric."""

    def __init__(self, *, metric_id: str, adapter_config: MlbProjectionAdapterConfig):
        self.metric_id = metric_id.strip().lower()
        self.adapter_config = adapter_config

    def project(self, config: ContestConfig) -> pd.DataFrame:
        """Project one metric across MLB entities for the configured season window."""

        source = prepare_mlb_projection_frame(
            self.adapter_config.input_dataset_path,
            entity_id_col=self.adapter_config.entity_id_col,
            date_col=self.adapter_config.date_col,
        )
        if source.empty:
            return pd.DataFrame(columns=NEUTRAL_OUTPUT_COLUMNS)

        source = source.copy()
        source = source.sort_values(
            [self.adapter_config.entity_id_col, self.adapter_config.date_col],
            kind="stable",
        )

        window_start, window_end = self._resolve_window(config)
        context = self._build_projection_context(
            source, window_start=window_start, window_end=window_end
        )
        confidence_frame = context["confidence_frame"]
        sample_sizes = context["sample_sizes"]

        if is_derived_rate_metric(self.metric_id):
            numerator, denominator = rate_metric_inputs(self.metric_id)
            numerator_mean = self._predict_count_mean_by_entity(
                metric_id=numerator,
                config=config,
                source=source,
            )
            denominator_mean = self._predict_count_mean_by_entity(
                metric_id=denominator,
                config=config,
                source=source,
            )
            numerator_details = self._predict_count_projection_result(
                metric_id=numerator,
                config=config,
                source=source,
            )
            denominator_details = self._predict_count_projection_result(
                metric_id=denominator,
                config=config,
                source=source,
            )
            if self.adapter_config.count_nonnegative_constraints:
                numerator_mean = numerator_mean.clip(lower=0.0)
                denominator_mean = denominator_mean.clip(lower=0.0)
            if (
                self.adapter_config.hits_leq_pa_constraint
                and numerator == "hits"
                and denominator == "plate_appearances"
            ):
                numerator_mean = pd.concat(
                    [numerator_mean, denominator_mean], axis=1
                ).min(axis=1)
            entity_index = numerator_mean.index.union(denominator_mean.index)
            numerator_mean = numerator_mean.reindex(entity_index).fillna(0.0)
            denominator_mean = denominator_mean.reindex(entity_index).fillna(0.0)
            with np.errstate(divide="ignore", invalid="ignore"):
                values = np.where(
                    denominator_mean.to_numpy(dtype="float64") > 0.0,
                    numerator_mean.to_numpy(dtype="float64")
                    / denominator_mean.to_numpy(dtype="float64"),
                    0.0,
                )
            mean_by_entity = pd.Series(
                values, index=entity_index, dtype="float64"
            ).clip(lower=0.0, upper=1.0)
            model_name = numerator_details.model_name
            if denominator_details.model_name != numerator_details.model_name:
                model_name = (
                    f"{numerator_details.model_name}+{denominator_details.model_name}"
                )
            sample_sizes = (
                sample_sizes.reindex(entity_index).fillna(1.0).clip(lower=1.0)
            )
            uncertainty = summarize_hit_rate_uncertainty_from_counts(
                hit_mean_by_entity=numerator_mean,
                pa_mean_by_entity=denominator_mean,
                sample_size_by_entity=sample_sizes,
                hit_residuals=numerator_details.residuals,
                pa_residuals=denominator_details.residuals,
                seed=self.adapter_config.seed,
                draws=max(self.adapter_config.hit_rate_uncertainty_draws, 100),
                residual_scale_global=self.adapter_config.hit_rate_residual_scale_global,
                residual_scale_by_bucket=self.adapter_config.hit_rate_residual_scale_by_bucket,
                bucket_by_entity=denominator_details.bucket_by_entity.reindex(
                    entity_index
                ).fillna("default"),
                hit_residuals_by_bucket=numerator_details.residuals_by_bucket,
                pa_residuals_by_bucket=denominator_details.residuals_by_bucket,
                min_bucket_residual_count=self.adapter_config.min_bucket_residual_count,
            )
        else:
            details = self._predict_count_projection_result(
                metric_id=self.metric_id,
                config=config,
                source=source,
            )
            mean_by_entity = details.mean_by_entity
            residuals = details.residuals
            model_name = details.model_name
            if self.adapter_config.count_nonnegative_constraints:
                mean_by_entity = mean_by_entity.clip(lower=0.0)
            sample_sizes = (
                sample_sizes.reindex(mean_by_entity.index).fillna(1.0).clip(lower=1.0)
            )
            uncertainty = summarize_empirical_uncertainty(
                mean_by_entity=mean_by_entity,
                sample_size_by_entity=sample_sizes,
                residuals=residuals,
            )
            if self.adapter_config.count_nonnegative_constraints:
                uncertainty["p10"] = uncertainty["p10"].clip(lower=0.0)
                uncertainty["p50"] = uncertainty["p50"].clip(lower=0.0)
                uncertainty["p90"] = uncertainty["p90"].clip(lower=0.0)
        entity_id_col = self.adapter_config.entity_id_col
        availability = availability_confidence_by_entity(
            confidence_frame,
            entity_id_col=entity_id_col,
            date_col=self.adapter_config.date_col,
            min_history_games=self.adapter_config.min_history_games,
        )

        window_end_effective = context["window_end_effective"]
        snapshot_id = (
            self.adapter_config.source_snapshot_id
            or str(config.metadata.get("source_snapshot_id", ""))
            or window_end_effective.date().isoformat()
        )

        source_model_version = (
            f"{model_name}_phase15_{self._feature_set_hash(self.metric_id)}"
        )
        output = pd.DataFrame(
            {
                "entity_id": mean_by_entity.index.astype(str),
                "sport": "mlb",
                "metric_id": self.metric_id,
                "horizon": "season",
                "window_start": window_start.date().isoformat(),
                "window_end": window_end.date().isoformat(),
                "game_id": None,
                "mean": mean_by_entity.values,
                "availability_confidence": availability.reindex(mean_by_entity.index)
                .fillna(0.0)
                .values,
                "source_model_version": source_model_version,
                "source_snapshot_id": snapshot_id,
            }
        )
        output = output.join(uncertainty, on="entity_id")
        output = output.sort_values(["entity_id"], kind="stable").reset_index(drop=True)

        return output.loc[:, list(NEUTRAL_OUTPUT_COLUMNS)]

    def _build_projection_context(
        self,
        source: pd.DataFrame,
        *,
        window_start: pd.Timestamp,
        window_end: pd.Timestamp,
    ) -> dict[str, Any]:
        window_end_effective = pd.Timestamp(window_end)
        if self.adapter_config.inference_anchor_date is not None:
            anchor = pd.to_datetime(
                self.adapter_config.inference_anchor_date, errors="coerce"
            )
            if not pd.isna(anchor):
                window_end_effective = min(window_end_effective, pd.Timestamp(anchor))

        feature_cutoff = window_start - pd.Timedelta(days=1)
        if self.adapter_config.inference_anchor_date is not None:
            anchor = pd.to_datetime(
                self.adapter_config.inference_anchor_date, errors="coerce"
            )
            if not pd.isna(anchor):
                feature_cutoff = min(feature_cutoff, pd.Timestamp(anchor))

        infer_history_frame = source[
            source[self.adapter_config.date_col] <= feature_cutoff
        ].copy()
        if infer_history_frame.empty:
            confidence_frame = source.groupby(
                self.adapter_config.entity_id_col, as_index=False
            ).head(1)
            confidence_frame = confidence_frame.copy()
            confidence_frame[self.adapter_config.date_col] = feature_cutoff
        else:
            confidence_frame = infer_history_frame.copy()

        horizon_days = max((window_end - window_start).days + 1, 1)
        expected_games = self._expected_games_by_entity(
            infer_history_frame=infer_history_frame,
            fallback_frame=confidence_frame,
            horizon_days=horizon_days,
        )
        return {
            "window_end_effective": window_end_effective,
            "feature_cutoff": feature_cutoff,
            "confidence_frame": confidence_frame,
            "sample_sizes": expected_games.clip(lower=1.0).astype("float64"),
        }

    def _predict_count_mean_by_entity(
        self,
        *,
        metric_id: str,
        config: ContestConfig,
        source: pd.DataFrame,
    ) -> pd.Series:
        """Predict season-horizon totals for a count-like metric."""

        details = self._predict_count_projection_result(
            metric_id=metric_id,
            config=config,
            source=source,
        )
        return details.mean_by_entity

    def _predict_count_projection_details(
        self,
        *,
        metric_id: str,
        config: ContestConfig,
        source: pd.DataFrame,
    ) -> tuple[pd.Series, pd.Series, str]:
        """Backward-compatible tuple projection details wrapper."""

        details = self._predict_count_projection_result(
            metric_id=metric_id,
            config=config,
            source=source,
        )
        return details.mean_by_entity, details.residuals, details.model_name

    def _predict_count_projection_result(
        self,
        *,
        metric_id: str,
        config: ContestConfig,
        source: pd.DataFrame,
    ) -> CountProjectionDetails:
        """Predict count totals with residual and bucket metadata."""

        window_start, window_end = self._resolve_window_for_metric(config, metric_id)
        context = self._build_projection_context(
            source,
            window_start=window_start,
            window_end=window_end,
        )
        feature_cutoff = context["feature_cutoff"]

        train_cutoff = pd.to_datetime(
            self.adapter_config.train_end_date, errors="coerce"
        )
        if pd.isna(train_cutoff):
            train_cutoff = feature_cutoff
        train_cutoff = min(pd.Timestamp(train_cutoff), pd.Timestamp(feature_cutoff))

        if metric_id in SNAPSHOT_COUNT_TARGETS:
            return self._predict_snapshot_count_projection_details(
                metric_id=metric_id,
                config=config,
                source=source,
                context=context,
                train_cutoff=pd.Timestamp(train_cutoff),
                feature_cutoff=pd.Timestamp(feature_cutoff),
            )
        return self._predict_count_projection_details_legacy(
            metric_id=metric_id,
            source=source,
            context=context,
            train_cutoff=pd.Timestamp(train_cutoff),
            feature_cutoff=pd.Timestamp(feature_cutoff),
        )

    def _predict_snapshot_count_projection_details(
        self,
        *,
        metric_id: str,
        config: ContestConfig,
        source: pd.DataFrame,
        context: dict[str, Any],
        train_cutoff: pd.Timestamp,
        feature_cutoff: pd.Timestamp,
    ) -> CountProjectionDetails:
        """Predict season totals for snapshot-trained count targets."""

        entity_id_col = self.adapter_config.entity_id_col
        date_col = self.adapter_config.date_col
        cleaned = build_hits_pa_training_view(
            source,
            entity_id_col=entity_id_col,
            date_col=date_col,
            regular_season_only=self.adapter_config.regular_season_only,
            require_batter_pa_dedup=self.adapter_config.require_batter_pa_dedup,
        )
        if cleaned.empty:
            return self._predict_count_projection_details_legacy(
                metric_id=metric_id,
                source=source,
                context=context,
                train_cutoff=train_cutoff,
                feature_cutoff=feature_cutoff,
            )

        snapshots = build_player_season_snapshots(
            cleaned,
            entity_id_col=entity_id_col,
            date_col=date_col,
            target_col=metric_id,
            snapshot_min_games=max(self.adapter_config.snapshot_min_games, 1),
            snapshot_anchor_frequency=self.adapter_config.snapshot_anchor_frequency,
        )
        if snapshots.empty:
            return self._predict_count_projection_details_legacy(
                metric_id=metric_id,
                source=source,
                context=context,
                train_cutoff=train_cutoff,
                feature_cutoff=feature_cutoff,
            )

        snapshots["anchor_date"] = pd.to_datetime(
            snapshots["anchor_date"], errors="coerce"
        )
        snapshots = snapshots.dropna(subset=["anchor_date"]).copy()
        target_col = f"target_rest_of_season_{metric_id}"
        if target_col not in snapshots.columns:
            snapshots[target_col] = 0.0
        snapshots[target_col] = pd.to_numeric(
            snapshots[target_col], errors="coerce"
        ).fillna(0.0)

        train_frame = snapshots[snapshots["anchor_date"] <= train_cutoff].copy()
        infer_frame = snapshots[snapshots["anchor_date"] <= feature_cutoff].copy()
        if infer_frame.empty:
            return self._predict_count_projection_details_legacy(
                metric_id=metric_id,
                source=source,
                context=context,
                train_cutoff=train_cutoff,
                feature_cutoff=feature_cutoff,
            )
        else:
            infer_frame = (
                infer_frame.sort_values("anchor_date", kind="stable")
                .groupby("entity_id", as_index=False)
                .tail(1)
                .copy()
            )

        features = list(
            HITS_SNAPSHOT_FEATURES if metric_id == "hits" else PA_SNAPSHOT_FEATURES
        )
        for frame in (train_frame, infer_frame):
            for column in features:
                if column not in frame.columns:
                    frame[column] = 0.0
                frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(
                    0.0
                )
            if target_col not in frame.columns:
                frame[target_col] = 0.0
            frame[target_col] = pd.to_numeric(
                frame[target_col], errors="coerce"
            ).fillna(0.0)
            if "season_to_date_pa" not in frame.columns:
                frame["season_to_date_pa"] = 0.0
            if f"season_to_date_{metric_id}" not in frame.columns:
                frame[f"season_to_date_{metric_id}"] = 0.0

        train_payload = train_frame.loc[:, [*features, target_col]].copy()
        infer_payload = infer_frame.loc[:, [*features]].copy()
        try:
            infer_prediction, residuals, model_name = self._apply_model(
                train_frame=train_payload,
                infer_frame=infer_payload,
                target_col=target_col,
                feature_columns=features,
            )
        except TypeError:
            infer_prediction, residuals, model_name = self._apply_model(
                train_frame=train_payload,
                infer_frame=infer_payload,
                target_col=target_col,
            )
        pred_remaining = pd.to_numeric(
            infer_prediction.get("prediction", 0.0), errors="coerce"
        ).fillna(0.0)
        if self.adapter_config.count_nonnegative_constraints:
            pred_remaining = pred_remaining.clip(lower=0.0)
        season_to_date = pd.to_numeric(
            infer_frame.get(f"season_to_date_{metric_id}", 0.0), errors="coerce"
        ).fillna(0.0)
        totals = (season_to_date + pred_remaining).astype("float64")
        if self.adapter_config.count_nonnegative_constraints:
            totals = totals.clip(lower=0.0)
        mean_by_entity = pd.Series(
            totals.to_numpy(dtype="float64"),
            index=infer_frame["entity_id"].astype(str),
            dtype="float64",
        )
        clean_residuals = (
            pd.to_numeric(residuals, errors="coerce").dropna().astype("float64")
        )
        bucket_by_entity = self._bucket_labels_from_values(
            pd.to_numeric(infer_frame.get("season_to_date_pa", 0.0), errors="coerce")
            .fillna(0.0)
            .set_axis(mean_by_entity.index)
        )
        residuals_by_bucket = self._build_residual_bank_by_bucket(
            values=pd.to_numeric(
                train_frame.get("season_to_date_pa", 0.0), errors="coerce"
            ).fillna(0.0),
            residuals=clean_residuals,
        )
        return CountProjectionDetails(
            mean_by_entity=mean_by_entity,
            residuals=clean_residuals,
            model_name=str(model_name),
            bucket_by_entity=bucket_by_entity,
            residuals_by_bucket=residuals_by_bucket,
        )

    def _predict_count_projection_details_legacy(
        self,
        *,
        metric_id: str,
        source: pd.DataFrame,
        context: dict[str, Any],
        train_cutoff: pd.Timestamp,
        feature_cutoff: pd.Timestamp,
    ) -> CountProjectionDetails:
        """Legacy per-game scaled fallback used when snapshot rows are unavailable."""

        train_frame = source[
            source[self.adapter_config.date_col] <= train_cutoff
        ].copy()
        infer_history_frame = source[
            source[self.adapter_config.date_col] <= feature_cutoff
        ].copy()

        if infer_history_frame.empty:
            infer_frame = source.groupby(
                self.adapter_config.entity_id_col, as_index=False
            ).head(1)
            infer_frame = infer_frame.copy()
            infer_frame[self.adapter_config.date_col] = feature_cutoff
            keep_cols = {
                self.adapter_config.entity_id_col,
                self.adapter_config.date_col,
            }
            for column in infer_frame.columns:
                if column in keep_cols:
                    continue
                infer_frame[column] = 0.0
        else:
            infer_frame = infer_history_frame.groupby(
                self.adapter_config.entity_id_col, as_index=False
            ).tail(1)

        infer_frame = infer_frame.sort_values(
            [self.adapter_config.entity_id_col, self.adapter_config.date_col],
            kind="stable",
        )
        try:
            infer_frame, residuals, model_name = self._apply_model(
                train_frame=train_frame,
                infer_frame=infer_frame,
                target_col=metric_id,
            )
        except TypeError:
            if metric_id != self.metric_id:
                raise
            infer_frame, residuals, model_name = self._apply_model(
                train_frame=train_frame,
                infer_frame=infer_frame,
            )

        entity_id_col = self.adapter_config.entity_id_col
        per_game_mean = (
            infer_frame.groupby(entity_id_col)["prediction"].mean().astype("float64")
        )
        sample_sizes = (
            context["sample_sizes"].reindex(per_game_mean.index.astype(str)).fillna(1.0)
        )
        sample_sizes.index = per_game_mean.index
        mean_by_entity = (per_game_mean * sample_sizes).astype("float64")
        clean_residuals = pd.to_numeric(residuals, errors="coerce").dropna()
        season_to_date_pa = (
            pd.to_numeric(
                infer_history_frame.groupby(entity_id_col)["plate_appearances"].sum(),
                errors="coerce",
            )
            .reindex(mean_by_entity.index)
            .fillna(0.0)
        )
        return CountProjectionDetails(
            mean_by_entity=mean_by_entity,
            residuals=clean_residuals.astype("float64"),
            model_name=str(model_name),
            bucket_by_entity=self._bucket_labels_from_values(season_to_date_pa),
            residuals_by_bucket={
                "default": clean_residuals.astype("float64"),
            },
        )

    def _resolve_window_for_metric(
        self, config: ContestConfig, metric_id: str
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
        for market in config.market_definitions:
            if (
                market.metric_id.strip().lower() == metric_id.strip().lower()
                and market.horizon.strip().lower() == "season"
            ):
                start = pd.to_datetime(market.window_start, errors="coerce")
                end = pd.to_datetime(market.window_end, errors="coerce")
                if not pd.isna(start) and not pd.isna(end):
                    return pd.Timestamp(start), pd.Timestamp(end)
        return self._resolve_window(config)

    def _expected_games_by_entity(
        self,
        *,
        infer_history_frame: pd.DataFrame,
        fallback_frame: pd.DataFrame,
        horizon_days: int,
    ) -> pd.Series:
        entity_id_col = self.adapter_config.entity_id_col
        date_col = self.adapter_config.date_col
        if infer_history_frame.empty:
            return pd.Series(
                1.0,
                index=fallback_frame[entity_id_col].astype(str).drop_duplicates(),
                dtype="float64",
            )

        history_span = infer_history_frame[[entity_id_col, date_col]].copy()
        history_span[date_col] = pd.to_datetime(history_span[date_col], errors="coerce")
        history_span = history_span.dropna(subset=[date_col])
        if history_span.empty:
            return pd.Series(
                1.0,
                index=fallback_frame[entity_id_col].astype(str).drop_duplicates(),
                dtype="float64",
            )
        span_stats = history_span.groupby(entity_id_col, dropna=False)[date_col].agg(
            ["min", "max", "size"]
        )
        observed_days = (
            (span_stats["max"] - span_stats["min"])
            .dt.days.add(1)
            .clip(lower=1)
            .astype("float64")
        )
        games_per_day = span_stats["size"].astype("float64") / observed_days
        expected_games_by_entity = (games_per_day * float(horizon_days)).clip(lower=1.0)
        expected_games_by_entity.index = expected_games_by_entity.index.astype(str)
        return expected_games_by_entity

    @staticmethod
    def _feature_set_hash(metric_id: str) -> str:
        normalized = metric_id.strip().lower()
        if normalized == "plate_appearances":
            ordered = "|".join(PA_SNAPSHOT_FEATURES)
        elif normalized == "hits":
            ordered = "|".join(HITS_SNAPSHOT_FEATURES)
        elif normalized == "hit_rate":
            ordered = (
                "counts_only|"
                + "|".join(HITS_SNAPSHOT_FEATURES)
                + "|"
                + "|".join(PA_SNAPSHOT_FEATURES)
            )
        else:
            ordered = "|".join(model_feature_columns_for_metric(metric_id))
        digest = hashlib.sha1(ordered.encode("utf-8")).hexdigest()
        return digest[:8]

    def _resolve_window(
        self, config: ContestConfig
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
        """Resolve projection window from contest market definitions."""

        for market in config.market_definitions:
            if (
                market.metric_id.strip().lower() == self.metric_id
                and market.horizon.strip().lower() == "season"
            ):
                start = pd.to_datetime(market.window_start, errors="coerce")
                end = pd.to_datetime(market.window_end, errors="coerce")
                if not pd.isna(start) and not pd.isna(end):
                    return pd.Timestamp(start), pd.Timestamp(end)

        fallback_end = pd.Timestamp.utcnow().tz_localize(None).normalize()
        fallback_start = fallback_end - pd.Timedelta(days=200)
        return fallback_start, fallback_end

    def _apply_model(
        self,
        *,
        train_frame: pd.DataFrame,
        infer_frame: pd.DataFrame,
        target_col: str | None = None,
        feature_columns: list[str] | None = None,
    ) -> tuple[pd.DataFrame, pd.Series, str]:
        """Fit/predict through existing MLB estimator utilities with safe fallback."""

        working_infer = infer_frame.copy()
        resolved_target_col = target_col or self.metric_id
        if resolved_target_col not in working_infer.columns:
            working_infer[resolved_target_col] = 0.0

        features = feature_columns or model_feature_columns_for_metric(
            resolved_target_col
        )

        if train_frame.empty:
            baseline = float(
                pd.to_numeric(
                    working_infer[resolved_target_col], errors="coerce"
                ).mean()
            )
            if np.isnan(baseline):
                baseline = 0.0
            working_infer["prediction"] = baseline
            residuals = pd.Series(dtype="float64")
            return working_infer, residuals, "baseline"

        working_train = train_frame.copy()
        if resolved_target_col not in working_train.columns:
            working_train[resolved_target_col] = 0.0
        working_train[resolved_target_col] = pd.to_numeric(
            working_train[resolved_target_col], errors="coerce"
        ).fillna(0.0)

        model_name = self._resolve_model_name_for_training(
            train_frame=working_train,
            target_col=resolved_target_col,
            features=features,
        )
        try:
            spec = get_model_spec(model_name)
        except KeyError:
            spec = get_model_spec("poisson")
            model_name = "poisson"

        try:
            fit_params: dict[str, int] | None = None
            if self._supports_random_state(spec):
                fit_params = {"random_state": self.adapter_config.seed}
            model = fit_estimator(
                working_train,
                spec=spec,
                features=features,
                target_col=resolved_target_col,
                params=fit_params,
            )
            train_pred = predict_estimator(
                working_train,
                model=model,
                features=features,
                name="prediction",
            )
            infer_pred = predict_estimator(
                working_infer,
                model=model,
                features=features,
                name="prediction",
            )
            working_infer["prediction"] = infer_pred.astype("float64")
            residuals = working_train[resolved_target_col].astype(
                "float64"
            ) - train_pred.astype("float64")
            return working_infer, residuals, model_name
        except Exception:
            baseline = float(working_train[resolved_target_col].mean())
            if np.isnan(baseline):
                baseline = 0.0
            working_infer["prediction"] = baseline
            residuals = working_train[resolved_target_col] - baseline
            return working_infer, residuals.astype("float64"), "baseline"

    def _resolve_model_name_for_training(
        self,
        *,
        train_frame: pd.DataFrame,
        target_col: str,
        features: list[str],
    ) -> str:
        """Resolve model family using deterministic score-based selection."""

        default_model_name = str(self.adapter_config.model_name).strip().lower()
        if not self.adapter_config.model_selection_enabled:
            return default_model_name

        candidates = [
            str(name).strip().lower()
            for name in self.adapter_config.model_selection_candidates
            if str(name).strip()
        ]
        if default_model_name and default_model_name not in candidates:
            candidates.insert(0, default_model_name)

        score_rows: list[dict[str, float | str]] = []
        for candidate in candidates:
            try:
                spec = get_model_spec(candidate)
            except KeyError:
                continue
            try:
                fit_params: dict[str, int] | None = None
                if self._supports_random_state(spec):
                    fit_params = {"random_state": self.adapter_config.seed}
                model = fit_estimator(
                    train_frame,
                    spec=spec,
                    features=features,
                    target_col=target_col,
                    params=fit_params,
                )
                prediction = predict_estimator(
                    train_frame,
                    model=model,
                    features=features,
                    name="prediction",
                )
                actual = pd.to_numeric(train_frame[target_col], errors="coerce")
                valid = (
                    actual.notna() & pd.to_numeric(prediction, errors="coerce").notna()
                )
                if not valid.any():
                    continue
                err = (
                    pd.to_numeric(prediction.loc[valid], errors="coerce")
                    - actual.loc[valid]
                ).astype("float64")
                mae = float(err.abs().mean())
                rmse = float(np.sqrt((err**2).mean()))
                abs_bias = float(abs(err.mean()))
                score_rows.append(
                    {
                        "model_name": candidate,
                        "mae": mae,
                        "rmse": rmse,
                        "abs_bias": abs_bias,
                    }
                )
            except Exception:
                continue

        if not score_rows:
            return default_model_name
        scores = pd.DataFrame(score_rows)
        return self._select_model_name_from_scores(
            scores=scores,
            default_model_name=default_model_name,
            min_delta_mae=float(self.adapter_config.selection_min_delta_mae),
        )

    @staticmethod
    def _select_model_name_from_scores(
        *,
        scores: pd.DataFrame,
        default_model_name: str,
        min_delta_mae: float,
    ) -> str:
        """Select candidate by MAE, RMSE, then abs-bias with anti-churn threshold."""

        required = {"model_name", "mae", "rmse", "abs_bias"}
        if scores.empty or not required.issubset(scores.columns):
            return str(default_model_name).strip().lower()

        ranked = scores.copy()
        ranked["model_name"] = ranked["model_name"].astype(str).str.strip().str.lower()
        for column in ("mae", "rmse", "abs_bias"):
            ranked[column] = pd.to_numeric(ranked[column], errors="coerce")
        ranked = ranked.dropna(subset=["model_name", "mae", "rmse", "abs_bias"])
        if ranked.empty:
            return str(default_model_name).strip().lower()

        ranked = ranked.sort_values(
            ["mae", "rmse", "abs_bias", "model_name"], kind="stable"
        ).reset_index(drop=True)
        best = ranked.iloc[0]
        winner = str(best["model_name"])

        default = str(default_model_name).strip().lower()
        if not default:
            return winner
        default_rows = ranked[ranked["model_name"] == default]
        if default_rows.empty:
            return winner
        default_mae = float(default_rows.iloc[0]["mae"])
        best_mae = float(best["mae"])
        if winner == default:
            return default

        improvement = default_mae - best_mae
        if improvement <= max(float(min_delta_mae), 0.0):
            return default
        return winner

    def _bucket_labels_from_values(self, values: pd.Series) -> pd.Series:
        """Build deterministic bucket labels from season-to-date PA values."""

        numeric = pd.to_numeric(values, errors="coerce").fillna(0.0).clip(lower=0.0)
        raw_edges = [
            float(edge) for edge in self.adapter_config.uncertainty_bucket_edges
        ]
        edges = sorted(set(raw_edges))
        if not edges:
            return pd.Series("default", index=numeric.index, dtype="object")

        labels: list[str] = []
        for idx in range(len(edges) - 1):
            start = int(edges[idx])
            end = int(edges[idx + 1])
            labels.append(f"{start}_{end}")
        labels.append(f"{int(edges[-1])}_plus")

        bucket_values = pd.cut(
            numeric,
            bins=[*edges, float("inf")],
            labels=labels,
            right=False,
            include_lowest=True,
        )
        return bucket_values.astype("object").fillna("default")

    def _build_residual_bank_by_bucket(
        self,
        *,
        values: pd.Series,
        residuals: pd.Series,
    ) -> dict[str, pd.Series]:
        """Group residual vectors into configured season-to-date PA buckets."""

        clean_residuals = (
            pd.to_numeric(residuals, errors="coerce").dropna().astype("float64")
        )
        if clean_residuals.empty:
            return {"default": pd.Series([0.0], dtype="float64")}
        aligned_values = (
            pd.to_numeric(values, errors="coerce")
            .reindex(clean_residuals.index)
            .fillna(0.0)
        )
        buckets = self._bucket_labels_from_values(aligned_values)
        banks: dict[str, pd.Series] = {"default": clean_residuals}
        for bucket, idx in buckets.groupby(buckets).groups.items():
            bank = clean_residuals.loc[list(idx)]
            if bank.empty:
                continue
            banks[str(bucket)] = bank.astype("float64")
        return banks

    @staticmethod
    def _supports_random_state(spec: Any) -> bool:
        """Return whether a model spec factory accepts `random_state`."""

        if "random_state" in dict(getattr(spec, "default_params", {})):
            return True
        try:
            signature = inspect.signature(spec.factory)
        except (TypeError, ValueError):
            return False
        return "random_state" in signature.parameters
