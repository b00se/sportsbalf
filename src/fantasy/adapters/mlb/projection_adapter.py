"""Phase 1 MLB season-horizon projection adapter."""

from __future__ import annotations

import hashlib
import inspect
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from src.fantasy.adapters.mlb.features import (
    is_derived_rate_metric,
    model_feature_columns_for_metric,
    prepare_mlb_projection_frame,
    rate_metric_inputs,
)
from src.fantasy.adapters.mlb.uncertainty import (
    availability_confidence_by_entity,
    summarize_empirical_uncertainty,
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
    pybaseball_priors_enabled: bool = False
    pybaseball_priors_cache_path: str = ""
    pybaseball_priors_seasons: tuple[int, ...] = ()
    pybaseball_priors_refresh: bool = False
    uncertainty_residual_bucket_col: str = "season_to_date_pa"
    uncertainty_bucket_edges: tuple[float, ...] = (0.0, 100.0, 250.0, 450.0, 700.0)
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
            source_snapshot_id=(
                str(effective["source_snapshot_id"])
                if effective.get("source_snapshot_id") is not None
                else None
            ),
        )


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
            ).clip(lower=0.0)
            _, numerator_residuals, numerator_model_name = (
                self._predict_count_projection_details(
                    metric_id=numerator,
                    config=config,
                    source=source,
                )
            )
            _, denominator_residuals, denominator_model_name = (
                self._predict_count_projection_details(
                    metric_id=denominator,
                    config=config,
                    source=source,
                )
            )
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
            mean_by_entity = pd.Series(values, index=entity_index, dtype="float64")
            model_name = numerator_model_name
            if denominator_model_name != numerator_model_name:
                model_name = f"{numerator_model_name}+{denominator_model_name}"
            residuals = numerator_residuals
            if residuals.empty:
                residuals = denominator_residuals
            sample_sizes = (
                sample_sizes.reindex(entity_index).fillna(1.0).clip(lower=1.0)
            )
        else:
            mean_by_entity, residuals, model_name = (
                self._predict_count_projection_details(
                    metric_id=self.metric_id,
                    config=config,
                    source=source,
                )
            )
            sample_sizes = (
                sample_sizes.reindex(mean_by_entity.index).fillna(1.0).clip(lower=1.0)
            )

        uncertainty = summarize_empirical_uncertainty(
            mean_by_entity=mean_by_entity,
            sample_size_by_entity=sample_sizes,
            residuals=residuals,
        )
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

        mean_by_entity, _residuals, _model_name = (
            self._predict_count_projection_details(
                metric_id=metric_id,
                config=config,
                source=source,
            )
        )
        return mean_by_entity

    def _predict_count_projection_details(
        self,
        *,
        metric_id: str,
        config: ContestConfig,
        source: pd.DataFrame,
    ) -> tuple[pd.Series, pd.Series, str]:
        """Predict count totals with residuals and resolved model provenance."""

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
        residuals = pd.to_numeric(residuals, errors="coerce").dropna()
        return mean_by_entity, residuals.astype("float64"), str(model_name)

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
    ) -> tuple[pd.DataFrame, pd.Series, str]:
        """Fit/predict through existing MLB estimator utilities with safe fallback."""

        working_infer = infer_frame.copy()
        resolved_target_col = target_col or self.metric_id
        if resolved_target_col not in working_infer.columns:
            working_infer[resolved_target_col] = 0.0

        features = model_feature_columns_for_metric(resolved_target_col)

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

        model_name = self.adapter_config.model_name
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
