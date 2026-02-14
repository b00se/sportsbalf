"""Phase 1 MLB season-horizon projection adapter."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from src.fantasy.adapters.mlb.features import (
    model_feature_columns_for_metric,
    prepare_mlb_projection_frame,
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
    """Runtime settings for the Phase 1 MLB projection adapter."""

    input_dataset_path: str
    entity_id_col: str = "batter"
    date_col: str = "game_date"
    seed: int = 2026
    min_history_games: int = 20
    model_name: str = "xgboost"
    train_end_date: str | None = None
    inference_anchor_date: str | None = None
    uncertainty_method: str = "empirical_quantiles"
    source_snapshot_id: str | None = None

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> MlbProjectionAdapterConfig:
        """Construct config from a plain mapping payload."""

        return cls(
            input_dataset_path=str(payload["input_dataset_path"]),
            entity_id_col=str(payload.get("entity_id_col", "batter")),
            date_col=str(payload.get("date_col", "game_date")),
            seed=int(payload.get("seed", 2026)),
            min_history_games=int(payload.get("min_history_games", 20)),
            model_name=str(payload.get("model_name", "xgboost")),
            train_end_date=(
                str(payload["train_end_date"])
                if payload.get("train_end_date") is not None
                else None
            ),
            inference_anchor_date=(
                str(payload["inference_anchor_date"])
                if payload.get("inference_anchor_date") is not None
                else None
            ),
            uncertainty_method=str(
                payload.get("uncertainty_method", "empirical_quantiles")
            ),
            source_snapshot_id=(
                str(payload["source_snapshot_id"])
                if payload.get("source_snapshot_id") is not None
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
        window_end_effective = window_end
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

        train_cutoff = pd.to_datetime(
            self.adapter_config.train_end_date, errors="coerce"
        )
        if pd.isna(train_cutoff):
            train_cutoff = feature_cutoff
        train_cutoff = min(pd.Timestamp(train_cutoff), feature_cutoff)

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
            confidence_frame = infer_frame
        else:
            confidence_frame = infer_history_frame
            infer_frame = infer_history_frame.groupby(
                self.adapter_config.entity_id_col, as_index=False
            ).tail(1)

        horizon_days = max((window_end - window_start).days + 1, 1)
        if infer_history_frame.empty:
            expected_games_by_entity = pd.Series(
                1.0,
                index=infer_frame[self.adapter_config.entity_id_col]
                .astype(str)
                .drop_duplicates(),
                dtype="float64",
            )
        else:
            history_span = infer_history_frame[
                [self.adapter_config.entity_id_col, self.adapter_config.date_col]
            ].copy()
            history_span[self.adapter_config.date_col] = pd.to_datetime(
                history_span[self.adapter_config.date_col], errors="coerce"
            )
            history_span = history_span.dropna(subset=[self.adapter_config.date_col])

            if history_span.empty:
                expected_games_by_entity = pd.Series(
                    1.0,
                    index=infer_frame[self.adapter_config.entity_id_col]
                    .astype(str)
                    .drop_duplicates(),
                    dtype="float64",
                )
            else:
                span_stats = history_span.groupby(
                    self.adapter_config.entity_id_col, dropna=False
                )[self.adapter_config.date_col].agg(["min", "max", "size"])
                observed_days = (
                    (span_stats["max"] - span_stats["min"])
                    .dt.days.add(1)
                    .clip(lower=1)
                    .astype("float64")
                )
                games_per_day = span_stats["size"].astype("float64") / observed_days
                expected_games_by_entity = (games_per_day * float(horizon_days)).clip(
                    lower=1.0
                )
                expected_games_by_entity.index = (
                    expected_games_by_entity.index.astype(str)
                )

        infer_frame = infer_frame.sort_values(
            [self.adapter_config.entity_id_col, self.adapter_config.date_col],
            kind="stable",
        )

        infer_frame, residuals, model_name = self._apply_model(
            train_frame=train_frame,
            infer_frame=infer_frame,
        )

        entity_id_col = self.adapter_config.entity_id_col
        per_game_mean = (
            infer_frame.groupby(entity_id_col)["prediction"].mean().astype("float64")
        )
        expected_games = expected_games_by_entity.reindex(
            per_game_mean.index.astype(str)
        ).fillna(1.0)
        expected_games.index = per_game_mean.index
        sample_sizes = (
            expected_games.astype("float64").clip(lower=1.0)
        )
        mean_by_entity = (per_game_mean * expected_games).astype("float64")

        uncertainty = summarize_empirical_uncertainty(
            mean_by_entity=mean_by_entity,
            sample_size_by_entity=sample_sizes,
            residuals=residuals,
        )
        availability = availability_confidence_by_entity(
            confidence_frame,
            entity_id_col=entity_id_col,
            date_col=self.adapter_config.date_col,
            min_history_games=self.adapter_config.min_history_games,
        )

        snapshot_id = (
            self.adapter_config.source_snapshot_id
            or str(config.metadata.get("source_snapshot_id", ""))
            or window_end_effective.date().isoformat()
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
                "source_model_version": f"{model_name}_phase1",
                "source_snapshot_id": snapshot_id,
            }
        )
        output = output.join(uncertainty, on="entity_id")
        output = output.sort_values(["entity_id"], kind="stable").reset_index(drop=True)

        return output.loc[:, list(NEUTRAL_OUTPUT_COLUMNS)]

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
    ) -> tuple[pd.DataFrame, pd.Series, str]:
        """Fit/predict through existing MLB estimator utilities with safe fallback."""

        working_infer = infer_frame.copy()
        target_col = self.metric_id
        if target_col not in working_infer.columns:
            working_infer[target_col] = 0.0

        features = model_feature_columns_for_metric(target_col)

        if train_frame.empty:
            baseline = float(
                pd.to_numeric(working_infer[target_col], errors="coerce").mean()
            )
            if np.isnan(baseline):
                baseline = 0.0
            working_infer["prediction"] = baseline
            residuals = pd.Series(dtype="float64")
            return working_infer, residuals, "baseline"

        working_train = train_frame.copy()
        if target_col not in working_train.columns:
            working_train[target_col] = 0.0
        working_train[target_col] = pd.to_numeric(
            working_train[target_col], errors="coerce"
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
                target_col=target_col,
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
            residuals = working_train[target_col].astype("float64") - train_pred.astype(
                "float64"
            )
            return working_infer, residuals, model_name
        except Exception:
            baseline = float(working_train[target_col].mean())
            if np.isnan(baseline):
                baseline = 0.0
            working_infer["prediction"] = baseline
            residuals = working_train[target_col] - baseline
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
