"""Tests for the Phase 1 MLB fantasy projection adapter."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from src.fantasy.core.contracts import ContestConfig, MarketDefinition
from src.fantasy.core.registry import (
    clear_fantasy_registry,
    get_projection_adapter,
    list_registered_fantasy_adapters,
)
from tests.helpers.assertions import assert_horizon_semantics

PHASE1_METRICS = (
    "plate_appearances",
    "hits",
    "total_bases",
    "walks",
    "strikeouts",
    "pa_vs_lhp",
    "pa_vs_rhp",
    "hard_hit_events",
    "hit_rate",
    "walk_rate",
    "strikeout_rate",
    "slugging_proxy",
)

REQUIRED_COLUMNS = (
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


class _StubNflAdapter:
    def project(self, config: ContestConfig) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "entity_id": "qb-1",
                    "sport": "nfl",
                    "metric_id": "pass_attempts",
                    "horizon": "season",
                    "window_start": "2026-09-01",
                    "window_end": "2027-01-10",
                    "game_id": None,
                    "mean": 523.4,
                    "p10": 470.0,
                    "p50": 520.0,
                    "p90": 580.0,
                    "stddev": 22.5,
                    "availability_confidence": 0.98,
                    "source_model_version": "stub-v1",
                    "source_snapshot_id": "snapshot-2026-02-13",
                }
            ]
        )


def _contest(metric_id: str, window_end: str = "2026-06-01") -> ContestConfig:
    market = MarketDefinition(
        market_id=f"m-{metric_id}",
        provider="underdog",
        sport="mlb",
        mode="season_long_tournament",
        metric_id=metric_id,
        horizon="season",
        operator=None,
        line_value=None,
        window_start="2026-03-01",
        window_end=window_end,
        game_id=None,
    )
    return ContestConfig(
        contest_id=f"c-{metric_id}",
        provider="underdog",
        sport="mlb",
        mode="season_long_tournament",
        scoring_ruleset_id="rules",
        market_definitions=(market,),
        mode_config={},
        metadata={},
    )


def _adapter_kwargs(dataset_path: Path) -> dict[str, object]:
    return {
        "input_dataset_path": str(dataset_path),
        "entity_id_col": "batter",
        "date_col": "game_date",
        "seed": 2026,
        "min_history_games": 2,
        "model_name": "poisson",
        "train_end_date": "2025-12-31",
        "inference_anchor_date": "2026-04-01",
        "uncertainty_method": "empirical_quantiles",
        "source_snapshot_id": "fixture-snapshot",
    }


def _assert_neutral_schema(frame: pd.DataFrame) -> None:
    assert tuple(frame.columns) == REQUIRED_COLUMNS


def setup_function() -> None:
    clear_fantasy_registry()


def test_schema_contract_and_uncertainty_fields() -> None:
    from src.fantasy.adapters.mlb.registration import register_mlb_projection_adapters

    dataset = Path("tests/testdata/fantasy/mlb_batter_games_phase1.csv")
    register_mlb_projection_adapters(**_adapter_kwargs(dataset))

    adapter = get_projection_adapter("mlb", "hits", "season")
    projected = adapter.project(_contest("hits"))

    _assert_neutral_schema(projected)
    assert set(projected["sport"]) == {"mlb"}
    assert set(projected["metric_id"]) == {"hits"}
    assert (projected["p10"] <= projected["p50"]).all()
    assert (projected["p50"] <= projected["p90"]).all()
    assert (projected["stddev"] >= 0.0).all()
    assert_horizon_semantics(projected, horizon="season", prediction_col="mean")


def test_phase1_registration_covers_all_metrics() -> None:
    from src.fantasy.adapters.mlb.registration import register_mlb_projection_adapters

    dataset = Path("tests/testdata/fantasy/mlb_batter_games_phase1.csv")
    register_mlb_projection_adapters(**_adapter_kwargs(dataset))

    summary = list_registered_fantasy_adapters()
    projection_keys = set(summary["projection_keys"])

    for metric_id in PHASE1_METRICS:
        assert ("mlb", metric_id, "season") in projection_keys


def test_deterministic_projection_same_seed_same_output() -> None:
    from src.fantasy.adapters.mlb.registration import register_mlb_projection_adapters

    dataset = Path("tests/testdata/fantasy/mlb_batter_games_phase1.csv")
    kwargs = _adapter_kwargs(dataset)
    register_mlb_projection_adapters(**kwargs)

    adapter = get_projection_adapter("mlb", "total_bases", "season")
    contest = _contest("total_bases")

    first = adapter.project(contest)
    second = adapter.project(contest)

    pd.testing.assert_frame_equal(first, second)


def test_sparse_input_columns_fallback_remains_valid() -> None:
    from src.fantasy.adapters.mlb.registration import register_mlb_projection_adapters

    dataset = Path("tests/testdata/fantasy/mlb_batter_games_sparse_phase1.csv")
    register_mlb_projection_adapters(**_adapter_kwargs(dataset))

    adapter = get_projection_adapter("mlb", "hard_hit_events", "season")
    projected = adapter.project(_contest("hard_hit_events"))

    _assert_neutral_schema(projected)
    assert (projected["mean"] >= 0.0).all()


def test_window_end_leakage_guard_excludes_future_rows() -> None:
    from src.fantasy.adapters.mlb.registration import register_mlb_projection_adapters

    dataset = Path("tests/testdata/fantasy/mlb_batter_games_phase1.csv")
    register_mlb_projection_adapters(**_adapter_kwargs(dataset))

    adapter = get_projection_adapter("mlb", "hits", "season")
    projected = adapter.project(_contest("hits", window_end="2026-04-01"))

    assert projected["mean"].max() < 10.0


def test_extensibility_gate_non_mlb_adapter_neutral_schema() -> None:
    output = _StubNflAdapter().project(_contest("hits"))

    _assert_neutral_schema(output)
    assert set(output["sport"]) == {"nfl"}


def test_metric_specific_feature_selection_excludes_target_leakage() -> None:
    from src.fantasy.adapters.mlb.features import model_feature_columns_for_metric

    hits_features = model_feature_columns_for_metric("hits")
    assert "hits" not in hits_features
    assert "hit_rate" not in hits_features

    total_bases_features = model_feature_columns_for_metric("total_bases")
    assert "total_bases" not in total_bases_features
    assert "slugging_proxy" not in total_bases_features

    walk_rate_features = model_feature_columns_for_metric("walk_rate")
    assert "walks" not in walk_rate_features
    assert "plate_appearances" not in walk_rate_features

    for metric_id in PHASE1_METRICS:
        assert model_feature_columns_for_metric(metric_id)


def test_project_fallback_window_without_matching_market_is_timezone_safe() -> None:
    from src.fantasy.adapters.mlb.projection_adapter import (
        MlbProjectionAdapterConfig,
        MlbSeasonProjectionAdapter,
    )

    adapter = MlbSeasonProjectionAdapter(
        metric_id="hits",
        adapter_config=MlbProjectionAdapterConfig(
            input_dataset_path="tests/testdata/fantasy/mlb_batter_games_phase1.csv",
            entity_id_col="batter",
            date_col="game_date",
            seed=2026,
            min_history_games=2,
            model_name="poisson",
            train_end_date=None,
            inference_anchor_date=None,
            uncertainty_method="empirical_quantiles",
            source_snapshot_id="fixture-snapshot",
        ),
    )
    contest = _contest("walks")

    projected = adapter.project(contest)

    assert not projected.empty
    _assert_neutral_schema(projected)


def test_inference_frame_uses_pre_window_history_only(monkeypatch) -> None:
    from src.fantasy.adapters.mlb.projection_adapter import (
        MlbProjectionAdapterConfig,
        MlbSeasonProjectionAdapter,
    )

    captured: dict[str, object] = {}

    def _fake_apply_model(self, *, train_frame, infer_frame):
        captured["infer_max_date"] = pd.to_datetime(
            infer_frame["game_date"], errors="coerce"
        ).max()
        predicted = infer_frame.copy()
        predicted["prediction"] = 1.0
        return predicted, pd.Series(dtype="float64"), "baseline"

    adapter = MlbSeasonProjectionAdapter(
        metric_id="hits",
        adapter_config=MlbProjectionAdapterConfig(
            input_dataset_path="tests/testdata/fantasy/mlb_batter_games_phase1.csv",
            entity_id_col="batter",
            date_col="game_date",
            seed=2026,
            min_history_games=2,
            model_name="poisson",
            train_end_date=None,
            inference_anchor_date="2026-04-01",
            uncertainty_method="empirical_quantiles",
            source_snapshot_id="fixture-snapshot",
        ),
    )
    monkeypatch.setattr(
        MlbSeasonProjectionAdapter,
        "_apply_model",
        _fake_apply_model,
    )

    projected = adapter.project(_contest("hits", window_end="2026-06-01"))

    assert not projected.empty
    infer_max_date = pd.Timestamp(captured["infer_max_date"])
    assert infer_max_date < pd.Timestamp("2026-03-01")


def test_output_window_end_matches_contest_window_when_anchor_is_earlier() -> None:
    from src.fantasy.adapters.mlb.projection_adapter import (
        MlbProjectionAdapterConfig,
        MlbSeasonProjectionAdapter,
    )

    adapter = MlbSeasonProjectionAdapter(
        metric_id="hits",
        adapter_config=MlbProjectionAdapterConfig(
            input_dataset_path="tests/testdata/fantasy/mlb_batter_games_phase1.csv",
            entity_id_col="batter",
            date_col="game_date",
            seed=2026,
            min_history_games=2,
            model_name="poisson",
            train_end_date="2025-12-31",
            inference_anchor_date="2026-04-01",
            uncertainty_method="empirical_quantiles",
            source_snapshot_id="fixture-snapshot",
        ),
    )

    projected = adapter.project(_contest("hits", window_end="2026-06-01"))

    assert set(projected["window_end"]) == {"2026-06-01"}


def test_phase15b_config_controls_parse_from_nested_sections() -> None:
    from src.fantasy.adapters.mlb.projection_adapter import MlbProjectionAdapterConfig

    config = MlbProjectionAdapterConfig.from_mapping(
        {
            "mlb_projection_phase15": {
                "input_dataset_path": (
                    "tests/testdata/fantasy/mlb_batter_games_phase1.csv"
                ),
                "model_name": "poisson",
                "modeling": {
                    "selection_min_delta_mae": 0.02,
                },
                "uncertainty": {
                    "hit_rate_residual_scale_global": 0.75,
                    "hit_rate_residual_scale_by_bucket": {
                        "0_100": 0.6,
                        "100_250": 0.8,
                    },
                    "coverage_target": 0.82,
                    "calibration_objective": "coverage_width_tradeoff",
                    "min_bucket_residual_count": 25,
                },
            }
        }
    )

    assert config.selection_min_delta_mae == 0.02
    assert config.hit_rate_residual_scale_global == 0.75
    assert config.hit_rate_residual_scale_by_bucket == {"0_100": 0.6, "100_250": 0.8}
    assert config.coverage_target == 0.82
    assert config.calibration_objective == "coverage_width_tradeoff"
    assert config.min_bucket_residual_count == 25


def test_model_selection_threshold_keeps_default_for_small_mae_gain() -> None:
    from src.fantasy.adapters.mlb.projection_adapter import MlbSeasonProjectionAdapter

    scores = pd.DataFrame(
        [
            {"model_name": "poisson", "mae": 1.00, "rmse": 1.20, "abs_bias": 0.10},
            {
                "model_name": "hist_gradient_boosting",
                "mae": 0.995,
                "rmse": 1.18,
                "abs_bias": 0.08,
            },
        ]
    )

    selected = MlbSeasonProjectionAdapter._select_model_name_from_scores(
        scores=scores,
        default_model_name="poisson",
        min_delta_mae=0.01,
    )

    assert selected == "poisson"


def test_inference_frame_uses_latest_pre_window_row_per_entity(monkeypatch) -> None:
    from src.fantasy.adapters.mlb.projection_adapter import (
        MlbProjectionAdapterConfig,
        MlbSeasonProjectionAdapter,
    )

    captured: dict[str, int] = {}

    def _fake_apply_model(self, *, train_frame, infer_frame):
        captured["infer_rows"] = int(len(infer_frame))
        predicted = infer_frame.copy()
        predicted["prediction"] = 1.0
        return predicted, pd.Series(dtype="float64"), "baseline"

    adapter = MlbSeasonProjectionAdapter(
        metric_id="hits",
        adapter_config=MlbProjectionAdapterConfig(
            input_dataset_path="tests/testdata/fantasy/mlb_batter_games_phase1.csv",
            entity_id_col="batter",
            date_col="game_date",
            seed=2026,
            min_history_games=2,
            model_name="poisson",
            train_end_date=None,
            inference_anchor_date="2026-04-01",
            uncertainty_method="empirical_quantiles",
            source_snapshot_id="fixture-snapshot",
        ),
    )
    monkeypatch.setattr(
        MlbSeasonProjectionAdapter,
        "_apply_model",
        _fake_apply_model,
    )

    projected = adapter.project(_contest("hits", window_end="2026-06-01"))

    assert not projected.empty
    assert captured["infer_rows"] == 2


def test_availability_confidence_uses_full_pre_window_history(monkeypatch) -> None:
    from src.fantasy.adapters.mlb import projection_adapter as projection_module
    from src.fantasy.adapters.mlb.projection_adapter import (
        MlbProjectionAdapterConfig,
        MlbSeasonProjectionAdapter,
    )

    captured: dict[str, object] = {}

    def _fake_apply_model(self, *, train_frame, infer_frame):
        captured["infer_rows"] = int(len(infer_frame))
        predicted = infer_frame.copy()
        predicted["prediction"] = 1.0
        return predicted, pd.Series(dtype="float64"), "baseline"

    def _fake_availability(frame, *, entity_id_col, date_col, min_history_games):
        game_counts = frame.groupby(entity_id_col)[date_col].size().astype("float64")
        captured["confidence_rows"] = int(len(frame))
        captured["max_games_per_entity"] = float(game_counts.max())
        return pd.Series(0.5, index=game_counts.index, dtype="float64")

    adapter = MlbSeasonProjectionAdapter(
        metric_id="hits",
        adapter_config=MlbProjectionAdapterConfig(
            input_dataset_path="tests/testdata/fantasy/mlb_batter_games_phase1.csv",
            entity_id_col="batter",
            date_col="game_date",
            seed=2026,
            min_history_games=2,
            model_name="poisson",
            train_end_date=None,
            inference_anchor_date="2026-04-01",
            uncertainty_method="empirical_quantiles",
            source_snapshot_id="fixture-snapshot",
        ),
    )
    monkeypatch.setattr(
        MlbSeasonProjectionAdapter,
        "_apply_model",
        _fake_apply_model,
    )
    monkeypatch.setattr(
        projection_module,
        "availability_confidence_by_entity",
        _fake_availability,
    )

    projected = adapter.project(_contest("hits", window_end="2026-06-01"))

    assert not projected.empty
    assert captured["infer_rows"] == 2
    assert int(captured["confidence_rows"]) > captured["infer_rows"]
    assert float(captured["max_games_per_entity"]) > 1.0


def test_season_projection_scales_per_game_predictions_to_window(monkeypatch) -> None:
    from src.fantasy.adapters.mlb.projection_adapter import (
        MlbProjectionAdapterConfig,
        MlbSeasonProjectionAdapter,
    )

    def _fake_apply_model(self, *, train_frame, infer_frame):
        predicted = infer_frame.copy()
        predicted["prediction"] = 1.0
        return predicted, pd.Series(dtype="float64"), "baseline"

    adapter = MlbSeasonProjectionAdapter(
        metric_id="hits",
        adapter_config=MlbProjectionAdapterConfig(
            input_dataset_path="tests/testdata/fantasy/mlb_batter_games_phase1.csv",
            entity_id_col="batter",
            date_col="game_date",
            seed=2026,
            min_history_games=2,
            model_name="poisson",
            train_end_date=None,
            inference_anchor_date="2026-04-01",
            uncertainty_method="empirical_quantiles",
            source_snapshot_id="fixture-snapshot",
        ),
    )
    monkeypatch.setattr(
        MlbSeasonProjectionAdapter,
        "_apply_model",
        _fake_apply_model,
    )

    projected = adapter.project(_contest("hits", window_end="2026-06-01"))

    assert not projected.empty
    assert (projected["mean"] > 1.0).all()


def test_poisson_model_name_is_preserved_without_random_state_fallback() -> None:
    from src.fantasy.adapters.mlb.features import prepare_mlb_projection_frame
    from src.fantasy.adapters.mlb.projection_adapter import (
        MlbProjectionAdapterConfig,
        MlbSeasonProjectionAdapter,
    )

    adapter = MlbSeasonProjectionAdapter(
        metric_id="hits",
        adapter_config=MlbProjectionAdapterConfig(
            input_dataset_path="tests/testdata/fantasy/mlb_batter_games_phase1.csv",
            entity_id_col="batter",
            date_col="game_date",
            seed=2026,
            min_history_games=2,
            model_name="poisson",
            train_end_date="2025-12-31",
            inference_anchor_date="2026-04-01",
            uncertainty_method="empirical_quantiles",
            source_snapshot_id="fixture-snapshot",
        ),
    )
    source = prepare_mlb_projection_frame(
        "tests/testdata/fantasy/mlb_batter_games_phase1.csv",
        entity_id_col="batter",
        date_col="game_date",
    )
    train_frame = source[source["game_date"] <= pd.Timestamp("2025-12-31")].copy()
    infer_frame = source[source["game_date"] <= pd.Timestamp("2026-02-28")].copy()

    _predicted, _residuals, model_name = adapter._apply_model(
        train_frame=train_frame,
        infer_frame=infer_frame,
    )

    assert model_name == "poisson"


def test_phase15_rate_metric_is_derived_from_count_predictions(monkeypatch) -> None:
    from src.fantasy.adapters.mlb.projection_adapter import (
        MlbProjectionAdapterConfig,
        MlbSeasonProjectionAdapter,
    )

    def _fake_predict_count(self, *, metric_id, config, source):
        if metric_id == "plate_appearances":
            return pd.Series({"101": 100.0, "202": 40.0}, dtype="float64")
        if metric_id == "hits":
            return pd.Series({"101": 30.0, "202": 10.0}, dtype="float64")
        raise AssertionError(metric_id)

    adapter = MlbSeasonProjectionAdapter(
        metric_id="hit_rate",
        adapter_config=MlbProjectionAdapterConfig(
            input_dataset_path="tests/testdata/fantasy/mlb_batter_games_phase1.csv",
            entity_id_col="batter",
            date_col="game_date",
            seed=2026,
            min_history_games=2,
            model_name="poisson",
            train_end_date="2025-12-31",
            inference_anchor_date="2026-04-01",
            uncertainty_method="empirical_quantiles",
            source_snapshot_id="fixture-snapshot",
        ),
    )
    monkeypatch.setattr(
        MlbSeasonProjectionAdapter,
        "_predict_count_mean_by_entity",
        _fake_predict_count,
    )

    projected = adapter.project(_contest("hit_rate", window_end="2026-06-01"))

    rates = projected.set_index("entity_id")["mean"].astype("float64")
    assert float(rates.loc["101"]) == 0.3
    assert float(rates.loc["202"]) == 0.25


def test_phase15_uncertainty_uses_model_residuals(monkeypatch) -> None:
    from src.fantasy.adapters.mlb.projection_adapter import (
        MlbProjectionAdapterConfig,
        MlbSeasonProjectionAdapter,
    )

    def _fake_apply_model(self, *, train_frame, infer_frame):
        predicted = infer_frame.copy()
        predicted["prediction"] = 2.0
        residuals = pd.Series([-2.0, 0.0, 2.0], dtype="float64")
        return predicted, residuals, "poisson"

    adapter = MlbSeasonProjectionAdapter(
        metric_id="hits",
        adapter_config=MlbProjectionAdapterConfig(
            input_dataset_path="tests/testdata/fantasy/mlb_batter_games_phase1.csv",
            entity_id_col="batter",
            date_col="game_date",
            seed=2026,
            min_history_games=2,
            model_name="poisson",
            train_end_date=None,
            inference_anchor_date="2026-04-01",
            uncertainty_method="empirical_quantiles",
            source_snapshot_id="fixture-snapshot",
        ),
    )
    monkeypatch.setattr(MlbSeasonProjectionAdapter, "_apply_model", _fake_apply_model)

    projected = adapter.project(_contest("hits", window_end="2026-06-01"))

    assert (projected["stddev"] > 0.0).all()
    assert (projected["p10"] < projected["p90"]).all()


def test_phase15_source_model_version_uses_actual_fallback_model(monkeypatch) -> None:
    from src.fantasy.adapters.mlb.projection_adapter import (
        MlbProjectionAdapterConfig,
        MlbSeasonProjectionAdapter,
    )

    def _fake_apply_model(self, *, train_frame, infer_frame):
        predicted = infer_frame.copy()
        predicted["prediction"] = 1.0
        return predicted, pd.Series(dtype="float64"), "baseline"

    adapter = MlbSeasonProjectionAdapter(
        metric_id="hits",
        adapter_config=MlbProjectionAdapterConfig(
            input_dataset_path="tests/testdata/fantasy/mlb_batter_games_phase1.csv",
            entity_id_col="batter",
            date_col="game_date",
            seed=2026,
            min_history_games=2,
            model_name="xgboost",
            train_end_date=None,
            inference_anchor_date="2026-04-01",
            uncertainty_method="empirical_quantiles",
            source_snapshot_id="fixture-snapshot",
        ),
    )
    monkeypatch.setattr(MlbSeasonProjectionAdapter, "_apply_model", _fake_apply_model)

    projected = adapter.project(_contest("hits", window_end="2026-06-01"))

    assert projected["source_model_version"].str.startswith("baseline_phase15_").all()


def test_phase15_hit_rate_is_constrained_to_unit_interval(monkeypatch) -> None:
    from src.fantasy.adapters.mlb.projection_adapter import (
        MlbProjectionAdapterConfig,
        MlbSeasonProjectionAdapter,
    )

    def _fake_projection_details(self, *, metric_id, config, source):
        if metric_id == "plate_appearances":
            means = pd.Series({"101": -10.0, "202": 2.0}, dtype="float64")
            return means, pd.Series([0.0], dtype="float64"), "baseline"
        if metric_id == "hits":
            means = pd.Series({"101": 50.0, "202": 3.0}, dtype="float64")
            return means, pd.Series([0.0], dtype="float64"), "baseline"
        raise AssertionError(metric_id)

    adapter = MlbSeasonProjectionAdapter(
        metric_id="hit_rate",
        adapter_config=MlbProjectionAdapterConfig(
            input_dataset_path="tests/testdata/fantasy/mlb_batter_games_phase1.csv",
            entity_id_col="batter",
            date_col="game_date",
            seed=2026,
            min_history_games=2,
            model_name="xgboost",
            train_end_date=None,
            inference_anchor_date="2026-04-01",
            uncertainty_method="empirical_quantiles",
            source_snapshot_id="fixture-snapshot",
        ),
    )
    monkeypatch.setattr(
        MlbSeasonProjectionAdapter,
        "_predict_count_projection_details",
        _fake_projection_details,
    )

    projected = adapter.project(_contest("hit_rate", window_end="2026-06-01"))

    assert ((projected["mean"] >= 0.0) & (projected["mean"] <= 1.0)).all()
    assert ((projected["p10"] >= 0.0) & (projected["p10"] <= 1.0)).all()
    assert ((projected["p90"] >= 0.0) & (projected["p90"] <= 1.0)).all()
