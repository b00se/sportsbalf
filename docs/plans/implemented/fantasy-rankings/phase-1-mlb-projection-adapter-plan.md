# Phase 1 Plan: MLB Projection Adapter (Neutral Outputs, Broad Stat Surface)

Status: Implemented

## Summary
Implement Phase 1 of `docs/plans/planned/fantasy-rankings/cross-sport-fantasy-rankings-architecture-roadmap.md` by shipping an MLB projection adapter that produces sport-agnostic projection rows plus uncertainty for a broad season stat set, using model-reuse-first and in-memory outputs only.

This phase does not implement ranking logic, provider export logic, or persistent fantasy artifacts.

## Locked Decisions
1. Metric scope: Broad Set.
2. Output mode: In-memory only.
3. Projection engine: Model-reuse first.

## Scope
1. In scope:
- Add MLB fantasy projection adapter module(s) under `src/fantasy/adapters/mlb/`.
- Implement season-horizon projection generation for a broad metric set.
- Reuse existing modeling utilities from `src/mlb/models/` (`fit_estimator`, `predict_estimator`, model registry specs).
- Output neutral long-form projection surface aligned to fantasy core contracts.
- Register adapter instances in fantasy registry for `(sport, metric_id, horizon)`.
- Add offline deterministic tests for adapter behavior, schema, uncertainty fields, and extensibility gate.
- Add/extend fantasy config fixture(s) for Phase 1 usage.
- Update canonical docs and roadmap status notes.
2. Out of scope:
- Contest ranking/scoring optimization (Phase 2).
- Provider export schema generation (Phase 3).
- Backtest calibration harness (Phase 4).
- Workflow orchestration/versioned manifests (Phase 5).
- Any changes to existing MLB/NFL/NHL production sport/stat pipelines.

## Broad Metric Set (Phase 1 Contract)
Season-horizon base metrics (`horizon = season`):
1. `plate_appearances`
2. `hits`
3. `total_bases`
4. `walks`
5. `strikeouts`
6. `pa_vs_lhp`
7. `pa_vs_rhp`
8. `hard_hit_events` (or `hard_hit_rate_count_proxy`)
9. `hit_rate`
10. `walk_rate`
11. `strikeout_rate`
12. `slugging_proxy`

## Data Sources and Feature Baseline
1. Primary input frame: existing reusable batter-game table semantics from `src/mlb/pitcher_props/data.py::build_batter_game_table`.
2. Required raw columns:
- `batter`, `game_date`, `plate_appearances`, `hits`, `total_bases`, `walks`, `strikeouts`, `hard_hit_rate`, `pa_vs_lhp`, `pa_vs_rhp`
3. Tests remain offline fixture-first; network is never required.
4. Training/inference split policy is leakage-safe by date cutoff (`window_end` as upper bound).

## Architecture and Module Plan
1. Add:
- `src/fantasy/adapters/__init__.py`
- `src/fantasy/adapters/mlb/__init__.py`
- `src/fantasy/adapters/mlb/projection_adapter.py`
- `src/fantasy/adapters/mlb/features.py`
- `src/fantasy/adapters/mlb/uncertainty.py`
- `src/fantasy/adapters/mlb/registration.py`
2. Optional support module(s):
- `src/fantasy/adapters/mlb/schema.py`
- `src/fantasy/adapters/mlb/datasets.py`
3. Public interface additions:
- `MlbSeasonProjectionAdapter` implementing `SportProjectionAdapter.project(config: ContestConfig) -> pd.DataFrame`
- `register_mlb_projection_adapters(...)` helper to register all Phase-1 metrics to fantasy core registry

## Output Schema (Neutral, Long-Form)
Adapter `project(...)` returns one row per `(entity_id, metric_id, horizon)` with:
1. `entity_id`
2. `sport`
3. `metric_id`
4. `horizon`
5. `window_start`
6. `window_end`
7. `game_id`
8. `mean`
9. `p10`
10. `p50`
11. `p90`
12. `stddev`
13. `availability_confidence`
14. `source_model_version`
15. `source_snapshot_id`

No MLB-specific output columns are allowed in this projection surface.

## Modeling Strategy (Model-Reuse First)
1. Reuse `src/mlb/models/registry.py` model specs and `src/mlb/models/trainers.py` helpers.
2. Per metric:
- train separate estimator on historical batter-season frame
- produce point prediction for target season window
3. Uncertainty:
- residual distribution estimated from training residuals
- `stddev` from sample residual standard deviation
- `p10/p50/p90` from empirical quantiles
4. Determinism:
- explicit seed in adapter config
- deterministic sort before fit/predict
5. Availability confidence:
- deterministic function of recent plate appearances and historical games present

## Config and Wiring Plan
1. Add dedicated config file:
- `config/fantasy/mlb_phase1_projection_2026.yaml`
2. Add optional top-level section:
- `adapters.mlb_projection`
3. Adapter keys:
- `input_dataset_path`
- `entity_id_col` (default `batter`)
- `date_col` (default `game_date`)
- `seed` (default `2026`)
- `min_history_games` (default `20`)
- `model_name` (default `xgboost`)
- `train_end_date`
- `inference_anchor_date`
- `uncertainty_method` (default `empirical_quantiles`)
4. Registry wiring:
- register `(mlb, <metric>, season)` via `register_projection_adapter(...)`
- keep registration idempotent and normalized

## Extensibility Gate (Phase 1 Exit)
1. A stub non-MLB adapter can be registered and produce the same neutral output schema.
2. Downstream tests assert no consumer relies on MLB-only columns.
3. No shared core interface changes are required to add a second sport adapter stub.

## Implementation Sequence (TDD)
1. RED: tests for neutral projection output schema and required columns.
2. GREEN: implement base adapter skeleton returning empty/shape-valid frame.
3. RED: tests for broad metric registration and lookup across all metrics.
4. GREEN: implement registration helper and metric map.
5. RED: tests for deterministic predictions (`same seed/input -> same output`).
6. GREEN: wire model-reuse training/prediction path.
7. RED: tests for uncertainty columns and ordering (`p10 <= p50 <= p90`, `stddev >= 0`).
8. GREEN: implement uncertainty calculations.
9. RED: extensibility-gate tests with non-MLB stub adapter.
10. GREEN: finalize neutral-column enforcement and add guard tests.
11. Run `.venv/bin/pytest -q`.
12. Run `.venv/bin/ruff check .`.

## Test Cases and Scenarios
1. Schema contract: projection output contains required neutral columns.
2. Broad metric coverage: all metric IDs are registered and projectable.
3. Determinism: repeated calls with same input/config/seed match exactly.
4. Quantile sanity: `p10 <= p50 <= p90`; `stddev >= 0`.
5. Missing optional columns: graceful fallback/default behavior for sparse inputs.
6. Insufficient history: fallback prediction path still emits valid uncertainty fields.
7. Windowing correctness: no leakage from post-`window_end` rows.
8. Extensibility gate: non-MLB stub passes neutral schema expectations.
9. No runtime regression: existing MLB/NFL/NHL pipeline tests remain unchanged/passing.

## Docs Updates Required
1. `docs/contracts.md`
2. `docs/architecture.md`
3. `docs/config-schema.md`
4. `docs/plans/planned/fantasy-rankings/cross-sport-fantasy-rankings-architecture-roadmap.md`

## Acceptance Criteria
1. MLB season adapter produces neutral per-player projections plus uncertainty for all listed metrics.
2. Output is in-memory only and deterministic under fixed seed.
3. Adapter is registered via fantasy registry for `(mlb, metric_id, season)` keys.
4. No downstream component requires MLB-specific projection columns.
5. Existing production pipeline behavior is unchanged.
6. Extensibility gate passes with a non-MLB stub.

## Assumptions and Defaults
1. Horizon for Phase 1 is `season` only.
2. Input dataset is local/offline and pre-built.
3. Uncertainty default method is `empirical_quantiles`.
4. `hard_hit_events` may be represented via `hard_hit_rate` proxy if event counts are unavailable.
5. Model default is `xgboost`, with fallback to a simpler model if unavailable.
6. Phase 1 does not persist projection snapshots; outputs remain in-memory DataFrames.
