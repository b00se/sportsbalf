# Sport/Stat Contract Baseline (Current Behavior)

Status: Canonical (Current State)

Date: 2026-02-12

## Contract Source

Authoritative protocol:
- `SportStatPipeline` in `src/core/contracts.py`

## Stage Contract (Intended vs Current Adapter Behavior)

| Stage | Intended responsibility | Current adapter behavior | Required handoff artifact |
|---|---|---|---|
| `load_inputs(config)` | Gather raw sources | Minimal payload in adapters | `PipelineInputs` |
| `build_training_frame(inputs, config)` | Build model-ready frame | Compatibility no-op in adapters | `pd.DataFrame` |
| `train_or_load_model(frame, config, retrain)` | Return trained artifacts | Compatibility no-op in adapters | `ModelBundle` |
| `predict_lines(inputs, model_bundle, config)` | Build pre-simulation rows | Compatibility no-op in adapters | `pd.DataFrame` |
| `simulate(predictions, model_bundle, config)` | Run final simulation output | Performs full sport workflow | final `pd.DataFrame` |

## Simulate-Only Adapter Policy

Current simulate-only adapters are explicit and test-enforced:
- `src.mlb.pitcher_props.adapter.MlbPitcherPropsPipeline`
- `src.nfl.pass_attempts.pipeline.NflPassAttemptsPipeline`
- `src.nhl.shots_on_goal.pipeline.NhlShotsOnGoalPipeline`

Enforcement:
- `tests/test_engine_contract_enforcement.py`
- Any new simulate-only adapter must be deliberately allowlisted.

## Shared Simulation Contract

From `src/core/simulation.py`:
- `MonteCarloConfig`
- `simulate_row(...)`
- `apply_simulations(...)`

Shared naming is sport-neutral (`line`, `entity_id`) with caller-specified column mappings (`line_col`, `id_col`).

## Output Schema Baseline

### Shared simulation fields
- `prob_over`
- `prob_under`
- `prob_push`
- `ev_over`
- `ev_under`
- `edge_over`
- `edge_under`

### MLB fields
- Strikeouts: `predicted_strikeouts`, `k_line`
- Outs: `predicted_outs_recorded`, `outs_line`
- Earned runs: `predicted_earned_runs`, `er_line`
- Hits allowed: `predicted_hits_allowed`, `hits_line`
- Walks allowed: `predicted_bb_allowed`, `bb_line`
- Mode metadata where applicable: `run_mode`, `lines_status`

### NFL fields
- `predicted_pass_attempts`
- `attempts_line`
- plus shared simulation fields

### NHL fields (`shots_on_goal`)
Identity/line:
- `player_id`, `player_name`, `team`, `opponent`, `game_id`, `sog_line`

Prediction:
- `predicted_shots_on_goal`

Additive model metadata:
- `baseline_predicted_shots_on_goal`
- `model_residual_std`
- `training_rmse`
- `training_mae`
- `training_r2`
- `model_name`

Mode metadata:
- `run_mode`, `lines_status`

## NHL Data/Model Baseline

Curated canonical skater-game schema:
- `season`, `game_id`, `game_date`, `player_id`, `player_name`, `team`, `opponent`, `shots_on_goal`, `time_on_ice_minutes`

Current NHL model feature set:
- `sog_avg_last_5`
- `sog_avg_last_10`
- `sog_avg_season_to_date`
- `toi_avg_last_5`
- `toi_avg_last_10`
- `games_played_to_date`
- `days_since_last_game`
- `team_sog_for_avg_last_5`
- `opponent_sog_allowed_avg_last_5`

Model compatibility policy:
- NHL model artifact stores feature schema hash.
- Incompatible/corrupt artifact triggers retrain path.

## Stability Policy

- Existing output columns for shipped sport/stat pipelines are backward-compatible commitments.
- New features may add columns, but existing required columns should remain stable unless a deliberate contract migration is documented.

## Test Enforcement Baseline

Intent-first contract checks are documented and enforced through:
- `docs/testing-intent-matrix.md` (authoritative coverage matrix)
- `tests/helpers/assertions.py` (shared invariant assertion helpers)
- `tests/contracts/test_pipeline_output_contracts.py` (cross-sport contract parity)

Current parity checks validate shared simulation semantics across shipped
MLB/NFL/NHL pipelines, including probability algebra and run-mode semantics.

## Fantasy Phase 0 Unified Projection Core

Source modules:
- `src/fantasy/core/contracts.py`
- `src/fantasy/core/registry.py`
- `src/fantasy/core/config.py`
- `src/fantasy/core/validation.py`
- `src/fantasy/core/mapping.py`
- `src/fantasy/core/derived.py`

Canonical primitive:
- `Projection(entity, metric, horizon, distribution)` represented by:
  - `ProjectionKey`
  - `ProjectionDistribution`
  - `ProjectionRow`

Unified contract layers:
- Base stat projection (`SportProjectionAdapter.project`)
- Derived metrics (`DerivedMetricAdapter.derive`)
- Market transform (`MarketTransformAdapter.transform`)
- Provider export (`ExportAdapter.export`)

Shared config contracts:
- `ContestConfig`
- `MarketDefinition`
- `DerivedMetricSpec`
- `ProviderPlayerMapping`

Phase 0 policy:
- Fantasy points are modeled as derived metrics from base stats.
- Market surfaces reference existing `(metric_id, horizon)` pairs.
- Mapping resolution emits `mapped`, `unmapped`, `duplicate_provider_id`.

## Fantasy Phase 1 MLB Projection Adapter

Source modules:
- `src/fantasy/adapters/mlb/projection_adapter.py`
- `src/fantasy/adapters/mlb/features.py`
- `src/fantasy/adapters/mlb/uncertainty.py`
- `src/fantasy/adapters/mlb/registration.py`

Registered contract keys:
- `(mlb, plate_appearances, season)`
- `(mlb, hits, season)`
- `(mlb, total_bases, season)`
- `(mlb, walks, season)`
- `(mlb, strikeouts, season)`
- `(mlb, pa_vs_lhp, season)`
- `(mlb, pa_vs_rhp, season)`
- `(mlb, hard_hit_events, season)`
- `(mlb, hit_rate, season)`
- `(mlb, walk_rate, season)`
- `(mlb, strikeout_rate, season)`
- `(mlb, slugging_proxy, season)`

Neutral output schema for `SportProjectionAdapter.project(...)`:
- `entity_id`
- `sport`
- `metric_id`
- `horizon`
- `window_start`
- `window_end`
- `game_id`
- `mean`
- `p10`
- `p50`
- `p90`
- `stddev`
- `availability_confidence`
- `source_model_version`
- `source_snapshot_id`

Phase 1 policy:
- Output remains in-memory only.
- Schema is neutral and contains no MLB-specific columns.
- Uncertainty defaults to empirical residual quantiles.

## Fantasy Phase 1.5 MLB Projection Hardening

Additional modules:
- `src/fantasy/adapters/mlb/datasets.py`
- `src/fantasy/adapters/mlb/feature_engineering.py`
- `src/fantasy/adapters/mlb/priors.py`
- `src/fantasy/adapters/mlb/backtest.py`

Phase 1.5 policy:
- Train count-like targets directly and derive rate metrics from predicted counts.
- Keep neutral output schema unchanged.
- Use leakage-safe shifted rolling features for snapshot construction.
- Keep pybaseball priors optional with cache-first/offline fallbacks.
