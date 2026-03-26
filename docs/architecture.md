# Architecture Baseline (Current Runtime Reality)

Status: Canonical (Current State)

Date: 2026-02-12

## Authoritative Entrypoints

- CLI entry: `pipeline/main.py`
- Engine orchestration: `src/pipeline/engine.py`
- Pipeline registration catalog: `src/pipeline/registration.py`
- Registry runtime map: `src/core/registry.py`
- Config loading + validation: `src/core/config.py`

## Non-Authoritative / Legacy Surfaces

- `cli/main.py`: compatibility-only, not canonical for new integration work
- `src/models/ensemble.py`: legacy helper, not part of canonical sport/stat engine path
- `ingest/*`: standalone utilities, not onboarding authority

New integration work should route through:
- `pipeline/main.py`
- `src/pipeline/engine.py`
- `src/pipeline/registration.py`

## Engine Lifecycle (Fixed Stage Order)

`run_pipeline(...)` and `run_pipeline_with_overrides(...)` in `src/pipeline/engine.py` execute:
1. `load_inputs(config)`
2. `build_training_frame(inputs, config)`
3. `train_or_load_model(training_frame, config, retrain)`
4. `predict_lines(inputs, model_bundle, config)`
5. `simulate(predictions, model_bundle, config)`

## Adapter Pattern (Current State)

Current adapters are simulate-centric (compatibility pattern):
- MLB: `src/mlb/pitcher_props/adapter.py`
- NFL: `src/nfl/pass_attempts/pipeline.py`
- NHL: `src/nhl/shots_on_goal/pipeline.py`

Business logic remains delegated into sport orchestration modules via `simulate(...)`:
- MLB: `src/mlb/pitcher_props/pipeline.py`
- NFL: `src/nfl/pipeline.py`
- NHL: `src/nhl/pipeline.py`

This pattern is intentionally allowed and test-enforced; new sports should document any deviation explicitly.

## Module Relationship Map

Core:
- `src/core/contracts.py`: protocols/dataclasses
- `src/core/config.py`: sectioned config identity + typed runtime-critical validation
- `src/core/registry.py`: registration and lookup
- `src/core/simulation.py`: shared simulation primitives

Engine:
- `src/pipeline/engine.py`: orchestration + override routing
- `src/pipeline/registration.py`: default sport/stat catalog + idempotent bootstrap

Sports:
- MLB: `src/mlb/*`
- NFL: `src/nfl/*`
- NHL: `src/nhl/*`

## NHL Runtime Topology

Data layer:
- `src/nhl/data/moneypuck_ingest.py`: normalize raw snapshot + build curated cache
- `src/nhl/data/providers/*`: curated-cache provider abstraction
- `src/nhl/data/shot_snapshot.py`: shot-level -> skater-game snapshot builder utilities

Feature/model layer:
- `src/nhl/features/shots_on_goal.py`: leakage-safe training + inference features
- `src/nhl/models/predict.py`: train/load/predict + feature schema hash compatibility
- `src/nhl/models/bootstrap.py`: residual bootstrap sampler

Orchestration:
- `src/nhl/pipeline.py`: provider load/refresh + model lifecycle + simulation outputs

## Stability Invariants

- Existing output schema columns for MLB/NFL are backward-compatible commitments.
- NHL output columns are contract-stable for current `shots_on_goal` pipeline.
- Model artifact compatibility is schema-hash based where implemented (NHL today).
- Tests are offline by default; network behavior must be optional and guarded.

Testing strategy baseline:
- Intent matrix: `docs/testing-intent-matrix.md`
- Cross-sport parity suite: `tests/contracts/test_pipeline_output_contracts.py`
- Shared invariant helpers: `tests/helpers/assertions.py`

## Operational Footguns

- Wrong entrypoint (`cli/main.py`) causes confusion and non-canonical behavior.
- Empty-diff reviews often indicate wrong branch/path context.
- `.worktrees/` appearing in status is normal local noise; do not stage unless explicitly requested.

## Fantasy Phase 0 Architecture

Phase 0 introduces a reusable fantasy core that is separate from current
`pipeline/main.py` runtime behavior.

Modules:
- `src/fantasy/core/contracts.py`: projection/market dataclasses + adapter protocols
- `src/fantasy/core/config.py`: unified config loader for fantasy market surfaces
- `src/fantasy/core/validation.py`: strict wiring validation + soft mode-config shape checks
- `src/fantasy/core/derived.py`: dependency checks + no-op derivation scaffolding
- `src/fantasy/core/mapping.py`: provider-player mapping resolver
- `src/fantasy/core/registry.py`: adapter registries for projection/derived/market/export

Canonical flow:
1. projection
2. derived metric
3. market transform
4. export

The flow is horizon-aware and uses a single schema for season-long fantasy,
short-slate fantasy, single-game pick'em, and season-long stat pick'em.

## Fantasy Phase 1 Architecture

Phase 1 adds an MLB season projection adapter package while preserving the
separation from canonical `pipeline/main.py` runtime behavior.

Modules:
- `src/fantasy/adapters/mlb/projection_adapter.py`: season-horizon projection adapter by metric
- `src/fantasy/adapters/mlb/features.py`: adapter feature assembly + fallback normalization
- `src/fantasy/adapters/mlb/uncertainty.py`: empirical quantile/stddev summaries + availability confidence
- `src/fantasy/adapters/mlb/registration.py`: idempotent registration helper for all Phase 1 metrics

Modeling path:
1. load reusable batter-game frame from configured local dataset
2. build leakage-safe training/inference slices by date cutoffs
3. reuse `src/mlb/models/trainers.py` (`fit_estimator`, `predict_estimator`)
4. emit neutral projection rows with uncertainty and provenance fields

## Fantasy Phase 1.5 Architecture

Phase 1.5 extends MLB adapter quality controls while preserving the same registry
surface and output schema.

Modules:
- `src/fantasy/adapters/mlb/datasets.py`: player-season anchor snapshots and rest-of-season labels
- `src/fantasy/adapters/mlb/feature_engineering.py`: shifted rolling, playing-time stability, and leakage guards
- `src/fantasy/adapters/mlb/priors.py`: pybaseball prior-table cache load and deterministic fallback joins
- `src/fantasy/adapters/mlb/backtest.py`: walk-forward fold generation and MAE/RMSE/bias aggregation

Runtime semantics:
1. `hits` and `plate_appearances` use cleaned regular-season batter-game inputs and snapshot/rest-of-season labels
2. `hit_rate` is derived from constrained count outputs (`hits`, `plate_appearances`)
3. uncertainty is sampled from count residual banks and transformed to bounded rate intervals with configurable residual scaling and sparse-bucket fallback
4. output contract remains stable (`p10/p50/p90/stddev` + provenance fields)
5. optional model-family selection can switch off default model only when MAE gain clears configured anti-churn threshold
