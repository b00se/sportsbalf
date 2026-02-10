# Architecture Baseline (Current Runtime Reality)

Status: Canonical (Current State)

Date: 2026-02-08

## Authoritative entrypoints
- CLI entry: `pipeline/main.py`
- Engine orchestration: `src/pipeline/engine.py`
- Sport/stat registry: `src/core/registry.py`
- Default registration bootstrap: `src/pipeline/registration.py`
- Config loading + identity resolution: `src/core/config.py`

## Legacy/non-authoritative modules
- `cli/main.py`: compatibility stub only; do not use for canonical pipeline integration.
- `src/models/ensemble.py`: legacy helper module; not part of the authoritative sport/stat engine path.
- `ingest/parse_ud_strikeouts.py`: standalone ingestion utility, retained for compatibility/utility usage.
- `ingest/park_factors.py`: standalone park-factor utility, retained for compatibility/utility usage.
- Canonical path for new integration work remains `pipeline/main.py` and `src/pipeline/engine.py`.

## Runtime lifecycle (actual engine order)
`run_pipeline(...)` and `run_pipeline_with_overrides(...)` in `src/pipeline/engine.py` run this fixed sequence:
1. `load_inputs(config)`
2. `build_training_frame(inputs, config)`
3. `train_or_load_model(training_frame, config, retrain)`
4. `predict_lines(inputs, model_bundle, config)`
5. `simulate(predictions, model_bundle, config)` and return final output

## Current adapter pass-through behavior
- MLB adapter `src/mlb/pitcher_props/adapter.py` and NFL adapter `src/nfl/pass_attempts/pipeline.py` implement the `SportStatPipeline` protocol.
- In both adapters, the first four contract stages are compatibility no-ops.
- The full sport workflow is delegated in `simulate(...)`:
  - MLB: `run_mlb_pitcher_prop_pipeline(...)` from `src/mlb/pitcher_props/pipeline.py`
  - NFL: `run_pass_attempts_pipeline(...)` from `src/nfl/pipeline.py`
- This is intentional compatibility behavior for current MLB/NFL integrations, not a long-term protocol guarantee for future sports.

## Module relationship map
- Core layer
  - `src/core/contracts.py`: shared protocol and typed dataclasses
  - `src/core/config.py`: config schema resolution (sectioned + legacy fallback)
  - `src/core/registry.py`: pipeline factory registration and lookup
  - `src/core/simulation.py`: shared Monte Carlo simulation primitives
- Engine layer
  - `src/pipeline/engine.py`: registration bootstrap + stage sequencing
  - `src/pipeline/registration.py`: canonical default sport/stat catalog and idempotent registration bootstrap
- Sport adapters
  - `src/mlb/pitcher_props/adapter.py`
  - `src/nfl/pass_attempts/pipeline.py`
- Sport orchestration modules
  - `src/mlb/pipeline.py`: MLB compatibility shim
  - `src/mlb/pitcher_props/pipeline.py`: shared MLB pitcher-prop orchestration
  - `src/nfl/pipeline.py`: NFL pass-attempts orchestration

## Known architecture debt
- Stage semantics are not yet enforced by dedicated contract tests (planned follow-up: PR#2 in NHL onboarding sequence).

## PR#4 simulation API update
- Shared simulation APIs in `src/core/simulation.py` now use sport-neutral naming (`line`, `entity_id`) with explicit line and ID column mapping at call sites.
- NFL pass-attempt orchestration no longer introduces `k_line`/`pitcher_id` alias columns before simulation.
