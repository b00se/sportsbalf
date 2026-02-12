# PR#3 Execution Plan: Shared Simulation Interface Extraction (No Output Changes)

Status: Implemented

Date: 2026-02-08

## Summary
Extract Monte Carlo simulation primitives from MLB-specific modules into a sport-agnostic core interface, then switch NFL to use the shared core module directly while preserving backward compatibility for existing MLB imports. This removes the current NFL->MLB simulation dependency without changing MLB/NFL output schemas or runtime behavior.

## Scope and Non-Goals
### In scope
- Add a new shared simulation module at `src/core/simulation.py`
- Keep `src/mlb/models/monte_carlo.py` as thin compatibility wrappers/re-exports
- Update NFL orchestration imports to consume the shared core interface
- Add/adjust tests for shared behavior and wrapper compatibility
- Update architecture/contracts docs to reflect the new simulation boundary

### Out of scope
- No NFL alias cleanup (`ud_line -> k_line`, `qb_id -> pitcher_id`) in this PR (deferred to PR#4)
- No `SportStatPipeline` protocol changes
- No registry/discovery/config-schema hardening changes (deferred to PR#5/PR#6)
- No MLB/NFL output schema changes

## Public API / Interface / Type Changes
### Runtime/public code interfaces
- New shared module exports in `src/core/simulation.py`:
  - `MonteCarloConfig`
  - `simulate_row(...)`
  - `apply_simulations(...)`

### Compatibility interfaces preserved
- `src/mlb/models/monte_carlo.py` remains import-compatible and continues exporting:
  - `MonteCarloConfig`
  - `simulate_row(...)`
  - `apply_simulations(...)`

### Internal import boundary changes
- `src/nfl/pipeline.py` imports simulation helpers from `src/core/simulation.py` instead of `src/mlb/models/monte_carlo.py`

## Detailed Implementation Plan

### 1) Extract shared simulation module
1. Create `src/core/simulation.py`.
2. Move generic simulation logic from `src/mlb/models/monte_carlo.py` into the new module.
3. Preserve behavior and output fields exactly:
   - probabilities: `prob_over`, `prob_under`, `prob_push`
   - EV/edge: `ev_over`, `ev_under`, `edge_over`, `edge_under`
   - summary stats: `simulated_mean`, `simulated_std`, `simulated_median`
4. Preserve deterministic RNG behavior (`np.random.default_rng(config.random_seed)`).

### 2) Preserve MLB compatibility wrappers
1. Refactor `src/mlb/models/monte_carlo.py` to thin wrappers/re-exports from `src/core/simulation.py`.
2. Keep symbol names and import path stable to avoid breaking existing MLB callsites and tests.
3. Keep wrapper layer side-effect free and minimal.

### 3) Decouple NFL from MLB simulation internals
1. Update import in `src/nfl/pipeline.py` to:
   - `from src.core.simulation import MonteCarloConfig, apply_simulations`
2. Do not alter NFL output mapping logic in this PR.
3. Do not alter NFL line/id aliasing logic in this PR.

### 4) Add tests (TDD-compatible)
1. RED: add targeted tests and run until failures are meaningful.
2. GREEN: implement shared module + wrappers until tests pass.
3. Required scenarios:
   - `simulate_row` keys and NaN-mean behavior
   - sampler path is used when provided
   - `apply_simulations` accepts `std_dev` column-name input
   - MLB wrapper compatibility (`src.mlb.models.monte_carlo` exports expected symbols and remains behavior-equivalent)
   - NFL offline integration path still returns expected schema columns (`predicted_pass_attempts`, `attempts_line`, probability fields)

### 5) Update docs
1. Update `docs/architecture.md` to reflect the new shared simulation boundary and remove the cross-sport simulation debt note.
2. Update `docs/contracts.md` with a short PR#3 note indicating shared simulation primitives now live in `src/core/simulation.py` and MLB wrapper compatibility is retained.

## Acceptance Criteria (Decision-Complete)
1. `src/nfl/pipeline.py` no longer imports `src/mlb/models/monte_carlo.py`.
2. Shared simulation interface exists in `src/core/simulation.py` and is used by NFL.
3. `src/mlb/models/monte_carlo.py` remains backward-compatible via wrappers/re-exports.
4. MLB and NFL output schemas remain unchanged in existing integration tests.
5. Lint and full offline tests pass.

## Verification and Review Checklist
1. Targeted tests:
   - `.venv/bin/pytest -q tests/test_core_simulation.py`
   - `.venv/bin/pytest -q tests/test_monte_carlo.py`
   - `.venv/bin/pytest -q tests/test_qb_pipeline.py tests/integration/test_nfl_pass_attempts_pipeline.py`
2. Boundary verification:
   - `rg -n "from src\\.mlb\\.models\\.monte_carlo" src/nfl -S`
   - expected: no matches
3. Repo gates:
   - `.venv/bin/ruff check .`
   - `.venv/bin/pytest -q`
4. Review focus:
   - no behavior drift from extraction
   - wrapper compatibility preserved
   - PR scope does not expand into PR#4 cleanup work

## Assumptions and Defaults
1. PR#2 guardrails are already on `main` and remain unchanged.
2. Shared simulation ownership is `src/core` for cross-sport clarity.
3. No static import-gate test is added in PR#3; boundary is verified through code review plus verification command.
4. MLB/NFL runtime behavior and output columns are backward-compatible commitments for this PR.
