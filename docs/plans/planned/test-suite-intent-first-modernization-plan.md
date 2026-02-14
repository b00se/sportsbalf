# Test Suite Modernization Plan (Intent-First, Core-First, Phased Gates)

Status: Planned

Date: 2026-02-14

## Summary
Build a decision-complete test strategy that prioritizes behavioral intent and contract invariants over shape-only assertions, then systematically closes high-risk coverage gaps across MLB/NFL/NHL/Fantasy pipelines.

Rollout defaults:
- Priority: core intent first
- CI policy: phased quality gates (tighten over time)

## Current-State Audit (What We Found)

1. Strong areas
- Engine orchestration contract checks are solid (`tests/test_engine_contract_enforcement.py`).
- Config validation has good negative-path coverage (`tests/test_core_config.py`, `tests/fantasy/core/test_config_validation.py`).
- Recent MLB fantasy adapter regressions are now covered.

2. Spotty areas
- Several integration tests mostly verify schema/presence, not behavioral correctness (examples: `tests/integration/test_mlb_strikeouts_pipeline.py`, `tests/integration/test_nfl_pass_attempts_pipeline.py`).
- Some tests are smoke-like and under-assert intent (examples: `tests/test_data_loading.py`, `tests/test_nfl_slips.py`, `tests/test_qb_model.py`).
- Duplicate assertion patterns exist without added intent (notably core-vs-wrapper Monte Carlo checks in `tests/test_core_simulation.py` and `tests/test_monte_carlo.py`).
- Gaps in explicit direct coverage for key modules (heuristic): `src/core/model_selection.py`, `src/fantasy/core/validation.py`, `src/nhl/pipeline.py` behavior branches, and legacy `src/models/ensemble.py` (status/guard behavior).

3. Systemic gap
- No explicit intent matrix tying docs/contracts to tests, so regressions can pass while violating market/horizon semantics.

## Workstreams and Sequence

### Phase 1: Define Intent Contract Matrix
Create a test intent map as source of truth:
- For each sport/stat/horizon, specify:
  - semantic unit (per-game vs per-season)
  - leakage constraints
  - fallback behavior (missing lines/model/data/provider)
  - determinism guarantees (seeded paths)
  - output contract invariants (not just columns; numeric bounds/relationships)

Deliverable:
- `docs/testing-intent-matrix.md` with table keyed by:
  - `sport`, `stat`, `horizon`, `critical invariants`, `test file(s)`, `gap status`

Acceptance:
- Every shipped pipeline path and fantasy adapter key has at least one explicit invariant row in the matrix.

### Phase 2: Assertion Quality Refactor (Core Intent First)
Refactor existing tests from shape-only to invariant-first assertions in priority order.

1. Pipeline integration tests
- Upgrade to assert behavioral invariants:
  - probability algebra: `prob_over + prob_under + prob_push ~= 1`
  - EV/edge sign consistency with probability and prices
  - horizon scaling invariants (season totals not one-game scale)
  - deterministic repeatability under fixed seeds
- Targets:
  - `tests/integration/test_mlb_strikeouts_pipeline.py`
  - `tests/integration/test_nfl_pass_attempts_pipeline.py`
  - `tests/integration/test_mlb_outs_recorded_pipeline.py`
  - `tests/integration/test_nhl_shots_on_goal_pipeline.py`

2. Weak smoke tests
- Replace “not empty”/shape-only checks with scenario-specific expectations:
  - `tests/test_data_loading.py`
  - `tests/test_nfl_slips.py`
  - `tests/test_qb_model.py`

3. Monte Carlo duplication cleanup
- Keep wrapper-export checks in `tests/test_monte_carlo.py`.
- Move simulation behavior truth to one canonical suite (`tests/test_core_simulation.py`) and remove redundant behavior assertions from wrapper tests.

Acceptance:
- Each prioritized test file includes at least one business invariant that would fail on a plausible regression.

### Phase 3: Fill Module/Branch Gaps
Add focused unit tests for uncovered or weakly covered logic branches.

1. `src/core/model_selection.py`
- unknown metric rejection
- maximize/minimize tie-break behavior
- epsilon filtering boundaries
- empty leaderboard rejection

2. `src/fantasy/core/validation.py`
- mode/horizon normalization edge cases
- malformed definitions and transform params
- unresolved policy validation branches
- market mode mismatch plus definition-level failures

3. `src/nhl/pipeline.py`
- `_resolve_sigma_series` fallback hierarchy and clipping
- `_safe_read_inference_input` exception branches
- bootstrap on/off branch behavior
- training-season filtering/min-games cutoffs

4. `src/models/ensemble.py` (legacy guardrail)
- minimal status/contract test confirming legacy module remains non-authoritative and deterministic with fixed seed.

Acceptance:
- Each target module has explicit branch tests for failure and fallback paths (not only happy path).

### Phase 4: Cross-Module Contract Parity Tests
Add shared contract tests to ensure all current shipped pipelines uphold the same core semantics:
- consistent simulation field presence and numeric constraints
- run_mode/lines_status semantics across sports
- market horizon compatibility checks

Add file:
- `tests/contracts/test_pipeline_output_contracts.py` (parametrized by sport/stat fixtures)

Acceptance:
- Contract parity tests fail on any sport-specific drift in common required semantics.

### Phase 5: Phased CI Gates
Implement progressive enforcement.

1. Gate A (immediate)
- all tests pass
- lint pass
- no new shape-only integration tests (enforced via review checklist + helper assertions)

2. Gate B (after matrix complete)
- enforce critical intent tests for each matrix row (presence + pass)
- require regression test for every production bug fix

3. Gate C (later hardening)
- add coverage tooling (`pytest-cov`) and set thresholds:
  - per-critical-module threshold first
  - global threshold second

## Important Changes to Public APIs / Interfaces / Types
No production runtime API changes are planned.

Test-suite and process additions only:
1. New documentation contract
- `docs/testing-intent-matrix.md` (authoritative test-intent map)

2. New shared test helper interface (internal)
- `tests/helpers/assertions.py` with standardized assertions:
  - `assert_probability_columns_valid(...)`
  - `assert_simulation_contract(...)`
  - `assert_no_temporal_leakage(...)`
  - `assert_horizon_semantics(...)`

3. New cross-sport contract suite
- `tests/contracts/test_pipeline_output_contracts.py`

## Test Cases and Scenarios to Add (Concrete)
1. Horizon semantics
- Season adapters: per-game predictions must be scaled to season window.
- Non-season horizons: prevent unintended scaling.

2. Probability/EV consistency
- Probabilities finite, bounded, and summing to 1 within tolerance.
- EV/edge fields consistent with probabilities and prices.

3. Leakage resistance
- Future-row mutation should not alter earlier outputs.
- Same-day/order-sensitive grouping does not cross-contaminate entities.

4. Fallback robustness
- Missing lines, missing models, incompatible model artifacts, provider failures.
- All fallback paths produce contract-safe outputs.

5. Determinism
- Same seed/config/input yields equal outputs for deterministic paths.

6. Schema + semantic contract
- Required columns + semantic constraints for each sport/stat.

## Assumptions and Defaults
1. Chosen defaults
- Rollout order: core intent first.
- CI strategy: phased gates.

2. Implementation defaults
- Keep tests offline; no network dependence in tests.
- Prefer deterministic fixtures and explicit seeds.
- Prioritize regression tests for previously observed failures.
- Avoid changing production code unless needed to satisfy newly formalized intent tests.
