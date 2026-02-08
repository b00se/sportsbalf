# PR#4 Execution Plan: NFL Decoupling Cleanup via Strict Neutral Simulation API

Status: Approved

Date: 2026-02-08

Source: PR#4 scope from `docs/plans/planned/nhl-onboarding-sequenced-pr-plan.md`

## Summary
Execute PR#4 by removing NFL orchestration alias shims (`ud_line -> k_line`, `qb_id -> pitcher_id`) and standardizing simulation/sampler interfaces to a strict sport-neutral ID contract. This plan intentionally applies the neutral API to both NFL and MLB samplers/call sites in the same PR so repo behavior remains green while eliminating naming debt.

## Scope and Non-Goals
### In scope
- Remove NFL simulation alias columns from `src/nfl/pipeline.py`.
- Make shared simulation interfaces neutral (no sport-specific ID keyword names).
- Update NFL and MLB residual sampler signatures and simulation call sites to the neutral ID keyword.
- Update tests to enforce no NFL alias shim behavior and strict neutral sampler usage.
- Keep NFL/MLB output schemas stable.

### Out of scope
- No registry/config-schema hardening (PR#5+).
- No NHL module additions (PR#8+).
- No changes to `SportStatPipeline` protocol.
- No changes to output column names (`predicted_pass_attempts`, `attempts_line`, `prob_*`, `ev_*`, `edge_*`, MLB equivalents).

## Public API / Interface / Type Changes
### Shared simulation API (`src/core/simulation.py`)
- Update `CountSampler` protocol to neutral IDs:
  - `sample_counts(mean, entity_id, simulations, rng)`.
- Update `simulate_row(...)`:
  - Replace sport-specific ID arg with neutral `entity_id`.
- Update `apply_simulations(...)`:
  - Add/standardize `id_col` input for row-level entity ID lookup.
  - Keep `line_col` explicit at call sites; do not rely on NFL shims.
  - Pass neutral `entity_id` to `simulate_row` and sampler.

### NFL sampler API (`src/nfl/models/bootstrap.py`)
- `can_bootstrap(...)` accepts neutral `entity_id` only.
- `sample_counts(...)` accepts neutral `entity_id` only.
- Remove `pitcher_id` alias kwargs from NFL bootstrapper methods.

### MLB sampler API (`src/mlb/models/distributions.py`)
- Update `sample_counts(...)` to accept neutral `entity_id` (internally mapped to pitcher pool lookup).
- Update any sampler call sites accordingly.

## Detailed Implementation Plan
### 1) RED: add failing tests that define PR#4 behavior
1. Add/update shared simulation tests in `tests/test_core_simulation.py`:
- `apply_simulations` uses `line_col="ud_line"` and `id_col="qb_id"` directly.
- Sampler receives neutral `entity_id`.
- No requirement for `pitcher_id` column.

2. Update NFL bootstrap tests in `tests/test_qb_bootstrap.py`:
- Replace pitcher-alias acceptance test with strict neutral-ID tests.
- Add regression that old alias kwargs are rejected (or no longer used).

3. Update NFL pipeline tests:
- `tests/test_qb_pipeline.py`
- `tests/integration/test_nfl_pass_attempts_pipeline.py`
- Assert stable schema includes `predicted_pass_attempts`, `attempts_line`, probability/EV fields.
- Assert alias shim columns are absent from final result (`k_line`, `pitcher_id`).

4. Update Monte Carlo compatibility tests in `tests/test_monte_carlo.py` to neutral API usage while preserving MLB module import-path compatibility.

Run targeted tests and capture expected failures before code changes.

### 2) GREEN: implement neutral simulation interface and remove NFL shims
1. Edit `src/core/simulation.py`:
- Apply neutral `entity_id` protocol/signature changes.
- Wire `apply_simulations(..., id_col=...)` to pass row ID via neutral path.

2. Edit `src/nfl/models/bootstrap.py`:
- Remove pitcher alias params.
- Use neutral `entity_id` in pooling/sampling logic.

3. Edit `src/mlb/models/distributions.py`:
- Accept neutral `entity_id` and map to pitcher residual key behavior.

4. Edit `src/nfl/pipeline.py`:
- Remove:
  - `sim_input["k_line"] = sim_input["ud_line"]`
  - `sim_input["pitcher_id"] = sim_input["qb_id"]`
  - cleanup drop of `k_line`/`pitcher_id`.
- Call `apply_simulations(...)` with:
  - `line_col="ud_line"`
  - `id_col="qb_id"`
  - existing `mean_col="prediction"` and `std_dev="simulation_sigma"`.

5. Edit MLB call site in `src/mlb/pitcher_props/pipeline.py`:
- Pass explicit `id_col="pitcher_id"` to neutralized `apply_simulations`.
- Preserve current line-column behavior via `descriptor.line_col`.

6. Keep `src/mlb/models/monte_carlo.py` as shared-core re-export module (no path break).

### 3) Docs updates
1. Update `docs/architecture.md`:
- Remove/adjust NFL alias-cleanup debt note after PR#4 implementation.
- Reflect neutral shared simulation ID interface.

2. Update `docs/contracts.md`:
- Add PR#4 note: NFL no longer uses line/ID alias shim in orchestration.
- Confirm output-schema stability commitment unchanged.

## Acceptance Criteria
1. `src/nfl/pipeline.py` contains no alias-shim creation for `k_line` or `pitcher_id`.
2. Shared simulation path supports explicit neutral `id_col` and is used by NFL (`qb_id`) and MLB (`pitcher_id`) call sites.
3. NFL bootstrapper no longer accepts pitcher alias kwargs.
4. NFL integration outputs remain stable (`predicted_pass_attempts`, `attempts_line`, `prob_*`, `ev_*`, `edge_*`).
5. MLB and NFL offline tests pass without schema regressions.
6. Lint and full test suite pass.

## Verification Checklist
1. Targeted tests:
- `.venv/bin/pytest -q tests/test_core_simulation.py`
- `.venv/bin/pytest -q tests/test_monte_carlo.py`
- `.venv/bin/pytest -q tests/test_qb_bootstrap.py`
- `.venv/bin/pytest -q tests/test_qb_pipeline.py tests/integration/test_nfl_pass_attempts_pipeline.py`

2. Boundary/alias checks:
- `rg -n "k_line|pitcher_id" src/nfl/pipeline.py -S`
- expected: no NFL simulation-shim assignment/drop pattern remains.
- `rg -n "pitcher_id\\s*=" src/nfl/models/bootstrap.py -S`
- expected: no alias kwargs in method signatures.

3. Repo gates:
- `.venv/bin/ruff check .`
- `.venv/bin/pytest -q`

## Assumptions and Defaults
1. Chosen approach: strict neutral API (no backward-compat alias kwargs in samplers).
2. Chosen impact policy: include MLB sampler/call-site updates in this PR to keep repo green.
3. Output-column stability for MLB/NFL is required.
4. Tests remain offline-only and deterministic.
5. No modifications under `data/`, `models/`, `notebooks/`, `betslips/`.
