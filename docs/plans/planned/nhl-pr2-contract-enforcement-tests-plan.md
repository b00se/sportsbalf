# PR#2 Execution Plan: Contract Enforcement Tests (No Behavior Change)

Status: Planned

Date: 2026-02-08

## Summary
Add a focused contract-enforcement test suite that codifies engine stage-order invariants and explicitly enforces temporary simulate-only adapter exceptions (MLB/NFL) via a test allowlist. This PR introduces guardrails to prevent silent contract drift during refactors and makes no runtime behavior changes.

## Scope and Non-Goals
### In scope
- Add tests for strict engine stage sequencing and stage-handoff invariants
- Add tests that detect simulate-only adapter behavior and require explicit allowlisting
- Add a small `docs/contracts.md` note tying temporary adapter exceptions to enforcement tests
- Validate with `.venv/bin/ruff check .` and `.venv/bin/pytest -q`

### Out of scope
- No runtime code behavior changes under `src/`
- No contract/protocol method signature changes in `src/core/contracts.py`
- No registry/discovery refactors (covered in later PRs)

## Public API / Interface / Type Changes
### Runtime/public code interfaces
- None

### Test interfaces introduced
- New enforcement module: `tests/test_engine_contract_enforcement.py`
- Temporary exception allowlist constant of fully qualified adapter class names for simulate-only behavior

### Documentation interfaces updated
- `docs/contracts.md` gains a short PR#2 note that temporary simulate-only adapters are test-enforced via explicit allowlist

## Detailed Implementation Plan

### 1) Add engine sequencing + handoff invariant tests
Create `tests/test_engine_contract_enforcement.py` with the following tests:

1. `test_engine_runs_stages_in_strict_order`
   - Use a fake pipeline implementation that records stage call order.
   - Monkeypatch `src.pipeline.engine.load_pipeline_config` and `src.pipeline.engine.get_pipeline`.
   - Execute `run_pipeline(...)`.
   - Assert exact ordered stage calls:
     1. `load_inputs`
     2. `build_training_frame`
     3. `train_or_load_model`
     4. `predict_lines`
     5. `simulate`
   - Assert engine returns the `simulate(...)` output unmodified.

2. `test_engine_handoff_artifacts_between_stages`
   - Use the same fake pipeline, with assertions in each stage that it receives the expected artifact type and prior-stage object identity where relevant.
   - Assert required handoff types:
     - `build_training_frame` gets `PipelineInputs`
     - `train_or_load_model` gets a `pd.DataFrame`
     - `predict_lines` gets `PipelineInputs` + `ModelBundle`
     - `simulate` gets prediction `pd.DataFrame` + `ModelBundle`

3. `test_run_pipeline_with_overrides_passes_cli_overrides`
   - Monkeypatch `load_pipeline_config` to capture arguments.
   - Execute `run_pipeline_with_overrides(..., sport=..., stat=...)`.
   - Assert `sport_override` and `stat_override` are passed exactly.

4. `test_default_registrations_match_expected_pairs`
   - Monkeypatch `register_pipeline` in engine module to capture calls.
   - Execute `_ensure_default_registrations()`.
   - Assert exact set contains:
     - `mlb.strikeouts`
     - `mlb.outs_recorded`
     - `mlb.earned_runs`
     - `mlb.hits_allowed`
     - `mlb.bb_allowed`
     - `nfl.pass_attempts`

### 2) Add explicit simulate-only exception enforcement tests
In the same module:

1. Define allowlist constant:
   - `src.mlb.pitcher_props.adapter.MlbPitcherPropsPipeline`
   - `src.nfl.pass_attempts.pipeline.NflPassAttemptsPipeline`

2. Add helper that classifies a registered adapter as simulate-only compatibility pattern by running first four stages with minimal typed artifacts and checking:
   - empty frame from `build_training_frame(...)`
   - empty `ModelBundle(payload={})` from `train_or_load_model(...)`
   - empty frame from `predict_lines(...)`

3. `test_simulate_only_adapters_must_be_explicitly_allowlisted`
   - Capture current default registrations from `_ensure_default_registrations()`.
   - For each registered adapter class, run classifier.
   - If simulate-only pattern is detected, require fully qualified class name in allowlist.
   - This is the PR#2 regression gate: new silent no-op adapters fail unless intentionally marked temporary.

4. `test_allowlist_entries_must_exist_in_default_registrations`
   - Prevent stale allowlist drift by asserting each allowlist entry appears in active default registrations.

### 3) Add minimal contracts documentation note
Update `docs/contracts.md` with a short section:
- PR#2 test-enforcement baseline
- Temporary simulate-only adapter behavior is explicitly allowlisted in tests
- Any new simulate-only adapter must be intentionally declared, not introduced silently

### 4) Verification flow (TDD-compatible)
1. RED: add tests and run targeted module until failures are meaningful
   - `.venv/bin/pytest -q tests/test_engine_contract_enforcement.py`
2. GREEN: finalize test logic and docs note
3. Repo gates:
   - `.venv/bin/ruff check .`
   - `.venv/bin/pytest -q`

## Acceptance Criteria (Decision-Complete)
1. Engine lifecycle invariants are codified by tests:
   - strict stage order
   - stage handoff artifact expectations
   - override plumbing invariants
2. Temporary adapter exceptions are explicit and enforced:
   - simulate-only compatibility adapters are allowlisted by fully qualified class name
   - unallowlisted simulate-only adapters fail tests
3. Docs explicitly call out this PR#2 enforcement mechanism in `docs/contracts.md`
4. No runtime behavior changes under `src/`

## Verification and Review Checklist
1. Targeted tests pass:
   - `tests/test_engine_contract_enforcement.py`
2. Full repo checks pass:
   - `.venv/bin/ruff check .`
   - `.venv/bin/pytest -q`
3. Review focus:
   - tests enforce present behavior without introducing functional refactors
   - exception policy is explicit and intentionally narrow

## Assumptions and Defaults
1. MLB and NFL adapters remain temporary simulate-only compatibility patterns for now.
2. Enforcement remains test-level in PR#2 (runtime enforcement deferred).
3. Fully qualified class names are used for allowlist clarity and failure diagnostics.
4. No network dependency is introduced for PR#2 tests.
