# PR#6 Plan: Pipeline Registration + Discovery Standardization

Status: Implemented

## Summary

Implement PR#6 by standardizing how sport/stat pipelines are declared, registered, and discovered.
The objective is to make onboarding a new sport/stat (for example PR#8 NHL) mostly declarative wiring plus a pipeline class, while preserving current MLB/NFL behavior.

This PR centralizes default registration metadata, removes ad-hoc inline engine registration logic, and adds explicit discovery/onboarding hooks with test coverage.

## Public API / Interface / Type Changes

1. `src/core/registry.py`

- Keep existing functions/signatures:
  - `register_pipeline(sport, stat, factory)`
  - `get_pipeline(sport, stat)`
  - `clear_registry()`
- Add non-breaking discovery helpers:
  - `list_registered_pipelines() -> tuple[RegisteredPipeline, ...]`
  - `is_registered(sport, stat) -> bool`
- Add typed registration metadata shape:
  - `RegisteredPipeline` containing normalized `sport`, `stat`, and `factory`.

2. New module: `src/pipeline/registration.py`

- Add canonical default registration catalog for current implemented pipelines.
- Add bootstrap entrypoint:
  - `ensure_default_pipeline_registrations()`
- Requirements:
  - idempotent,
  - single source of truth for default sport/stat registration set,
  - no behavior regressions for current engine flows.

3. `src/pipeline/engine.py`

- Replace inline default registration mapping with call into centralized bootstrap.
- Keep `run_pipeline(...)` and `run_pipeline_with_overrides(...)` signatures unchanged.
- Preserve `UnknownPipelineError` behavior for unregistered pairs.

4. Optional onboarding validation hook

- Add lightweight validator for registration declarations used by tests/bootstrap checks:
  - factory is callable,
  - instantiated object exposes required pipeline stage methods,
  - sectioned config path expectations for declared pairs are test-validated.

## Validation/Behavior Contract for PR#6

1. Registration source of truth

- Current default pairs are declared exactly once in the registration catalog:
  - `mlb.strikeouts`
  - `mlb.outs_recorded`
  - `mlb.earned_runs`
  - `mlb.hits_allowed`
  - `mlb.bb_allowed`
  - `nfl.pass_attempts`

2. Discovery behavior

- `list_registered_pipelines()` returns normalized, stable entries derived from live registry state.
- `is_registered(...)` resolves case/whitespace through existing normalization semantics.

3. Bootstrap behavior

- Repeated calls to `ensure_default_pipeline_registrations()` do not change outcomes or throw.
- Engine still ensures defaults before resolving pipelines.

4. Compatibility and PR#5 alignment

- Sectioned schema requirements from PR#5 remain unchanged.
- Unknown sport/stat with valid config section still fails at registry lookup if not registered.
- No runtime output schema changes.

## Detailed Implementation Plan

1. RED: add failing tests first

- Extend `tests/test_core_registry.py` and `tests/test_engine_contract_enforcement.py` (or add `tests/test_pipeline_registration.py`) with tests for:
  - centralized bootstrap registers expected default pairs,
  - bootstrap idempotency,
  - discovery API correctness (`list_registered_pipelines`, `is_registered`),
  - engine path still calls default bootstrap abstraction,
  - unknown sport/stat error contract unchanged,
  - minimal dummy sport/stat onboarding in tests with tiny fake pipeline class requiring minimal boilerplate.
- Confirm new tests fail before production edits.

2. GREEN: implement registry/discovery standardization

- In `src/core/registry.py`:
  - add `RegisteredPipeline` metadata type,
  - add `list_registered_pipelines()` and `is_registered(...)`.
- In `src/pipeline/registration.py`:
  - add default registration catalog,
  - add idempotent `ensure_default_pipeline_registrations()`.
- In `src/pipeline/engine.py`:
  - route default registration through the new bootstrap entrypoint,
  - preserve existing public function signatures.

3. Refactor and harden

- Remove duplicate hardcoded default registration lists.
- Keep import direction clean to avoid circular dependencies.
- Keep changes surgical; avoid unrelated refactors.

4. Docs updates

- `docs/architecture.md`:
  - document centralized registration bootstrap flow.
- `docs/new-sport-playbook.md`:
  - add explicit onboarding steps for adding a new sport/stat via catalog + tests.
- `docs/plans/planned/nhl-onboarding-sequenced-pr-plan.md`:
  - align PR#6 wording with centralized registry/discovery implementation.

## Test Cases and Scenarios

1. Registry core

- register/get roundtrip with normalization.
- discovery list includes all defaults after bootstrap.
- `is_registered` true/false cases.
- `clear_registry` still works for tests.

2. Bootstrap

- first bootstrap call registers expected default set.
- second bootstrap call is no-op and stable.

3. Engine integration

- stage order and handoff tests remain green.
- override flow remains unchanged.
- unknown pair still raises `UnknownPipelineError`.

4. Onboarding ergonomics

- dummy sport/stat registration in tests requires minimal code and passes engine contract checks.

## Verification Commands (for implementation PR)

1. RED evidence

- `.venv/bin/pytest -q tests/test_core_registry.py tests/test_engine_contract_enforcement.py` (and new registration test module) before implementation.

2. GREEN evidence

- same targeted test command after implementation.

3. Repo gates

- `.venv/bin/ruff check .`
- `.venv/bin/pytest -q`

## Assumptions and Defaults

1. PR#5 sectioned-only config validation is baseline and remains unchanged.
2. Registry/discovery remains explicit and code-driven (no dynamic filesystem module scanning).
3. Engine/CLI public signatures remain unchanged in PR#6.
4. Unknown sport/stat remains a registry lookup error unless explicitly registered.
5. No changes under `data/`, `models/`, `notebooks/`, `betslips/`.
