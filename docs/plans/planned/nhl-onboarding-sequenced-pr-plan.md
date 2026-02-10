# NHL Onboarding Readiness Program: Sequenced PR Plan (Shots on Goal, Small-PR Track)

Status: Planned (Partially Implemented)

## Summary
Prepare the repo for smooth in-season NHL onboarding by first removing architecture bottlenecks (contract drift, cross-sport coupling, shallow config validation, legacy ambiguity), then adding reusable sport-agnostic scaffolding, and finally shipping an NHL `shots_on_goal` pipeline skeleton with offline-safe tests.

Sequence is optimized for low-risk integration: each PR has explicit acceptance gates and leaves the system runnable.

## Progress Tracker
- PR 1: Implemented and merged on 2026-02-08 (`#11`)
- PR 2: Planned (this branch target: `feature/nhl-pr2-contract-enforcement`)
- PR 3-PR 10: Planned

## PR Sequence

### PR 1: Architecture Baseline + Canonical Docs
- Scope: add `docs/architecture.md`, `docs/contracts.md`, `docs/new-sport-playbook.md`, `docs/config-schema.md`; relabel existing `docs/plans/*` as planned vs implemented.
- Purpose: create single source of truth before code refactors.
- Acceptance: docs map current reality (including adapter pass-through behavior), include sport/stat lifecycle, output contract fields, and NHL onboarding checklist.
- Dependency: none.

### PR 2: Contract Enforcement Tests (No Behavior Change)
- Scope: add tests that assert `SportStatPipeline` stage expectations and engine flow invariants; document temporary adapter exceptions explicitly.
- Purpose: prevent further drift while refactoring.
- Acceptance: tests fail on silent no-op stage regressions unless explicitly marked temporary.
- Dependency: PR 1 docs terminology.

### PR 3: Shared Simulation Interface Extraction
- Scope: introduce sport-agnostic simulation module under `src/core` or `src/shared` and move generic Monte Carlo config/outputs there; keep backward-compatible wrappers.
- Purpose: remove NFL reliance on MLB simulation internals.
- Acceptance: no cross-import from `src/nfl/*` to `src/mlb/*` for shared simulation path; MLB/NFL outputs unchanged.
- Dependency: PR 2 guardrails.

### PR 4: NFL Decoupling Cleanup
- Scope: update NFL pipeline to use shared simulation types directly; remove alias shims like `ud_line -> k_line` and `qb_id -> pitcher_id` in NFL orchestration.
- Purpose: eliminate leaky naming debt before adding NHL.
- Acceptance: NFL integration tests still pass; schema remains stable (`predicted_pass_attempts`, `attempts_line`, probabilities/EV fields).
- Dependency: PR 3 shared interface.

### PR 5: Config Schema Validation Hardening
- Scope: enforce sectioned-only root schema (`pipeline.sport`, `pipeline.stat`, `{sport}.{stat}`), add typed runtime-critical validation for implemented sport/stats, and add migration/rejection tests for legacy flat config shape.
- Purpose: fail fast at config load time instead of runtime.
- Acceptance: invalid configs fail with specific field-path errors; current `config/mlb.yaml` and `config/nfl.yaml` validate cleanly.
- Dependency: PR 1 config schema doc.

### PR 6: Pipeline Registration + Discovery Standardization
- Scope: centralize default sport/stat declarations in `src/pipeline/registration.py`, formalize idempotent registry bootstrap, and add explicit onboarding/discovery hooks for new sports/stats (factory registration, required stage map, config section checks).
- Purpose: make adding NHL mostly configuration + module wiring.
- Acceptance: adding a dummy sport/stat in tests requires minimal boilerplate and passes registry/engine contract tests.
- Dependency: PRs 2 and 5.

### PR 7: Legacy Surface Area Clarification
- Scope: add explicit non-authoritative status markers and docs clarifications for legacy modules (`src/models/ensemble.py`, root `ingest/`, `cli/main.py`) while keeping runtime behavior unchanged.
- Purpose: reduce ambiguity and onboarding mistakes.
- Acceptance: contributors can identify authoritative pipeline entrypoints unambiguously (`pipeline/main.py`, `src/pipeline/engine.py`).
- Dependency: PR 1 docs.

### PR 8: NHL Skeleton (Shots on Goal)
- Scope: add `src/nhl/` package, `src/nhl/shots_on_goal/pipeline.py` adapter, `src/nhl/pipeline.py` orchestrator stub, `config/nhl.yaml`, registry wiring.
- Purpose: create production-shaped NHL path without full model complexity yet.
- Acceptance: offline integration test runs engine with NHL override and returns stable output schema (even with fallback/default inference mode).
- Dependency: PRs 3, 5, 6.

### PR 9: NHL Data/Feature Provider Abstraction (In-Season Ready Base)
- Scope: implement NHL provider interface modeled after NFL provider pattern; add guarded network ingestion + offline fallback fixtures; build minimal feature set for shots-on-goal inference.
- Purpose: support in-season operation with graceful degradation.
- Acceptance: offline tests deterministic; online path optional and failure-tolerant; no test requires network.
- Dependency: PR 8.

### PR 10: NHL Model + Simulation MVP
- Scope: train/load path for NHL shots-on-goal model, residual/error handling, simulation outputs aligned with shared contract, plus slip-ready columns.
- Purpose: complete first end-to-end NHL stat pipeline.
- Acceptance: integration test validates output columns, deterministic seed behavior, and model compatibility/retrain behavior.
- Dependency: PR 9.

## Important Public Interfaces and Type Changes
- `SportStatPipeline` remains the top-level protocol, but stage behavior becomes test-enforced.
- New shared simulation contract (config + result schema) becomes the cross-sport API surface.
- Config validation evolves from permissive mapping checks to required-key typed schema checks per `sport.stat`.
- Registry API stays stable, with stricter onboarding-time validation and clearer factory expectations.
- Output schema policy: keep existing MLB/NFL columns stable; NHL adopts shared probability/EV conventions from day one.

## Test Plan and Scenarios
- Contract tests: stage sequencing, required handoff artifacts, adapter exception markers.
- Schema tests: config required/optional keys, defaults, type validation, legacy migration behavior.
- Integration tests:
  - MLB strikeouts unchanged schema.
  - NFL pass attempts unchanged schema with decoupled simulation internals.
  - NHL shots_on_goal pipeline returns stable schema in offline mode.
- Failure-mode tests:
  - Missing provider/network failure falls back safely.
  - Missing optional live context uses neutral defaults.
  - Incompatible saved model triggers retrain path.
- Determinism tests: Monte Carlo seed reproducibility across sports.
- Repo health gates per PR: `.venv/bin/pytest -q` and `.venv/bin/ruff check .`.

## Assumptions and Defaults
- Chosen rollout: 10 small PRs.
- First NHL stat target: `shots_on_goal`.
- Existing MLB/NFL user-facing outputs stay backward compatible during refactor.
- No changes to `data/`, `models/`, `notebooks/`, `betslips/` as part of code PRs except transient runtime artifacts.
- Tests remain offline-only; network-enabled behavior is optional and guarded.
- CLI authority remains `pipeline/main.py`; `cli/main.py` stays non-primary unless promoted in a dedicated PR.
