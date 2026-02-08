# PR#1 Execution Plan: Architecture Baseline + Canonical Docs (NHL Onboarding Program)

Status: Implemented

Date: 2026-02-08

Completion note (2026-02-08):
- Implemented and merged via PR #11:
  - https://github.com/b00se/sportsbalf/pull/11

## Summary
Create a canonical documentation baseline that reflects the current code reality before any refactors. This PR is docs-first, with no behavior changes. It introduces four authoritative docs and normalizes plan-status labeling under `docs/plans/`.

## Scope and Non-Goals
### In scope
- Add `docs/architecture.md`
- Add `docs/contracts.md`
- Add `docs/new-sport-playbook.md`
- Add `docs/config-schema.md`
- Relabel existing `docs/plans/*` content so status is unambiguous (`planned` vs `implemented`)

### Out of scope
- No code changes under `src/`, `pipeline/`, `cli/`
- No config shape changes
- No refactors, no registry/contract enforcement changes
- No test logic changes (docs-only PR)

## Public API / Interface / Type Changes
### Runtime/public code interfaces
- None

### Documentation interfaces introduced
- Canonical architecture reference for pipeline entrypoints and lifecycle
- Canonical contract reference for stage behavior and output fields
- Canonical onboarding playbook for adding a new sport/stat
- Canonical config schema reference covering current schema + legacy fallback behavior

## Detailed Implementation Plan

### 1) Add `docs/architecture.md` (single source of architecture truth)
1. Document authoritative runtime entrypoints:
   - `pipeline/main.py` (CLI entry)
   - `src/pipeline/engine.py` (orchestration)
   - `src/core/registry.py` (sport/stat lookup)
   - `src/core/config.py` (config load + identity resolution)
2. Document actual execution lifecycle in engine order:
   - `load_inputs`
   - `build_training_frame`
   - `train_or_load_model`
   - `predict_lines`
   - `simulate`
3. Explicitly document current adapter pass-through behavior:
   - MLB adapter `src/mlb/pitcher_props/adapter.py` and NFL adapter `src/nfl/pass_attempts/pipeline.py` currently no-op first four stages and perform full workflow in `simulate(...)`.
   - State this is intentional compatibility behavior, not a protocol guarantee for future sports.
4. Include module relationship map (textual, not diagram required):
   - Core layer (`src/core/*`)
   - Engine layer (`src/pipeline/engine.py`)
   - Sport adapters (MLB/NFL)
   - Sport orchestration modules (`src/mlb/pipeline.py`, `src/mlb/pitcher_props/pipeline.py`, `src/nfl/pipeline.py`)
5. Add a short "Known architecture debt" section:
   - Stage semantics not enforced yet (tracked for PR#2)
   - Cross-sport simulation reuse boundary pending PR#3/PR#4

### 2) Add `docs/contracts.md` (current contract behavior + outputs)
1. Document `SportStatPipeline` protocol methods from `src/core/contracts.py` with:
   - Intended responsibility
   - Current actual behavior
   - Required input/output artifact at each stage
2. Add an "adapter exception" section:
   - MLB/NFL adapters satisfy interface but delegate end-to-end work at `simulate`
   - Mark this as temporary compatibility pattern
3. Define output contract fields by sport/stat family:
   - Common simulation fields: `prob_over`, `prob_under`, `prob_push`, `ev_over`, `ev_under`, `edge_over`, `edge_under`
   - MLB stat-specific fields: `predicted_*` + stat line column (`k_line`, `outs_line`, `er_line`, `hits_line`, `bb_line`)
   - MLB mode fields where applicable: `run_mode`, `lines_status`
   - NFL fields: `predicted_pass_attempts`, `attempts_line`, plus probability/EV columns
4. Add field stability policy:
   - Existing MLB/NFL output columns are backward-compatible commitments for onboarding work
   - New NHL output must align with shared probability/EV conventions

### 3) Add `docs/config-schema.md` (current config truth)
1. Document root schema:
   - `pipeline.sport` (required in sectioned schema)
   - `pipeline.stat` (required in sectioned schema)
2. Document section resolution behavior:
   - Active section resolved as `{sport}.{stat}`
   - Legacy flat schema fallback currently supported in `src/core/config.py`
3. Provide required-vs-optional key tables for current implemented stats:
   - MLB `strikeouts`
   - MLB multi-stat pitcher props (`outs_recorded`, `earned_runs`, `hits_allowed`, `bb_allowed`)
   - NFL `pass_attempts`
4. Include validation behavior and current error modes:
   - Missing `pipeline.sport` / `pipeline.stat`
   - Missing sport or stat section in sectioned schema
   - Legacy fallback defaults
5. Add one canonical YAML example per sport:
   - Minimal valid MLB
   - Minimal valid NFL
   - Include note that stricter typed validation is planned in PR#5

### 4) Add `docs/new-sport-playbook.md` (NHL onboarding checklist baseline)
1. Create a stepwise checklist from idea to runnable skeleton:
   - Define sport/stat key
   - Create pipeline adapter implementing `SportStatPipeline`
   - Register in engine bootstrap
   - Add config section and default paths
   - Define output schema and simulation fields
   - Add offline integration test
2. Include "definition of done for onboarding-ready skeleton":
   - Runs via `pipeline/main.py --sport <sport> --stat <stat>`
   - Deterministic offline test path
   - No network-required tests
   - Output schema documented in `docs/contracts.md`
3. Include NHL `shots_on_goal`-specific starter checklist:
   - Package layout targets under `src/nhl/`
   - Config target `config/nhl.yaml`
   - Registry wiring expectations
4. Add pitfalls section:
   - Avoid copying MLB/NFL domain-specific aliases (`qb_id -> pitcher_id` style leaks)
   - Keep adapter behavior explicit
   - Preserve column stability in existing sports

### 5) Relabel `docs/plans/*` status taxonomy
1. Normalize status language in `docs/plans/README.md` and `docs/plans/implemented/README.md` to clearly define lifecycle states:
   - `planned`: intended, partially done, or blocked
   - `implemented`: fully shipped against acceptance criteria
2. Ensure each plan document in `docs/plans/planned/` and `docs/plans/implemented/` has an explicit status header line near top:
   - `Status: Planned` or `Status: Implemented`
   - For partial plans, keep under `planned` with `Status: Planned (Partially Implemented)` when needed
3. Leave filenames stable unless there is a direct ambiguity conflict; prefer content relabeling over renaming.
4. Keep `docs/plans/plan-doneness-audit-2026-02-08.md` as an audit artifact, but add explicit note that it is an audit report, not a plan spec.

## Acceptance Criteria (Decision-Complete)
1. Four new docs exist and are internally consistent:
   - `docs/architecture.md`
   - `docs/contracts.md`
   - `docs/new-sport-playbook.md`
   - `docs/config-schema.md`
2. Docs match current runtime reality:
   - Engine stage flow order is correct
   - Adapter pass-through behavior is explicitly documented
   - Config section/legacy fallback behavior matches `src/core/config.py`
   - Output field tables match existing integration-test expectations
3. Plan lifecycle labeling is unambiguous:
   - Planned vs implemented meaning is explicit in README(s)
   - Existing plan docs are clearly labeled with status
4. No runtime behavior change:
   - No source code changes outside docs tree for this PR

## Verification and Review Checklist
1. Consistency checks (manual):
   - Cross-check docs against `src/pipeline/engine.py`, `src/core/contracts.py`, `src/core/config.py`, `src/mlb/pitcher_props/adapter.py`, `src/nfl/pass_attempts/pipeline.py`
   - Cross-check output field tables against integration tests in `tests/integration/*`
2. Repo checks:
   - Run `.venv/bin/ruff check .`
   - Run `.venv/bin/pytest -q` (optional but recommended even for docs PR to prove no regressions)
3. PR review focus:
   - "Is this how the system behaves today?" not "how it should behave later"
   - No hidden normative refactor commitments in docs text

## Assumptions and Defaults
1. Assumption:
   - `docs/plans/` folder split already exists; PR#1 performs normalization and explicit labeling, not structural overhaul.
2. Defaults chosen:
   - Status labels added in-document instead of mass renaming files.
   - No new tooling (markdown lint, doc generator) added in this PR.
   - Docs are authored as present-state references with explicit notes where behavior is temporary.
