# PR#7 Plan: Legacy Surface Area Clarification

Status: Implemented

## Summary

Implement PR#7 as a low-risk clarification pass that removes contributor ambiguity about which runtime paths are authoritative.

This PR is intentionally non-disruptive:
- docs + lightweight code markers only,
- no runtime gating or hard failures,
- no module moves/deletions,
- no pipeline behavior changes.

Primary objective: make authoritative entrypoints unambiguous (`pipeline/main.py`, `src/pipeline/engine.py`) while clearly labeling retained legacy/non-authoritative surfaces (`cli/main.py`, `src/models/ensemble.py`, `ingest/*`).

## Public API / Interface / Type Changes

No public API signature changes.

Additive module-level metadata constants only:
- `MODULE_STATUS`
- `AUTHORITATIVE_ENTRYPOINT`
- `STATUS_NOTE`

Legacy targets to annotate:
- `cli/main.py`
- `src/models/ensemble.py`
- `ingest/parse_ud_strikeouts.py`
- `ingest/park_factors.py`

Optional canonical markers (recommended for symmetry/testability):
- `pipeline/main.py`: `MODULE_STATUS = "authoritative_entrypoint"`
- `src/pipeline/engine.py`: `MODULE_STATUS = "authoritative_engine"`

## Validation/Behavior Contract for PR#7

1. Authoritative path clarity
- Contributors can identify canonical execution entrypoints directly from code/docs.

2. Legacy path clarity
- Legacy/non-authoritative modules expose explicit status markers and canonical pointer.

3. No behavior regression
- Existing runtime behavior remains unchanged for MLB/NFL execution paths.

4. No onboarding ambiguity
- New contributors can identify where to integrate/extend sport/stat pipeline functionality without relying on legacy modules.

## Detailed Implementation Plan

1. RED: add failing clarity tests first
- Add `tests/test_surface_area_clarity.py` asserting:
  - authoritative marker presence/values in canonical entrypoints,
  - legacy marker presence/values in non-authoritative modules,
  - canonical pointer references are explicit and non-empty.
- Confirm tests fail before marker additions.

2. GREEN: add lightweight code markers
- Add concise module docstrings/comments + status constants in:
  - `cli/main.py`
  - `src/models/ensemble.py`
  - `ingest/parse_ud_strikeouts.py`
  - `ingest/park_factors.py`
- Add canonical status markers in:
  - `pipeline/main.py`
  - `src/pipeline/engine.py`
- Keep all runtime behavior unchanged.

3. Docs updates
- `docs/architecture.md`:
  - add explicit “Legacy/non-authoritative modules” section listing target modules and canonical path guidance.
- `docs/new-sport-playbook.md`:
  - add a brief “do not treat these as authority” note for legacy modules.
- `docs/plans/planned/nhl-onboarding-sequenced-pr-plan.md`:
  - align PR#7 wording with final implementation shape (docs + markers; no runtime gating in PR#7).

4. Hardening and consistency
- Keep edits surgical and scoped.
- Do not move/remove modules in PR#7.
- Preserve all current tests and behavior expectations.

## Test Cases and Scenarios

1. Authoritative declaration tests
- `pipeline/main.py` exposes authoritative status marker.
- `src/pipeline/engine.py` exposes authoritative status marker.

2. Legacy declaration tests
- Each legacy target exposes:
  - `MODULE_STATUS == "legacy_non_authoritative"`
  - valid non-empty `AUTHORITATIVE_ENTRYPOINT`
  - valid non-empty `STATUS_NOTE`.

3. Compatibility verification
- Existing engine contract/integration tests remain green with no behavior change.

4. Docs consistency checks
- `docs/architecture.md` and `docs/new-sport-playbook.md` reflect the same authoritative/legacy distinction used in code markers.

## Verification Commands (for implementation PR)

1. RED evidence
- `.venv/bin/pytest -q tests/test_surface_area_clarity.py` before implementation.

2. GREEN evidence
- same targeted test command after implementation.

3. Repo gates
- `.venv/bin/ruff check .`
- `.venv/bin/pytest -q`

## Assumptions and Defaults

1. PR#7 remains non-disruptive and does not add runtime warning/gating behavior.
2. Legacy modules are retained for compatibility/utility usage in this PR.
3. Any future warning/gating/removal is deferred to a separate follow-up PR.
4. Canonical execution authority remains:
   - CLI: `pipeline/main.py`
   - orchestration: `src/pipeline/engine.py`
5. No changes under `data/`, `models/`, `notebooks/`, or `betslips/` for this plan PR.
