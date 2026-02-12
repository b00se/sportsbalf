# Onboarding Conventions and Footguns

Status: Operational guide

Date: 2026-02-12

## Why this exists

This repo has multiple sports, legacy compatibility surfaces, and frequent autonomous edits. Most costly failures come from context mistakes, not model code.

## Conventions (Team Standard)

1. Use canonical entrypoint and engine path:
- `pipeline/main.py`
- `src/pipeline/engine.py`

2. Use repo-local executables:
- `.venv/bin/python`
- `.venv/bin/pytest`
- `.venv/bin/ruff`
- `.venv/bin/black`

3. Prefer branch-first workflow for substantial changes:
- create feature branch from `main`
- implement + validate on that branch
- merge back when green

4. Keep tests offline and deterministic.

5. Treat output schemas as stable contracts for shipped stats.

6. Keep paths config-driven (`config/*.yaml`) and avoid hardcoded absolute paths in code.

## High-Impact Footguns

### Git/context

- Running review on wrong branch/path and seeing empty diff.
- Staging `.worktrees/` or unrelated files.
- Mixing plan-doc commits with unrelated code changes.

Prevent with:
```bash
pwd
git branch --show-current
git status --short
```

### Pipeline/runtime

- Using `cli/main.py` for onboarding instead of `pipeline/main.py`.
- Forgetting to register new pipelines in `DEFAULT_PIPELINE_REGISTRATIONS`.
- Adding a new feature column in training only (not inference).

### Model compatibility

- Changing feature lists without compatibility/retrain behavior.
- Assuming persisted models remain valid after feature changes.

### Data and tests

- Introducing network calls in tests.
- Writing tests against large mutable `data/` artifacts instead of fixtures.
- Hardcoding local file paths in source code.

## New Feature Checklist (Before Commit)

1. Feature names centralized and shared by train + inference.
2. Missing data fallback path is explicit.
3. Deterministic behavior under fixed seed.
4. Tests added/updated and pass offline.
5. Docs updated where contracts/config changed.

## Review Checklist

1. Confirm non-empty diff scope.
2. Verify contract stability (output columns, required fields).
3. Verify config validator behavior for new required keys.
4. Verify no test introduces mandatory network dependency.
5. Verify no accidental changes under `data/`, `models/`, `notebooks/`, `betslips/`.

## Fast Command Reference

```bash
.venv/bin/ruff check .
.venv/bin/pytest -q
.venv/bin/python -m pipeline.main --sport <sport> --stat <stat> --config <path> [--retrain]
```
