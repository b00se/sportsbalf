# Skill: repo-autonomy

## Purpose
Run end-to-end repo tasks with minimal user intervention while preserving safety, determinism, and reviewability.

## Current Repo Mode
- Model-first/offseason:
  - Prioritize modeling improvements and offline validation loops.
  - Defer non-blocking live reliability churn unless explicitly requested.

## Use This Skill When
- User asks for autonomous execution ("just do it", "handle everything", "run full flow").
- User asks for fast merge readiness checks (lint/tests/e2e/PR updates).
- User asks for real ingestion smoke tests.

## Workflow
1. Confirm active branch/worktree and use a dedicated worktree if substantial changes are needed.
2. Gather only needed context (`rg`, targeted file reads).
3. Implement with surgical edits.
4. Validate:
   - `.venv/bin/ruff check .`
   - `.venv/bin/pytest -q`
   - optional e2e run requested by user
5. Summarize with exact outputs, unresolved risks, and next action.

## Repo-Specific Safeguards
- Prefer offline fixtures by default.
- Real ingestion only on explicit request.
- Generated data may be written under `data/` only in active worktree and never auto-cleaned.
- If dependency install changes versions (e.g., `nfl_data_py`), always rerun full tests.

## Preferred Commands
- Search/files: `rg`, `rg --files`
- Lint: `.venv/bin/ruff check .`
- Format: `.venv/bin/black .`
- Tests: `.venv/bin/pytest -q`
- Install deps: `.venv/bin/pip install -r requirements.txt`
- Pipeline:
  - `.venv/bin/python -m pipeline.main --sport <sport> --stat <stat> --config <path> [--retrain]`
