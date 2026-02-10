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
1. Print context preflight:
   - `pwd`
   - `git branch --show-current`
   - `git status --short`
2. Confirm active branch/path and use a dedicated feature branch in `/Users/jbrys/sportsbalf` for substantial changes.
3. Use worktrees only when explicitly requested or when parallel branches are required.
4. Gather only needed context (`rg`, targeted file reads).
5. Implement with surgical edits.
6. Validate:
   - `.venv/bin/ruff check .`
   - `.venv/bin/pytest -q`
   - optional e2e run requested by user
7. Run review rounds against `main...HEAD` as requested, then summarize with exact outputs, unresolved risks, and next action.

## Mainline Plan-Commit Workflow
Use this when user intent is to commit an approved plan doc on `main`.

1. Work from `/Users/jbrys/sportsbalf` (primary repo), not a feature worktree.
2. Ensure branch is `main`.
3. Pull latest safely:
   - `git pull --ff-only origin main`
4. Confirm tracked working tree is clean before staging.
5. Stage only intended plan/doc paths (for example `docs/plans/planned/<file>.md`).
6. Commit only those files with a specific message.
7. Confirm status/log after commit.

## Review Context Workflow
Use this when user asks for review/findings.

1. Capture context preflight (`pwd`, branch, status).
2. Confirm intended diff scope (`merge-base` or `main...HEAD`) and ensure non-empty diff.
3. If diff is empty:
   - report no actionable review due to context,
   - provide exact branch/path switch command(s),
   - rerun only after context correction.

## Repo-Specific Safeguards
- Prefer offline fixtures by default.
- Real ingestion only on explicit request.
- Generated data may be written under `data/` only in active checkout and never auto-cleaned.
- If dependency install changes versions (e.g., `nfl_data_py`), always rerun full tests.
- Never stage `.worktrees/` or unrelated files in plan-doc commits.
- Do not continue with major actions when unexpected tracked changes are present; ask first.

## Preferred Commands
- Search/files: `rg`, `rg --files`
- Lint: `.venv/bin/ruff check .`
- Format: `.venv/bin/black .`
- Tests: `.venv/bin/pytest -q`
- Install deps: `.venv/bin/pip install -r requirements.txt`
- Pipeline:
  - `.venv/bin/python -m pipeline.main --sport <sport> --stat <stat> --config <path> [--retrain]`
