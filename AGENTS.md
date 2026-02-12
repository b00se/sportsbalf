# AGENTS.md

Scope: This file applies to the entire repository.

Purpose: Guide Codex when extending modeling features and sport/stat pipelines with simple, consistent practices.

## Goals
- Prioritize modeling features and pipeline extensions.
- Current operating mode: model-first/offseason. Prefer work that improves offline
  model quality, backtests, and feature engineering over live reliability churn.
- Live reliability fixes are still allowed when they block model development or
  break current behavior.
- Keep changes minimal, focused, and consistent with current structure.
- Maintain offline-safe tests; allow online pipeline execution.
- Keep human-facing docs (`README.md`, `docs/*`) and agent docs (`AGENTS.md`,
  `CODEX.md`, `instructions/*`) synchronized when workflows or contracts change.

## Python & Tooling
- Target Python: 3.11
- Package manager: `pip`
- Command policy: use repo-local virtualenv executables explicitly
  (`.venv/bin/python`, `.venv/bin/pip`, `.venv/bin/pytest`, `.venv/bin/ruff`,
  `.venv/bin/black`) instead of relying on activated shells.
- Allowed dependencies (when beneficial): `pyarrow` (parquet I/O), `black` (format), `ruff` (lint). Add others only when there is a clear modeling or performance gain.

## Style & Conventions
- Type hints: Use across new/modified functions and public APIs.
- Docstrings: Google style for functions, classes, and modules.
- Logging: Prefer `logging` over `print` in new code. Use module-level loggers (e.g., `logger = logging.getLogger(__name__)`).
- Pandas/Numpy: Prefer vectorized operations; avoid per-row Python loops where possible.
- Determinism: Use explicit seeds for stochastic steps (e.g., Monte Carlo) when practical and surface them via config.
- Keep patches surgical: Do not refactor unrelated code; preserve public interfaces unless explicitly requested.

## Directory Rules
- Do not modify: `data/`, `notebooks/`, `models/`, `betslips/` (treat as inputs/outputs or scratch).
- Primary targets for code changes: `src/`, `pipeline/`, `cli/`, `tests/`, `config/`, `scripts/` (code only).
- Config-driven paths: Read locations from `config/*.yaml`; do not hardcode file paths in code.

## Codebase Overview (for orientation)
- Pipeline entry: `pipeline/main.py` routes via `src/pipeline/engine.py` using `--sport` and `--stat`.
- Core modular layers:
  - `src/core/contracts.py` (sport/stat protocol)
  - `src/core/registry.py` (pipeline registry)
  - `src/core/config.py` (typed config + sectioned-schema validation)
- MLB pipeline core: `src/mlb/pipeline.py` (compat shim + strikeouts orchestration helpers).
- NFL pipeline core: `src/nfl/pipeline.py` (compat shim + pass attempts orchestration helpers).
- NHL pipeline core: `src/nhl/pipeline.py` (shots-on-goal orchestration with
  provider refresh/load, model train/load, and simulation).
- Features: `src/mlb/features/` (aggregation, rolling, park/opponent context).
- Modeling utils:
  - `src/mlb/models/` (feature list, XGBoost, residual bootstrap, Monte Carlo helpers)
  - `src/nfl/models/` (NFL feature/model/bootstrap helpers)
- Utilities: `src/utils/io.py` (CSV/Parquet I/O, config loading).
- CLI: `cli/` (lightweight; expand as needed without breaking `pipeline/main.py`).
- Tests: `tests/` plus fixtures in `tests/testdata/`.

## Documentation Hygiene
- If config keys, output schemas, or canonical workflows change, update:
  - `docs/config-schema.md`
  - `docs/contracts.md`
  - `docs/architecture.md`
  - `README.md` (command/examples if user-visible)
- Plan lifecycle docs:
  - keep in `docs/plans/planned/` while not fully shipped
  - move to `docs/plans/implemented/` when shipped
  - ensure each plan doc includes a `Status:` line
- Keep references current when moving docs (avoid stale `planned/` paths in
  implemented docs).

## Autonomy Defaults (Codex)
- Branch-first: For substantial changes, create and use a dedicated feature branch in the primary repo checkout (`/Users/jbrys/sportsbalf`) before editing.
- Worktrees are opt-in only: use a dedicated worktree only when the user explicitly requests it or when parallel branch work is required.
- Real e2e policy:
  - Default: use offline fixtures and `/tmp` outputs.
  - If user explicitly requests real ingestion/e2e, it is allowed to write transient artifacts under `data/` in the active checkout.
- Safety: Never delete or reset generated `data/` artifacts automatically; leave cleanup to explicit user request.
- After dependency changes, re-run `.venv/bin/pytest -q` and `.venv/bin/ruff check .`
  before reporting completion.

## Preferred Git Workflow (Simple)
Use this default sequence unless the user asks for a different flow:
1. Write/approve plan docs.
2. Create a feature branch from `main`.
3. Execute plan changes on that same branch.
4. Run one or more review rounds against `main...<feature-branch>`.
5. Apply fixes and re-run review as needed.
6. Open PR from the feature branch so diffs are easy to review.

## Branch Context Guardrails
- Before major git actions (review, commit, PR, branch switching), report:
  - current path (`pwd`)
  - current branch (`git branch --show-current`)
  - current status (`git status --short`)
- If working tree contains unexpected tracked changes, stop and ask before proceeding.
- Treat untracked `.worktrees/` as non-commit noise unless user explicitly asks to include it.

## Review Guardrails
- Before any review-style response, verify there is a non-empty diff in the intended comparison context.
- If requested base/merge-base diff is empty:
  - do not provide fake findings,
  - explain why review is empty,
  - provide exact branch/path switching steps to review the right changes.

## Suggested Approval Prefixes
- For smoother autonomous operation, pre-approve these command prefixes when possible:
  - `[".venv/bin/ruff", "check", "."]`
  - `[".venv/bin/pytest", "-q"]`
  - `[".venv/bin/black"]`
  - `["git", "push"]`
  - `["gh", "pr", "create"]`
  - `["gh", "pr", "edit"]`
  - `[".venv/bin/pip", "install"]`
  - `[".venv/bin/python", "-m", "pipeline.main"]`
  - `[".venv/bin/python", "scripts/fetch_statcast_raw.py"]`
  - `[".venv/bin/python", "scripts/build_qb_attempts_dataset.py"]`

## Local Skill & Runbook
- Autonomy runbook: `instructions/codex_autonomy_runbook.md`
- Local skill: `instructions/skills/repo-autonomy/SKILL.md`
- Local skill: `instructions/skills/plan-ops/SKILL.md`

## Modeling & Pipeline Changes
- Feature columns: Centralized in `src/mlb/models/predict.py::FEATURES`. If adding/removing features, update this list and ensure:
  - Training uses the same features.
  - Inference paths populate these features (with sane fallbacks) before prediction.
- Model compatibility: The pipeline already retrains when a saved model is incompatible. Maintain this behavior and avoid breaking the `run()` signature.
- Monte Carlo: Use `MonteCarloConfig` and keep output column names stable (`predicted_strikeouts`, `prob_over`, `ev_over`, etc.).
- I/O helpers: Use `src/utils/io.read_csv` to load CSV/Parquet. If adding Parquet, prefer `pyarrow` engine.

## Network vs Offline
- Tests must be offline-only. Any network calls (e.g., `pybaseball`) must be optional and guarded with robust fallbacks so tests pass without network.
- The pipeline may use the network in normal runs, but should degrade gracefully (e.g., use last-known or average opponent/park context when network fails).

## Testing
- Runner: `.venv/bin/pytest`
- Keep tests deterministic and fast; avoid network/file downloads.
- Use/extend `tests/testdata/` for small fixture files. Do not change large input/output directories.
- TDD default for behavior changes:
  - Write or update a failing test first (RED) before editing production code.
  - Implement the smallest code change to pass the test (GREEN), then refactor if needed.
  - For bug fixes, add a regression test that fails before the fix and passes after.
  - In completion notes, include RED/GREEN evidence (commands + brief outcomes) unless the change is docs-only or purely non-behavioral refactor.

## Minimal Commands (reference)
- Install deps: `.venv/bin/pip install -r requirements.txt`
- Run tests (offline): `.venv/bin/pytest -q`
- Run pipeline (online allowed): `.venv/bin/python -m pipeline.main --sport <sport> --stat <stat> --config <path> [--retrain]`
- Format: `.venv/bin/black .`
- Lint: `.venv/bin/ruff check .`

## When Adding Dependencies
- Prefer widely used, well-supported libs. Justify additions with clear modeling or performance benefits.
- Update `requirements.txt` accordingly and ensure tests still pass offline.

## PR Checklist (lightweight)
- Tests pass locally (offline).
- New/changed code is typed, logged, and documented (Google docstrings).
- No changes to `data/`, `notebooks/`, `models/`, or `betslips/`.
