# AGENTS.md

Scope: This file applies to the entire repository.

Purpose: Guide Codex when extending modeling features and the MLB prediction pipeline with simple, consistent practices.

## Goals
- Prioritize modeling features and pipeline extensions.
- Keep changes minimal, focused, and consistent with current structure.
- Maintain offline-safe tests; allow online pipeline execution.

## Python & Tooling
- Target Python: 3.11
- Package manager: `pip`
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
  - `src/core/config.py` (typed config + migration fallback)
- MLB pipeline core: `src/mlb/pipeline.py` (compat shim + strikeouts orchestration helpers).
- NFL pipeline core: `src/nfl/pipeline.py` (compat shim + pass attempts orchestration helpers).
- Features: `src/mlb/features/` (aggregation, rolling, park/opponent context).
- Modeling utils:
  - `src/mlb/models/` (feature list, XGBoost, residual bootstrap, Monte Carlo helpers)
  - `src/nfl/models/` (NFL feature/model/bootstrap helpers)
- Utilities: `src/utils/io.py` (CSV/Parquet I/O, config loading).
- CLI: `cli/` (lightweight; expand as needed without breaking `pipeline/main.py`).
- Tests: `tests/` plus fixtures in `tests/testdata/`.

## Autonomy Defaults (Codex)
- Worktree-first: For substantial changes, create and use a dedicated git worktree branch (`*-wt`) before editing.
- Real e2e policy:
  - Default: use offline fixtures and `/tmp` outputs.
  - If user explicitly requests real ingestion/e2e, it is allowed to write transient artifacts under `data/` in the active worktree.
- Safety: Never delete or reset generated `data/` artifacts automatically; leave cleanup to explicit user request.
- After dependency changes, re-run `pytest -q` and `ruff check .` before reporting completion.

## Suggested Approval Prefixes
- For smoother autonomous operation, pre-approve these command prefixes when possible:
  - `["ruff", "check", "."]`
  - `["black"]`
  - `["git", "push"]`
  - `["gh", "pr", "create"]`
  - `["gh", "pr", "edit"]`
  - `["/Users/jbrys/sportsbalf/.venv/bin/pip", "install"]`
  - `["/Users/jbrys/sportsbalf/.venv/bin/python", "-m", "pipeline.main"]`
  - `["/Users/jbrys/sportsbalf/.venv/bin/python", "scripts/fetch_statcast_raw.py"]`
  - `["/Users/jbrys/sportsbalf/.venv/bin/python", "scripts/build_qb_attempts_dataset.py"]`

## Local Skill & Runbook
- Autonomy runbook: `instructions/codex_autonomy_runbook.md`
- Local skill: `instructions/skills/repo-autonomy/SKILL.md`

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
- Runner: `pytest`
- Keep tests deterministic and fast; avoid network/file downloads.
- Use/extend `tests/testdata/` for small fixture files. Do not change large input/output directories.

## Minimal Commands (reference)
- Install deps: `pip install -r requirements.txt`
- Run tests (offline): `pytest`
- Run pipeline (online allowed): `python -m pipeline.main --sport <sport> --stat <stat> --config <path> [--retrain]`
- Format: `black .`
- Lint: `ruff check .`

## When Adding Dependencies
- Prefer widely used, well-supported libs. Justify additions with clear modeling or performance benefits.
- Update `requirements.txt` accordingly and ensure tests still pass offline.

## PR Checklist (lightweight)
- Tests pass locally (offline).
- New/changed code is typed, logged, and documented (Google docstrings).
- No changes to `data/`, `notebooks/`, `models/`, or `betslips/`.
