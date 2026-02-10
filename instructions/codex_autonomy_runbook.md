# Codex Autonomy Runbook (sportsbalf)

## Goal
Reduce operator back-and-forth for common repo workflows while staying safe and reproducible.

## Current Focus
- Offseason/model-first mode:
  - Prioritize modeling experiments, training data quality, backtest rigor, and
    feature engineering.
  - Treat live reliability work as secondary unless it blocks model iteration.

## Default Execution Pattern
1. Create/use a dedicated worktree branch for substantial work.
2. Implement changes with minimal surface area.
3. Run quality gates in this order:
   - `.venv/bin/ruff check .`
   - `.venv/bin/pytest -q`
4. If requested, run e2e smoke checks and report exact commands + outputs.
5. Push branch and open/update PR with validation summary.

## Preflight Checklist
Run before mutate/review/PR actions.

1. Confirm context:
   - `pwd`
   - `git branch --show-current`
   - `git status --short`
2. Confirm intent-to-branch match:
   - substantial implementation -> worktree branch
   - approved plan/doc commit-on-main -> `/Users/jbrys/sportsbalf` on `main`
3. Confirm sync state when targeting `main`:
   - `git pull --ff-only origin main`
4. If unexpected tracked changes are present, stop and ask before proceeding.

## Context Recovery Protocol
- If in wrong path/branch:
  1. report current context and intended context,
  2. switch to correct repo path/branch/worktree,
  3. re-run preflight checklist.
- If review result is unexpectedly empty, treat as context mismatch until proven otherwise.

## Review Quality Gate
- Before review output, verify non-empty diff for intended comparison.
- If diff is empty:
  - return "no actionable findings due to empty diff context",
  - provide exact commands to switch context and rerun review.
- Do not invent findings when comparison scope is empty.

## Quick Commands
- Lint:
  - `.venv/bin/ruff check .`
- Tests:
  - `.venv/bin/pytest -q`
- CLI pipeline:
  - `.venv/bin/python -m pipeline.main --sport <sport> --stat <stat> --config <path> [--retrain]`

## E2E Smoke Modes
- Offline mode (preferred default):
  - Use `tests/testdata/*` and `/tmp` configs/artifacts.
- Real ingestion mode (on explicit user request):
  - NFL:
    - `.venv/bin/python scripts/build_qb_attempts_dataset.py --years 2023 --output /tmp/sportsbalf-e2e/qb_attempts_2023.parquet`
    - run `pipeline.main` against `/tmp` config.
  - MLB:
    - `.venv/bin/python scripts/fetch_statcast_raw.py --season 2024 --start 04-01 --end 04-03`
    - `.venv/bin/python scripts/get_top_starters.py --season 2024 --top 80`
    - `.venv/bin/python scripts/generate_pitcher_dataset_from_raw.py --season 2024`
    - if UD lines are empty/offseason, generate a temporary `data/lines/strikeouts_e2e_*.csv` and run pipeline with that.

## Operational Notes
- `nfl_data_py` may pin/downgrade `numpy`/`pandas`; re-run tests after install.
- Avoid relying on shell activation state; always call `.venv/bin/...` explicitly.
- Network commands may require escalated execution depending on sandbox state.
- Keep generated artifacts inside active worktree only.
