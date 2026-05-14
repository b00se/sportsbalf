# sportsbalf

Modular, config-driven sports prop pipelines with a shared engine and per-sport adapters.

Current production-shaped stats:
- MLB: `strikeouts`, `outs_recorded`, `earned_runs`, `hits_allowed`, `bb_allowed`
- NFL: `pass_attempts`
- NHL: `shots_on_goal`

## Quick Start

### 1) Environment

Use Python 3.11 and repo-local executables.

```bash
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Command convention in this repo:
- Always prefer `.venv/bin/...` over shell activation assumptions.

### 2) Run Pipelines

Authoritative entrypoint:
- `pipeline/main.py`

Examples:

```bash
.venv/bin/python -m pipeline.main --sport mlb --stat strikeouts --config config/mlb.yaml
.venv/bin/python -m pipeline.main --sport nfl --stat pass_attempts --config config/nfl.yaml
.venv/bin/python -m pipeline.main --sport nhl --stat shots_on_goal --config config/nhl.yaml
```

Force retrain before inference:

```bash
.venv/bin/python -m pipeline.main --sport nhl --stat shots_on_goal --config config/nhl.yaml --retrain
```

MLB live shadow run:

```bash
.venv/bin/python scripts/build_mlb_live_betslips.py \
  --config config/mlb.yaml \
  --stat-id strikeouts=PickemStat_... \
  --stat-id outs_recorded=PickemStat_... \
  --stat-id earned_runs=PickemStat_... \
  --stat-id hits_allowed=PickemStat_... \
  --stat-id bb_allowed=PickemStat_...
```

This writes dated live line snapshots under `data/lines/` and JSON slip
artifacts plus a run summary under `betslips/mlb_live/` by default.

Print a saved live summary in app-entry format:

```bash
.venv/bin/python scripts/print_slips.py betslips/mlb_live/mlb_live_2026-05-14_summary.json
.venv/bin/python scripts/print_slips.py betslips/mlb_live/mlb_live_2026-05-14_summary.json --tag fullsend
```

### 3) Validate Changes

```bash
.venv/bin/ruff check .
.venv/bin/pytest -q
```

### 4) Fantasy MLB Phase 1.5 Helpers

Build snapshot dataset:

```bash
.venv/bin/python scripts/build_mlb_batter_projection_dataset.py --input tests/testdata/fantasy/mlb_batter_games_phase1.csv --output /tmp/mlb_phase15_snapshots.csv --metric hits
```

Aggregate backtest metrics:

```bash
.venv/bin/python scripts/backtest_mlb_fantasy_projections.py --predictions /tmp/mlb_backtest_rows.csv --output /tmp/mlb_backtest_scores.csv
```

Phase 1.5 quality controls (under `adapters.mlb_projection_phase15`) support:
- regular-season + terminal-PA cleaning (`data_cleaning.*`)
- count consistency constraints (`modeling.count_nonnegative_constraints`, `modeling.hits_leq_pa_constraint`)
- count-derived `hit_rate` uncertainty draws (`modeling.hit_rate_uncertainty_draws`)
- model-family anti-churn selection threshold (`modeling.selection_min_delta_mae`)
- hit-rate calibration controls (`uncertainty.hit_rate_residual_scale_*`, `uncertainty.coverage_target`, `uncertainty.min_bucket_residual_count`)

## Documentation Map

Core docs:
- Architecture: `docs/architecture.md`
- Runtime contracts: `docs/contracts.md`
- Config schema: `docs/config-schema.md`
- Testing intent matrix: `docs/testing-intent-matrix.md`
- New sport onboarding: `docs/new-sport-playbook.md`
- Onboarding conventions + footguns: `docs/onboarding-footguns.md`

Planning docs:
- Planned work: `docs/plans/planned/`
- Shipped plans: `docs/plans/implemented/`
- Plan lifecycle rules: `docs/plans/README.md`

Agent/autonomy docs:
- Repo agent policy: `AGENTS.md`
- Codex workflow policy: `CODEX.md`
- Autonomy runbook: `instructions/codex_autonomy_runbook.md`

## High-Value Conventions

- Use config-driven paths; do not hardcode data/model locations.
- Keep tests offline and deterministic; network paths must degrade gracefully.
- Preserve output schema stability for existing sports.
- Treat `.worktrees/` as local noise unless explicitly requested.
- Do not modify `data/`, `models/`, `notebooks/`, `betslips/` as part of normal code changes.

## Common Footguns

- Running from the wrong entrypoint (`cli/main.py`) instead of `pipeline/main.py`.
- Using global Python/pip instead of `.venv/bin/...`.
- Reviewing an empty diff due to wrong branch/context.
- Changing feature columns without updating model compatibility handling.
- Staging broad file sets when only targeted docs/code were intended.

See `docs/onboarding-footguns.md` for concrete prevention checklists.
