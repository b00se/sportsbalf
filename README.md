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

### 3) Validate Changes

```bash
.venv/bin/ruff check .
.venv/bin/pytest -q
```

## Documentation Map

Core docs:
- Architecture: `docs/architecture.md`
- Runtime contracts: `docs/contracts.md`
- Config schema: `docs/config-schema.md`
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
