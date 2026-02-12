# New Sport Playbook (Onboarding Baseline)

Status: Canonical (Current Process Baseline)

Date: 2026-02-12

## Goal

Add a new `{sport}.{stat}` pipeline through the canonical engine path with deterministic offline tests and stable output contracts.

## Canonical Path (Do This)

1. Choose stable keys:
- `sport` (example: `nhl`)
- `stat` (example: `shots_on_goal`)

2. Implement a `SportStatPipeline` adapter.

3. Register in `src/pipeline/registration.py::DEFAULT_PIPELINE_REGISTRATIONS`.

4. Add sectioned config:
- `pipeline.sport`
- `pipeline.stat`
- `{sport}.{stat}` section in `config/<sport>.yaml`

5. Define output schema:
- stat-specific prediction and line columns
- shared simulation columns (`prob_*`, `ev_*`, `edge_*`)

6. Add deterministic offline integration test using `tests/testdata/*`.

## Definition of Done

- Runs through:
  - `.venv/bin/python -m pipeline.main --sport <sport> --stat <stat> --config <path>`
- No network dependency in tests
- Config validator covers runtime-critical keys
- Output schema documented in `docs/contracts.md`

## Implemented Reference (NHL)

NHL `shots_on_goal` is the current full reference implementation:
- Adapter: `src/nhl/shots_on_goal/pipeline.py`
- Orchestration: `src/nhl/pipeline.py`
- Data ingest: `src/nhl/data/moneypuck_ingest.py`
- Providers: `src/nhl/data/providers/`
- Features: `src/nhl/features/shots_on_goal.py`
- Models: `src/nhl/models/predict.py`, `src/nhl/models/bootstrap.py`
- Snapshot builder utility: `src/nhl/data/shot_snapshot.py`, `scripts/build_nhl_skater_games_snapshot_from_shots.py`
- Config: `config/nhl.yaml`
- Integration test: `tests/integration/test_nhl_shots_on_goal_pipeline.py`

## Required Guardrails

- Use `pipeline/main.py`; do not onboard through `cli/main.py`.
- Use `.venv/bin/...` commands in docs/scripts.
- Keep tests offline-only.
- Preserve existing output columns for already-shipped stats.
- Keep path config-driven; avoid hardcoding repo-local file paths in code.

## Common Footguns

- Forgetting registration entry in `DEFAULT_PIPELINE_REGISTRATIONS`.
- Adding features in training path but not inference path.
- Introducing network calls in tests.
- Changing output columns without contract/test updates.
- Reviewing wrong branch/context and missing real diff.
