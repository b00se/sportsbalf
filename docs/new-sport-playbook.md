# New Sport Playbook (Onboarding Baseline)

Status: Canonical (Current Process Baseline)

Date: 2026-02-08

## Stepwise checklist: idea to runnable skeleton
1. Define keys:
   - choose `sport` and `stat` identifiers (example: `nhl`, `shots_on_goal`)
2. Add adapter implementing `SportStatPipeline`:
   - create sport/stat adapter class matching `src/core/contracts.py`
3. Register adapter in centralized catalog:
   - add `("<sport>", "<stat>", <Factory>)` entry in `src/pipeline/registration.py::DEFAULT_PIPELINE_REGISTRATIONS`
4. Add config section:
   - update YAML so `pipeline.sport`, `pipeline.stat`, and `{sport}.{stat}` section resolve correctly
5. Define output schema:
   - include shared simulation columns (`prob_*`, `ev_*`, `edge_*`) plus stat-specific prediction/line columns
6. Add deterministic offline integration test:
   - run through `run_pipeline_with_overrides(...)` with test fixtures and no network requirements

## Definition of done: onboarding-ready skeleton
- Runs via `pipeline/main.py --sport <sport> --stat <stat> --config <path>`
- Includes deterministic offline test path
- Requires no network in tests
- Output schema is documented in `docs/contracts.md`

## NHL `shots_on_goal` starter checklist
1. Package layout:
   - `src/nhl/`
   - `src/nhl/pipeline.py` (orchestration shim)
   - `src/nhl/shots_on_goal/pipeline.py` (contract adapter)
2. Config target:
   - `config/nhl.yaml` with:
     - `pipeline.sport: nhl`
     - `pipeline.stat: shots_on_goal`
     - `nhl.shots_on_goal` section
3. Registry wiring:
   - add `("nhl", "shots_on_goal", ...)` to `DEFAULT_PIPELINE_REGISTRATIONS`
4. Test target:
   - `tests/integration/test_nhl_shots_on_goal_pipeline.py` using fixtures under `tests/testdata/`

## Common pitfalls to avoid
- Do not leak MLB/NFL domain aliases into new sports (`qb_id`/`pitcher_id` style cross-domain naming).
- Keep adapter behavior explicit and documented (especially if using temporary pass-through stages).
- Preserve output column stability for existing sports while adding new sport outputs.
- Do not treat legacy modules as onboarding authority (`cli/main.py`, `src/models/ensemble.py`, `ingest/*`); integrate via `pipeline/main.py` + `src/pipeline/engine.py`.
