# PR#8 Plan: NHL `shots_on_goal` Skeleton (Offline-First, Registry-Wired)

Status: Implemented

## Summary
Deliver a production-shaped NHL skeleton that is fully runnable through the existing engine/registry flow, but intentionally lightweight: no live NHL provider ingestion yet, and no full model training stack yet.

This PR establishes the NHL package, adapter/orchestrator seams, config, registry registration, and deterministic offline integration coverage so PR9 and PR10 can build on stable interfaces.

Locked defaults for PR#8:
- Canonical NHL line column: `sog_line`
- Pre-PR9 input source: config-driven local fixture path (CSV/Parquet), no network fetch in PR#8

## Scope and Non-Goals
### In scope
- Add NHL module skeleton and contract adapter.
- Add NHL config file and registry wiring.
- Add deterministic offline integration + registration/contract test updates.
- Document NHL schema and onboarding path.

### Out of scope
- MoneyPuck ingestion/provider abstraction (deferred to PR9).
- NHL model training/selection residual bootstrap sophistication (deferred to PR10).
- Runtime network calls in tests.

## Public APIs / Interfaces / Type Changes
No breaking API signature changes.

### Additions
1. `src/nhl/shots_on_goal/pipeline.py`
- New adapter class `NhlShotsOnGoalPipeline(SportStatPipeline)` using current compatibility pattern (stages 1-4 minimal pass-through, work executed in `simulate(...)`).
- Mirrors MLB/NFL adapter behavior to stay contract-consistent.

2. `src/nhl/pipeline.py`
- New orchestration shim: `run_shots_on_goal_pipeline(config: PipelineConfig, retrain: bool = False) -> pd.DataFrame`.
- New compatibility wrapper `run(config_path: str | Path | None = None, retrain: bool = False)` that resolves NHL config and calls `run_shots_on_goal_pipeline`.

3. `config/nhl.yaml`
- Sectioned schema:
  - `pipeline.sport: nhl`
  - `pipeline.stat: shots_on_goal`
  - `nhl.shots_on_goal` section with offline fixture path and simulation defaults.

4. `src/pipeline/registration.py`
- Add default registration tuple for `("nhl", "shots_on_goal", NhlShotsOnGoalPipeline)`.

5. Output schema contract for PR#8 NHL path
- Required columns:
  - `player_id`
  - `player_name`
  - `team`
  - `opponent`
  - `game_id`
  - `sog_line`
  - `predicted_shots_on_goal`
  - `prob_over`
  - `prob_under`
  - `prob_push`
  - `ev_over`
  - `ev_under`
  - `edge_over`
  - `edge_under`
  - `run_mode`
  - `lines_status`

## Implementation Design (Decision-Complete)
### 1. New package/layout
- Add `src/nhl/__init__.py`.
- Add `src/nhl/shots_on_goal/__init__.py`.
- Add `src/nhl/shots_on_goal/pipeline.py`.
- Add `src/nhl/pipeline.py`.

### 2. NHL orchestration behavior in `src/nhl/pipeline.py`
- Read `config.section`.
- Load inference rows from `inference_input_path` using `src/utils/io.read_csv`.
- If path missing/unreadable/empty:
  - Return empty DataFrame with the full required NHL schema.
  - Set `run_mode="fallback"` and `lines_status="missing"` columns present in schema.
- For non-empty input:
  - Validate/coerce required input columns:
    - `player_id`, `player_name`, `team`, `opponent`, `game_id`, `sog_line`.
  - Add default prices if missing:
    - `over_decimal_price` from config default.
    - `under_decimal_price` from config default.
  - Produce baseline prediction:
    - `predicted_shots_on_goal` from config fallback scalar (or simple deterministic column if provided).
    - Keep deterministic behavior with explicit seed.
  - Run `apply_simulations(...)` from `src/core/simulation.py` with:
    - `mean_col="predicted_shots_on_goal"`
    - `line_col="sog_line"`
    - `id_col="player_id"`
    - `std_dev` from config `fallback_std`
    - `MonteCarloConfig(simulations, random_seed)`.
  - Set metadata columns:
    - `run_mode="prediction"`
    - `lines_status="present"`.

### 3. Adapter behavior in `src/nhl/shots_on_goal/pipeline.py`
- Match existing MLB/NFL adapter pattern exactly:
  - `load_inputs` returns config_path payload.
  - `build_training_frame` returns empty DataFrame.
  - `train_or_load_model` stores retrain flag in instance and returns empty ModelBundle.
  - `predict_lines` returns empty DataFrame.
  - `simulate` delegates to `run_shots_on_goal_pipeline(config=config, retrain=self.retrain)`.

### 4. Config file `config/nhl.yaml` contents
- `pipeline` section:
  - `sport: nhl`
  - `stat: shots_on_goal`
- `nhl.shots_on_goal` section keys:
  - `inference_input_path` (fixture path used by tests)
  - `model_path` placeholder path for future PR10 compatibility
  - `monte_carlo_simulations`
  - `monte_carlo_seed`
  - `fallback_std`
  - `fallback_prediction`
  - `default_over_decimal_price`
  - `default_under_decimal_price`

### 5. Registration and contract test updates
- Update `src/pipeline/registration.py` default catalog to include NHL pair.
- Update `tests/test_pipeline_registration.py` expected default set to include `("nhl", "shots_on_goal")`.
- Update `tests/test_engine_contract_enforcement.py`:
  - expected default pair set includes NHL.
  - `SIMULATE_ONLY_ALLOWLIST` includes `src.nhl.shots_on_goal.pipeline.NhlShotsOnGoalPipeline` (since PR8 follows simulate-only compatibility pattern).

### 6. Documentation updates
- `docs/contracts.md`:
  - Add NHL PR8 baseline output fields, explicitly including `sog_line` and `predicted_shots_on_goal`.
- `docs/architecture.md`:
  - Add NHL adapter/orchestrator modules to module map.
- `docs/new-sport-playbook.md`:
  - Mark NHL skeleton path as implemented example.
- `docs/plans/planned/nhl-onboarding-sequenced-pr-plan.md`:
  - Update tracker: PR6 implemented, PR8 in progress/implemented once merged.

## TDD Plan (RED -> GREEN)
### 1. RED tests first
- Add `tests/integration/test_nhl_shots_on_goal_pipeline.py` asserting:
  - engine override run works for NHL.
  - deterministic offline result.
  - required schema columns are present.
  - fallback mode returns empty-but-schema-stable output when input file missing.
- Extend registration/contract tests to include NHL default pair + allowlist expectation.
- Run targeted RED command and capture failures:
  - `.venv/bin/pytest -q tests/test_pipeline_registration.py tests/test_engine_contract_enforcement.py tests/integration/test_nhl_shots_on_goal_pipeline.py`

### 2. GREEN implementation
- Implement NHL package, config, registry, docs.
- Re-run targeted tests until green.

### 3. Final verification gates
- `.venv/bin/ruff check .`
- `.venv/bin/pytest -q`

## Test Cases and Scenarios
- Engine integration (offline):
  - `run_pipeline_with_overrides(config, sport="nhl", stat="shots_on_goal")` returns DataFrame with NHL schema.
- Determinism:
  - same input + same `monte_carlo_seed` yields stable simulation outputs.
- Missing input fallback:
  - missing `inference_input_path` returns empty DataFrame with NHL schema + `run_mode`/`lines_status` columns.
- Registry behavior:
  - NHL pair appears in default registrations and discovery.
- Contract enforcement compatibility:
  - simulate-only adapter allowlist remains explicit and complete.
- Non-regression:
  - existing MLB/NFL integration tests remain unchanged and green.

## Risks and Mitigations
- Risk: schema churn before PR9/PR10.
  - Mitigation: lock `sog_line` and `predicted_shots_on_goal` now and codify in docs/tests.
- Risk: hidden coupling to live providers.
  - Mitigation: PR8 uses fixture-path-only ingestion, no network dependency.
- Risk: break existing registration tests.
  - Mitigation: update all expected default-pair assertions in same PR.

## Assumptions and Defaults
- PR8 remains skeleton-only and does not add live MoneyPuck/provider ingestion.
- NHL config validation remains sectioned-only baseline from PR5; no NHL typed validator is introduced in PR8.
- Adapter keeps current simulate-only compatibility pattern to align with existing MLB/NFL architecture.
- Canonical NHL line column is `sog_line`.
- Offline fixture-path ingestion is authoritative for PR8 testability.
