# Plan Doneness Audit (2026-02-08)

Status: Audit Report (Not a Plan Spec)

Note: This file is an audit artifact summarizing plan-completion evidence. It is not an executable plan specification.

## Scope audited
- `docs/plans/implemented/2026-03-25-mlb-underdog-lines-betslips-design.md`
- `docs/plans/implemented/2026-03-25-mlb-underdog-lines-betslips-implementation-plan.md`
- `docs/plans/implemented/mlb-multi-stat-expansion-plan.md`
- `docs/plans/implemented/mlb-multistat-tournament-plan.md`
- `docs/plans/planned/mlb-pybaseball-live-features-plan.md`
- `docs/plans/implemented/nhl-pr1-architecture-baseline-canonical-docs-plan.md`
- `docs/plans/planned/nhl-onboarding-sequenced-pr-plan.md`
- `docs/plans/planned/nhl-pr2-contract-enforcement-tests-plan.md`
- `docs/plans/planned/nhl-pr3-shared-simulation-interface-extraction-plan.md`
- `docs/plans/planned/nhl-pr4-nfl-decoupling-cleanup-plan.md`

## Summary verdict
- `2026-03-25-mlb-underdog-lines-betslips-design.md`: **Implemented**.
- `2026-03-25-mlb-underdog-lines-betslips-implementation-plan.md`: **Implemented**.
- `mlb-multi-stat-expansion-plan.md`: **Implemented**.
- `mlb-multistat-tournament-plan.md`: **Implemented** (acceptance criteria met by code + tests).
- `mlb-pybaseball-live-features-plan.md`: **Partially implemented** (engineering scope implemented; strict MAE+ gate still not met).
- `nhl-pr1-architecture-baseline-canonical-docs-plan.md`: **Implemented** (merged via PR #11).
- `nhl-onboarding-sequenced-pr-plan.md`: **Partially implemented** (roadmap-level plan; PR1/PR2/PR3 done, PR4+ pending).
- `nhl-pr2-contract-enforcement-tests-plan.md`: **Implemented** (merged via PR #12).
- `nhl-pr3-shared-simulation-interface-extraction-plan.md`: **Implemented** (merged via PR #13).
- `nhl-pr4-nfl-decoupling-cleanup-plan.md`: **Planned (Approved)** (decision-complete execution plan approved; not yet implemented).

## Evidence

### 0) MLB Underdog Lines and Betslips Design + Implementation Plan
Status: **Implemented**

Implemented evidence:
- Live Underdog MLB ingestion helpers added in `src/mlb/data/underdog.py`.
- Multi-stat live line normalization and snapshot writing added in
  `src/mlb/pitcher_props/live_lines.py` and `src/mlb/data/load_props.py`.
- Unified multi-stat slate orchestration added in `src/mlb/pitcher_props/slate.py`.
- Mixed-stat and same-pitcher MLB slip generation supported in `src/mlb/slips.py`.
- Shadow-run CLI added in `scripts/build_mlb_live_betslips.py`.
- Config contract and defaults wired in `src/core/config.py` and `config/mlb.yaml`.
- Canonical docs updated in `README.md`, `docs/architecture.md`,
  `docs/contracts.md`, and `docs/config-schema.md`.
- Offline validation coverage added in:
  - `tests/test_mlb_underdog_lines.py`
  - `tests/test_mlb_pitcher_prop_live_lines.py`
  - `tests/test_mlb_pitcher_prop_slate.py`
  - `tests/test_mlb_mixed_prop_slips.py`
  - `tests/test_build_mlb_live_betslips.py`
  - `tests/test_core_config.py`

Validation evidence:
- Targeted Task 8 tests pass.
- `.venv/bin/ruff check .` passes.
- `.venv/bin/pytest -q` passes.

### 1) MLB Multi-Stat Expansion Plan
Status: **Implemented**

Implemented evidence:
- Multi-stat registration in `src/pipeline/engine.py`.
- New stat descriptors and shared core in `src/mlb/pitcher_props/descriptors.py`, `src/mlb/pitcher_props/pipeline.py`, `src/mlb/pitcher_props/data.py`.
- Config sections and `allow_missing_lines` in `config/mlb.yaml`.
- `run_mode` / `lines_status` behavior in `src/mlb/pitcher_props/pipeline.py`.
- Integration coverage in `tests/integration/test_mlb_outs_recorded_pipeline.py` and `tests/integration/test_mlb_multi_stat_pitcher_props_pipeline.py`.
- Data integrity and ER fallback tests in `tests/test_mlb_pitcher_prop_data_integrity.py`.

Completion evidence:
- Optional high-fidelity ER source integration and precedence in
  `src/mlb/pitcher_props/data.py`.
- High-fidelity/fallback label-quality reporting in
  `src/mlb/pitcher_props/pipeline.py`.
- Strikeouts now routed through shared pitcher-prop adapter in
  `src/pipeline/engine.py`.
- Strikeouts compatibility shim still exposed in `src/mlb/pipeline.py`.

### 2) Reusable Multi-Stat Tournament Plan
Status: **Implemented**

Implemented evidence:
- Reusable selection contracts in `src/core/model_selection.py` (`StatAdapter`, `BucketStrategy`, selection policy).
- Quantile + kmeans strategies in `src/mlb/models/buckets.py`.
- Deterministic champion selection and fallback handling in `src/mlb/models/evaluation.py`.
- Champion metadata + leaderboard persistence in `src/mlb/pipeline.py` and `scripts/backtest_mlb_strikeouts.py`.
- Segmentation config support in `config/mlb.yaml` and pipeline loading paths.
- Test coverage for strategy behavior, tie-breaks, and roundtrip artifacts in `tests/test_mlb_model_selection.py` and `tests/test_mlb_pitcher_prop_model_selection.py`.

### 3) Pybaseball Live Features Plan
Status: **Partially implemented**

Implemented evidence:
- Live-context service and fallback policies in `src/mlb/features/live_context.py`.
- New weather/venue/umpire/handedness modules in `src/mlb/features/weather.py`, `src/mlb/features/venue.py`, `src/mlb/features/umpire.py`, `src/mlb/features/handedness.py`.
- Historical parity + leakage-aware feature build in `src/mlb/features/feature_store.py`.
- Pipeline integration and coverage logging in `src/mlb/pipeline.py`.
- Feature list expansion in `src/mlb/models/predict.py`.
- Config block present in `config/mlb.yaml` (`live_features.*`).
- Strong unit/integration tests in `tests/test_mlb_live_features.py`, `tests/test_mlb_lookahead_guards.py`, `tests/test_historical_live_feature_parity.py`.

Open gap:
- Plan acceptance gate requires positive MAE improvement vs baseline.
- Current validation report (`docs/reports/mlb-live-features-mae-validation-2026-02-08.md`)
  shows no tested candidate set meeting positive MAE lift.

### 4) NHL PR#1 Architecture Baseline + Canonical Docs Plan
Status: **Implemented**

Implemented evidence:
- Canonical docs added:
  - `docs/architecture.md`
  - `docs/contracts.md`
  - `docs/config-schema.md`
  - `docs/new-sport-playbook.md`
- Plan-status taxonomy normalization delivered across `docs/plans/*`.
- Merged to `main` via PR #11:
  - `https://github.com/b00se/sportsbalf/pull/11`

### 5) NHL Onboarding Sequenced PR Plan
Status: **Partially implemented**

Implemented evidence:
- PR 1 track item completed and merged (`#11`).
- PR 2 track item completed and merged (`#12`).
- PR 3 track item completed and merged (`#13`).

Open gaps:
- PR 4 through PR 10 are not implemented yet.

### 6) NHL PR#2 Contract Enforcement Tests Plan
Status: **Implemented**

Implemented evidence:
- Enforcement test suite added at `tests/test_engine_contract_enforcement.py`:
  - strict stage ordering
  - stage handoff artifact/type invariants
  - CLI override passthrough assertions
  - default registration pair assertions
  - simulate-only allowlist enforcement for temporary adapters
- Contracts documentation updated with PR#2 enforcement note in `docs/contracts.md`.
- Merged to `main` via PR #12:
  - `https://github.com/b00se/sportsbalf/pull/12`

### 7) NHL PR#3 Shared Simulation Interface Extraction Plan
Status: **Implemented**

Implemented evidence:
- Shared simulation module added at `src/core/simulation.py`.
- MLB compatibility re-export kept in `src/mlb/models/monte_carlo.py`.
- NFL import boundary updated in `src/nfl/pipeline.py` to shared simulation core.
- Validation coverage includes:
  - `tests/test_core_simulation.py`
  - `tests/test_monte_carlo.py`
  - NFL pipeline integration coverage.
- Merged to `main` via PR #13:
  - `https://github.com/b00se/sportsbalf/pull/13`

### 8) NHL PR#4 NFL Decoupling Cleanup Plan
Status: **Planned (Approved)**

Current state:
- Decision-complete execution plan approved at
  `docs/plans/planned/nhl-pr4-nfl-decoupling-cleanup-plan.md`.
- Implementation has not been started yet.

## Recommended folder placement
- Move to `implemented/` now:
  - `mlb-multi-stat-expansion-plan.md`
  - `mlb-multistat-tournament-plan.md`
  - `nhl-pr1-architecture-baseline-canonical-docs-plan.md`
  - `nhl-pr2-contract-enforcement-tests-plan.md`
  - `nhl-pr3-shared-simulation-interface-extraction-plan.md`
- Keep in `planned/` until deferred/gap items are closed:
  - `mlb-pybaseball-live-features-plan.md`
  - `nhl-onboarding-sequenced-pr-plan.md`
  - `nhl-pr4-nfl-decoupling-cleanup-plan.md`
