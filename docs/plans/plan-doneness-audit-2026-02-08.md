# Plan Doneness Audit (2026-02-08)

Status: Audit Report (Not a Plan Spec)

Note: This file is an audit artifact summarizing plan-completion evidence. It is not an executable plan specification.

## Scope audited
- `docs/plans/implemented/mlb-multi-stat-expansion-plan.md`
- `docs/plans/implemented/mlb-multistat-tournament-plan.md`
- `docs/plans/planned/mlb-pybaseball-live-features-plan.md`

## Summary verdict
- `mlb-multi-stat-expansion-plan.md`: **Implemented**.
- `mlb-multistat-tournament-plan.md`: **Implemented** (acceptance criteria met by code + tests).
- `mlb-pybaseball-live-features-plan.md`: **Partially implemented** (engineering scope implemented; strict MAE+ gate still not met).

## Evidence

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

## Recommended folder placement
- Move to `implemented/` now:
  - `mlb-multi-stat-expansion-plan.md`
  - `mlb-multistat-tournament-plan.md`
- Keep in `planned/` until deferred/gap items are closed:
  - `mlb-pybaseball-live-features-plan.md`
