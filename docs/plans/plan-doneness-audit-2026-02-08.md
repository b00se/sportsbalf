# Plan Doneness Audit (2026-02-08)

## Scope audited
- `docs/plans/planned/mlb-multi-stat-expansion-plan.md`
- `docs/plans/planned/mlb-multistat-tournament-plan.md`
- `docs/plans/planned/mlb-pybaseball-live-features-plan.md`

## Summary verdict
- `mlb-multi-stat-expansion-plan.md`: **Partially implemented** (major scope complete; deferred items remain).
- `mlb-multistat-tournament-plan.md`: **Implemented** (acceptance criteria met by code + tests).
- `mlb-pybaseball-live-features-plan.md`: **Partially implemented** (engineering scope implemented; MAE-lift gate evidence not captured in repo docs/artifacts).

## Evidence

### 1) MLB Multi-Stat Expansion Plan
Status: **Partially implemented**

Implemented evidence:
- Multi-stat registration in `src/pipeline/engine.py`.
- New stat descriptors and shared core in `src/mlb/pitcher_props/descriptors.py`, `src/mlb/pitcher_props/pipeline.py`, `src/mlb/pitcher_props/data.py`.
- Config sections and `allow_missing_lines` in `config/mlb.yaml`.
- `run_mode` / `lines_status` behavior in `src/mlb/pitcher_props/pipeline.py`.
- Integration coverage in `tests/integration/test_mlb_outs_recorded_pipeline.py` and `tests/integration/test_mlb_multi_stat_pitcher_props_pipeline.py`.
- Data integrity and ER fallback tests in `tests/test_mlb_pitcher_prop_data_integrity.py`.

Open/deferred evidence:
- Plan explicitly lists deferred items, including high-fidelity ER label join and strikeouts migration into shared core.
- Current ER logic still includes fallback mechanism in `src/mlb/pitcher_props/data.py` (`earned_runs_fallback_used`).

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
- Plan acceptance gate requires positive MAE improvement vs baseline; no committed artifact/report in `docs/` or `runtime/` proving that gate.

## Recommended folder placement
- Move to `implemented/` now:
  - `mlb-multistat-tournament-plan.md`
- Keep in `planned/` until deferred/gap items are closed:
  - `mlb-multi-stat-expansion-plan.md`
  - `mlb-pybaseball-live-features-plan.md`
