# Phase 1.5b Plan: Hit-Rate Family Accuracy and Calibration Hardening

Status: In Progress

Date: 2026-02-14

## Summary
This plan targets the next accuracy iteration for the Phase 1.5 hit-rate family:
`hit_rate`, `hits`, and `plate_appearances`.

Phase 1.5a established major point-accuracy gains and fixed critical upstream
defects (P1/P2). The primary remaining quality gap from findings is interval
calibration: `hit_rate_coverage` was over-wide in scoped runs. Phase 1.5b keeps
the same output contract while improving model selection rigor and uncertainty
calibration quality.

## Goals
- Improve point-forecast robustness for `hits` and `plate_appearances`.
- Improve derived `hit_rate` accuracy through stronger count model selection.
- Calibrate `hit_rate` uncertainty intervals into target coverage band.
- Preserve no-invalid-prediction guarantees and stable interfaces.

## Scope
### In scope
- MLB Phase 1.5 adapter path for:
  - `hit_rate`
  - `hits`
  - `plate_appearances`
- Walk-forward model-family selection and diagnostics.
- Hit-rate uncertainty calibration from count residual simulation.
- Additional deterministic acceptance gates and reporting.

### Out of scope
- Other Phase 1.5 stats (`walk_rate`, `slugging_proxy`, etc.).
- Provider/export schema changes.
- Breaking API changes.

## Baseline and Problem Statement
- Baseline context is documented in:
  - `docs/plans/implemented/fantasy-rankings/phase-1.5a-hit-rate-upstream-hardening-findings.md`
- Findings show:
  - Material point-accuracy improvement vs `main`.
  - Invalid predictions and extreme-rate issues resolved in scoped runs.
  - Remaining red flag: `hit_rate_coverage = 1.0` in scoped runs (target
    acceptance band: `0.70` to `0.90`).

## Design (Decision Complete)

### 1) Post-fix baseline freeze
- Capture post-P1/P2 reference artifacts for:
  - Scoped 2025 fold (top100, top300).
  - Full walk-forward folds (2022..2025).
- Treat these artifacts as the comparator for Phase 1.5b acceptance.

### 2) Count model selection hardening
- Candidate families remain:
  - `poisson`
  - `elastic_net`
  - `hist_gradient_boosting`
  - `xgboost`
- Selection policy:
  - Primary metric: fold MAE on target count.
  - Tie-break 1: RMSE.
  - Tie-break 2: absolute bias.
- Add anti-churn threshold:
  - Promote non-default winner only when MAE delta exceeds a configurable
    minimum improvement threshold.

### 3) Feature-group ablation gate
- Evaluate incremental effect of feature groups on out-of-fold performance:
  - playing-time/exposure features
  - contact-quality features
  - smoothed priors
- Keep groups only when they improve MAE or reduce bias without destabilizing
  inference behavior.

### 4) Hit-rate uncertainty calibration
- Keep count-derived Monte Carlo interval generation.
- Introduce deterministic residual scaling controls:
  - global residual scale multiplier
  - optional bucket-specific multipliers by `season_to_date_pa` buckets
- Calibrate multipliers using walk-forward objective:
  - minimize coverage error toward target center (`0.80`)
  - penalize unnecessarily wide intervals (`p90 - p10`)
- Keep hard bounds and monotonic quantiles:
  - `0 <= p10 <= p50 <= p90 <= 1`

### 5) Residual-bank quality controls
- Require minimum residual count per bucket before bucket-specific use.
- Fall back to default/global residual bank for sparse buckets.
- Emit residual-support diagnostics by fold and bucket.

### 6) Red-flag dashboard expansion
- Retain existing key metrics and add:
  - median interval width (`p90 - p10`)
  - p95 interval width
  - coverage error vs target center (`0.80`)
  - selected model family per target/fold
  - active uncertainty scale metadata

## Public API / Interface Changes
- No external output schema changes.
- No `project(config)` signature changes.

### Non-breaking config additions (under `adapters.mlb_projection_phase15`)
- `modeling.selection_min_delta_mae: float` (default `0.0`)
- `uncertainty.hit_rate_residual_scale_global: float` (default `1.0`)
- `uncertainty.hit_rate_residual_scale_by_bucket: dict[str, float]` (default `{}`)
- `uncertainty.coverage_target: float` (default `0.80`)
- `uncertainty.calibration_objective: str` (default `"coverage_width_tradeoff"`)
- `uncertainty.min_bucket_residual_count: int` (default `100`)

## Acceptance Criteria
Run walk-forward folds 2022..2025 and require all:
- `hit_rate_mae` improved by >= 20% vs comparator baseline.
- `hits_mae` improved by >= 10% vs comparator baseline.
- `plate_appearances_mae` improved by >= 10% vs comparator baseline.
- `hit_rate_invalid_predictions = 0`.
- `hit_rate_extreme_fraction` remains near zero with no regression.
- `hit_rate` coverage in `[0.70, 0.90]` per fold.
- No catastrophic fold regression (>10% MAE degradation) vs post-fix reference.

## Test Plan
### Unit tests
- Uncertainty calibration preserves bounds and quantile monotonicity.
- Bucket fallback triggers when residual support is below threshold.
- Calibration objective ranking is deterministic with fixed seed.

### Integration tests
- Adapter output schema and ordering unchanged.
- Source model provenance fields remain populated and deterministic.
- Red-flag dashboard emits new calibration/width metadata columns.

### E2E tests
- Scoped top100/top300 2025 fold checks.
- Full walk-forward folds 2022..2025 with gate evaluation table.

## Implementation Steps
1. Freeze and persist post-fix comparator artifacts.
2. Add config keys and defaults for selection/calibration controls.
3. Implement model-selection threshold logic.
4. Add feature-group ablation runner and summary artifact.
5. Implement uncertainty scaling and calibration objective scoring.
6. Add residual support thresholds and fallback behavior.
7. Expand red-flag dashboard outputs.
8. Execute scoped + walk-forward evaluation and apply gates.
9. Update docs (`README.md`, `docs/config-schema.md`, `docs/contracts.md`,
   `docs/architecture.md`) for any new keys/diagnostics.

## Implementation Progress (2026-02-14)
- Completed:
  - Step 2: config keys/defaults wired in adapter config parsing.
  - Step 3: model selection anti-churn threshold logic implemented.
  - Step 5: hit-rate uncertainty residual scaling controls implemented.
  - Step 6: bucket residual minimum-support fallback implemented.
  - Step 7: red-flag dashboard now emits interval-width and coverage-target diagnostics.
  - Step 9: docs/config updates landed for new controls.
- Remaining:
  - Step 1: persist post-fix comparator artifacts for acceptance baseline.
  - Step 4: add feature-group ablation runner/artifact.
  - Step 8: run scoped + full walk-forward gate evaluation against comparator.

## Assumptions and Defaults
- Scope is limited to hit-rate family metrics only.
- Phase 1.5a P1/P2 bug fixes are present before this plan starts.
- Coverage target center is `0.80` with acceptable band `[0.70, 0.90]`.
- Offline deterministic backtests remain the acceptance source of truth.
