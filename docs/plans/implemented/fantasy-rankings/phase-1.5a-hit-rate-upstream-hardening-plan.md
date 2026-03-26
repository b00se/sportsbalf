# Phase 1.5a Plan: Fix `hit_rate` by hardening upstream `hits` + `plate_appearances` first

Status: Implemented

## Summary
We will address the worst metric (`hit_rate`) by improving its upstream count predictions (`hits`, `plate_appearances`) end-to-end, then re-derive `hit_rate` from those improved counts.
This plan focuses on two fundamentals:

1. Data cleaning correctness (regular-season-only canonical batter-game truth, robust snapshot labels).
2. Feature engineering quality (playing-time and contact-quality features that stabilize count forecasts).

Key observed problems from current full-data e2e:
- `hit_rate` is worst (`+218.6%` MAE vs baseline).
- Extreme predictions (e.g., rate near `0.8`) indicate unstable upstream counts.
- Count/rate outputs still produce invalid negatives in many rows.
- Sparse-player filtering alone does not fix the MAE regression materially.

User-selected strategy: upstream counts first.

## Scope
### In scope
- Rebuild the `hits` and `plate_appearances` season-horizon modeling path on snapshot/rest-of-season labels.
- Clean and harden batter-game inputs used for those two targets.
- Derive `hit_rate` only from improved `hits` + `plate_appearances`.
- Add walk-forward evaluation and red-flag diagnostics specific to this metric family.

### Out of scope (this pass)
- Other metrics (`walk_rate`, `slugging_proxy`, etc.) except incidental compatibility.
- Full multi-metric feature overhaul.
- Provider/export logic changes.

## Design (decision complete)

### 1) Canonical cleaned batter-game dataset for this pass
Create a cleaned training view for `hits`/`PA` with explicit rules:
- Source: `statcast_raw_2021..2025.parquet`.
- Keep only `game_type == "R"` (regular season).
- Plate appearance terminal row selection:
  - Keep `events` not null and not `"none"/"nan"`.
  - Sort by `pitch_number`.
  - Deduplicate on `["game_pk", "batter", "game_date", "at_bat_number"]` (explicit batter key).
- Aggregate to batter-game (`["batter","game_date","game_pk"]` first, then daily rollup as needed).
- Enforce nonnegative integer-ish domains:
  - `plate_appearances >= 0`, `hits >= 0`, `hits <= plate_appearances`.
- Add cleaning audit columns in intermediate tables (not required in final adapter output):
  - `is_regular_season`, `pa_terminal_dedup_applied`, `qa_invalid_row_flag`.

### 2) Snapshot labeling strategy (replace per-game scaled season projection for these targets)
For `hits` and `plate_appearances`, train on:
- Entity: `(player_id, season, anchor_date)` weekly anchors.
- Features: all prior-to-anchor only.
- Labels:
  - `target_rest_of_season_hits`
  - `target_rest_of_season_plate_appearances`
- Inference semantics:
  - Predict remaining-season counts.
  - Final season forecast for each count = `season_to_date + predicted_remaining` (in-season anchors).
  - Preseason: remaining-only.

### 3) Feature engineering (metric-focused)
#### `plate_appearances` model features
- Existing leakage-safe rolling features (7/14/30) for:
  - `PA`, `games_played`, `PA/game`, `days_since_last_game`.
- Add exposure/stability features:
  - `team_games_seen_last_30` (from available game rows),
  - `player_game_share_last_30`,
  - `recent_consecutive_games_played`.
- Add split usage:
  - rolling `pa_vs_lhp_share`, `pa_vs_rhp_share`.

#### `hits` model features
- Include all PA-model features plus contact/performance:
  - rolling `hits`, `hit_rate`, `hard_hit_rate`, `total_bases`, `slugging_proxy`.
- Add empirical-Bayes smoothed hit-rate priors:
  - `smoothed_hit_rate_rolling_30`,
  - `smoothed_hit_rate_season_to_date`,
  - league prior fallback.
- Keep features strictly pre-anchor and numeric-safe.

### 4) Modeling approach
- PA model: direct regression on remaining PA counts.
- Hits model: direct regression on remaining hits counts, with predicted remaining PA as a feature during inference.
- Candidate models: `poisson`, `elastic_net`, `hist_gradient_boosting`, `xgboost`.
- Selection: walk-forward MAE on target metric, RMSE tie-break.
- Prediction constraints:
  - `pred_remaining_pa = max(pred, 0)`
  - `pred_remaining_hits = max(pred, 0)`
  - `pred_remaining_hits <= pred_remaining_pa` (consistency constraint).
- Derive `hit_rate` from constrained totals:
  - `hit_rate = total_hits / total_pa` when `total_pa > 0`, else `0`.

### 5) Uncertainty
- Build OOS residual banks from walk-forward for both count models.
- Bucket residuals by `season_to_date_pa` buckets.
- For `hit_rate` intervals:
  - Monte Carlo sample count residuals jointly (independent first pass, optional correlation in next pass),
  - enforce count constraints per draw,
  - transform draws to rate quantiles (`p10/p50/p90`) and stddev.
- Ensure non-degenerate but bounded output:
  - `0 <= p10 <= p50 <= p90 <= 1`.

## Public API / interface changes
1. `src/fantasy/adapters/mlb/projection_adapter.py`
- Internal API addition (non-breaking public entrypoint):
  - dedicated snapshot-based path for `hits` and `plate_appearances`.
- Keep `project(config)` signature unchanged.

2. Config additions under `adapters.mlb_projection_phase15` (non-breaking):
- `data_cleaning.regular_season_only: bool` (default `true`)
- `data_cleaning.require_batter_pa_dedup: bool` (default `true`)
- `modeling.count_nonnegative_constraints: bool` (default `true`)
- `modeling.hits_leq_pa_constraint: bool` (default `true`)
- `modeling.hit_rate_derivation_source: "counts_only"` (default)
- `evaluation.primary_metric_focus: "hit_rate"`

3. New/updated artifacts:
- walk-forward diagnostics CSV with per-fold red-flag columns.
- no changes to neutral projection output schema.

## Implementation steps
1. Add cleaned batter-game builder for `hits`/`PA` training view.
2. Extend snapshot builder to produce explicit labels/features for these two targets.
3. Implement metric-specific feature sets for PA and hits models.
4. Replace current per-game-scaled season path for these two metrics with snapshot models.
5. Add count consistency constraints before deriving `hit_rate`.
6. Update uncertainty path to sample from count residual banks and transform to rate intervals.
7. Add e2e walk-forward evaluation runner for this metric family.
8. Wire diagnostics/red-flag report generation.
9. Update docs/config schema for new knobs and semantics.

## Tests and scenarios
### Unit tests
- Cleaning:
  - regular-season filter applied when `game_type` present.
  - dedup keys produce one terminal row per batter PA.
  - invalid rows (`hits > PA`, negative values) are corrected/flagged.
- Snapshot leakage:
  - no post-anchor rows used in features.
- Constraint correctness:
  - `pred_hits >= 0`, `pred_pa >= 0`, `pred_hits <= pred_pa`.
  - derived `hit_rate` always in `[0,1]`.

### Integration tests
- Adapter outputs unchanged schema and deterministic ordering.
- `source_model_version` reflects selected/fallback model correctly.
- Uncertainty monotonicity and bounded rate intervals.

### E2E acceptance (full dataset)
- Walk-forward on folds: train<=N-1, test=N for 2022..2025.
- Pass criteria:
  - `hit_rate` MAE improves by at least 20% vs current Phase 1.5 adapter baseline run.
  - `hits` and `PA` each improve MAE by at least 10% vs current run.
  - `hit_rate` fold rows with `|bias| > 0.03` are reduced by at least 50%.
  - `p10-p90` coverage for `hit_rate` in `[0.70, 0.90]` for each fold.
  - Zero invalid projections:
    - no negative means for `hits`/`PA`,
    - no `hit_rate` outside `[0,1]`.

## Red-flag dashboard (must be emitted each run)
- Worst 20 entities by absolute `hit_rate` error.
- Fraction of predictions at extreme rates (`>0.45`, `<0.10`) with actual comparison.
- Fold-level:
  - MAE delta vs baseline,
  - bias,
  - coverage,
  - invalid prediction counts.

## Assumptions and defaults
- `hit_rate` remains a derived metric, not directly predicted in this pass.
- Baseline comparator for acceptance remains the current Phase 1.5 adapter e2e run artifacts.
- Regular-season-only filtering is default and required for this metric pass.
- Weekly anchors remain default for stability/performance balance.

## Post-Implementation Findings (Pre-Fix)
Date: 2026-02-14

Implementation is complete, but two post-implementation defects were identified and
must be fixed before final acceptance:
- Anchor-day labeling bug in snapshot target construction (`src/fantasy/adapters/mlb/datasets.py`).
- Hard-hit fallback conversion bug when only `hard_hit_rate` is available (`src/fantasy/adapters/mlb/datasets.py`).

Detailed issue log, modeling results, and testing notes are documented in:
- `docs/plans/implemented/fantasy-rankings/phase-1.5a-hit-rate-upstream-hardening-findings.md`

## Follow-up Plan
Next iteration planning for accuracy and interval calibration is tracked in:
- `docs/plans/planned/fantasy-rankings/phase-1.5b-hit-rate-accuracy-calibration-plan.md`
