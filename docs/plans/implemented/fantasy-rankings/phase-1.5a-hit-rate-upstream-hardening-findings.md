# Phase 1.5a Hit-Rate Upstream Hardening Findings (Pre-Fix)

Status: Archived (Historical Pre-Fix Findings)

Date: 2026-02-14

## Summary
This document records post-implementation findings before code fixes. It captures:
- Confirmed defects discovered in review.
- Current modeling and testing signals from scoped E2E comparisons.
- Known risks and acceptance gates to satisfy after remediation.

This file is intentionally retained in `implemented/` as a historical checkpoint.
It should not be treated as the latest post-fix acceptance record.

## Scope and Dataset Context
- Evaluation fold used in this report: 2025 season (train through 2024).
- Scoped datasets used for fast iteration:
  - `/tmp/batter_games_2021_2025_top100.parquet`
  - `/tmp/batter_games_2021_2025_top300.parquet`
- Comparator baseline: `main` branch run in worktree `/tmp/sportsbalf-main` using
  the same harness and datasets.

## Confirmed Defects
### P1: Anchor-day games dropped from snapshot labels
- Severity: P1
- File: `src/fantasy/adapters/mlb/datasets.py`
- Behavior:
  - History uses `< anchor_date`.
  - Future label uses `> anchor_date`.
  - Games on `anchor_date` are excluded from both sides.
- Impact:
  - Systematic undercount in `target_rest_of_season_*`.
  - Strongest risk when anchors are daily.
  - Biases count labels downward for model training.

### P2: Hard-hit fallback signal dropped in non-Statcast path
- Severity: P2
- File: `src/fantasy/adapters/mlb/datasets.py`
- Behavior:
  - `hard_hit_events` is created earlier in fallback logic.
  - Later conversion guard checks for missing column, which is then never true.
  - `hard_hit_rate -> hard_hit_events` conversion is skipped when only rate exists.
- Impact:
  - Hard-hit feature signal can be silently zeroed.
  - Reduces feature quality for hit-rate-related modeling.

## Modeling Results (Current vs Baseline)
### Top100 (2025 fold)
- Current:
  - `hit_rate_mae`: `0.02284852370839386`
  - `hits_mae`: `38.47352982441854`
  - `plate_appearances_mae`: `146.80186464977405`
  - `hit_rate_coverage`: `1.0`
  - `hit_rate_extreme_fraction`: `0.0`
  - `hit_rate_invalid_predictions`: `0`
- Baseline (`main`):
  - `hit_rate_mae`: `19.698855647059194`
  - `hits_mae`: `63.18713769805639`
  - `plate_appearances_mae`: `164.25817549661855`
  - `hit_rate_coverage`: `0.35789473684210527`
  - `hit_rate_extreme_fraction`: `1.0`
  - `hit_rate_invalid_predictions`: `95`

### Top300 (2025 fold)
- Current:
  - `hit_rate_mae`: `0.027228268086485822`
  - `hits_mae`: `38.66469938832016`
  - `plate_appearances_mae`: `159.37667710829683`
  - `hit_rate_coverage`: `1.0`
  - `hit_rate_extreme_fraction`: `0.0`
  - `hit_rate_invalid_predictions`: `0`
- Baseline (`main`):
  - `hit_rate_mae`: `15.28944604101476`
  - `hits_mae`: `53.40722639558069`
  - `plate_appearances_mae`: `170.92260533186723`
  - `hit_rate_coverage`: `0.4859437751004016`
  - `hit_rate_extreme_fraction`: `1.0`
  - `hit_rate_invalid_predictions`: `249`

## Red Flags and Risk Assessment
- Positive:
  - Invalid prediction counts dropped to zero in scoped runs.
  - Extreme-rate prediction fraction dropped to zero in scoped runs.
  - Count and rate MAE improved materially versus baseline in scoped runs.
- Open red flag:
  - Coverage is `1.0` in current scoped runs, above target acceptance band
    (`0.70` to `0.90`), indicating intervals are likely too wide.
- Open risk:
  - Defects P1/P2 can materially distort labels/features. Current gains are not
    considered decision-final until fixes and re-evaluation.

## Model-Family Sweep Findings (Current Branch)
Runs executed on 2025 fold for fast comparison:
- `poisson`
- `elastic_net`
- `hist_gradient_boosting`
- `xgboost` (top100 only; top300 run was compute-heavy in interactive window)

Observed pattern:
- `elastic_net`, `hist_gradient_boosting`, and `xgboost` improved count MAEs
  over `poisson` on top100.
- `hist_gradient_boosting` and `elastic_net` improved count MAEs over `poisson`
  on top300.
- All tested models retained:
  - `hit_rate_invalid_predictions = 0`
  - `hit_rate_extreme_fraction = 0`
  - `hit_rate_coverage = 1.0` (still over-wide).

## Testing Methodology
- Harness script: `/tmp/run_phase15a_e2e_eval.py`
- Compared outputs:
  - current branch (`feat/phase1-5-mlb-projection-quality-hardening`)
  - baseline `main` in worktree (`/tmp/sportsbalf-main`)
- Metrics captured:
  - `hit_rate_mae`
  - `hits_mae`
  - `plate_appearances_mae`
  - `hit_rate_coverage`
  - `hit_rate_extreme_fraction`
  - `hit_rate_invalid_predictions`
- Additional artifacts:
  - fold metric tables
  - red-flag summary tables
  - worst-error lists

## Repro Commands
Run scoped E2E for one dataset on the current branch checkout:

```bash
cd /Users/jbrys/sportsbalf
PYTHONPATH=. ./.venv/bin/python /tmp/run_phase15a_e2e_eval.py \
  --dataset /tmp/batter_games_2021_2025_top100.parquet \
  --output-dir /tmp/e2e_phase15a_current_top100 \
  --model-name poisson \
  --season-min 2025 \
  --season-max 2025
```

Run the baseline from the `main` worktree:

```bash
cd /tmp/sportsbalf-main
PYTHONPATH=. /Users/jbrys/sportsbalf/.venv/bin/python /tmp/run_phase15a_e2e_eval.py \
  --dataset /tmp/batter_games_2021_2025_top100.parquet \
  --output-dir /tmp/e2e_phase15a_main_top100 \
  --model-name poisson \
  --season-min 2025 \
  --season-max 2025
```

## Artifact Paths
- Current scoped outputs:
  - `/tmp/e2e_phase15a_current_top100`
  - `/tmp/e2e_phase15a_current_top300`
- Baseline scoped outputs:
  - `/tmp/e2e_phase15a_main_top100`
  - `/tmp/e2e_phase15a_main_top300`
- Sweep outputs:
  - `/tmp/e2e_phase15a_sweep_results.json`
  - `/tmp/e2e_phase15a_current_top100_xgboost`

## Pre-Fix Acceptance Criteria for Next Iteration
After fixing P1/P2, rerun scoped evaluations and require:
- `hit_rate`, `hits`, and `plate_appearances` MAE remain better than baseline.
- `hit_rate_invalid_predictions = 0`.
- `hit_rate_extreme_fraction` stays near zero.
- Coverage calibrated into target band `0.70` to `0.90` for scoped folds.

Then run wider/full-population evaluation before final sign-off.

## Follow-up Plan
The next planned iteration for improving hit-rate-family point accuracy and
uncertainty calibration is documented in:
- `docs/plans/planned/fantasy-rankings/phase-1.5b-hit-rate-accuracy-calibration-plan.md`
