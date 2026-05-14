# MLB Proof-Run Hardening Plan

Status: Planned

Date: 2026-05-14

## Goal
Harden the existing MLB live shadow workflow so it can explicitly distinguish a
valid proof run, a healthy no-play slate, a debug-only subset run, and a failed
run caused by runtime or sanity issues.

## Why this exists
The repo already supports MLB live Underdog shadow runs, but the current CLI has
two operator gaps:

1. It requires manual `--stat-id` input even though config already stores live
   stat ids.
2. It writes runtime summaries without an explicit verdict about whether the run
   was trustworthy, degraded, or suitable only for debugging.

This plan adds operator-facing correctness gates without mixing in model-refresh
or retraining work.

## Scope
### In scope
- Extend `scripts/build_mlb_live_betslips.py`; do not create a second command.
- Add explicit run modes:
  - `proof` (default)
  - `debug`
- Default live stat-id sourcing from `mlb.live_underdog.stat_ids` in
  `config/mlb.yaml`.
- Keep CLI `--stat-id` values as overrides.
- Require full five-market stat-id coverage in `proof` mode.
- Allow subset stat-id overrides in `debug` mode.
- Add explicit verdict/output semantics:
  - `passed`
  - `failed`
  - `no_play_slate`
- Add structured failure reasons for failed proof runs:
  - `missing_required_stat_ids`
  - `runtime_failure`
  - `stat_mix_gate_failed`
  - `confidence_gate_failed`
- Add proof-run evidence to the summary JSON and CLI output.
- Add hard sanity-gate enforcement in `proof` mode and warning-only reporting in
  `debug` mode.

### Out of scope
- Data refresh or retraining for current-season MLB models.
- Historical calibration of gate thresholds.
- Auto-submission or account automation.
- New MLB prop markets.

## Domain decisions locked
- Proof-run hardening and freshness/retraining are separate plans.
- A proof run validates workflow correctness; it does not imply betting
  readiness.
- A no-play slate is a successful proof-run outcome when the workflow and
  sanity checks are healthy.
- Proof mode requires all currently supported MLB pitcher-prop markets.
- Debug mode exists for subset runs and targeted provider/model investigation.
- Proof and debug runs share one summary schema with an explicit `mode` field.

## Initial sanity gates
### Stat-Mix Gate
- Inspect the slip-eligible pool, not the full scored slate.
- Fail `proof` mode when any one `stat_id` exceeds 70% of the slip-eligible
  pool.
- Report as warning only in `debug` mode.

### Confidence Gate
- Inspect the slip-eligible pool, not the full scored slate.
- Fail `proof` mode when any slip-eligible leg has:
  - `prob <= 0.20`
  - `prob >= 0.80`
  - `ev >= 0.35`
- Report as warning only in `debug` mode.
- Treat these thresholds as initial defaults pending later historical
  calibration.

## Summary contract changes
Add explicit proof-run evidence to the existing summary shape:
- `mode`
- `outcome`
- `failure_reasons`
- `completed_stats`
- `skipped_stats`
- `failed_stats`
- `combined_rows`
- `slip_eligible_rows`
- `stat_mix`
- `probability_extremes`
- `ev_extreme`
- `slip_counts`

The goal is that an operator can tell whether the run:
- passed cleanly,
- passed as a no-play slate,
- failed due to missing required coverage,
- failed due to runtime breakage,
- failed due to sanity-gate triggers,

without opening raw JSON slip artifacts by hand.

## Execution slices
### Slice 1: Config-backed stat-id defaults and run-mode contract
Files:
- Modify: `scripts/build_mlb_live_betslips.py`
- Review: `src/core/config.py`
- Test: `tests/test_build_mlb_live_betslips.py`
- Modify: `README.md`

Behavior:
- Add explicit run mode argument with default `proof`.
- Load stat ids from `mlb.live_underdog.stat_ids` when CLI overrides are not
  supplied.
- In `proof` mode, hard-fail before fetch/scoring if full supported-market
  coverage is missing after config + CLI override resolution.
- In `debug` mode, allow subset runs.

Validation:
- Proof mode uses config-backed stat ids by default.
- CLI overrides replace configured ids for matching stats.
- Proof mode rejects missing required stat ids.
- Debug mode allows subset stat-id runs.

### Slice 2: Proof-run verdict model and summary schema
Files:
- Modify: `scripts/build_mlb_live_betslips.py`
- Test: `tests/test_build_mlb_live_betslips.py`

Behavior:
- Compute and persist one explicit summary schema for both proof and debug
  modes.
- Add:
  - `mode`
  - `outcome`
  - `failure_reasons`
  - proof-run evidence payload fields
- Distinguish:
  - `passed`
  - `failed`
  - `no_play_slate`

Validation:
- Summary JSON includes mode and explicit outcome.
- Failed proof runs include structured failure reasons.
- Healthy no-play outcomes are explicit and not mislabeled as failures.

### Slice 3: Slip-eligible pool derivation for operator checks
Files:
- Modify: `scripts/build_mlb_live_betslips.py`
- Review: `src/mlb/slips.py`
- Test: `tests/test_build_mlb_live_betslips.py`

Behavior:
- Derive a stable slip-eligible pool from the same normalized candidate-leg
  surface used for slip construction.
- Record pool size and pool-level aggregates in proof-run evidence.

Validation:
- Proof-run evidence reports `slip_eligible_rows`.
- Stat mix, probability bounds, and EV bounds are computed from that pool
  rather than the full scored slate.

### Slice 4: Stat-Mix Gate and Confidence Gate
Files:
- Modify: `scripts/build_mlb_live_betslips.py`
- Test: `tests/test_build_mlb_live_betslips.py`
- Consider: targeted new regression test file if CLI tests become too dense

Behavior:
- Implement hard proof-mode failures for:
  - one-stat dominance above 70% of the slip-eligible pool
  - probability bounds outside `[0.20, 0.80]`
  - EV bound at or above `0.35`
- In debug mode, keep the same evidence but downgrade gate trips to warnings.

Validation:
- Proof mode fails with `stat_mix_gate_failed` when dominance threshold is
  exceeded.
- Proof mode fails with `confidence_gate_failed` when probability/EV thresholds
  are exceeded.
- Debug mode preserves output artifacts and summary evidence while not claiming
  proof-run validity.

### Slice 5: Runtime vs no-play semantics
Files:
- Modify: `scripts/build_mlb_live_betslips.py`
- Test: `tests/test_build_mlb_live_betslips.py`

Behavior:
- Treat partial stat failures as acceptable degradation unless they prevent:
  - a non-empty slip-eligible pool, and
  - at least one valid slip artifact
- Distinguish between:
  - `runtime_failure`
  - `no_play_slate`

Validation:
- Runtime failures are emitted only when the workflow cannot produce a usable
  actionable surface.
- Healthy abstention is emitted as `no_play_slate`.

### Slice 6: Docs and operator usage
Files:
- Modify: `README.md`
- Modify: `docs/config-schema.md`
- Modify: `docs/contracts.md`
- Modify: `docs/architecture.md`

Behavior:
- Document proof/debug modes.
- Document config-backed stat-id defaults and CLI override behavior.
- Document summary/verdict semantics and initial sanity gates.

## TDD guidance
For each slice:
1. Add or update failing targeted tests first.
2. Implement the smallest change that passes.
3. Re-run targeted tests.
4. Re-run repo-level verification at the end.

## Acceptance criteria
- The existing MLB live shadow CLI defaults to `proof` mode.
- Proof mode loads MLB live stat ids from config by default.
- Proof mode hard-fails when full supported-market stat-id coverage is missing.
- Debug mode allows subset stat-id runs.
- Proof and debug runs share one summary schema with an explicit `mode`.
- Proof mode emits explicit outcomes:
  - `passed`
  - `failed`
  - `no_play_slate`
- Failed proof runs emit structured failure reasons from the agreed initial set.
- Proof-mode summary evidence includes slip-eligible pool shape plus stat-mix,
  probability, and EV diagnostics.
- Stat-mix and confidence gates hard-fail proof mode and warn in debug mode.
- Healthy abstention is surfaced explicitly as `no_play_slate`.

## Final validation
Run at minimum:

```bash
.venv/bin/pytest -q tests/test_build_mlb_live_betslips.py
.venv/bin/pytest -q
.venv/bin/ruff check .
```
