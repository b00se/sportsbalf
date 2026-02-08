# Plan Completion Record: MLB Multi-Stat Expansion Plan

Status: Implemented

## Metadata
- Plan file: `docs/plans/implemented/mlb-multi-stat-expansion-plan.md`
- Owner: Codex
- Completion date: 2026-02-08
- Related PRs: local branch `roadmap/complete-partial-plans` implementation set
- Related issues/tickets: N/A

## Original Scope
- Summary of original objective:
  - ship MLB multi-stat pitcher-prop support (`outs_recorded`, `earned_runs`,
    `hits_allowed`, `bb_allowed`) with shared core orchestration and offseason-safe
    lines-missing behavior.
- Explicitly out of scope:
  - new external dependencies and non-MLB sport expansion.

## Done Checklist
- [x] All in-scope deliverables implemented.
- [x] Public interfaces/contracts updated and documented.
- [x] Backward compatibility validated (or intentionally broken with migration notes).
- [x] Offline tests added/updated.
- [x] `.venv/bin/pytest -q` passes.
- [x] `.venv/bin/ruff check .` passes.
- [x] Required docs/config updates completed.

## Acceptance Criteria Mapping
1. Criterion: All four new MLB stat pipelines train and backtest offline.
   - Evidence: integration tests in
     `tests/integration/test_mlb_multi_stat_pitcher_props_pipeline.py`.
2. Criterion: Missing lines do not fail offseason runs.
   - Evidence: lines-missing tests in
     `tests/integration/test_mlb_outs_recorded_pipeline.py`.
3. Criterion: Internal rolling park factors used for supported stats.
   - Evidence: descriptor/stat park factor plumbing in
     `src/mlb/pitcher_props/descriptors.py` and
     `src/mlb/pitcher_props/park_factors.py`.
4. Criterion: Full Statcast-derived batter foundation exists.
   - Evidence: `build_batter_game_table` and persistence paths in
     `src/mlb/pitcher_props/data.py`.
5. Criterion: Strikeouts pipeline backward compatible.
   - Evidence: compatibility shim remains in `src/mlb/pipeline.py`,
     routing now delegates to shared core; schema integration tests pass.

## Validation Evidence
- Test commands and summary:
  - `.venv/bin/ruff check .` (pass)
  - `.venv/bin/pytest -q` (98 passed)
- Integration/e2e evidence:
  - MLB strikeouts and multi-stat integration tests green.
- Artifacts produced (paths):
  - model and leaderboard artifact paths in `config/mlb.yaml`.
- Logs/metrics snapshots (if applicable):
  - label quality reporting now includes high-fidelity and fallback share columns.

## Follow-ups
- Deferred work:
  - none for this plan.
- Known gaps/risks:
  - earned-runs high-fidelity feed quality depends on configured source quality.
- Suggested next plan:
  - `docs/plans/planned/mlb-pybaseball-live-features-plan.md`.

## Final Status
- Status: `implemented`
- Notes:
  - moved strikeouts execution onto the shared pitcher-prop core path via
    engine registration while keeping compatibility wrapper for existing callers.
