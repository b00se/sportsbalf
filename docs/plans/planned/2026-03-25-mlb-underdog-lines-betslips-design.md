# MLB Underdog Lines and Betslips Design

Status: Planned

Date: 2026-03-25

## Goal
Add a shadow-mode MLB workflow that fetches live Underdog pitcher-prop lines for
 all supported modeled stats, scores the available props with existing MLB
 pitcher-prop pipelines, and generates JSON betslips from the combined candidate
 pool.

## Scope
### In scope
- Live Underdog ingestion for all currently modeled MLB pitcher props:
  - `strikeouts`
  - `outs_recorded`
  - `earned_runs`
  - `hits_allowed`
  - `bb_allowed`
- Mixed-stat slip generation across the full MLB pitcher-prop slate.
- Same-pitcher stacking by default.
- JSON slip outputs only.
- Shadow/manual workflow only.

### Out of scope
- Auto-submit or account automation.
- Batter props.
- New modeled MLB stats.
- Correlation modeling beyond explicit slip validity rules.

## Current Repo State
- NFL already has a working Underdog ingestion helper in
  `src/nfl/data/underdog.py`.
- MLB pitcher-prop scoring already exists per stat via
  `pipeline/main.py` -> `src/pipeline/engine.py` ->
  `src/mlb/pitcher_props/pipeline.py`.
- MLB slip generation exists, but it is strikeouts-specific in
  `src/mlb/slips.py` and the wrapper script `scripts/build_betslips.py`.
- MLB currently expects prepared `data/lines/*.csv` inputs and does not yet have
  a live Underdog line importer.

## Design Decisions
### 1) Use a unified MLB slate orchestrator over existing stat pipelines
Do not build a brand-new MLB modeling pipeline. Reuse the existing stat-specific
 MLB pitcher-prop pipelines as scoring engines and add a thin orchestration layer
 above them.

### 2) Fetch all supported MLB pitcher props from Underdog
Add an MLB Underdog ingestion module that:
- discovers or uses configured `PickemStat_*` ids per stat,
- fetches raw payloads from the same Underdog API shape used by NFL,
- normalizes the live lines into stat-specific frames compatible with the
  current MLB line loaders,
- writes dated line snapshots for shadow/manual runs.

### 3) Normalize scored props into a common candidate-leg schema
Each scored prop row should be converted into a common schema that includes:
- player identity,
- team,
- opponent,
- scheduled date/time if available,
- stat id,
- line value,
- side (`over`/`under`),
- probabilities,
- EV,
- payout fields,
- model output metadata.

This removes the strikeouts-specific assumptions from slip construction.

### 4) Allow same-pitcher stacking by default
Underdog now supports stacking one player's stats, so same-pitcher legs are
 valid candidates by default.

### 5) Enforce slip validity with a 2-player / 2-team minimum
Every emitted slip must include:
- at least 2 distinct players,
- at least 2 distinct teams.

This is a hard rule even when same-pitcher stacks are present.

### 6) Keep the first version shadow-only
The first version should:
- fetch live lines,
- score available props,
- generate JSON slips,
- write artifacts for review.

It should not place picks or automate any downstream submission flow.

## Data Flow
1. Fetch live Underdog MLB pitcher-prop payloads for all configured stats.
2. Normalize the payloads into stat-specific live line frames.
3. Persist dated line snapshots under `data/lines/`.
4. Run the existing MLB stat pipelines against those line files.
5. Normalize all scored rows into a combined candidate-leg table.
6. Build mixed slips from that table under the configured slip rules.
7. Write JSON slip artifacts and a run summary.

## Error Handling
- If a stat id is missing or Underdog returns no lines for that stat, skip that
  stat and continue.
- If a stat payload shape is invalid, fail that stat loudly and continue with the
  others.
- If a stat pipeline fails, record it in the run summary and continue unless all
  stats fail.
- If the candidate pool cannot form valid slips under the 2-player / 2-team
  rule, emit zero slips with a clear explanation.
- Keep tests offline by mocking Underdog payloads and pipeline outputs.

## Config Direction
The implementation should keep unstable provider ids and operator choices out of
deep code paths. Underdog stat ids and orchestration defaults should live in
config and/or top-level constants that are easy to update.

## Acceptance Criteria
- The repo can fetch MLB live Underdog pitcher-prop lines for all supported
  modeled stats.
- The repo can score all available props for a target slate with existing MLB
  models.
- The repo can generate JSON slips containing mixed stats and same-pitcher
  stacks.
- No emitted slip violates the 2-player / 2-team minimum rule.
- Offline tests cover ingestion normalization, candidate-leg normalization, and
  slip-validation behavior.
