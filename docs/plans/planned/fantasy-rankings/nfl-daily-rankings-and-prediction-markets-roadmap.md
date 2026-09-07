# NFL Daily Rankings and Prediction Markets Roadmap

Status: Planned

Last updated: 2026-09-06

## 1. Outcome

Revitalize sportsbalf in two ordered tracks:

1. Ship a weekly, slate-specific Underdog NFL Daily rankings workflow first.
2. Add a pregame NFL Prediction Markets decision system for the six standard game contracts (both moneyline sides, both spread sides, over, and under), with an interface that can later accept live game state.

The rankings launch is quality-gated, not Week-1-at-all-costs. Prediction Markets work begins after the rankings workflow produces a verified upload.

## 2. Product Decisions From the 2026-09-06 Interview

### 2.1 Rankings

- The operator manually downloads one UD rankings CSV per slate and manually uploads the generated CSV.
- V1 assumes every pick is made by Autopilot.
- The only controls are one overall row ordering and maximum counts by position.
- One rankings policy is used for every entry on a slate. Entry count is user-controlled.
- Default scoring is half PPR unless the contest explicitly states full PPR.
- Main/grouped slates use six roster slots: QB 1, RB 1, WR 2, FLEX 1 (RB/WR/TE), TE 1.
- Single-game slates use four SFLEX slots and may include kickers. Implement only after the standard-slate vertical slice unless it is the next available slate.
- Draft rooms contain commonly four or six entrants; each entrant makes the contest's number of roster selections.
- The tournament field is approximately 500 entries, up to 15 entries per person, with a top-heavy payout through roughly the top 25%. Exact slate terms remain config inputs.
- Primary optimizer objective: expected net tournament payout. Secondary diagnostics: cash rate, top-1%, first-place rate, variance, and drawdown proxy.
- Entry sizing and bankroll advice are out of scope. Entry count is a simulation input only.
- Default UD ADP/projected points may be used as features, priors, ensemble inputs, or benchmarks, but their role must be selected by walk-forward evidence.
- Component-stat models are primary. Direct fantasy-point regression is a benchmark or residual model only.
- Confirmed OUT/IR/inactive players are excluded; uncertain availability is probability-weighted and prominently reported.
- Rookies use conservative draft-capital/role/UD priors and wider uncertainty. A full college-to-NFL rookie model is post-launch.

### 2.2 Prediction Markets

- Launch priority is pregame; live game-state updates are a later adapter, not a rewrite.
- Scope is the 6box only: two moneyline sides, two spread sides, over, and under.
- UD execution is functionally taker-only. The executable UD quote and displayed all-in cost/fee/payout are authoritative.
- Kalshi, Nadex, and UDX are opaque venue metadata. Kalshi data may be a movement/consensus proxy, not an assumed executable price.
- Use a hybrid model: independent joint score distribution plus de-vigged public market consensus.
- Target output combines an evidence-backed watchlist with fair-probability ranges and conditional maximum all-in cost.
- Manual transcription of six quotes is not an acceptable normal workflow. Investigate an authorized, read-only UD API.
- No order placement or entry automation is in scope.

### 2.3 Deferred Work

- Automated rankings download/upload.
- Draft entry or market order automation.
- Same-day repeated rankings refresh optimization.
- Authenticated scraping of completed contest score, rank, and payout. Run only a feasibility spike after launch.
- Full rookie-transition model.
- Live/in-game Prediction Markets decisions.
- Best Ball and non-NFL daily sports.

## 3. Repository Baseline and R0 Record

R0 recovery, salvage, Python 3.11 baseline, tooling modernization, and
closeout are complete and merged to `main`. The authoritative records are in
[the NFL 2026 evidence directory](../nfl-2026/evidence/), including the
[closeout](../nfl-2026/evidence/R0.closeout.md). The raw pre-R0 QB work remains
preserved at `refs/recovery/r0.1-qb`; it must be reimplemented only where later
wave contracts authorize it, never replayed verbatim.

Future waves start from current `main`, with a clean working tree and a fresh
branch for the active wave. Never modify `data/`, `notebooks/`, `models/`, or
`betslips/`. Runtime artifacts belong in configured ignored output paths. Tests
must be offline.

## 4. Target Architecture

```text
UD slate CSV + contest config + public source snapshots
                         |
                         v
       identity / availability / slate validation
                         |
                         v
       leakage-safe component-stat projections
                         |
                         v
     correlated player fantasy-score simulations
                         |
                         v
  Autopilot draft simulator + field/payout simulator
                         |
                         v
 ranking/cap optimizer -> upload CSV + audit report

Pregame team data + public consensus + UD 6box quotes
                         |
                         v
 independent joint-score model + consensus calibration
                         |
                         v
 fair ranges / action zones / evidence report
                         |
             future game-state updater
```

Reuse `src/fantasy/core` projection primitives. Put NFL-specific logic under `src/fantasy/adapters/nfl/`, contest behavior under `src/fantasy/contests/`, and UD serialization/ingestion under provider adapters. Do not add NFL-only fields to shared contracts unless a second sport can use them.

## 5. Swarm Execution Protocol

Each task below is one bounded Luna-agent packet, normally 30-120 minutes. An orchestrator may run tasks in parallel only when their dependencies and owned files do not overlap.

Every packet prompt must include:

- Task ID, objective, dependency commit hashes, and exact allowed files.
- Relevant contract/fixture paths and explicit non-goals.
- Required test command and artifact/evidence to return.
- Instruction to inspect existing code before editing and preserve unrelated changes.
- A loop: implement, run focused tests, diagnose every failure, patch minimally, rerun, then run stated broader gates.
- Stop conditions: ambiguous provider behavior, required network in offline tests, leakage, schema drift, or a requested change outside owned files.

An agent may report complete only when all exit criteria pass. “Code written” is not completion. Evidence must include commands, exit codes, test counts, changed files, and any measured metrics. Flaky tests must pass three consecutive runs before acceptance. Commit one task per commit using `codex/` branches; integrate in dependency order.

## 6. Phase R0 — Complete

R0 is complete and merged to `main`. Its task-level provenance is retained in
the [R0 evidence directory](../nfl-2026/evidence/); the closeout links the raw
recovery ref, branch audit, offline baseline, and tooling record. R0 is not an
active work queue. Future work begins with R1 from current `main`.

## 7. Phase R1 — Lock Provider and Contest Contracts

### R1.1 Golden UD rankings fixture and schema contract

**Depends on:** R0.4
**Owns:** `src/fantasy/exporters/`, `tests/testdata/`, focused tests

Create a sanitized miniature of the supplied 12-column CSV. The importer must preserve column names/order, IDs, nulls, and source values. Export changes row order only in V1.

**Exit criteria**

- Exact schema: `id, playerId, firstName, lastName, adp, projectedPoints, salary, positionRank, slotName, teamName, lineupStatus, byeWeek`.
- Round-trip without ranking changes is semantically identical and byte-stable after an explicitly documented newline policy.
- Duplicate IDs, missing IDs, unknown positions, malformed numerics, and zero-player slates fail with actionable messages.
- Input file is never overwritten.

### R1.2 Controlled Autopilot ordering proof

**Depends on:** R1.1
**Owns:** evidence document/fixture only

Generate an obviously reordered validation CSV while preserving all cell values. The user uploads it to a low-risk controlled draft and observes the first eligible Autopilot pick.

**Exit criteria**

- Observed pick equals the first row eligible under roster/cap constraints.
- Repeat once after a position cap blocks the highest row; the next eligible row is selected.
- If either fails, inspect `salary`, `projectedPoints`, and other ordering fields and revise R1.1 before proceeding.

This is a hard launch gate.

### R1.3 Contest/scoring configuration

**Depends on:** R0.4
**Owns:** `config/fantasy/`, shared config validators/tests

Represent half-PPR standard slate, explicit full-PPR override, single-game SFLEX, draft entrants, rounds, position caps, field size, entry fee, and payout ladder. Unknown scoring must fail closed.

**Exit criteria**

- The 2025 Royale example and 2026 Wed/Thu four-entrant/six-round format validate from fixtures.
- Roster feasibility is proven before simulation.
- Payouts reconcile to configured prize pool within one cent.
- Invalid cap/roster combinations and payout totals fail clearly.

### R1.4 Exposure-export contract

**Depends on:** R0.4
**Owns:** ingest adapter and offline tests

Parse the 27-column emailed exposure CSV. Reconstruct each user entry by `Draft Entry`, order picks, and retain draft/pool metadata.

**Exit criteria**

- The supplied example yields 36 selections, six complete entries, six unique drafts, four-player draft size, and six picks per entry.
- Duplicate/missing picks and mixed pool IDs are detected.
- Nullable tournament fields remain nullable rather than becoming zeros.

### R1.5 Snapshot manifest and provenance

**Depends on:** R1.1, R1.3
**Owns:** runtime/provenance utilities and tests

Define immutable manifests containing source hashes, fetch times, contest/scoring config hash, model versions, seed, code commit, and output hashes.

**Exit criteria**

- Same inputs/config/seed yield identical manifest and rankings output.
- Changed input changes snapshot ID.
- Secrets, cookies, and account identifiers are absent.

## 8. Phase R2 — Data and Identity Foundation

### R2.1 nflreadpy provider currency audit

**Depends on:** R0.3-R0.4
**Owns:** `src/nfl/data/providers/`, provider tests

Audit 2026 schedules, player stats, PBP, rosters, depth charts, NGS, participation, and betting-line availability. Network calls live only in runtime adapters; tests use frozen fixtures.

**Exit criteria**

- A capability matrix records history source, fields, seasons, cadence, license, and failure fallback.
- Provider returns typed freshness/skipped-season metadata.
- Partial source failure degrades explicitly; it never silently fabricates current data.

### R2.2 Player/team/game identity graph

**Depends on:** R1.1, R2.1
**Owns:** NFL mapping adapter/tests

Map UD player/appearance/team/game IDs to GSIS/nflverse and consensus-source IDs. Prefer stable IDs; normalized names are a reviewed fallback.

**Exit criteria**

- 100% of slate starters/high-projection players resolve or block export.
- No duplicate provider ID maps to two active players.
- Every fuzzy match records method/confidence and requires a configured threshold.
- Historical team aliases and traded players have tests.

### R2.3 Availability and injury source tournament

**Depends on:** R2.2
**Owns:** availability adapters/tests

Evaluate UD `lineupStatus`, Sleeper, ESPN, roster/depth-chart data, and official reports where accessible. Cache responsibly and honor source licensing. Select primary/cross-check/fallback by observed freshness and accuracy.

**Exit criteria**

- Seven-day shadow comparison records update lag, missingness, disagreements, and player-match coverage.
- Confirmed OUT/IR/inactive rules are deterministic.
- Questionable/doubtful statuses map to configurable play probabilities with provenance.
- Stale/conflicting high-impact status blocks unattended export and appears in the report.

### R2.4 Free projection/consensus source tournament

**Depends on:** R2.2
**Owns:** source adapters, license registry, fixtures

Survey GitHub and free web/API sources. Admit a source only if use is permitted, snapshots are reproducible, player IDs resolve, timestamps exist, and historical evaluation is possible. Do not make scraped consensus a single point of failure.

**Exit criteria**

- Each candidate has license/terms, cadence, schema, coverage, and provenance recorded.
- At least two independent baselines exist: UD default and internal/statistical baseline.
- A source that cannot be legally/reliably snapshotted is excluded with reason.

### R2.5 Leakage-safe game/player feature store

**Depends on:** R2.1-R2.3
**Owns:** `src/fantasy/adapters/nfl/feature_store.py`, tests

Build as-of features for plays, pace, neutral pass rate, opportunity shares, routes/snaps where available, opponent strength, rest, weather, spread/total, role, and availability. All rolling values stop before the target game.

**Exit criteria**

- Perturbing future rows cannot change an earlier feature row.
- Week 1 uses prior-season/hierarchical priors with explicit flags.
- As-of timestamp and source snapshot accompany every inference row.
- Missingness and fallback rates are reported by feature/season.

## 9. Phase R3 — Bottom-Up NFL Projection System

Implement tasks R3.1-R3.4 in parallel after R2.5, with separate owned modules.

### R3.1 Team environment models

Project plays, pass attempts, rush attempts, points, and scoring opportunities as distributions, reconciled so player opportunity cannot exceed team opportunity.

**Exit criteria**

- Rolling-origin folds cover at least three held-out seasons and weekly 2025 where data exists.
- Each promoted model beats a trailing-average baseline on its primary proper loss; otherwise retain the baseline.
- Team/player opportunity reconciliation error is below 0.5% per simulated game.
- Prediction intervals have reported 50%/80% empirical coverage and no impossible negative counts.

### R3.2 QB component models

Model attempts, completions conditional on attempts, pass yards, pass TDs, interceptions, rush attempts/yards/TDs. Salvage useful QB-attempt work after leakage review.

**Exit criteria**

- Walk-forward metrics include MAE/RMSE, count deviance where appropriate, calibration, and baseline deltas.
- No market line or target-game outcome enters training features.
- Generated stats obey completions <= attempts and nonnegative yards/counts.
- Promoted components beat or tie simple baselines across a majority of folds and do not regress the latest season materially (>2% primary loss) without documented approval.

### R3.3 RB component models

Model active probability, snaps/opportunities, carries, targets, receptions, rush/receiving yards, and TDs using hierarchical player/team priors.

**Exit criteria:** same temporal/model gates as R3.2; player carries/targets reconcile to team totals; sparse backups widen uncertainty rather than receive false-zero certainty.

### R3.4 WR/TE component models

Model routes/participation, targets, receptions, receiving yards, TDs, and rare rushing usage. Allow shared architecture with position-specific parameters.

**Exit criteria:** same temporal/model gates as R3.2; receptions <= targets; target shares reconcile; WR and TE calibration reported separately.

### R3.5 Rookie and new-role V1 priors

**Depends on:** R3.1-R3.4
**Owns:** prior module/tests

Use position, draft capital, depth-chart role, limited college descriptors if reliable, and UD/consensus priors. Raw preseason yards/TDs are excluded or separately tested; first-team usage may inform role.

**Exit criteria**

- Every no-history player receives an explicit prior source and uncertainty multiplier.
- Leave-one-rookie-cohort-out tests show no worse primary loss than position replacement priors.
- If evidence is inconclusive, conservative replacement/UD blend remains the default.

### R3.6 Availability-weighted stat distributions

**Depends on:** R2.3, R3.1-R3.5

Represent play probability separately from conditional production. Confirmed inactive players have zero selection eligibility; questionable players retain a mixture distribution.

**Exit criteria**

- Expected value equals `P(active) * E[value | active]` in deterministic tests.
- Report distinguishes inactive risk from performance volatility.
- Conflicting status blocks appear before CSV export.

### R3.7 Derived half-PPR scoring and direct-FP benchmark

**Depends on:** R3.2-R3.6

Derive fantasy points from simulated components. Train a direct fantasy-points benchmark/residual corrector without making it primary.

**Exit criteria**

- Hand-calculated scoring fixtures match exactly, including interceptions/fumbles/2PT.
- Half-PPR default and full-PPR override differ only by reception coefficient.
- Direct-FP or residual model is used only if rolling-origin CRPS/MAE and rank metrics improve after multiple-comparison controls; otherwise its weight is zero.

### R3.8 Projection ensemble tournament

Compare internal-only, UD-only, public-consensus, and blended variants using nested rolling-origin selection.

**Exit criteria**

- Blend weights are fit without test-fold leakage.
- Report includes component loss, fantasy-point CRPS/MAE, Spearman rank correlation, top-k recall, and calibration by position.
- Champion is deterministic under a documented tie-breaker and cannot be promoted for a single lucky week.

## 10. Phase R4 — Correlated Simulation and Rankings Optimization

### R4.1 Joint player outcome simulator

**Depends on:** R3.7-R3.8

Simulate team environments first, then allocate correlated opportunities and production to players. Preserve QB-receiver correlation and teammate competition.

**Exit criteria**

- Fixed seed is bitwise deterministic.
- Marginal simulated means/quantiles match projection inputs within Monte Carlo tolerance.
- Team totals reconcile; invalid stat combinations are zero.
- Historical correlation direction/magnitude is reported and reproduced within predeclared tolerances.

### R4.2 Exact Autopilot draft engine

**Depends on:** R1.2-R1.3

Implement snake order for four/six entrants, roster eligibility, two-team rule, position caps, and highest-ranked-eligible selection.

**Exit criteria**

- Golden drafts cover every draft slot, turn, cap block, flex assignment, and two-team rule.
- No roster is illegal or incomplete when a feasible player pool exists.
- Engine reproduces the controlled real Autopilot proof.

### R4.3 Opponent behavior model

**Depends on:** R4.2, R1.5

Model opponents as stochastic UD-ADP followers with configurable noise, position behavior, and limited correlation preferences. Calibrate forward from archived default rankings and observed pick numbers in exposure exports.

**Exit criteria**

- At least conservative, primary, and adversarial opponent scenarios exist.
- Simulated aggregate pick-position distributions match default ADP by construction and expose sensitivity to noise.
- Rankings are never promoted solely under one assumed opponent model.

### R4.4 Tournament field and payout simulator

**Depends on:** R4.1-R4.3

Generate an approximately 500-entry field by grouping draft rooms, calculate entry scores, ties, rank, and the exact configured payout ladder.

**Exit criteria**

- Prize conservation holds within one cent per simulation.
- Tie handling is explicit and tested.
- Outputs include expected gross/net payout, cash/top-1%/win rates, variance, and Monte Carlo confidence intervals.

### R4.5 Ranking and position-cap optimizer

**Depends on:** R4.4

Search permutations through tractable score transforms/local moves rather than factorial brute force. Jointly search QB/RB/WR/TE caps within legal bounds (platform maxima QB 4, RB 9, WR 10, TE 4; contest feasibility further constrains them).

**Exit criteria**

- Always returns a deterministic legal complete ranking and feasible caps.
- Re-evaluates finalists on fresh seeds not used in search.
- Candidate is non-inferior to default ADP in conservative/adversarial scenarios and has positive expected-net-payout delta in the primary scenario; confidence intervals and inconclusive results are shown.
- If the gate fails, ship the best validated projection order labeled experimental, or retain default ADP; never claim an edge.

### R4.6 Rankings exporter and audit report

**Depends on:** R1.1, R4.5

Reorder original rows only; preserve source values and emit caps separately. Produce Markdown/HTML with model/version, projection ranges, injury conflicts, largest UD disagreements, optimizer evidence, unresolved mappings, and upload checklist.

**Exit criteria**

- Output passes R1.1 schema/round-trip validators.
- Every input player appears exactly once with unchanged cell values.
- OUT/inactive players cannot be Autopilot-selected.
- Report names scoring mode and slate; unknown scoring blocks export.

### R4.7 One-command weekly CLI

**Depends on:** R4.6

Add a command taking source CSV, contest config, output directory, seed, and optional entry count. Stages: validate, snapshot, fetch/cache, project, simulate, optimize, export, verify.

**Exit criteria**

- Offline fixture run completes end-to-end with network disabled.
- Live failures retain the prior valid snapshot but clearly mark it stale; no partial CSV is presented as upload-ready.
- Exit code is nonzero for mapping, scoring, injury, schema, or simulation gate failures.
- Repeated identical runs produce identical outputs.

### R4.8 Week-1 shadow and launch gate

**Depends on:** R4.7

Run against the Wed/Thu four-entrant, six-round standard slate when the Admin contest appears. Archive source/default/our outputs and perform the controlled Autopilot proof.

**Exit criteria**

- Exact contest scoring, roster, field, and payout configuration is reviewed.
- 100% high-impact identity and availability resolution.
- Upload accepted and Autopilot proof passes.
- Report contains no blocking warnings.
- If simulation edge is inconclusive, language remains “validated custom rankings,” not “profitable.”

## 11. Phase R5 — Forward Learning Loop

### R5.1 Weekly snapshot/archive workflow

Archive default rankings before modification, generated rankings/caps, manifests, exposure export, projections, and realized player stats. Do not require scraped contest results.

**Exit criteria**

- Missing weekly artifact is detected.
- PII/account identifiers are excluded or locally redacted.
- Re-running a historical week uses only information available before lock.

### R5.2 Exposure and draft-simulation calibration

Reconstruct user rosters from exposure CSV and compare actual pick positions/exposures with simulator predictions under default and custom rankings.

**Exit criteria**

- Exact user rosters and picks reconstruct without duplicates.
- Report identifies model miss versus draft-engine miss versus unavailable-opponent uncertainty.
- Opponent parameters update only after a minimum sample threshold defined in config.

### R5.3 Projection outcome monitoring

Join official weekly player results and score under the exact contest rules. Track drift and position calibration.

**Exit criteria**

- Every projection snapshot joins or is explicitly unresolved.
- Weekly and cumulative metrics compare internal, UD, and blended variants.
- Champion changes require the same promotion policy as R3.8, not one-week results.

### R5.4 Completed-contest extraction feasibility spike (follow-on)

Inspect authenticated completed-contest pages for read-only score, rank, and payout extraction. No implementation commitment.

**Exit criteria**

- Document accessible fields, stability, authentication boundary, ToS/compliance risk, and rate limits.
- Prototype performs no writes and uses sanitized fixtures.
- Go/no-go decision is explicit. A no-go does not block rankings.

## 12. Phase P0 — Pregame Prediction Markets After Rankings Launch

### P0.1 Contract/rules registry

Capture current NFL moneyline/spread/total contract terms and version them by venue/effective date. Represent overtime, ties, postponements, forfeits, settlement source, tick size, and payout.

**Exit criteria**

- Six contract definitions map to executable predicates over final scores.
- Golden examples cover ties, exact spread pushes/boundaries, overtime, and cancellation contingencies.
- Unknown terms block recommendations.

### P0.2 Read-only UD Predict API discovery spike

Observe public/application network behavior with an ordinary authorized session; do not guess endpoints, use employee-only credentials, or place orders. Compare with existing fantasy-line API patterns.

**Exit criteria**

- Documented endpoint/schema/auth/freshness for event, market, side, line, executable cost, fee, payout, timestamp, and venue where exposed.
- Sanitized offline fixtures and a read-only proof fetch exist.
- If unavailable, declare blocked and test alternative authorized public feeds; manual six-box entry is not the normal product.

### P0.3 Canonical 6box quote adapter

Normalize UD quotes independently of Kalshi/Nadex/UDX venue details. Compute executable break-even from displayed total cost and payout.

**Exit criteria**

- All six sides are paired and internally checked.
- Stale/incomplete/line-mismatched quotes block action labels.
- Fee math matches supplied transaction examples to the cent.

### P0.4 Public consensus adapter tournament

Evaluate authorized free sportsbook/exchange sources, including Kalshi as an optional proxy. Normalize timestamps and remove vig for consensus probabilities.

**Exit criteria**

- At least two independent sources or a documented single-source limitation.
- Source lag, missingness, line mismatch, and outliers are explicit.
- Consensus cannot substitute for the executable UD quote.

### P0.5 Independent joint-score model

Use team strength, EPA/success rate, pace, matchup, rest, travel, weather, injuries, and starting QBs to produce a calibrated joint home/away score distribution.

**Exit criteria**

- Strict season/week walk-forward backtest.
- Report log loss/Brier/calibration for ML and threshold probabilities, plus score/spread/total MAE and CRPS.
- Beats simple home-field/Elo and closing-consensus baselines where claimed; otherwise baseline remains active.

### P0.6 Hybrid calibration and action zones

Blend independent and consensus probabilities using nested out-of-sample calibration. Convert to uncertainty ranges and maximum acceptable all-in costs after taker fee and safety margin.

**Exit criteria**

- Hybrid weights/calibration never see test outcomes.
- Reliability plots and expected calibration error reported by market type.
- Recommendation requires fresh executable UD quote, minimum configured edge after fees, and uncertainty buffer.
- Wording distinguishes fair-value estimate from guaranteed profit.

### P0.7 Pregame report and shadow launch

Rank six sides per game with fair range, UD break-even, discrepancy, supporting signals, invalidation conditions, freshness, and `no edge/watch/actionable` state. No trade automation.

**Exit criteria**

- Four-week or configured minimum shadow period logs every emitted state before outcome.
- No selective deletion of losing calls; manifests are immutable.
- Promotion requires calibration/non-inferiority gates and positive after-fee simulated EV with uncertainty disclosed.

### P0.8 Future live-update interface only

Define, but do not implement, a `GameStateSnapshot` input containing score, clock, possession, field position, timeouts, and material availability/usage events.

**Exit criteria**

- Pregame is representable as initial game state.
- A mock halftime snapshot updates the same downstream 6box contract API without schema changes.
- No live-data dependency enters the pregame launch path.

## 13. Recommended Dependency Waves

1. **Wave 1:** R1.1, R1.3, R1.4 in parallel; then R1.2 and R1.5.
2. **Wave 2:** R2.1 and R2.4 in parallel; then R2.2; then R2.3/R2.5.
3. **Wave 3:** R3.1-R3.4 in parallel; then R3.5-R3.7; then R3.8.
4. **Wave 4:** R4.1 and R4.2 in parallel; then R4.3; then R4.4-R4.7; finally R4.8.
5. **Wave 5:** R5.1-R5.3. R5.4 is independent follow-on.
6. **PM waves:** P0.1/P0.2/P0.4 may run after R4.8; P0.3 follows P0.2; P0.5 follows stable NFL team features; P0.6-P0.7 integrate them; P0.8 closes architecture.

## 14. Global Release Gates

### Rankings release gate

- Clean current-upstream implementation base and lossless recovery proof.
- Full offline tests and lint pass.
- UD row-order and cap behavior proven in a controlled real draft.
- Exact schema preservation and no input overwrite.
- Complete identity/status resolution for material slate players.
- Leakage-safe walk-forward evidence for every promoted component/blend.
- Legal rosters in 100% of at least 100,000 deterministic simulation trials per supported format.
- Optimized policy passes baseline-relative robustness gates or is explicitly labeled experimental.
- One-command output includes upload CSV, caps, manifest, and readable report.

### Prediction Markets release gate

- Versioned settlement rules and canonical 6box mapping.
- Authorized read-only UD quote ingestion with freshness and fee fields.
- Nested out-of-sample hybrid calibration.
- After-fee decision thresholds and uncertainty/no-action states.
- Immutable shadow log; no order placement capability.

## 15. Definition of Excellent

The system is excellent when it is reproducible, honest about uncertainty, and operationally boring. It must prefer a transparent baseline over a complex model that does not win out of sample; refuse stale, ambiguous, or mismatched slates; preserve every source snapshot; and produce the same decision from the same information. Agents should loop on failing evidence, not relax gates to declare success.

## 16. Research References

- Underdog NFL rules supplied by the user on 2026-09-06; snapshot them into test/documentation fixtures during R1.3 rather than depending on mutable webpage text.
- nflverse data releases and availability: https://nflreadr.nflverse.com/reference/nflverse_releases.html
- nflverse update cadence and known injury-feed limitation: https://nflreadr.nflverse.com/articles/nflverse_data_schedule.html
- nflreadpy loaders: https://nflreadpy.nflverse.com/api/load_functions/
- Sleeper read-only API and usage/licensing notice: https://docs.sleeper.com/
- Kalshi public API overview: https://help.kalshi.com/en/articles/13823854-kalshi-api
- Kalshi fee overview: https://help.kalshi.com/en/articles/13823805-fees
- Kalshi NFL spread terms discovered during research: https://kalshi-public-docs.s3.amazonaws.com/contract_terms/NFLQSPREAD.pdf

These references are discovery inputs, not permanent runtime contracts. Each adapter task must snapshot the exact schema/terms it implements and record the effective date.
