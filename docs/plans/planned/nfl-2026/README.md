# NFL 2026 Execution Index

Status: R0 complete; R1 accepted by operator override; R2 accepted pending merge

## Objective

Ship a verified Underdog NFL Daily rankings workflow, then build a pregame 6box Prediction Markets decision system. The authoritative product specification is [the master roadmap](../fantasy-rankings/nfl-daily-rankings-and-prediction-markets-roadmap.md).

## R1 Contract Surfaces

- Lossless 12-column Underdog rankings import/reorder/export and 27-column
  exposure reconstruction are offline fixture contracts.
- Contest configuration is typed and fail-closed: half-PPR is the default,
  full-PPR requires an explicit override, and unknown scoring is rejected.
- Snapshot manifests canonically hash source inputs, configuration, outputs,
  source timestamps, and non-sensitive run metadata.
- R1.4/R1.5 closeout evidence, including the local controlled-CSV manifest, is
  recorded in [R1.4-R1.5-closeout.md](evidence/R1.4-R1.5-closeout.md).
- The controlled Autopilot proof and the operator's upload/order acceptance
  record are documented in
  [R1.2-autopilot-proof.md](evidence/R1.2-autopilot-proof.md).

## Execution

1. Start one fresh Codex task per wave.
2. Use GPT-5.6 Terra at medium effort for the foreman.
3. Use at most two GPT-5.6 Luna implementers concurrently; reserve one slot for an independent Luna-high reviewer.
4. Read only AGENTS.md, decisions, orchestration, the active wave file, and direct dependency evidence.
5. Integrate one task commit at a time and stop at the wave gate.

## Waves

| Wave | Purpose | File |
|---|---|---|
| R0 | Recovery, salvage, and tooling baseline | Complete; see [R0 evidence](evidence/R0.closeout.md) |
| R1 | Provider and contest contracts | [R1](waves/R1-contracts.md) |
| R2 | Data, identity, availability, features | [R2](waves/R2-data-identity.md) |
| R3 | Bottom-up component projections | [R3](waves/R3-projections.md) |
| R4A | Joint and draft simulation | [R4A](waves/R4A-simulation.md) |
| R4B | Rankings optimization and launch | [R4B](waves/R4B-rankings-launch.md) |
| R5 | Forward learning | [R5](waves/R5-forward-learning.md) |

## Global stop rules

- Never alter, discard, or replay the preserved raw QB recovery outside its assigned later-wave contracts.
- Never modify data, notebooks, models, or betslips.
- Tests must work offline.
- Do not weaken a test or acceptance gate to finish.
- Do not claim an edge when confidence intervals are inconclusive.
- Do not automate drafts, uploads, entries, or market orders.
- A task is complete only with committed code, independent review, and evidence.

## Templates

- [Foreman prompt](templates/foreman-prompt.md)
- [Implementer prompt](templates/implementer-prompt.md)
- [Reviewer prompt](templates/reviewer-prompt.md)
- [Completion evidence](templates/completion-evidence.md)

## R0 record and next-wave start point

R0 is complete and merged to `main`. Its recovery, salvage, Python 3.11
baseline, tooling, and closeout records are retained under [evidence/](evidence/).
The authoritative raw QB recovery remains reachable at `refs/recovery/r0.1-qb`;
it is historical evidence, not an implementation starting point.

R2 is accepted on its feature branch and awaits merge. R3 is the next wave,
but it must start only in a fresh task after R2 has merged and the working tree
is clean. Do not begin R3 from this closeout packet.
