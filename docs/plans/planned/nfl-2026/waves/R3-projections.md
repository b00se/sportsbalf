# Wave R3 — Bottom-Up Projections

Status: Complete and merged; no model promotion. The operator accepted the
direct-FP baseline as the R4 input on 2026-09-13; R4A may begin in a fresh task.

## Packets

- R3.1: team plays, pass/rush volume, points, and scoring opportunities.
- R3.2: QB passing/rushing components; salvage reviewed QB-attempt work.
- R3.3: RB availability, opportunity, yards, receptions, and touchdowns.
- R3.4: WR/TE routes, targets, receptions, yards, touchdowns, and rare rushing.
- R3.5: conservative rookie/new-role priors.
- R3.6: availability mixture distributions.
- R3.7: exact scoring derivation and direct-FP benchmark.
- R3.8: nested walk-forward internal/UD/consensus ensemble tournament.

## Parallelism

R3.1-R3.4 may use separate modules concurrently. Integrate before R3.5-R3.7. R3.8 runs after all component contracts stabilize.

## Gate

The promotion gate completed with no promoted candidate: all evaluated models
must beat or tie declared baselines under rolling-origin evaluation, constraints
must reconcile, uncertainty must be calibrated or honestly reported, and blend
selection must never see its test folds. The operator accepted the resulting
direct-FP baseline for R4 simulation input; this is not a predictive-model
promotion or an edge claim. See [R3.23 closeout](../evidence/R3.23-wave-closeout.md),
[R3.24 bakeoff](../evidence/R3.24-weekly-regressor-bakeoff.md), and
[R3.25 operator handoff](../evidence/R3.25-operator-handoff.md).
