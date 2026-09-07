# Wave R3 — Bottom-Up Projections

Status: Planned; not started.

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

All promoted models beat or tie declared baselines under rolling-origin evaluation, constraints reconcile, uncertainty is calibrated/reported, and blend selection never sees its test folds.
