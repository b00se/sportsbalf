# Wave R4A — Simulation

Status: Planned; not started.

## Packets

- R4.1: correlated joint player outcome simulator.
- R4.2: exact four/six-entrant Autopilot draft engine.
- R4.3: stochastic opponent model around archived UD ADP.
- R4.4: approximately 500-entry field and exact payout simulator.

## Parallelism

R4.1 and R4.2 may run concurrently. R4.3 follows R4.2; R4.4 integrates all three.

## Gate

Fixed seeds reproduce results, statistical marginals/correlations meet tolerances, every feasible roster is legal, ties are explicit, and every simulated prize pool conserves cash within one cent.
