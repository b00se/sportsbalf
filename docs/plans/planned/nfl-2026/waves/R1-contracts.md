# Wave R1 — Contracts

Status: Contract implementation complete; R1.2 human launch gate pending.

## Packets

- R1.1: golden 12-column UD rankings CSV importer/exporter contract.
- R1.2: human-controlled proof that row order and position caps govern Autopilot.
- R1.3: half-PPR/full-PPR, roster, draft, field, cap, and payout configuration.
- R1.4: 27-column exposure CSV parser and exact user-entry reconstruction.
- R1.5: deterministic snapshot manifest and provenance.

## Parallelism

R1.1, R1.3, and R1.4 may run concurrently with disjoint files. R1.5 follows R1.1/R1.3. R1.2 is a human gate after R1.1.

## Gate

All fixture contracts must pass offline, payouts must reconcile, source CSV
values must survive reordering, manifests must be deterministic, and the real
human-operated Autopilot proof must succeed. Implementation tests do not close
the R1.2 launch gate.
