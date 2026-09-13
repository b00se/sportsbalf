# R4A: Simulation wave closeout

Status: Complete.

## Delivered packets

- R4.1: deterministic correlated joint player outcomes, with visible
  non-promoted direct-FP provenance.
- R4.2: exact offline four/six-entrant serpentine Autopilot draft simulation
  with legal roster and cap enforcement.
- R4.3: deterministic archived-Underdog-ADP opponent rank scenarios.
- R4.4: approximately 500-entry offline field simulation with explicit ties
  and exact Decimal payout allocation.

Each packet has a dedicated evidence record and an independent Luna-high
review. The R2 fixture-byte repair commits included in this branch preserve
strict raw-byte integrity checks across Windows LF/CRLF checkouts.

## Wave gate

On the R4A branch, the complete offline suite passed:

```text
.venv\\Scripts\\pytest.exe -q -n 16
829 passed, 388 warnings in 53.22s
.venv\\Scripts\\ruff.exe check .
git diff --check 007b1d9..HEAD
```

Fixed-seed behavior, marginals/correlations, roster legality, explicit ties,
and prize-pool cent conservation are covered by the packet suites. No draft,
upload, entry, transaction, or predictive-edge automation was added.

## Next-wave boundary

R4B is ready, but must begin in a fresh task. Its launch and controlled
Autopilot-proof gate remain in force.
