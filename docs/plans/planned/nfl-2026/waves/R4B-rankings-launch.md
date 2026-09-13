# Wave R4B — Optimization, Export, and Launch

Status: Ready for a fresh task. Do not begin from this closeout packet.

## Packets

- R4.5: joint ranking and position-cap optimizer.
- R4.6: value-preserving UD exporter and human-readable audit report.
- R4.7: one-command weekly CLI.
- R4.8: real-slate shadow run and launch gate, including an end-to-end
  projections-to-rankings-to-CSV exercise on an archived or user-supplied
  slate fixture.

## Gate

The optimizer is deterministic/legal, finalists use fresh seeds, custom rankings are robustly non-inferior or labeled experimental, CSV preservation passes, stale/ambiguous inputs block readiness, and the controlled Autopilot proof succeeds. The R4.8 shadow run must also exercise the complete fail-closed path from reconciled, as-of direct-FP projections through optimized rankings to a lossless 12-column Underdog CSV export. It must retain source hashes and run provenance, block unresolved identity or availability inputs before export, and label the result as experimental whenever its projection input remains unpromoted.
