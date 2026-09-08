# R2 Data, Identity, Availability, and Features Closeout

Status: Accepted; pending merge.

R2's offline contracts are accepted on `codex/nfl-r2-data-identity`. This
record is the current closeout authority; older diagnosis entries remain in
their packet evidence as historical audit material. The user approved the
private personal-project snapshot-fixture policy and the R2.5 timestamp-gated
feature-store simplification.

## Accepted packets

| Packet | Accepted evidence | Final commit or review record |
|---|---|---|
| R2.1 provider capability and freshness | [R2.1 audit](R2.1-nflreadpy-audit.md) | Provider fixture assertion `14f5257`; accepted review record `9643532` |
| R2.2 canonical/scoped identity | [R2.2 identity graph](R2.2-identity-graph.md) | Consolidation `799fd2e`; independent Luna-high accepted |
| R2.3 availability export gate | [R2.3 availability tournament](R2.3-availability-tournament.md) | Acceptance proof `ef0f047`; independent Luna-high accepted |
| R2.4 projection source scorer | [R2.4 scorer closeout](R2.4-scorer-repair-loop-1.md) | Hard-link protection `860fe02`; review evidence `e37f324` |
| R2.5 as-of feature store | [R2.5 feature store](R2.5-asof-feature-store.md) | Simplification `f79fcc7`; closure `f68a3d0`; independent Luna-high accepted |

## Wave-gate evidence

- Material player identity resolves through `nflverse_id`; appearance and
  Underdog identifiers remain explicitly scoped references (R2.2).
- Provider and projection sources record freshness, policy, and provenance;
  the personal non-commercial fixture policy is explicit (R2.1 and R2.4).
- Missing, stale, unknown, or high-impact conflicting availability blocks the
  unattended export before it changes the destination (R2.3).
- Feature selection uses only source observations at or before each explicit
  cutoff, so later rows cannot change earlier features (R2.5).

## Final validation

- Post-consolidation full offline suite: `.venv\\Scripts\\pytest.exe -q` —
  `530 passed` (173 pre-existing warnings).
- Historical R2.5 closure validation at `f68a3d0`: `533 passed`; that count
  predates the reviewed feature-store consolidation and is retained in the
  R2.5 packet evidence rather than used as this branch's final result.
- Full lint: `.venv\\Scripts\\ruff.exe check .` — passed.
- Whitespace: `git diff --check` — passed.

R3 is the next wave, but it must not start until this branch is merged and a
fresh task begins from a clean working tree.
