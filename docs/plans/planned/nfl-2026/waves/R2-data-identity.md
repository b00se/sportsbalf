# Wave R2 — Data, Identity, Availability, and Features

Status: Accepted; pending merge.

## Packets

- R2.1: nflreadpy currency/capability audit with typed freshness and failure metadata.
- R2.2: UD-to-GSIS player/team/game identity graph.
- R2.3: availability source tournament using UD, Sleeper, ESPN, and public roster/depth data.
- R2.4: free projection/consensus source tournament with licensing and reproducibility gates.
- R2.5: as-of feature store with strict future-data perturbation tests.

## Parallelism

R2.1 and R2.4 first. R2.2 follows provider knowledge. R2.3 follows identity. R2.5 integrates accepted sources.

## Gate

Material slate players resolve, sources have recorded licenses/freshness, high-impact status conflicts block unattended export, and future rows cannot affect historical features.

## Closeout

The R2 gate is accepted subject to the recorded packet evidence in
[R2-closeout.md](../evidence/R2-closeout.md). R3 is the next wave, but must
begin only after this branch is merged and in a fresh task; this document does
not authorize R3 work.
