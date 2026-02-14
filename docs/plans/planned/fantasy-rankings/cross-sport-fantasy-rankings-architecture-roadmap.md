# Cross-Sport Fantasy Rankings Architecture Roadmap (MLB Season-Long MVP First)

Status: Planned (Partially Implemented)

## Summary
Deliver an MLB season-long best-ball rankings CSV pipeline now, but design every layer so NFL/NHL and daily slates can plug in later without core rewrites.
Implementation remains phased and small-scope, with explicit extensibility gates that must pass before moving to the next phase.

## Current Status
1. Phase 0 is implemented and tracked at:
   - `docs/plans/implemented/fantasy-rankings/phase-0-unified-projection-core-plan.md`
2. Phase 1 is implemented and tracked at:
   - `docs/plans/implemented/fantasy-rankings/phase-1-mlb-projection-adapter-plan.md`
3. Remaining phases (2-5) are still planned.

## Locked Architecture Principles (Non-Negotiable)
1. Core is sport-agnostic.
2. Sport-specific logic lives only in adapters.
3. Contest/scoring transforms are separate from projection generation.
4. Provider/export formats are adapter-based (Underdog is one adapter).
5. Registry + config determine behavior (no hardcoded sport/provider branching).
6. No MLB-specific fields in shared core interfaces.

## Target MVP (Phase-Scoped)
- First shipped product: MLB 2026 season-long Underdog rankings CSV.
- Data mode: in-house only.
- Ranking profile: aggressive upside.
- Output: upload-compatible full-schema CSV with deterministic ordering.

## Layered Architecture (Future-Proof by Design)

## Layer A: Fantasy Core (Shared)
- Shared contracts/types:
  - `PlayerKey`, `ProjectionRow`, `DistributionRow`, `RankingRow`, `ContestConfig`, `ScoringRuleSet`, `ExportRow`.
- Shared services:
  - scoring engine interface
  - ranking engine interface
  - validation framework
  - deterministic tie-break + seeding utilities
- Shared registry:
  - `sport` adapter registration
  - `contest` adapter registration
  - `export` adapter registration

## Layer B: Sport Adapters
- `mlb` adapter implemented first.
- Future: `nfl`, `nhl` adapters conform to same interfaces.
- Adapter duties:
  - feature assembly
  - sport model inference/training
  - neutral projection distribution output to core schema

## Layer C: Contest/Slate Adapters
- Season-long tournament adapter implemented first.
- Future daily/slate adapters use same core projection outputs with different transforms (replacement levels, volatility weighting, roster logic).

## Layer D: Provider Export Adapters
- Underdog rankings CSV adapter implemented first.
- Future adapters (other providers/slate uploads) reuse the same ranking output schema.

## Phase Plan with Extensibility Gates

## Phase 0: Contracts + Mapping Foundations
- Define shared fantasy-core contracts (not MLB-specific).
- Define ID mapping contract (`internal_player_id` <-> provider player ID).
- Define contest/scoring schema independent of sport.
- Extensibility gate:
  - A hypothetical NHL/NFL adapter can compile against contracts without schema edits.

## Phase 1: MLB Projection Adapter (Neutral Outputs, Implemented)
- Build MLB adapter that outputs neutral per-player projections + uncertainty.
- Keep output schema sport-agnostic (`expected_points`, `volatility`, `availability_confidence`, etc.).
- Extensibility gate:
  - No downstream consumer depends on MLB-only columns.

## Phase 2: Season-Long Contest Ranking Adapter
- Implement season-long ranking transform (advancement/top-heavy aware).
- Add position scarcity/replacement logic via contest config, not hardcoded MLB assumptions.
- Extensibility gate:
  - Same ranking engine runs with a different contest config and no code change.

## Phase 3: Underdog Export Adapter + Validators
- Implement adapter to produce exact Underdog schema/column order.
- Preserve full file compatibility and deterministic row order.
- Extensibility gate:
  - Adding a second provider requires only a new export adapter module.

## Phase 4: Backtest + Calibration Framework
- Add shared evaluation harness for ranking quality and robustness.
- Include baseline-relative metrics and sensitivity analysis for upside coefficients.
- Extensibility gate:
  - Harness accepts any adapter implementing `ProjectionRow`/`RankingRow`.

## Phase 5: Ops, Versioning, and Runbook
- One-command orchestration (snapshot -> project -> rank -> export -> validate).
- Artifact manifests include model/data hash + config hash + seed.
- Extensibility gate:
  - New sport or slate adds config + adapter registration, not core workflow edits.

## Important Interfaces / API Additions
- New shared package:
  - `src/fantasy/core/contracts.py`
  - `src/fantasy/core/registry.py`
  - `src/fantasy/core/ranking.py`
  - `src/fantasy/core/validation.py`
- New MLB adapter package:
  - `src/fantasy/adapters/mlb/...`
- New contest adapter package:
  - `src/fantasy/contests/season_long_best_ball.py`
- New export adapter package:
  - `src/fantasy/exporters/underdog_rankings_csv.py`
- Config additions:
  - `config/fantasy/*.yaml` for contest/scoring/export settings.
  - Sport adapter config remains isolated under sport-specific sections.

## Testing and Acceptance Scenarios
- Contract tests:
  - Adapter conformance to shared interfaces.
- Determinism tests:
  - Same snapshot + config + seed => identical ranking output.
- Compatibility tests:
  - Underdog CSV schema and column-order validation.
- Extensibility tests:
  - Stub non-MLB adapter passes core pipeline without core code edits.
- Regression tests:
  - leakage guards for temporal features in adapter layer.
- End-to-end tests:
  - fixture-based offline run from inputs to upload-ready CSV.

## Assumptions and Defaults
- MVP scope is season-long MLB only, but architecture must remain cross-sport/slate ready.
- In-house data only for MVP.
- Aggressive upside ranking profile is default for this contest.
- Full Underdog schema preservation is required until provider docs say otherwise.
- Every lower-level phase plan must include an "Extensibility Gate" section and cannot proceed without passing it.
