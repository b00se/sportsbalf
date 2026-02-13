# Phase 0 Plan: Unified Projection Core Across Fantasy + Stat Markets

Status: Planned

## Summary
Build a single reusable foundation where the canonical primitive is `Projection(entity, metric, horizon, distribution)`.
This supports all required products without contract rewrites:
1. Season-long fantasy projections.
2. Shorter-slate fantasy projections (daily/weekly/slate).
3. Single-game fantasy higher/lower markets.
4. Season-long stat higher/lower markets.

Phase 0 delivers contracts, registries, config schema, structural validators, mapping, and offline tests. It does not implement ranking/slip optimization yet.

## Scope
1. In scope:
- Add shared contracts under `src/fantasy/core/`.
- Add neutral registries for projection, derived metrics, market transforms, and export adapters.
- Add scoring/derivation interfaces so fantasy points are derived from stat projections.
- Add unified market config with horizon + metric semantics.
- Add ID mapping contract and resolver.
- Add strict/soft validators and fixtures.
- Update `docs/contracts.md`, `docs/config-schema.md`, `docs/architecture.md`.
2. Out of scope:
- Portfolio optimization for tournaments.
- Slip construction optimization for pick'em.
- Live provider export wiring.
- Changes to current `pipeline/main.py` runtime behavior.

## Core Design Decisions
1. Base projections are always stat-first.
2. Fantasy points are always derived metrics.
3. Horizons are first-class and required (`game`, `day`, `week`, `slate`, `season`).
4. Markets reference `(metric_id, horizon)`; they do not define new projection primitives.
5. Tournament and pick'em are market modes over the same projection graph.

## Public Interfaces / Types
1. File: `src/fantasy/core/contracts.py`.
2. Add `ProjectionKey`:
- `entity_id: str`
- `sport: str`
- `metric_id: str`
- `horizon: str`
- `window_start: str`
- `window_end: str`
- `game_id: str | None`
3. Add `ProjectionDistribution`:
- `mean: float`
- `p10: float`
- `p50: float`
- `p90: float`
- `stddev: float | None`
- `params: dict[str, float]`
4. Add `ProjectionRow`:
- `key: ProjectionKey`
- `distribution: ProjectionDistribution`
- `availability_confidence: float`
- `source_model_version: str`
- `source_snapshot_id: str`
5. Add `DerivedMetricSpec`:
- `derived_metric_id: str`
- `input_metric_ids: tuple[str, ...]`
- `transform_id: str`
- `transform_params: dict[str, float]`
6. Add `MarketDefinition`:
- `market_id: str`
- `provider: str`
- `sport: str`
- `mode: str`
- `metric_id: str`
- `horizon: str`
- `operator: str | None`
- `line_value: float | None`
- `window_start: str`
- `window_end: str`
- `game_id: str | None`
7. Add `ContestConfig`:
- `contest_id: str`
- `provider: str`
- `sport: str`
- `mode: str`
- `scoring_ruleset_id: str | None`
- `market_definitions: tuple[MarketDefinition, ...]`
- `mode_config: dict[str, Any]`
- `metadata: dict[str, str | int | float]`
8. Add `ProviderPlayerMapping`:
- `provider: str`
- `sport: str`
- `provider_player_id: str`
- `internal_player_id: str`
- `provider_player_name: str`
- `canonical_name: str`
- `is_active: bool`
- `source: str`
- `updated_at_utc: str`
9. Add protocols:
- `SportProjectionAdapter.project(config: ContestConfig) -> pd.DataFrame`
- `DerivedMetricAdapter.derive(base_projections: pd.DataFrame, spec: DerivedMetricSpec) -> pd.DataFrame`
- `MarketTransformAdapter.transform(projections: pd.DataFrame, market: MarketDefinition, config: ContestConfig) -> pd.DataFrame`
- `ExportAdapter.export(surface: pd.DataFrame, config: ContestConfig) -> pd.DataFrame`

## Registry Architecture
1. File: `src/fantasy/core/registry.py`.
2. Add registries:
- Projection registry keyed by `(sport, metric_id, horizon)`.
- Derived metric registry keyed by `(derived_metric_id)`.
- Market transform registry keyed by `(provider, mode, operator)`.
- Export registry keyed by `(provider, mode, export_kind)`.
3. Add APIs:
- `register_projection_adapter`
- `register_derived_metric_adapter`
- `register_market_transform_adapter`
- `register_export_adapter`
- `get_*` for each registry
- `list_registered_fantasy_adapters`
- `clear_fantasy_registry`
4. Behavior:
- Normalize keys to lowercase trimmed values.
- Deterministic overwrite on duplicate registration.
- Typed lookup errors for missing entries.

## Config Schema
1. Add file: `config/fantasy/mlb_unified_szn_core_2026.yaml`.
2. Required top-level sections:
- `contest`
- `metrics`
- `scoring` (optional unless fantasy derived metrics are declared)
- `mapping`
- `markets`
3. `metrics`:
- `base_metrics` list with `(metric_id, horizon, adapter_key)`.
- `derived_metrics` list with `(derived_metric_id, input_metric_ids, transform_id)`.
4. `markets`:
- `mode` values allowed: `season_long_tournament`, `short_slate_fantasy`, `single_game_pickem`, `season_long_stat_pickem`.
- each market item declares `metric_id`, `horizon`, optional `line_value`, optional `operator`, and window bounds.
5. `mode_config` branches:
- `season_long_tournament` soft fields: `roster`, `advancement`, `payouts`.
- `short_slate_fantasy` soft fields: `roster`, `slate_selection_rule`.
- `single_game_pickem` soft fields: `slip_constraints`, `payout_ladder`.
- `season_long_stat_pickem` soft fields: `slip_constraints`, `payout_ladder`.
6. Mapping section:
- `player_id_map_path`
- `unresolved_policy` enum: `fail`, `warn`, `drop`.

## Validation Policy
1. Strict in Phase 0:
- Every market must reference a declared metric and compatible horizon.
- `derived_metrics.input_metric_ids` must exist in declared base metrics.
- If a market references a derived fantasy metric, a scoring ruleset must exist.
- `operator` must be present when `line_value` is present.
- mapping fields required and valid enum for unresolved policy.
2. Soft in Phase 0:
- tournament `advancement` and `payouts` are shape-validated only.
- slate/pick'em constraints are shape-validated only.
- no optimization semantics enforced yet.

## Implementation Sequence (TDD)
1. RED: add tests for contract parsing and config validation across all 4 target use-cases.
2. GREEN: implement contracts and config loader/validator.
3. RED: add tests for registry keys and typed lookup failures.
4. GREEN: implement registries.
5. RED: add tests for derived metric dependency resolution.
6. GREEN: implement derivation scaffolding and no-op adapter stubs.
7. RED: add tests for mapping resolver statuses.
8. GREEN: implement mapping resolver and status outputs.
9. Run `.venv/bin/pytest -q`.
10. Run `.venv/bin/ruff check .`.

## Test Cases and Scenarios
1. Season-long fantasy projection config loads and validates.
2. Daily/weekly/slate fantasy config loads and validates.
3. Single-game fantasy higher/lower market config loads and validates with line/operator.
4. Season-long stat higher/lower market config loads and validates.
5. Derived fantasy metric resolves from declared base stats.
6. Invalid metric-horizon reference fails validation.
7. Missing base metric for derived metric fails validation.
8. Duplicate registry keys overwrite deterministically.
9. Unknown registry key raises typed error.
10. Mapping returns statuses `mapped`, `unmapped`, `duplicate_provider_id`.

## Files to Add
1. `src/fantasy/core/contracts.py`
2. `src/fantasy/core/registry.py`
3. `src/fantasy/core/config.py`
4. `src/fantasy/core/validation.py`
5. `src/fantasy/core/mapping.py`
6. `src/fantasy/core/derived.py`
7. `config/fantasy/mlb_unified_szn_core_2026.yaml`
8. `tests/fantasy/core/test_contracts.py`
9. `tests/fantasy/core/test_config_validation.py`
10. `tests/fantasy/core/test_registry.py`
11. `tests/fantasy/core/test_mapping.py`
12. `tests/fantasy/core/test_derived_metrics.py`

## Docs Updates
1. `docs/contracts.md`: add projection primitive, derived metric layer, market definitions, adapter protocols.
2. `docs/config-schema.md`: add unified schema for metrics/horizons/markets and strict-vs-soft validation table.
3. `docs/architecture.md`: add pipeline shape `projection -> derived metric -> market transform -> export`.

## Acceptance Criteria
1. All four requested product surfaces are representable in one schema without contract changes.
2. Fantasy points are modeled only as derived metrics from stat projections.
3. Registries support projection, derivation, market, and export separation.
4. Config validation catches invalid metric/horizon wiring before runtime.
5. Existing MLB/NFL/NHL production pipeline behavior is unchanged.

## Assumptions and Defaults
1. `horizon` canonical enum is fixed in Phase 0: `game`, `day`, `week`, `slate`, `season`.
2. Date windows are stored as ISO strings in config/contracts for now.
3. Phase 0 enforces structural correctness, not market optimization behavior.
4. Unresolved player mapping defaults to `fail` for correctness-first operation.
