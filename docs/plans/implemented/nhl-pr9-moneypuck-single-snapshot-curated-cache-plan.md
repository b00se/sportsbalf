# PR#9 Plan: NHL MoneyPuck Single-Snapshot + Curated Cache Foundation

Status: Implemented

## Summary
Implement PR#9 using MoneyPuck's large all-seasons CSV pattern: download one full raw snapshot, build a curated filtered cache for runtime, and drive NHL `shots_on_goal` inference from that curated layer.

This keeps PR#9 simple and fast while establishing a scalable structure for later tables (shot-level, lines, Corsi, and more) without redesign.

Locked decisions:
- MoneyPuck ingestion mode: full all-seasons snapshot + curated runtime cache
- Runtime provider failure policy: fail hard
- Feature scope: minimal deterministic features for inference baseline
- No model training in PR#9 (deferred to PR#10)

## Scope and Non-Goals
### In scope
- Add MoneyPuck ingestion flow for one full all-seasons game-level skater CSV.
- Add curated cache materialization (column-pruned, typed, season-filtered parquet).
- Add NHL provider abstraction that reads curated cache.
- Add minimal deterministic NHL feature engineering from skater game history.
- Keep NHL output schema unchanged from PR#8.
- Add offline deterministic tests and config validation updates.
- Update docs and PR tracker for PR#9 baseline.

### Out of scope
- NHL model training/loading and residual bootstrap sophistication (PR#10).
- Shot-level feature usage in runtime path.
- Multi-table joins in PR#9 runtime flow.
- MLB/NFL behavior changes.

## Public APIs / Interfaces / Type Changes
No breaking API signature changes.

### Additions
1. `src/nhl/data/providers/base.py`
- `ProviderName = Literal["moneypuck_snapshot"]`
- `DEFAULT_PROVIDER_NAME = "moneypuck_snapshot"`
- `LoadResult` dataclass:
  - `data: pd.DataFrame`
  - `metadata: dict[str, Any]`
- `NhlDataProvider` protocol:
  - `load_skater_games(seasons: Sequence[int]) -> LoadResult`
- `get_provider(name: str | ProviderName | None) -> NhlDataProvider`

2. `src/nhl/data/providers/moneypuck_snapshot.py`
- Provider implementation that reads curated skater-game cache and returns canonical schema.

3. `src/nhl/data/providers/__init__.py`
- Provider exports and factory surface.

4. `src/nhl/data/moneypuck_ingest.py`
- `refresh_skater_games_snapshot(...)`
- `build_skater_games_curated_cache(...)`
- Shared normalization helpers for raw snapshot -> curated schema.

5. `src/nhl/features/shots_on_goal.py`
- `build_sog_inference_features(...) -> pd.DataFrame` with minimal deterministic features and baseline prediction.

### Updates
1. `src/nhl/pipeline.py`
- Keep `run_shots_on_goal_pipeline(config, retrain=False)` and `run(...)` signatures unchanged.
- Integrate provider + feature builder before simulation.

2. `src/core/config.py`
- Add NHL validator for PR#9 runtime-critical keys and types.

3. `config/nhl.yaml`
- Extend `nhl.shots_on_goal` with snapshot and provider configuration.

## Config Spec (Decision-Complete)
Required under `nhl.shots_on_goal`:
- `provider: moneypuck_snapshot`
- `inference_input_path: <path>`
- `provider_seasons: <non-empty list[int]>`
- `moneypuck_skater_games_snapshot_path: <raw full-csv local path>`
- `moneypuck_skater_games_curated_cache_path: <parquet cache path>`
- `feature_rolling_windows: <non-empty list[int]>`
- `auto_refresh_snapshot: <bool>`
- `fail_on_provider_error: true`

Optional with defaults:
- `fallback_prediction: 2.5`
- `fallback_std: 1.0`
- `default_over_decimal_price: 1.91`
- `default_under_decimal_price: 1.91`
- `monte_carlo_simulations: 10000`
- `monte_carlo_seed: 42`

Validation behavior:
- Missing/invalid required values raise `ConfigValidationError` with dotted field path.
- Unsupported provider value raises `ValueError` via provider factory.

## Data Layering and Runtime Flow
### Data layers
1. Raw snapshot (single source of truth):
- one full MoneyPuck all-seasons skater-game CSV.
2. Curated cache (runtime source):
- typed, column-pruned, season-filtered parquet materialization.
3. Inference rows:
- loaded from `inference_input_path` (existing PR#8 anchor).

### Runtime behavior
1. Load inference rows from `inference_input_path`.
2. Optionally refresh raw snapshot and curated cache when `auto_refresh_snapshot=true`.
3. Load curated skater games for `provider_seasons` through provider abstraction.
4. Build minimal deterministic features:
   - `sog_avg_last_5`
   - `sog_avg_last_10`
   - `sog_avg_season`
5. Compute baseline prediction:
   - `predicted_shots_on_goal = 0.5 * sog_avg_last_5 + 0.3 * sog_avg_last_10 + 0.2 * sog_avg_season`
   - fallback to `fallback_prediction` when history is insufficient.
6. Fill missing over/under prices from defaults.
7. Run `apply_simulations(...)` using existing PR#8 schema conventions.
8. Return exactly the PR#8 NHL output columns in stable order.

## Canonical Curated Schema (PR#9)
Required curated columns:
- `season`
- `game_id`
- `game_date`
- `player_id`
- `player_name`
- `team`
- `opponent`
- `shots_on_goal`
- `time_on_ice_minutes` (nullable allowed)

## Failure Modes and Policy
- Snapshot refresh or provider load failure: raise explicit runtime error (`fail_on_provider_error=true`).
- Missing/empty curated cache for requested seasons: raise explicit runtime error.
- Missing/unreadable/empty inference input: preserve PR#8 empty schema fallback.
- Missing player history for a row: keep row, use fallback prediction.
- Missing optional pricing/prediction inputs: retain robust default-fill behavior from PR#8 fix.

## File-Level Change Plan
### Add files
- `src/nhl/data/__init__.py`
- `src/nhl/data/moneypuck_ingest.py`
- `src/nhl/data/providers/base.py`
- `src/nhl/data/providers/moneypuck_snapshot.py`
- `src/nhl/data/providers/__init__.py`
- `src/nhl/features/__init__.py`
- `src/nhl/features/shots_on_goal.py`
- `tests/test_nhl_moneypuck_ingest.py`
- `tests/test_nhl_providers.py`
- `tests/test_nhl_features.py`
- `tests/testdata/nhl/moneypuck/skater_games_full_snapshot_sample.csv`

### Modify files
- `src/nhl/pipeline.py`
- `src/core/config.py`
- `config/nhl.yaml`
- `tests/integration/test_nhl_shots_on_goal_pipeline.py`
- `docs/architecture.md`
- `docs/contracts.md`
- `docs/new-sport-playbook.md`
- `docs/plans/planned/nhl-onboarding-sequenced-pr-plan.md`

## TDD Plan (RED -> GREEN)
### RED
- Add config validation tests for required PR#9 NHL keys.
- Add snapshot ingest/normalization tests using offline fixture CSV.
- Add provider tests for curated cache load + season filtering.
- Add feature tests for rolling-window deterministic behavior + fallback.
- Extend NHL integration tests for provider-backed deterministic outputs and fail-hard provider errors.
- Run:
  - `.venv/bin/pytest -q tests/test_nhl_moneypuck_ingest.py tests/test_nhl_providers.py tests/test_nhl_features.py tests/integration/test_nhl_shots_on_goal_pipeline.py`

### GREEN
- Implement ingest module, provider abstraction, feature builder, and pipeline integration.
- Implement NHL PR#9 config validator updates.
- Re-run targeted tests to green.

### Final verification gates
- `.venv/bin/ruff check .`
- `.venv/bin/pytest -q`

## Test Cases and Scenarios
- Full-snapshot fixture normalizes to canonical curated schema.
- Provider loads curated cache and filters to requested seasons.
- NHL pipeline remains deterministic with fixed Monte Carlo seed.
- Output columns remain exactly PR#8-required NHL contract.
- Missing optional columns in inference input still default safely.
- Forced provider/refresh failure raises explicit runtime error under fail-hard mode.
- MLB/NFL non-regression suites remain green.

## Risks and Mitigations
- Risk: runtime cost from giant raw CSV.
  - Mitigation: runtime reads curated cache, not raw snapshot.
- Risk: future table expansion causes schema drift.
  - Mitigation: lock canonical curated schema conventions now and add table-by-table onboarding pattern.
- Risk: provider/network instability.
  - Mitigation: explicit fail-hard behavior with actionable error messages.

## Assumptions and Defaults
- Game-level skater CSV is sufficient for PR#9 baseline features.
- Shot-level and other advanced tables are deferred but use the same raw->curated pattern.
- Tests remain offline-only with local fixtures.
- PR#10 will consume this curated layer for training and richer feature/model behavior.
