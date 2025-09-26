# Migration Plan: Transition from nfl_data_py to nflreadpy

This document captures the action plan to migrate all NFL data ingestion from `nfl_data_py` to the new `nflreadpy` library. The migration must maintain backward compatibility during rollout, unblock 2025 data availability, and minimise disruption to downstream tooling.

## 1. Goals & Constraints

- Restore access to current-season weekly data now published exclusively through `nflreadpy`.
- Avoid breaking existing pipelines/tests while the old provider is still in use.
- Keep feature derivations and model training deterministic; only the upstream loader changes.
- Provide a runtime switch so we can fall back to `nfl_data_py` temporarily if needed.

## 2. High-Level Approach

1. **Introduce a provider abstraction** encapsulating the four data loads we rely on today (weekly stats, schedules, PBP, NGS passing).
2. **Add two provider implementations**:
   - `NFLDataPyProvider` (thin wrapper around current code) for parity.
   - `NFLReadPyProvider` that uses `nflreadpy` loaders and converts Polars frames to pandas.
3. **Normalize schemas** inside the providers so downstream code receives the same column names/dtypes regardless of backend.
4. **Select provider via config/CLI** (`provider: nflreadpy | nfl_data_py`), defaulting to `nflreadpy` once validated.
5. **Expand tests** to cover both providers with shared fixtures, ensuring identical outputs for common scenarios.
6. **Stage rollout**: ship both providers behind the switch, run in staging, then flip the default and finally remove the legacy implementation when comfortable.

## 3. Detailed Tasks

### 3.1 Provider Interface

- Create `src/nfl/data/providers/base.py` defining an abstract class with methods:
  ```python
  class NFLDataProvider(Protocol):
      def load_weekly(self, years: Sequence[int]) -> pd.DataFrame: ...
      def load_schedules(self, years: Sequence[int]) -> pd.DataFrame: ...
      def load_pbp(self, years: Sequence[int]) -> pd.DataFrame: ...
      def load_ngs_passing(self, years: Sequence[int]) -> pd.DataFrame: ...
  ```
- Optional: add a simple factory (`get_provider(name: str)`).

### 3.2 `nfl_data_py` Provider

- Move the existing loader functions from `qb_attempts.py` into `providers/datapy.py`.
- Keep our 404 fallback logic for seasons not yet published.
- Ensure caching behaviour remains unchanged.

### 3.3 `nflreadpy` Provider

- New module `providers/readpy.py` implementing the same interface.
- Use the following `nflreadpy` calls:
  - Weekly stats: `nfl.load_player_stats(years, summary_level="week")`
  - Schedules: `nfl.load_schedules(years)` (or `True` for all when needed)
  - Play-by-play: `nfl.load_pbp(years)`
  - NGS passing: `nfl.load_nextgen_stats(years, stat_type="passing")`
- Convert Polars `DataFrame` to pandas with `.to_pandas()` and standardise column names:
  - Weekly: map to `position`, `season`, `week`, `game_id`, `player_id`, `player_display_name`, `recent_team`, `opponent_team`, `attempts`.
  - Schedules: ensure `game_id`, `season`, `week`, `home_team`, `away_team`, `spread_line`, `total_line`, `gameday`.
  - PBP: align with existing columns (`posteam`, `defteam`, `pass_attempt`, etc.).
  - NGS: keep passing metrics, rename to match current feature expectations.

### 3.4 Integration in `qb_attempts.py`

- Inject provider instance (default from config) into `build_qb_attempts_dataset` and loader helpers.
- Remove direct imports of `nfl_data_py`; replace with provider calls.
- Preserve caching paths (PBP/NGS parquet caches) by checking provider name; only cache when provider returns pandas.
- Ensure the year-by-year fallback (skipping missing seasons) is retained for `nflreadpy` in case certain seasons are unpublished.

### 3.5 Configuration & CLI

- Update `config/nfl.yaml` with `provider: nflreadpy`.
- Add `--provider` flag to `scripts/build_qb_attempts_dataset.py` and propagate to the pipeline.
- Document environment variables for `nflreadpy` caching if we want to override defaults.

### 3.6 Testing

- Extend unit tests:
  - Parameterise existing dataset pipeline tests to run with both providers using stub data.
  - Add dedicated tests for schema normalisation (e.g., ensure `attempts` extracted for QBs).
  - Verify the 404 fallback still raises informative errors when all seasons are missing.
- (Optional) Add integration tests guarded by a `requires_network` mark that hit real `nflreadpy` endpoints.

### 3.7 Rollout Plan

1. Merge provider abstraction with both implementations; default remains `nfl_data_py`.
2. Run smoke tests in staging using `provider: nflreadpy` for the new season.
3. Flip default to `nflreadpy` and monitor outputs.
4. After deprecation window, remove `nfl_data_py` dependency, provider, and related code.

## 4. Risks & Mitigations

- **Schema drift between providers**: mitigate through explicit renaming/typing in providers plus tests covering both paths.
- **Performance impact**: `nflreadpy` returns Polars; converting to pandas has overhead. Cache conversions if necessary (e.g., reuse the same provider instance).
- **Data availability gaps**: maintain our season-skipping fallback and informative logs.
- **Dependency footprint**: add `nflreadpy` (and optionally `polars`) to `requirements.txt`; keep `nfl_data_py` until the switchover is complete.

## 5. Deliverables

- Provider modules + factory
- Config/CLI updates and documentation
- Updated tests (unit + optional integration)
- Release notes/README updates
- Final follow-up to remove legacy provider once nflreadpy is stable in production

