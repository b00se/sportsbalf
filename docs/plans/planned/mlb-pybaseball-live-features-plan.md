# Plan: Mine Pybaseball Live Features (Handedness, Umpire, Weather, Humidity, Roof State) and Validate MAE Lift

Status: Planned (Partially Implemented)

## Summary
Implement a new MLB feature-mining layer that pulls pregame context from pybaseball-first sources (with optional secondary fallback), caches it for live daily scoring, and integrates these features into the existing multi-model tournament framework.  
Locked decisions:
1. Include weather now.
2. Add humidity and stadium roof/open-closed state.
3. Use live fetch in-season.
4. On live fetch failures, use stale cache fallback.
5. Source policy is pybaseball-first (allow one secondary source only if a key field is missing).
6. Success gate is any MAE improvement vs current baseline.

## Validation Status (Updated: 2026-02-08)
1. Current strict gate status: **not met**.
2. Latest measured comparison on current config:
   - baseline MAE: `1.467579`
   - enriched MAE: `1.491491`
   - MAE improvement (`baseline - enriched`): `-0.023912`
3. Rebuild/recheck status:
   - historical feature-engineering datasets were rebuilt for 2021–2025
   - MAE comparison remained negative after rebuild
4. Reference evidence:
   - `docs/reports/mlb-live-features-mae-validation-2026-02-08.md`

## Current State (Grounded)
1. Existing model features are still only:
   - `rolling_K_avg_3`, `rolling_K_avg_5`, `rolling_pitch_count_5`, `rolling_K_rate`, `opponent_k_pct`, `opponent_k_rate`, `park_factor_K`, `rest_days`.
2. Raw statcast parquet already includes useful fields:
   - `stand`, `p_throws`, `umpire`, `game_pk`.
3. Tournament plumbing and strategy selection are already in place and working.
4. Existing pipeline already tolerates network issues in some areas; this will be extended to live feature fetches.

## Public API / Interface Changes
1. Keep `run(config_path: str | None = None, retrain: bool = False)` unchanged.
2. Add non-breaking config block under `mlb.strikeouts`:
   - `live_features.enabled: true`
   - `live_features.source_policy: pybaseball_first`
   - `live_features.fallback_policy: stale_cache`
   - `live_features.cache_path: data/cache/mlb_live_features.parquet`
   - `live_features.cache_ttl_hours: 24`
   - `live_features.weather.enabled: true`
   - `live_features.weather.primary_source: pybaseball_team_game_logs`
   - `live_features.weather.secondary_source: statsapi_game_feed` (only if primary misses required fields)
3. Expand `FEATURES` in `src/mlb/models/predict.py` with new columns (below).
4. Persist champion metadata additions:
   - `live_feature_set_version`
   - `live_feature_sources`
   - `live_fetch_timestamp`
   - `cache_age_hours`
5. Preserve output schema for betting outputs.

## New Feature Set (Decision-Complete)
Add these model features and ensure training/inference parity:

### A) Handedness/Matchup
1. `pitcher_throws_encoded`
2. `projected_batter_stand_mix_L`
3. `projected_batter_stand_mix_R`
4. `same_hand_matchup_rate`

Source:
- Historical from statcast `stand` + `p_throws`.
- Live via pybaseball-first lineup/splits route; fallback to recent opponent batter-side mix.

### B) Umpire
1. `umpire_k_boost_expanding`
2. `umpire_sample_size`
3. `umpire_known_flag`

Source:
- Historical from statcast `umpire`.
- Live probable umpire via pybaseball-first endpoint; fallback to neutral + cached prior.

### C) Weather / Environment
1. `game_temp_f`
2. `humidity_pct`
3. `wind_speed_mph`
4. `wind_out_to_cf_flag` (or encoded directional bucket)
5. `weather_run_env_idx` (composite from temp/wind/humidity)
6. `humidity_x_temp`
7. `weather_known_flag`

Source:
- Primary: pybaseball team game logs / pybaseball-accessible game context.
- Secondary (if required): StatsAPI game feed only for missing keys.
- Fallback: stale cache, else neutral defaults.

### D) Roof / Dome Context
1. `roof_state` (`open`, `closed`, `retractable_open`, `retractable_closed`, `unknown`)
2. `roof_closed_flag`
3. `weather_effective_flag` (0 when roof closed, 1 otherwise)
4. optional interactions:
   - `wind_speed_effective = wind_speed_mph * weather_effective_flag`
   - `humidity_effective = humidity_pct * weather_effective_flag`

Source:
- Pybaseball-first game context when available.
- Secondary source allowed if pybaseball lacks reliable roof state for that game.
- Fallback to `unknown` with neutral behavior.

## Implementation Plan

### 1) New Live Feature Service Layer
Files:
1. `src/mlb/features/live_context.py` (new)
2. `src/mlb/features/weather.py` (new)
3. `src/mlb/features/umpire.py` (new)
4. `src/mlb/features/handedness.py` (new)
5. `src/mlb/features/venue.py` (new, for roof-state normalization)

Responsibilities:
1. Fetch and normalize live context for target slate date.
2. Build deterministic joins keyed by (`game_date`, `pitcher_id`, `opponent_team`, optional `game_pk`).
3. Cache fetched payload + normalized feature frame.
4. Return features + provenance metadata.

### 2) Historical Feature Builder for Training
Files:
1. `src/mlb/features/feature_store.py` (new)
2. `scripts/generate_pitcher_dataset_from_raw.py` (extend)
3. `scripts/update_pitcher_dataset_from_raw.py` (extend)

Responsibilities:
1. Compute historical versions of same features for 2021–2025.
2. Enforce leakage prevention:
   - expanding/rolling umpire and matchup features shifted by 1 game.
3. Materialize new columns in processed parquet for reproducible training.

### 3) Pipeline Integration
Files:
1. `src/mlb/pipeline.py`
2. `src/mlb/models/predict.py`

Responsibilities:
1. Merge live context features into prediction rows before scoring.
2. Apply fallback chain:
   - fresh fetch -> stale cache -> neutral defaults.
3. Track and log per-run coverage:
   - weather known %, roof known %, umpire known %, handedness known %.
4. Ensure training/inference feature ordering parity.

### 4) Tournament and Backtest Evaluation
Files:
1. `scripts/backtest_mlb_strikeouts.py`
2. `src/mlb/models/evaluation.py` (only if extra reporting fields needed)

Responsibilities:
1. Compare baseline vs expanded feature set on identical folds.
2. Emit diagnostics:
   - MAE delta vs baseline
   - feature availability coverage
   - slice metrics by roof state (open vs closed/unknown).
3. Add explicit ablation outputs to identify feature drag:
   - baseline only (8 core features)
   - baseline + handedness only
   - baseline + umpire only
   - baseline + weather only
   - baseline + roof interactions only
   - baseline + weather + roof
   - full enriched set

### 5) Caching and Fallback Policy (Live)
Cache schema:
1. `cache_key` (`date`, `team`, `pitcher_id`)
2. normalized feature columns
3. `fetched_at`
4. `source_used` (`primary`/`secondary`)
5. `is_stale`

Behavior:
1. Attempt fresh pybaseball-first fetch.
2. If partial data missing (e.g., humidity/roof), fill only missing keys via secondary.
3. On fetch failure, use last cache snapshot within TTL.
4. If still missing, neutral defaults + known flags.

## Tests and Scenarios

### Unit Tests
1. Handedness feature builder:
   - correct encoding and mix calculations.
2. Umpire feature builder:
   - shifted expanding metrics, no leakage.
3. Weather feature builder:
   - temp/humidity/wind normalization and default handling.
4. Roof feature builder:
   - canonical mapping and effective-weather masking.
5. Cache policy:
   - fresh success, stale fallback, neutral fallback.
6. Source policy:
   - pybaseball primary first; secondary only for missing keys.

### Integration Tests
1. Pipeline inference with full live features available.
2. Pipeline inference with weather missing but roof present.
3. Pipeline inference with roof unknown.
4. Pipeline inference with forced fetch failure uses stale cache.
5. Pipeline inference with no cache uses neutral defaults and still outputs schema.

### Regression/Acceptance Tests
1. `FEATURES` parity test for new columns.
2. Walk-forward split invariance before/after feature expansion.
3. MAE gate:
   - accept if any positive mean MAE improvement vs baseline.
4. Coverage guard:
   - if humidity/roof availability too low, report and automatically degrade to neutralized interactions.

### MAE Recovery Experiments (Required Before Closing Plan)
1. Ablation sweep:
   - run the feature-set matrix above on identical folds/seeds and rank by MAE.
   - identify at least one subset with non-negative MAE delta.
2. Distribution/quality diagnostics:
   - compare train vs inference distributions for weather/roof/umpire features.
   - flag features with extreme missingness, low variance, or unstable scaling.
3. Temporal robustness checks:
   - compute MAE deltas by season and by month.
   - ensure no single-season artifact is hiding broad degradation.
4. Segment diagnostics:
   - MAE by `roof_state` (`open`, `closed`, `unknown`).
   - MAE by weather-known vs default-imputed rows.
   - MAE by stale-cache vs fresh/live rows (where applicable).
5. Leakage sanity checks:
   - confirm all historical expanding features are shifted and do not use same-game outcomes.
6. Feature transformation tuning:
   - test clipped/scaled variants of weather composites (`weather_run_env_idx`, interactions).
   - test removing weakest contributors identified by ablation.
7. Model-spec robustness:
   - rerun winning ablation on multiple model candidates to avoid model-specific overfitting.
8. Promotion criterion:
   - close plan only when chosen enriched subset shows positive MAE lift on locked comparison protocol.

## Rollout and Monitoring
1. Phase 1 (shadow):
   - compute live features and log coverage/fallback without affecting model.
2. Phase 2 (active):
   - train/evaluate with new features and promote only if MAE improves.
3. Monitoring metrics:
   - `live_feature_coverage_pct`
   - `humidity_known_pct`
   - `roof_known_pct`
   - `stale_cache_usage_pct`
   - `neutral_default_usage_pct`
   - `mae_delta_vs_baseline`
4. Add model-debug metrics to backtest artifacts:
   - `mae_delta_by_season`
   - `mae_delta_by_roof_state`
   - `mae_delta_weather_known_vs_default`
   - ablation leaderboard CSV (variant-level).

## Assumptions and Defaults
1. Pybaseball provides most daily context; one secondary source may be needed for consistent humidity/roof fields.
2. Live scoring must continue during outages via stale cache fallback.
3. Roof state gates weather effect features to avoid overfitting dome games.
4. Output schema remains unchanged.
5. Any positive MAE lift is sufficient for acceptance in this PR.
