# Phase 1.5 Plan: MLB Batter Projection Quality Hardening (Feature Engineering + Walk-Forward Fit/Backtest)

Status: Planned

## Summary
Phase 1 shipped a schema-valid MLB projection adapter. Phase 1.5 will harden projection quality before ranking/export phases by adding leakage-safe batter feature engineering, multi-season walk-forward training/backtesting, and pybaseball seasonal priors (Statcast-first policy).
Primary acceptance objective is out-of-sample MAE improvement.

## Locked Decisions
1. Optimization target: Out-of-sample MAE (walk-forward).
2. Data policy: Statcast-first with selective pybaseball priors.
3. Keep fantasy output schema unchanged (`entity_id`, `metric_id`, `mean`, `p10/p50/p90`, etc.).
4. Keep tests offline-only; pybaseball network calls must be optional/cached and bypassed in tests.
5. No changes to existing MLB/NFL/NHL production sportsbook pipelines.

## Scope
1. In scope:
- Batter projection feature engineering for Phase 1 metrics.
- Multi-season snapshot dataset builder for season-horizon modeling.
- Walk-forward backtest harness and acceptance gates.
- Adapter modeling upgrade from baseline to quality-controlled model artifacts.
- Pybaseball seasonal prior ingestion and ID mapping.
- Config/schema/docs/test updates for new workflow.
2. Out of scope:
- Contest ranking logic (Phase 2).
- Provider CSV export (Phase 3).
- Full orchestration command chain/manifests (Phase 5).
- New non-MLB fantasy adapters.

## Grounded Data Inputs (Already Available)
1. Local Statcast raw parquet: `data/raw/statcast/statcast_raw_2021.parquet` through `statcast_raw_2025.parquet`.
2. Local batter foundation: `data/processed/mlb_batter_games_foundation_2025.parquet` with `batter`, `game_date`, `plate_appearances`, `hits`, `total_bases`, `walks`, `strikeouts`, `pa_vs_lhp`, `pa_vs_rhp`, `hard_hit_rate`.
3. Pybaseball available in env (`2.2.7`) with working endpoints:
- `batting_stats`
- `statcast_batter`
- `playerid_reverse_lookup`

## Modeling Design (Decision Complete)
1. Train only count-like base targets directly:
- `plate_appearances`
- `hits`
- `total_bases`
- `walks`
- `strikeouts`
- `pa_vs_lhp`
- `pa_vs_rhp`
- `hard_hit_events` (derived from `hard_hit_rate * PA` at game-level)
2. Derive rate metrics from predicted counts for coherence:
- `hit_rate = hits / plate_appearances`
- `walk_rate = walks / plate_appearances`
- `strikeout_rate = strikeouts / plate_appearances`
- `slugging_proxy = total_bases / plate_appearances`
3. Train on player-season snapshots, not raw game rows:
- Each row is `(player_id, season, anchor_date)` with features from games before anchor.
- Label is rest-of-season total for each target from anchor+1 to season end.
4. Inference:
- Build current snapshot at `inference_anchor_date`.
- Predict remaining-season totals.
- Output `season_to_date + predicted_remaining` when anchor is in-season.
- Output `predicted_remaining` only when preseason anchor has no season-to-date games.
5. Uncertainty:
- Residual quantiles from walk-forward OOS residuals, stratified by PA-volume bucket.
- `stddev` from OOS residual std by bucket.
- Quantiles clipped to enforce `p10 <= p50 <= p90`.
6. Availability confidence:
- Function of recent PA, games played, and lineup persistence proxy (`games_started_recent / team_games_recent`).

## Feature Set for Phase 1.5
1. Existing foundation features:
- PA/hits/TB/walks/K, split PA, hard-hit proxy.
2. Rolling batter form (shifted, leakage-safe):
- 7/14/30 game rolling means for PA, hits, TB, BB, K.
- Rolling rates for hit/walk/K/slug proxy.
- Rolling hard-hit-rate (contact-only denominator).
3. Playing-time stability:
- Games played last 14/30 days.
- PA per game last 14/30.
- Days since last game.
4. Opponent/park context from Statcast:
- Opposing pitcher handedness mix faced recently.
- Park run factor proxy from historical team park scoring environment.
5. Base-state/contact quality:
- RISP PA share proxy (`on_2b`/`on_3b` present at PA).
- Mean launch speed and xwOBA proxy where available.
6. Pybaseball seasonal priors (joined by MLBAM->FanGraphs mapping):
- From `batting_stats`: `PA`, `BB%`, `K%`, `ISO`, `BABIP`, `HardHit%`, `Barrel%`, `Contact%`, `SwStr%`, `xwOBA`, `wRC+`.
- Prior season and season-to-date variants at each anchor.
- Fallback to league median priors when missing.

## New/Changed Interfaces and Config
1. Add config section under fantasy config:
- `adapters.mlb_projection_phase15`
- `training_data_paths`
- `snapshot_anchor_frequency` (`weekly` default)
- `snapshot_min_games`
- `model_selection.enabled`
- `model_selection.candidates`
- `model_selection.primary_metric` (`mae`)
- `model_selection.max_trials_per_model`
- `pybaseball_priors.enabled`
- `pybaseball_priors.cache_path`
- `pybaseball_priors.seasons`
- `pybaseball_priors.refresh`
- `uncertainty.residual_bucket_col` (`season_to_date_pa`)
- `uncertainty.bucket_edges`
2. Keep adapter public entrypoint unchanged:
- `MlbSeasonProjectionAdapter.project(config: ContestConfig) -> pd.DataFrame`
3. Keep registry contract unchanged:
- `register_mlb_projection_adapters(...)`
4. Add source versioning convention:
- `source_model_version = "<champion_model>_phase15_<feature_set_hash>"`

## Module/File Plan
1. New modules:
- `src/fantasy/adapters/mlb/datasets.py`
- `src/fantasy/adapters/mlb/feature_engineering.py`
- `src/fantasy/adapters/mlb/priors.py`
- `src/fantasy/adapters/mlb/backtest.py`
2. Extend existing:
- `src/fantasy/adapters/mlb/projection_adapter.py`
- `src/fantasy/adapters/mlb/features.py`
- `src/fantasy/adapters/mlb/uncertainty.py`
- `src/fantasy/adapters/mlb/registration.py`
3. New scripts:
- `scripts/build_mlb_batter_projection_dataset.py`
- `scripts/backtest_mlb_fantasy_projections.py`
4. Tests:
- `tests/fantasy/adapters/test_mlb_projection_phase15_features.py`
- `tests/fantasy/adapters/test_mlb_projection_phase15_backtest.py`
- `tests/fantasy/adapters/test_mlb_projection_phase15_priors.py`
- Extend `tests/fantasy/adapters/test_mlb_projection_adapter.py`

## Implementation Sequence (TDD)
1. RED: snapshot dataset contract tests.
2. GREEN: build snapshot builder from batter-game data.
3. RED: leakage tests (`anchor_date` never uses future game rows).
4. GREEN: implement shifted rolling features and temporal filters.
5. RED: pybaseball priors join tests with missing-ID fallback.
6. GREEN: implement priors loader/cache/mapping join.
7. RED: backtest harness tests for walk-forward fold generation and MAE aggregation.
8. GREEN: implement backtest runner using existing model registry/trainers.
9. RED: adapter integration tests for count-first then rate-derivation behavior.
10. GREEN: wire adapter to trained artifacts and derived-rate outputs.
11. RED: uncertainty bucket tests and quantile ordering.
12. GREEN: implement OOS residual bucket uncertainty.
13. Run `.venv/bin/pytest -q`.
14. Run `.venv/bin/ruff check .`.

## Backtest Protocol (Acceptance Gate)
1. Training seasons: 2021-2025 available data.
2. Fold policy:
- Train through season `N-1`, test on season `N`.
- Weekly anchors within each test season.
3. Metrics:
- Primary: MAE per metric.
- Secondary: RMSE, bias (`mean(pred-actual)`), coverage for p10/p90.
4. Acceptance:
- At least 7 of 8 directly modeled count metrics must beat Phase 1 baseline MAE by >= 1%.
- No modeled metric may regress by more than 3%.
- Derived-rate metrics must maintain bounded error and no systematic bias > 0.01 absolute rate.
5. If gate fails:
- Auto-fallback to previous champion artifact and keep `source_model_version` unchanged.

## Pybaseball Data Handling
1. Build/update priors cache file by season (Parquet) under configured cache path.
2. Use `playerid_reverse_lookup(..., key_type="mlbam")` for MLBAM->FanGraphs mapping.
3. Fetch-only path guarded by config flag.
4. Network failure fallback:
- Use cached priors if present.
- Else league-median priors with `prior_imputed_flag=1`.
5. Tests never hit network; use fixture priors tables.

## Test Cases and Scenarios
1. Snapshot row correctness for known toy season.
2. Leakage guard with intentionally future-only signal column.
3. Count-to-rate coherence (rates exactly match predicted counts/PA).
4. Missing pybaseball mapping IDs produce deterministic median fallback.
5. Determinism with fixed seed and fixed cached priors.
6. Uncertainty monotonic quantiles and nonnegative stddev.
7. Sparse player history still emits valid row with fallback priors.
8. Cross-metric registration remains complete for all 12 Phase 1 metrics.
9. Existing sportsbook integration tests remain green.

## Docs Updates Required
1. `docs/contracts.md` (Phase 1.5 modeling semantics and rate derivation policy).
2. `docs/architecture.md` (new dataset/backtest/priors modules).
3. `docs/config-schema.md` (new `adapters.mlb_projection_phase15` keys).
4. `README.md` (new offline dataset/backtest commands).
5. `docs/plans/planned/fantasy-rankings/cross-sport-fantasy-rankings-architecture-roadmap.md` (add Phase 1.5 status line).
6. Add plan doc:
- `docs/plans/planned/fantasy-rankings/phase-1.5-mlb-projection-quality-hardening-plan.md`

## Assumptions and Defaults
1. Default anchor frequency is weekly.
2. Default model candidate list: `poisson`, `elastic_net`, `hist_gradient_boosting`, `xgboost`.
3. Default champion selection metric is MAE with RMSE tie-break.
4. Default pybaseball prior seasons: same as training seasons.
5. Default uncertainty method remains empirical quantiles, now OOS and bucketed.
6. Phase 1.5 remains in-memory projection output; artifact persistence is for model/backtest internals only.
