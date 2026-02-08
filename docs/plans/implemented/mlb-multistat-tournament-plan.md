# Updated Plan: Reusable Multi-Stat Model Tournament with Quantile vs K-Means Bucketing

Status: Implemented

## Summary
Build a stat-agnostic model tournament framework (reused later for additional pitcher stats) while implementing MLB strikeouts as the first adapter.  
For segmented modeling, implement **both** workload bucketing strategies:
1. deterministic 3-quantile buckets, and
2. k-means buckets,  
then select the better strategy by walk-forward MAE using fixed tie-break rules.

Primary objective remains lowest prediction error (MAE), with a runtime budget of ~30 minutes per tournament run.

## Scope
In scope:
1. Preserve target-integrity fixes (pitch dedupe + strikeout sanity checks).
2. Refactor tournament/training/evaluation into reusable stat-agnostic core.
3. Keep MLB strikeouts as concrete implementation.
4. Add segmented strategy with quantile and k-means bucketing, evaluate both.
5. Auto-select and persist champion strategy/model/metadata.
6. Keep pipeline output schema non-breaking.

Out of scope:
1. New external providers/dependencies.
2. Neural nets/heavy infra.
3. Breaking existing run signatures or output columns.

## Public API / Interface Changes
1. No change to `run(config_path: str | None = None, retrain: bool = False)`.
2. Add non-breaking config fields under `mlb.strikeouts.model_selection`:
   1. `enabled`
   2. `candidates`
   3. `primary_metric: mae`
   4. `tie_breakers: [rmse, r2]`
   5. `runtime_budget_minutes: 30`
   6. `tuning.enabled`
   7. `tuning.max_trials_per_model`
   8. `segmentation.enabled`
   9. `segmentation.bucket_methods: [quantile3, kmeans]`
   10. `segmentation.kmeans.n_clusters: 3`
   11. `segmentation.min_bucket_size`
3. New reusable core interfaces (internal):
   1. `StatAdapter` (target col, features, prep hooks, inference mapping)
   2. `BucketStrategy` (`fit`, `assign`, `serialize`, `deserialize`)
   3. `TournamentRunner` (walk-forward evaluate/score/select)
4. Preserve existing output DataFrame schema for MLB strikeouts.

## Architecture and Implementation

### 1) Target Integrity (keep first)
1. Keep canonical pitch-row dedupe in feature aggregation.
2. Keep strikeout-scale diagnostics (mean/p95/max, impossible values).
3. Ensure dedupe logic is used consistently in training-data assembly paths.

### 2) Reusable Core (stat-agnostic)
1. Create core modules for:
   1. model registry + factories,
   2. generic trainers (fit/predict, impute/scale behavior),
   3. walk-forward evaluation,
   4. deterministic champion selection,
   5. artifact/metadata persistence.
2. Move stat-specific assumptions behind adapter methods:
   1. `target_col`
   2. `feature_columns`
   3. training frame prep
   4. inference row prep/fallbacks

### 3) Segmentation: Quantile vs K-Means
1. Implement `QuantileBucketStrategy`:
   1. derive 3 workload buckets from training-fold rolling pitch-count quantiles.
2. Implement `KMeansBucketStrategy`:
   1. features for clustering: expected workload-related features only (no target leakage),
   2. fit scaler + k-means on train fold only,
   3. assign train/test rows by trained centroids.
3. Stability guardrails:
   1. enforce `min_bucket_size` per fold,
   2. if invalid/degenerate cluster distribution, fallback to quantile strategy for that fold.
4. Compare strategies on identical splits and candidate models; select strategy+model by same champion rule.

### 4) Model/Tuning Tournament
1. Candidate models:
   1. XGBoost
   2. RandomForest
   3. HistGradientBoosting
   4. ElasticNet
   5. PoissonRegressor
2. Add bounded tuning under runtime budget:
   1. deterministic seed,
   2. capped trials per model,
   3. narrow high-impact hyperparameter spaces.
3. Evaluate on seasonal walk-forward splits:
   1. MAE primary,
   2. RMSE tie-break 1,
   3. R² tie-break 2,
   4. simplicity order tie-break 3.

### 5) Champion Packaging and Pipeline Integration
1. Persist winner artifact and metadata:
   1. selected model and strategy,
   2. quantile thresholds or k-means scaler+centroids,
   3. training window,
   4. metric snapshot,
   5. feature schema hash,
   6. fold-level metrics.
2. In inference path:
   1. load champion strategy/model,
   2. assign incoming rows to bucket if segmented,
   3. predict via routed model(s),
   4. preserve existing downstream simulation/output columns.
3. Fallback behavior:
   1. if champion artifact missing/incompatible, retrain/load baseline and warn.

### 6) Reporting
1. Extend backtest script outputs:
   1. leaderboard CSV (model + strategy level),
   2. champion JSON metadata,
   3. fold-level CSV,
   4. optional error-slice CSV.
2. Terminal summary includes:
   1. winning strategy,
   2. winning model,
   3. MAE/RMSE/R².

## Files (Planned Touchpoints)
1. `src/mlb/pipeline.py` (adapter wiring + champion load logic)
2. `src/mlb/models/predict.py` (feature list remains centralized)
3. `src/mlb/models/registry.py` (shared candidate specs)
4. `src/mlb/models/trainers.py` (shared fit/predict)
5. `src/mlb/models/evaluation.py` (walk-forward + selection)
6. New reusable core modules under `src/core/` or `src/models/` (stat-agnostic abstractions)
7. New bucket strategy module(s) under MLB models or shared modeling package
8. `scripts/backtest_mlb_strikeouts.py` (strategy-aware tournament reporting)
9. `config/mlb.yaml` (non-breaking model_selection additions)
10. `README.md` (updated usage + interpretation)

## Test Cases and Scenarios
1. Dedup integrity: duplicate pitch rows do not inflate strikeouts.
2. Registry/trainer: all candidates instantiate and fit/predict deterministically.
3. Walk-forward parity: all models and both bucket methods use identical split boundaries.
4. K-means leakage guard: centroids/scaler fit only on train folds.
5. Bucket fallback: degenerate k-means distribution triggers quantile fallback.
6. Selection determinism: same inputs/seeds produce same strategy+model winner.
7. Routing compatibility: champion artifact loads and predicts in pipeline inference.
8. Schema compatibility: MLB output columns unchanged.
9. Offline suite: tests pass without network.

## Acceptance Criteria
1. Tournament runs end-to-end within ~30 minutes.
2. Leaderboard includes strategy (`global`, `quantile3`, `kmeans`) + model metrics.
3. Champion is deterministically selected by MAE tie-break rules.
4. Pipeline loads champion and produces standard outputs with no schema break.
5. Metadata fully reproduces winner strategy/model setup.
6. Tests pass offline; no protected-directory code changes.

## Assumptions and Defaults
1. Primary objective fixed to lowest MAE.
2. Temporal CV fixed to season-based walk-forward.
3. Default segmentation comparison includes both `quantile3` and `kmeans`.
4. `kmeans` defaults to 3 clusters unless config overrides.
5. No new dependencies or external providers introduced in this PR.
6. If runtime budget is exceeded, reduce tuning trial counts before reducing model/strategy coverage.
