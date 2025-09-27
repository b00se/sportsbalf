# Phase 2: NFL QB Pass Attempts – Dataset Expansion, Modeling, Simulation

## 0. Objective & Deliverables

Build an NFL pass-attempts pipeline that mirrors the MLB strikeout workflow. Phase 2 should deliver:
- An expanded QB attempts dataset (v1.1) with advanced features and documented schema.
- Config-driven training + inference code that trains a model, persists it, and returns predictions.
- Monte Carlo probabilities aligned with existing MLB simulation tooling.
- Tests covering feature engineering, modeling, and simulation behaviors.
- CLI/documentation so the end-to-end NFL workflow is runnable alongside MLB.

## 1. Dataset Expansion & Feature Engineering

Location: `src/nfl/data/qb_attempts.py` (extend) and new feature helpers under `src/nfl/features/` as needed.

1. **Source Data**
   - Continue using `nfl_data_py.import_weekly_data(years)` and schedules from Phase 1.
   - Pull supporting datasets:
     - `nfl.import_pbp_data(years)` for play volume, pass rate, pace.
     - `nfl.import_ngs_data(years)` (optional) for `CPOE`, `air_yards`. Gracefully skip if unavailable.
   - Cache raw pulls to `data/raw/` (e.g., `pbp_2015_2024.parquet`) to avoid repeated downloads.

2. **Feature Engineering**
   - Create reusable helpers (e.g., `src/nfl/features/team_rates.py`) to compute:
     - **Player metrics**: `season_avg_attempts`, `career_avg_attempts`, `rush_attempts`, `CPOE`, `EPA`, `air_yards`.
     - **Team metrics**: neutral-script pass rate (score within ±7 in Q1-3), pace (plays per game), pass rate over expected.
     - **Opponent metrics**: pass attempts allowed, defensive CPOE allowed, pressure/blitz proxies.
     - **Game context**: `is_home_game`, `spread`, `total`, `short_week` (<=4 days rest), `is_divisional`.
   - Ensure merges use `season`, `week`, `game_id`, `qb_id`. Keep `game_id` as string.

3. **Missing Data Strategy**
   - Player stats: backfill with season average; fallback to career average; final fallback to league QB median.
   - Team/opponent metrics: use rolling values up to prior week; Week 1 falls back to prior season averages.
   - Document defaults in code comments and adopt config knobs if needed.

4. **Schema & Output**
   - Rename/align columns where new features are added; maintain existing naming (e.g., `pass_attempts`, `ud_line`).
   - Store metadata in parquet (e.g., `schema_version=\"1.1\"`).
   - Update dataset builder CLI (`scripts/build_qb_attempts_dataset.py`) to invoke new feature steps and log counts.

## 2. Modeling & Training

Code path: `src/nfl/models/predict.py` (new module) + updates to `src/nfl/pipeline.py`.

1. **Feature Set Definition**
   - Centralize in `NFL_FEATURES` list. Include engineered columns only (exclude target, IDs, UD lines).
   - Add validation to ensure features exist before training/prediction.

2. **Model Choice & Training**
   - Start with `XGBRegressor` (parity with MLB). Allow alternative models via config.
   - Split data chronologically: train 2015-2022, validate 2023, test 2024+.
   - Prevent leakage: rolling features must exclude current game; market lines should not enter training features.
   - Implement `train_model(df, params)`, `predict_attempts(df, model)`, `residual_std(y_true, y_pred)`.
   - Save model to `models/xgb_qb_attempts.joblib`; include training metadata (timestamp, params) in a sidecar JSON.

3. **Evaluation & Logging**
   - Compute RMSE, MAE, R² on validation/test splits.
   - Optional: feature importance & calibration plots; log metrics to stdout or JSON.
   - Stop for clarification if metrics are unacceptably poor (e.g., R² < 0.3).

## 3. Monte Carlo Simulation

Aim: produce probabilities of exceeding or falling short of UD lines, consistent with MLB pipeline.

1. **Simulation Prep**
   - Use prediction outputs + residual standard deviation (or bootstrapper) as inputs.
   - Reuse `src/mlb/models/monte_carlo.apply_simulations` by generalizing columns or adding NFL wrapper (same signature).
   - Clamp simulated attempts to non-negative integers.

2. **Outputs**
   - Add `prob_higher`, `prob_lower`, `model_residual_std`, expected values if odds available.
   - Maintain column naming parity with MLB where possible (`upcoming_game_date`, `upcoming_opponent` if added).

## 4. Pipeline & CLI Integration

1. **Config**
   - Create `config/nfl.yaml` mirroring MLB config structure:
     - Dataset paths, training years, model path, UD algolia ID (`PickemStat_de868934-c920-405c-b827-693c15aa47a1`), Monte Carlo settings.

2. **Pipeline Entry (`src/nfl/pipeline.py`)**
   - Load config, dataset, and UD lines.
   - If `retrain` set or model missing, train and persist before inference.
   - Build prediction rows (latest game info, opponent context) similar to MLB `_latest_games` logic.
   - Run model inference + Monte Carlo; return enriched DataFrame.

3. **CLI**
   - Add `scripts/predict_qb_attempts.py` to run the NFL pipeline and persist predictions (CSV/Parquet) for slips.
   - Consider extending `scripts/build_betslips.py` to consume NFL outputs or add a new slip builder for NFL props.

## 5. Testing & Validation

1. **Unit Tests**
   - Feature joins & defaults: ensure no leakage, correct handling of Week 1, and team normalization.
   - Modeling: smoke test training/prediction on fixture dataset; verify metric calculation.
   - Simulation: test probability monotonicity relative to line, and non-negative outputs.
   - Place tests under `tests/` (e.g., `test_qb_features.py`, `test_qb_model.py`, `test_qb_simulation.py`).

2. **Integration Test**
   - Add pipeline smoke test that mocks UD lines and ensures output schema matches expectations.

3. **Warnings & Quality**
   - Keep `pytest` warnings clean (existing `pytest.ini` filters cover pandas/numpy notices).

## 6. Documentation & Follow-Up

- Update README or create `instructions/qb_attempts_phase2.md` summary after implementation.
- Document config options, required environment packages (`nfl_data_py`, `xgboost`, etc.).
- List open questions (e.g., handling limited NGS coverage, correlation between slips).
- Future phases: integrate odds shopping, correlation adjustments, scheduling automation.

## 7. When To Ask For Help

- Data source unavailable or fields missing (e.g., `import_ngs_data` returns empty).
- Model performance below agreed thresholds after reasonable tuning.
- Significant schema drift that impacts downstream consumers.
- Need guidance on prioritizing features vs. modeling complexity.
