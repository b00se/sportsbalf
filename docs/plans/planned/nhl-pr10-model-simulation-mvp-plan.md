# PR#10 Plan: NHL Shots-on-Goal Model MVP with Rich Features + Baseline Uplift

Status: Planned

## Summary
Implement NHL model training/loading and residual-aware simulation using the current NHL shots-on-goal pipeline baseline, with richer feature engineering (including time-on-ice context), and report improvement versus the existing deterministic weighted-average baseline in the same PR.

The objective is to deliver the first model-backed NHL MVP while preserving output compatibility and deterministic offline tests.

## Scope and Non-Goals
### In scope
- Add NHL train/load/retrain model path for `shots_on_goal`.
- Expand NHL feature engineering beyond raw SOG history.
- Add residual/error handling for simulation quality.
- Preserve existing required NHL output schema and append additive metadata columns.
- Add baseline-vs-model uplift reporting and tests.
- Keep tests offline-only and deterministic.

### Out of scope
- New NHL markets beyond `shots_on_goal`.
- Multi-table MoneyPuck joins beyond current curated skater-game cache.
- Major engine contract redesign.

## Public APIs / Interfaces / Type Changes
No breaking signature changes.

### Add files
1. `src/nhl/models/predict.py`
- NHL model helpers (`train_model`, `load_model`, `save_model`, `predict_sog`).
- Feature schema hash helpers and artifact compatibility checks.

2. `src/nhl/models/bootstrap.py`
- Residual bootstrap sampler (player-level with global fallback) for simulation.

3. `src/nhl/models/__init__.py`
- NHL model exports.

4. `tests/test_nhl_model_predict.py`
5. `tests/test_nhl_bootstrap.py`

### Modify files
1. `src/nhl/features/shots_on_goal.py`
- Split into leakage-safe training feature builder and inference feature builder.
- Add baseline predictor helper for uplift comparison.

2. `src/nhl/pipeline.py`
- Integrate model train/load/retrain path.
- Predict with model means at inference.
- Add residual sigma mapping and optional bootstrap sampling in simulation.

3. `src/core/config.py`
- Extend NHL validator for PR#10 model keys.

4. `config/nhl.yaml`
- Add model path and model/residual defaults.

5. `tests/integration/test_nhl_shots_on_goal_pipeline.py`
- Add model lifecycle and deterministic simulation assertions.

6. `tests/test_core_config.py`
- Add PR#10 NHL config validation tests.

7. Docs updates
- `docs/architecture.md`
- `docs/contracts.md`
- `docs/new-sport-playbook.md`
- `docs/plans/planned/nhl-onboarding-sequenced-pr-plan.md` tracker update

## Feature Set for PR#10
Model features for training and inference:
- `sog_avg_last_5`
- `sog_avg_last_10`
- `sog_avg_season_to_date`
- `toi_avg_last_5`
- `toi_avg_last_10`
- `games_played_to_date`
- `days_since_last_game`
- `team_sog_for_avg_last_5`
- `opponent_sog_allowed_avg_last_5`

Baseline comparator:
- Existing deterministic weighted formula output (`predicted_shots_on_goal` from PR#9 logic).

## Config Spec (Decision-Complete)
Required under `nhl.shots_on_goal`:
- Existing PR#9 required keys unchanged.
- `model_path: <non-empty string>`

Optional defaults:
- `training_seasons: <list[int]>` default `provider_seasons`
- `model_name: xgboost`
- `model_params: {}`
- `min_training_games_per_player: 5`
- `sigma_min_history: 5`
- `min_sigma: 0.5`
- `max_sigma: null`
- `bootstrap_enabled: true`
- `bootstrap_mix_global_prob: 0.25`
- `bootstrap_min_sigma: 0.25`

Validation additions:
- `model_path` required string.
- `training_seasons` (if present) non-empty `list[int]`.
- Numeric constraints:
  - `min_training_games_per_player >= 1`
  - `sigma_min_history >= 1`
  - `min_sigma >= 0`
  - `bootstrap_mix_global_prob` in `[0, 1]`

## Data Flow and Runtime Behavior
1. Load inference rows from `inference_input_path` (preserve empty schema fallback).
2. Refresh/load provider data from curated cache path per current flow.
3. Build leakage-safe training frame from provider history:
- sort by `player_id`, `game_date`
- lag all rolling/season features by one game (`shift(1)`) to prevent leakage
- target column remains current-game `shots_on_goal`
4. Train/load model artifact:
- load compatible artifact when present and `retrain=false`
- retrain on missing/incompatible/corrupt artifact
- always retrain when `retrain=true`
5. Build inference feature rows for requested lines.
6. Generate model predictions.
7. Compute baseline prediction in parallel for uplift comparison.
8. Compute simulation sigma per row:
- player sigma if sufficient residual history
- else global sigma
- else config fallback std
- clip to `min_sigma` / optional `max_sigma`
9. Run Monte Carlo with optional residual bootstrap sampler.
10. Return output with required stable columns first and additive metadata columns after.

## Output Schema Policy
Required existing NHL columns remain unchanged and in existing order.

Additive columns appended:
- `baseline_predicted_shots_on_goal`
- `model_residual_std`
- `training_rmse`
- `training_mae`
- `training_r2`
- `model_name`

## Failure Modes and Policy
- Provider load/refresh failures remain fail-hard under current config policy.
- Empty/missing inference input still returns empty stable schema frame.
- Missing player feature history for some rows uses baseline fallback prediction path.
- Incompatible model artifact auto-retrains and proceeds.
- Missing optional prices still default-fill robustly.

## TDD Plan (RED -> GREEN)
### RED
Add failing tests first:
- Config validation tests for new NHL model keys and numeric constraints.
- Model tests:
  - train/save/load roundtrip
  - schema hash mismatch -> retrain path
  - corrupted artifact -> retrain path
- Bootstrap tests:
  - player pool selection
  - global fallback behavior
  - deterministic seeded sampling behavior
- Integration tests:
  - deterministic repeated outputs with fixed seed
  - retrain false loads compatible artifact
  - incompatible artifact retrains automatically
  - required columns remain stable and additive columns present

Run:
- `.venv/bin/pytest -q tests/test_nhl_model_predict.py tests/test_nhl_bootstrap.py tests/test_nhl_features.py tests/integration/test_nhl_shots_on_goal_pipeline.py tests/test_core_config.py`

### GREEN
- Implement model modules, feature updates, config updates, and pipeline integration.
- Re-run targeted tests to green.

### Final verification gates
- `.venv/bin/ruff check .`
- `.venv/bin/pytest -q`

## Test Cases and Scenarios
- Model artifact creation and loading is deterministic and reproducible.
- Schema mismatch or broken model artifact triggers retrain instead of runtime crash.
- Residual sigma hierarchy behaves as designed (player -> global -> fallback).
- Simulation is deterministic with fixed Monte Carlo seed.
- Existing required NHL output columns remain stable.
- Additive model metadata columns are present and non-breaking.
- Baseline vs model prediction columns are both emitted for uplift measurement.

## Risks and Mitigations
- Risk: feature leakage from same-game rows.
  - Mitigation: explicit lagging and leakage regression tests.
- Risk: sparse player history hurts residual stability.
  - Mitigation: sigma fallback hierarchy and sigma clipping.
- Risk: artifact schema drift across feature changes.
  - Mitigation: feature schema hash checks + automatic retrain fallback.
- Risk: output schema regressions.
  - Mitigation: strict integration assertions on required column order.

## Assumptions and Defaults
- Current curated skater-game layer is sufficient for PR#10 MVP.
- Time-on-ice and opponent/team context features are available from current curated schema derivations.
- Additional MoneyPuck tables are deferred to later expansion PRs.
- Tests remain offline-only; network fetch remains optional in manual smoke runs.
