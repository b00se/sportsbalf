# Config Schema Baseline (Current Loader Truth)

Status: Canonical (Current State)

Date: 2026-02-12

## Loader Source of Truth

- `src/core/config.py`
  - `load_pipeline_config(...)`
  - `_resolve_pipeline_identity(...)`
  - `extract_stat_section(...)`

## Root Schema (Sectioned, Required)

Required structure:
- `pipeline.sport` (non-empty string)
- `pipeline.stat` (non-empty string)
- active stat section at `{sport}.{stat}`

Legacy flat schema is intentionally rejected.

## Resolution + Validation Flow

1. Resolve sport/stat from config and optional CLI overrides.
2. Resolve section `raw_config[sport][stat]`.
3. Apply runtime-critical typed validator for registered sport/stat where available.
4. Raise `ConfigValidationError` for missing/invalid required fields.

## Common Error Modes

- Missing/non-mapping config root.
- Missing/non-mapping `pipeline` section.
- Missing `pipeline.sport` or `pipeline.stat`.
- Missing `{sport}` or `{sport}.{stat}` section.
- Type/constraint failures in runtime-critical keys.

## Runtime-Critical Keys by Implemented Stat

### MLB (applies to `strikeouts`, `outs_recorded`, `earned_runs`, `hits_allowed`, `bb_allowed`)
Required:
- `pitch_data_path`
- `model_path`
- `lines_path`

Common optional keys (defaults/fallbacks exist in code):
- `training_data_paths`
- `allow_missing_lines`
- `model_selection.*`
- `monte_carlo_simulations`
- `monte_carlo_seed`
- `fallback_std`
- `live_features.*` (where applicable)

### NFL (`nfl.pass_attempts`)
Required:
- `training_years` (non-empty `list[int]`)

Common optional keys:
- `dataset_path`, `model_path`, `rebuild_dataset`
- `dataset_years`, `start_year`, `end_year`
- `validation_years`, `inference_years`
- `provider`, `model_params`
- simulation/bootstrap sigma controls
- `ud_algolia_id`

### NHL (`nhl.shots_on_goal`)
Required:
- `provider`
- `inference_input_path`
- `model_path`
- `moneypuck_skater_games_snapshot_path`
- `moneypuck_skater_games_curated_cache_path`
- `provider_seasons` (non-empty `list[int]`)
- `feature_rolling_windows` (non-empty `list[int]`)
- `auto_refresh_snapshot` (`bool`)
- `fail_on_provider_error` (`bool`, currently required `true`)

Validated optional constraints:
- `training_seasons` (if present: non-empty `list[int]`)
- `min_training_games_per_player >= 1`
- `sigma_min_history >= 1`
- `min_sigma >= 0`
- `bootstrap_mix_global_prob in [0, 1]`
- `bootstrap_min_sigma >= 0`

## Minimal Valid YAML Examples

### MLB
```yaml
pipeline:
  sport: mlb
  stat: strikeouts

mlb:
  strikeouts:
    pitch_data_path: tests/testdata/pitches.csv
    model_path: /tmp/mlb_strikeouts_model.joblib
    lines_path: tests/testdata/lines_with_odds.csv
```

### NFL
```yaml
pipeline:
  sport: nfl
  stat: pass_attempts

nfl:
  pass_attempts:
    training_years: [2023]
```

### NHL
```yaml
pipeline:
  sport: nhl
  stat: shots_on_goal

nhl:
  shots_on_goal:
    provider: moneypuck_snapshot
    inference_input_path: tests/testdata/nhl_shots_on_goal_input.csv
    model_path: /tmp/nhl_shots_on_goal.joblib
    provider_seasons: [2024]
    moneypuck_skater_games_snapshot_path: tests/testdata/nhl/moneypuck/skater_games_full_snapshot_sample.csv
    moneypuck_skater_games_curated_cache_path: /tmp/moneypuck_skater_games_curated.parquet
    feature_rolling_windows: [5, 10]
    auto_refresh_snapshot: false
    fail_on_provider_error: true
```
