# Config Schema Baseline (Current Loader Truth)

Status: Canonical (Current State)

Date: 2026-02-08

## Loader source of truth
- `src/core/config.py` (`load_pipeline_config`, `_resolve_pipeline_identity`, `extract_stat_section`)

## Root schema
### Sectioned schema (required)
- `pipeline.sport` (required)
- `pipeline.stat` (required)
- Active stat section located at `{sport}.{stat}`

### Legacy flat schema (rejected in PR#5)
- If `pipeline` is missing or non-mapping, config loading fails with a migration-focused `ConfigValidationError`.
- Required sectioned shape is explicitly referenced in the error (`pipeline.sport`, `pipeline.stat`, `{sport}.{stat}`).

## Section resolution behavior
1. Resolve sport/stat identity from `pipeline.*` or CLI overrides.
2. Load `raw_config[sport][stat]`.
3. Raise validation errors when sport/stat sections are missing.
4. Apply runtime-critical typed validators for implemented pipelines.
5. If no validator is registered for a resolved sport/stat, loader remains permissive for section contents.

## Validation behavior and current error modes
- Missing/non-mapping `pipeline`:
  - raises `ConfigValidationError` with migration guidance and required sectioned fields.
- Missing `pipeline.sport` in sectioned schema:
  - raises `ConfigValidationError("Invalid required field 'pipeline.sport': expected non-empty string.")`
- Missing `pipeline.stat` in sectioned schema:
  - raises `ConfigValidationError("Invalid required field 'pipeline.stat': expected non-empty string.")`
- Missing sport section:
  - raises `ConfigValidationError("Missing sport section '<sport>' in config.")`
- Missing stat section:
  - raises `ConfigValidationError("Missing stat section '<sport>.<stat>' in config.")`
- Non-mapping config root:
  - raises `ConfigValidationError("Config root must be a mapping.")`

## Key tables by implemented stat
Notes:
- Tables below show the practical baseline for current execution paths.
- Some keys have defaults in code and are optional; keys listed as required are directly indexed and expected in normal runs.

### MLB `strikeouts` (section: `mlb.strikeouts`)
Runtime-critical required:
- `pitch_data_path`
- `model_path`
- `lines_path`

Optional (current defaults/fallbacks exist):
- `training_data_paths`
- `fallback_std` (default `1.0`)
- `monte_carlo_simulations` (default `10000`)
- `monte_carlo_seed`
- `allow_missing_lines` (default `False`)
- `model_selection.*`
- `live_features.*`
- `earned_runs_labels_path`
- `pitcher_dataset_output_path`
- `batter_dataset_output_path`
- `park_factor_min_samples` (default `20`)
- `park_factor_half_life_games` (default `60`)

### MLB multi-stat pitcher props
Applies to sections:
- `mlb.outs_recorded`
- `mlb.earned_runs`
- `mlb.hits_allowed`
- `mlb.bb_allowed`

Runtime-critical required:
- `pitch_data_path`
- `model_path`
- `lines_path`

Optional:
- same optional set as MLB strikeouts above (stat-specific line column is inferred by stat key, not config key)

### NFL `pass_attempts` (section: `nfl.pass_attempts`)
Runtime-critical required:
- `training_years` (non-empty `list[int]`; bool and non-int entries are rejected with index-qualified field-path errors)

Optional (defaults exist):
- `dataset_path` (default `data/qb_attempts_dataset.parquet`)
- `model_path` (default NFL model constant)
- `rebuild_dataset` (default falsey)
- `dataset_years` or (`start_year`/`end_year`) for dataset build range
- `inference_years` / `validation_years` (fallback to latest season)
- `provider`
- `model_params`
- `sigma_min_history` (default `4`)
- `bootstrap_min_history` (default `5`)
- `bootstrap_mix_global_prob` (default `0.25`)
- `bootstrap_min_sigma` (default `0.5`)
- `fallback_std` (default `1.0`)
- `min_sigma` (default `1.5`)
- `max_sigma`
- `monte_carlo_simulations` (default `10000`)
- `monte_carlo_seed`
- `ud_algolia_id`

## Canonical minimal YAML examples
### MLB (minimal valid sectioned shape)
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

### NFL (minimal valid sectioned shape)
```yaml
pipeline:
  sport: nfl
  stat: pass_attempts

nfl:
  pass_attempts:
    training_years: [2023]
```

## Forward-looking note
PR#5 typed schema hardening is implemented. Legacy flat schema fallback is intentionally removed.
