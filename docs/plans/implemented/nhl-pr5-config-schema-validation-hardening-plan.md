# PR#5 Plan: Config Schema Validation Hardening (Sectioned-Only, Runtime-Critical Typed Checks)

Status: Implemented

## Summary

Implement PR#5 by hardening config validation in src/core/config.py so config loading fails fast with precise field-path errors before pipeline runtime.
This plan applies your chosen policy:

- legacy flat schema is no longer accepted (sectioned schema required),
- typed validation focuses on runtime-critical keys only.

Scope covers loader behavior, typed sport/stat validators for current implemented pipelines, regression tests (including migration/rejection tests for legacy shape), and docs updates.

## Public API / Interface / Type Changes

1. src/core/config.py

- Keep load_pipeline_config(...) signature unchanged.
- Keep extract_stat_section(...) signature unchanged.
- Add strict sectioned-schema enforcement:
  - reject missing/non-mapping pipeline with a migration-focused ConfigValidationError.
- Add typed sport/stat validation step after section extraction.
- Add validator dispatch keyed by (sport, stat) for implemented pipelines.

2. Error contract (new/updated behavior)

- Errors must include precise field paths (for example, pipeline.sport, mlb.strikeouts.lines_path, nfl.pass_attempts.training_years[0]).
- Legacy flat schema must produce a specific migration message (not silent fallback).
- Type coercion policy: validate types, do not silently coerce invalid critical types.

3. No changes

- PipelineConfig dataclass shape remains unchanged.
- Engine entrypoints and CLI override signatures remain unchanged.
- Registry behavior remains unchanged in this PR.

## Validation Spec (Runtime-Critical Only)

1. Root/schema-level

- pipeline must exist and be a mapping.
- pipeline.sport and pipeline.stat must be non-empty strings.
- {sport} section must exist and be a mapping.
- {sport}.{stat} section must exist and be a mapping.

2. MLB implemented stats (strikeouts, outs_recorded, earned_runs, hits_allowed, bb_allowed)

- Required keys in mlb.<stat>:
  - pitch_data_path: str
  - model_path: str
  - lines_path: str

3. NFL pass_attempts

- Required keys in nfl.pass_attempts:
  - training_years: list[int] and non-empty
- Validate each list element is integer-like and not bool.
- Keep existing optional defaults for all other keys.

4. Unknown sport/stat behavior

- If section exists but no typed validator is registered, keep current loader flow (no new failure here) so PR#6/PR#8 can extend without reworking loader contract.

## Detailed Implementation Plan

1. RED: add failing tests first

- Extend tests/test_core_registry.py with config-loader-focused tests (or add tests/test_core_config.py if cleaner).
- Add tests for:
  - rejecting missing/non-mapping pipeline (legacy flat config) with migration message.
  - missing pipeline.sport and pipeline.stat field-path errors.
  - missing sport section and stat section errors.
  - MLB runtime-critical key/type failures (one parametrized test per required key).
  - NFL training_years failures:
    - missing key,
    - empty list,
    - wrong type (scalar/string),
    - non-int list entries.
  - positive cases:
    - config/mlb.yaml loads for each implemented MLB stat using run_pipeline_with_overrides(...) identity resolution only.
    - config/nfl.yaml loads for nfl.pass_attempts.
- Confirm tests fail against current permissive loader.

2. GREEN: implement typed validation in loader

- In src/core/config.py:
  - Add internal helpers:
    - _validate_sectioned_schema_root(raw_config).
    - _validate_required_str(section, path).
    - _validate_required_non_empty_int_list(section, path).
    - _validate_required_str_key(section, path).
  - Add sport/stat validator dispatcher, for example:
    - _VALIDATORS: dict[tuple[str, str], Callable[[dict[str, Any], str], None]]
  - Call validator after extract_stat_section(...).
  - Keep load_pipeline_config(...) return type/shape unchanged.
- Normalize all new errors to ConfigValidationError with field-path-first messaging.

3. Migration/fallback coverage (legacy shape)

- Add explicit test proving flat config rejection with migration guidance:
  - expected message references required sectioned format:
    - pipeline.sport, pipeline.stat, and {sport}.{stat}.
- Add test proving CLI overrides do not bypass missing pipeline requirement in sectioned-only mode.

4. Docs updates

- docs/config-schema.md:
  - move legacy flat schema from “supported” to “rejected in PR#5”.
  - document typed runtime-critical required keys per implemented stat.
  - add exact error-mode examples with field paths.
- docs/plans/planned/nhl-onboarding-sequenced-pr-plan.md:
  - mark PR#5 scope details aligned to sectioned-only decision.
- Optional new plan doc:
  - docs/plans/planned/nhl-pr5-config-schema-validation-hardening-plan.md with final PR execution checklist.

## Test Cases and Scenarios

1. Root schema

- missing pipeline mapping -> fail with migration error.
- pipeline present but missing/blank sport or stat -> fail with exact field path.

2. Section resolution

- missing sport mapping -> fail Missing sport section '<sport>'.
- missing stat mapping -> fail Missing stat section '<sport>.<stat>'.

3. MLB critical keys

- each missing key (pitch_data_path, model_path, lines_path) -> fail with full path.
- wrong type for each key -> fail with full path and expected type.

4. NFL critical keys

- training_years missing/empty/non-list/non-int elements -> fail with field-path detail.

5. Positive config load

- config/mlb.yaml validates for all implemented MLB stats.
- config/nfl.yaml validates for pass_attempts.

## Verification Commands (for implementation PR)

1. RED evidence

- .venv/bin/pytest -q tests/test_core_registry.py (or new config test module) before implementation.

2. GREEN evidence

- .venv/bin/pytest -q tests/test_core_registry.py (and any new config test module) after implementation.

3. Repo gates

- .venv/bin/ruff check .
- .venv/bin/pytest -q

## Assumptions and Defaults

1. Legacy flat schema support is intentionally removed in PR#5 (sectioned-only required).
2. Typed validation is intentionally limited to runtime-critical keys; unknown optional/nested keys remain permissive.
3. No output schema changes for MLB/NFL pipelines.
4. No changes under data/, models/, notebooks/, betslips/.
5. Error messaging standard: field-path-specific ConfigValidationError for actionable debugging.
