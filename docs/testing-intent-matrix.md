# Testing Intent Matrix

Status: Implemented

Date: 2026-02-14

This matrix maps shipped sport/stat paths and fantasy adapter surfaces to explicit
behavioral invariants and the tests that enforce them.

| sport | stat / surface | horizon | critical invariants | test file(s) | gap status |
|---|---|---|---|---|---|
| mlb | strikeouts | game | simulation probabilities bounded and sum to 1; output simulation contract fields present | `tests/integration/test_mlb_strikeouts_pipeline.py`, `tests/contracts/test_pipeline_output_contracts.py` | covered |
| mlb | outs_recorded | game | `run_mode`/`lines_status` semantics; missing-lines fallback stable; simulation contract and probability algebra | `tests/integration/test_mlb_outs_recorded_pipeline.py`, `tests/contracts/test_pipeline_output_contracts.py` | covered |
| nfl | pass_attempts | game | deterministic outputs with fixed seed; simulation probabilities bounded and sum to 1; EV/edge fields finite | `tests/integration/test_nfl_pass_attempts_pipeline.py`, `tests/contracts/test_pipeline_output_contracts.py` | covered |
| nhl | shots_on_goal | game | deterministic outputs with fixed seed; provider/input fallback returns schema-safe output; simulation contract | `tests/integration/test_nhl_shots_on_goal_pipeline.py`, `tests/contracts/test_pipeline_output_contracts.py`, `tests/test_nhl_pipeline_branches.py` | covered |
| fantasy (mlb adapter) | season projection adapter | season | season-horizon scaling (season mean >= per-game baseline); leakage guard around window boundaries; deterministic seed behavior | `tests/fantasy/adapters/test_mlb_projection_adapter.py`, `tests/contracts/test_pipeline_output_contracts.py` | covered |
| core | model selection policy | n/a | empty leaderboard rejection; unknown metric rejection; maximize/minimize tie-breaks; epsilon boundary behavior | `tests/test_core_model_selection.py` | covered |
| fantasy core | config validation | n/a | mode/horizon normalization edge-cases; malformed definitions; unresolved policy enforcement; market mode mismatch | `tests/fantasy/core/test_config_validation_branches.py` | covered |
| nhl | pipeline branch logic | n/a | sigma fallback hierarchy + clipping; safe inference-input read exceptions; bootstrap on/off sampler wiring; season/min-games training filters | `tests/test_nhl_pipeline_branches.py` | covered |
| legacy | ensemble helper | n/a | legacy status guard remains explicit; deterministic training/prediction with fixed random state | `tests/test_ensemble_legacy_guard.py` | covered |
