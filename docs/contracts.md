# Sport/Stat Contract Baseline (Current Behavior)

Status: Canonical (Current State)

Date: 2026-02-08

## Contract source
The authoritative protocol is `SportStatPipeline` in `src/core/contracts.py`.

## Stage contract (intended vs current behavior)
| Stage | Intended responsibility | Current MLB/NFL behavior | Required handoff artifact |
|---|---|---|---|
| `load_inputs(config)` | Gather raw sources and return structured inputs | Returns minimal payload with `config_path` only | `PipelineInputs` |
| `build_training_frame(inputs, config)` | Build model-ready training frame | Returns empty `DataFrame` in adapters | `pd.DataFrame` |
| `train_or_load_model(frame, config, retrain)` | Produce/load trained artifacts | Stores retrain flag, returns empty `ModelBundle` | `ModelBundle` |
| `predict_lines(inputs, model_bundle, config)` | Produce pre-simulation prediction rows | Returns empty `DataFrame` in adapters | `pd.DataFrame` |
| `simulate(predictions, model_bundle, config)` | Run simulation and return final output | Performs full end-to-end workflow in current adapters | Final `pd.DataFrame` |

## Adapter exception (temporary compatibility pattern)
- MLB (`src/mlb/pitcher_props/adapter.py`) and NFL (`src/nfl/pass_attempts/pipeline.py`) satisfy the protocol but defer all business logic to `simulate(...)`.
- This compatibility pattern is intentionally preserved for current behavior and backward compatibility.
- Future sports should not assume this is required; stage-by-stage semantics may be enforced later.

## Output fields (current stability baseline)
### Common simulation fields
- `prob_over`
- `prob_under`
- `prob_push`
- `ev_over`
- `ev_under`
- `edge_over`
- `edge_under`

### MLB fields by stat family
- Strikeouts:
  - prediction + line: `predicted_strikeouts`, `k_line`
- Outs recorded:
  - prediction + line: `predicted_outs_recorded`, `outs_line`
- Earned runs:
  - prediction + line: `predicted_earned_runs`, `er_line`
- Hits allowed:
  - prediction + line: `predicted_hits_allowed`, `hits_line`
- Walks allowed:
  - prediction + line: `predicted_bb_allowed`, `bb_line`
- MLB mode/availability metadata (where applicable):
  - `run_mode`
  - `lines_status`

### NFL pass attempts fields
- prediction + line:
  - `predicted_pass_attempts`
  - `attempts_line`
- plus common simulation probability/EV/edge fields above

### NHL shots on goal fields (PR#10 model + simulation MVP)
- identity/line:
  - `player_id`
  - `player_name`
  - `team`
  - `opponent`
  - `game_id`
  - `sog_line`
- prediction:
  - `predicted_shots_on_goal`
- additive model metadata:
  - `baseline_predicted_shots_on_goal`
  - `model_residual_std`
  - `training_rmse`
  - `training_mae`
  - `training_r2`
  - `model_name`
- simulation outputs:
  - shared `prob_*`, `ev_*`, `edge_*` fields above
- mode metadata:
  - `run_mode`
  - `lines_status`

### NHL PR#10 runtime data/feature/model baseline
- Data provider abstraction:
  - `src/nhl/data/providers/base.py`
  - `src/nhl/data/providers/moneypuck_snapshot.py`
- Curated cache canonical schema:
  - `season`, `game_id`, `game_date`, `player_id`, `player_name`
  - `team`, `opponent`, `shots_on_goal`, `time_on_ice_minutes`
- Deterministic baseline feature fields:
  - `sog_avg_last_5`
  - `sog_avg_last_10`
  - `sog_avg_season_to_date`
- Additional model features:
  - `toi_avg_last_5`
  - `toi_avg_last_10`
  - `games_played_to_date`
  - `days_since_last_game`
  - `team_sog_for_avg_last_5`
  - `opponent_sog_allowed_avg_last_5`
- Model artifact compatibility:
  - NHL model artifacts persist feature schema hash and model metadata
  - incompatible/corrupted artifacts automatically retrain when running the pipeline

## Field stability policy for onboarding work
- Existing MLB/NFL output columns above are treated as backward-compatible commitments for this onboarding program.
- New NHL outputs should align to shared probability/EV conventions (`prob_*`, `ev_*`, `edge_*`) from day one.

## PR#2 enforcement baseline
- Engine contract invariants are enforced in `tests/test_engine_contract_enforcement.py`:
  strict stage order, stage handoff artifacts, CLI override passthrough, and default registration pairs.
- Temporary simulate-only adapter behavior is explicitly allowlisted in that test module via fully qualified class names.
- Any new simulate-only adapter must be intentionally added to the allowlist; silent introduction of new no-op adapters is a test failure.

## PR#3 shared simulation extraction note
- Shared simulation primitives now live in `src/core/simulation.py`:
  `MonteCarloConfig`, `simulate_row(...)`, and `apply_simulations(...)`.
- MLB compatibility imports remain supported via `src/mlb/models/monte_carlo.py`
  as thin re-exports to preserve existing callsites.

## PR#4 neutral simulation naming + NFL shim removal
- Shared simulation interfaces use sport-neutral argument names (`line`, `entity_id`).
- `apply_simulations(...)` uses explicit `line_col` and `id_col` mappings from callers.
- NFL pass-attempt orchestration no longer aliases `ud_line -> k_line` or `qb_id -> pitcher_id`.
- MLB/NFL output column commitments above remain unchanged.
