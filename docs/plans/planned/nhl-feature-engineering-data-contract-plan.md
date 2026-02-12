# NHL SOG Feature Engineering Plan (Data-Contract First, NHL API-First)

## Summary
Build a decision-complete feature engineering roadmap for NHL shots-on-goal that upgrades the current 9-feature model to usage/context-rich features, while keeping the existing pipeline contract stable.

Chosen preferences:
- Source strategy: NHL API-first
- Refresh cadence: Daily batch
- Market context: Include now

## Goals
1. Increase predictive lift with high-ROI opportunity features (TOI/PP usage first).
2. Add walk-forward backtesting to measure true out-of-sample gains.
3. Define exact datasets and schemas so sourcing can start immediately.
4. Keep inference execution as one-step pipeline run from config.

## Current State (Grounded in Repo)
1. Current model features are fixed in `src/nhl/models/predict.py` via `NHL_FEATURES` (9 columns).
2. Current canonical history schema from ingest is in `src/nhl/data/moneypuck_ingest.py` (`season, game_id, game_date, player_id, ... shots_on_goal, time_on_ice_minutes`).
3. Feature builders exist in `src/nhl/features/shots_on_goal.py` with leakage-safe lagged logic for existing features.
4. Pipeline training/inference orchestration is single-fit (not walk-forward) in `src/nhl/pipeline.py`.

## Public Interfaces / Config Changes (Planned)
1. Extend `nhl.shots_on_goal` config section with new dataset paths:
- `inference_input_path`
- `history_player_game_path`
- `history_team_game_path`
- `history_goalie_game_path`
- `schedule_context_path`
- `market_context_path`
- `feature_store_output_path`
2. Add feature toggles and defaults:
- `enable_market_features: true`
- `enable_goalie_features: true`
- `enable_pp_usage_features: true`
- `walkforward_enabled: true`
- `walkforward_train_window_games: 82`
- `walkforward_step_games: 1`
3. Add feature-set versioning:
- `feature_set_version: nhl_sog_v2`
4. Keep `run()` and pipeline entrypoint signatures unchanged.

## Exact Data Required (Sourceable Now)

### Dataset A: Player-Game Fact Table (Core Training Table)
Purpose: primary feature/target source for every player-game row.

Required columns:
- `season` (int)
- `game_id` (string)
- `game_date` (date)
- `player_id` (string)
- `player_name` (string)
- `team` (string)
- `opponent` (string)
- `is_home` (bool)
- `position` (string)
- `shots_on_goal` (float) <- target
- `goals` (float)
- `assists` (float)
- `points` (float)
- `time_on_ice_minutes` (float)
- `ev_toi_minutes` (float)
- `pp_toi_minutes` (float)
- `sh_toi_minutes` (float)
- `shifts` (float)
- `shot_attempts` (float) (Corsi individual)
- `unblocked_attempts` (float) (Fenwick individual)
- `ixg` (float) (individual expected goals)
- `individual_rebounds_created` (float) optional
- `individual_high_danger_attempts` (float) optional

Primary source plan:
- NHL API for boxscore/game logs baseline fields.
- MoneyPuck (or equivalent advanced table) for `ixg`, attempts, danger splits if NHL API lacks them.

### Dataset B: Team-Game Context Table
Purpose: pace and defensive environment features.

Required columns:
- `season`
- `game_id`
- `game_date`
- `team`
- `opponent`
- `team_shots_for`
- `team_shots_against`
- `team_corsi_for`
- `team_corsi_against`
- `team_fenwick_for`
- `team_fenwick_against`
- `team_xg_for`
- `team_xg_against`
- `team_pp_opportunities`
- `team_pp_goals`
- `team_pk_opportunities_against`
- `team_pk_goals_against`

Primary source plan:
- NHL API for shots, special teams basics.
- MoneyPuck-like advanced source for Corsi/Fenwick/xG if unavailable in NHL API.

### Dataset C: Goalie Opponent Table (Game-Level)
Purpose: adjust shot expectation by expected goalie quality.

Required columns:
- `season`
- `game_id`
- `game_date`
- `team` (goalie team)
- `goalie_id`
- `goalie_name`
- `is_confirmed_starter` (bool)
- `goalie_toi_minutes`
- `saves`
- `shots_against`
- `goals_against`
- `sv_pct`
- `xg_against` (optional)
- `goals_saved_above_expected` (optional)

Primary source plan:
- NHL API for starter/boxscore goalie stats.
- Optional advanced source for GSAx.

### Dataset D: Schedule + Rest/Travel Context
Purpose: capture fatigue and deployment pattern effects.

Required columns:
- `game_id`
- `game_date`
- `team`
- `opponent`
- `is_home`
- `team_days_rest`
- `opponent_days_rest`
- `team_back_to_back` (bool)
- `opponent_back_to_back` (bool)
- `team_3in4` (bool)
- `timezone_jump_hours` (optional proxy)

Primary source plan:
- NHL schedule endpoints.

### Dataset E: Market Context Table
Purpose: strong lift/calibration features.

Required columns:
- `game_id`
- `game_date`
- `player_id`
- `book`
- `sog_line`
- `over_decimal_price`
- `under_decimal_price`
- `opening_sog_line` (if available)
- `closing_sog_line` (if available)
- `line_move_abs` (derived)
- `game_total` (if available)
- `team_implied_total` (if available)

Primary source plan:
- Existing sportsbook/feed source used for props ingestion.

### Dataset F: Inference Slate Input (Daily All-Player)
Purpose: one row per player prediction request.

Required columns:
- `player_id`
- `player_name`
- `team`
- `opponent`
- `game_id`
- `sog_line`
- `over_decimal_price` (optional but recommended)
- `under_decimal_price` (optional but recommended)

## Feature Engineering Roadmap (Ordered by ROI)
1. Phase 1: Opportunity/Usage (highest ROI)
- `toi_avg_last_3`, `toi_avg_last_5`, `toi_trend_5v10`
- `pp_toi_avg_last_3`, `pp_toi_share_last_5`
- `ev_toi_avg_last_5`
- `team_toi_share_last_5`
- `games_played_last_14d`

2. Phase 2: Shooting Volume/Quality
- `sog_per60_last_5`
- `attempts_per60_last_5`
- `ixg_per60_last_5`
- `individual_shooting_pct_last_10` (stability-capped)

3. Phase 3: Opponent/Goalie Context
- `opp_shots_allowed_last_5`
- `opp_xga_last_5`
- `expected_goalie_sv_pct_rolling`
- `goalie_adjustment_factor`

4. Phase 4: Market Context
- `market_implied_sog_mean`
- `line_move_abs`
- `price_skew_over_under`
- `consensus_line_dispersion` (if multi-book)

## Walk-Forward Backtesting Plan
1. Implement rolling-origin evaluation by game date.
2. Train on prior window only, score next step (no leakage).
3. Report MAE/RMSE and calibration on:
- Overall
- By TOI bucket
- By line bucket (`2.5`, `3.5`, etc.)
4. Gate promotions: new feature set must beat baseline across at least 2 consecutive monthly splits.

## Implementation Plan (No Code Yet)
1. Define canonical schemas in `src/nhl/data/` as typed contracts.
2. Add ingestion adapters for each dataset path in config.
3. Build join layer keyed on `game_id`, `player_id`, `team`, `game_date`.
4. Add leakage-safe lagged feature transforms in `src/nhl/features/shots_on_goal.py`.
5. Update `NHL_FEATURES` in `src/nhl/models/predict.py` to v2 list.
6. Add walk-forward evaluation utility module under `src/nhl/models/`.
7. Keep inference output schema stable (`predicted_shots_on_goal`, `prob_over`, `ev_over`, etc.).

## Tests and Scenarios
1. Unit tests for each feature family:
- rolling windows
- lag/shift correctness
- null handling and fallback behavior
2. Data contract tests:
- required columns present
- type coercion rules
- duplicate key detection (`game_id + player_id`)
3. Walk-forward regression tests:
- no train-on-future leakage
- deterministic splits with fixed seed/config
4. Integration tests:
- end-to-end NHL run with offline fixtures
- output schema stability
- provider failure behavior unchanged

## Acceptance Criteria
1. All new datasets can be validated against explicit schemas before feature build.
2. Walk-forward report exists and compares baseline vs v2 features.
3. Pipeline still runs in one step from config.
4. No breaking changes to current output contract.

## Assumptions and Defaults
1. Daily batch is sufficient for v1 operationally.
2. NHL API-first is primary, but advanced shot-quality fields may require MoneyPuck/secondary source.
3. Market features are enabled by default and null-tolerant if missing per row.
4. If expected goalie is unavailable at run time, fallback uses opponent team rolling defensive context only.
5. Backtests remain offline and deterministic.
