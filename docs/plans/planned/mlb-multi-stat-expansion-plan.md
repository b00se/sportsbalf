# MLB Multi-Stat Expansion Plan (Pitcher Props Now, Batter-Ready Data Foundation)

## Implementation Status (Updated: 2026-02-08)
### Completed
1. Registered new MLB stat pipelines in `src/pipeline/engine.py`:
   - `mlb.outs_recorded`
   - `mlb.earned_runs`
   - `mlb.hits_allowed`
   - `mlb.bb_allowed`
2. Added stat sections in `config/mlb.yaml` for all four new pitcher stats with:
   - stat-specific line schemas (`outs_line`, `er_line`, `hits_line`, `bb_line`)
   - model/data paths
   - `allow_missing_lines: true`
3. Implemented shared pitcher-prop core under `src/mlb/pitcher_props/`:
   - stat descriptors
   - training/inference orchestration
   - lines-optional branching
   - rolling park factors by stat
4. Implemented reusable Statcast-derived tables:
   - multi-target pitcher-game table
   - batter-game foundation table
5. Implemented output metadata fields and offseason behavior:
   - `run_mode` (`prediction` / `train_backtest_only`)
   - `lines_status` (`present` / `missing`)
6. Fixed post-review reliability issues:
   - no-match line runs preserve full simulation schema columns
   - `rest_days` is derived from target slate date (not wall-clock time)
7. Added offline test coverage:
   - target derivation tests
   - park-factor fallback tests
   - integration tests for `outs_recorded`, `earned_runs`, `hits_allowed`, `bb_allowed` in lines-present and lines-missing modes
8. Brought shared-core model selection behavior to parity with legacy strikeouts path:
   - optional walk-forward tournament + champion selection for pitcher-prop shared core
   - strategy-aware champion artifact training/persistence/metadata
   - strategy-aware inference parity in prediction path
9. Expanded multi-stat data-integrity/leakage guard tests:
   - leakage-safe opponent tendency assertions
   - duplicate terminal plate-appearance collision coverage
   - earned-runs fallback integrity checks

### In Progress
1. None.

### Deferred / Not Yet Implemented
1. High-fidelity earned-runs label join from external game-log/event sources (current ER label is derived from available event/score context with fallback logic).
2. Full strikeouts migration onto shared core while preserving strict backward parity.

## Summary
Extend the MLB pipeline to support new pitcher stats in order: `outs_recorded`, `earned_runs`, `hits_allowed`, `bb_allowed`, while simultaneously building a full Statcast-based batter data foundation for future batter props.

Because it is offseason, new stat pipelines will run in `train/backtest-only` mode when lines are unavailable.

Park factors will be computed internally using rolling/recency-aware methods (no external static dependence).

## Locked Decisions
1. Ingest full Statcast lake now and prepare reusable batter features/datasets.
2. Do not block on line ingestion; if lines are missing, run data prep + model training + evaluation and emit artifacts only.
3. Compute our own rolling park factors per stat from internal data.
4. Keep stat-specific line schemas for future ingestion (`outs_line`, `er_line`, `hits_line`, `bb_line`).

## Public Interfaces / Config Changes
1. Register new pipelines in `src/pipeline/engine.py`:
   - `mlb.outs_recorded`
   - `mlb.earned_runs`
   - `mlb.hits_allowed`
   - `mlb.bb_allowed`
2. Add config sections in `config/mlb.yaml` for each stat including:
   - data paths
   - model paths
   - tournament settings
   - monte-carlo settings
   - `allow_missing_lines: true` (default for offseason)
3. Add output-mode metadata fields in pipeline outputs/artifacts:
   - `run_mode` = `prediction` or `train_backtest_only`
   - `lines_status` = `present` or `missing`

## Architecture Changes
1. Introduce shared MLB pitcher-prop core module (e.g. `src/mlb/pitcher_props/`) for:
   - training frame build
   - model train/load
   - model selection tournament
   - prediction/simulation orchestration
   - lines-optional execution branching
2. Replace strikeout hardcoding with stat descriptors:
   - `target_col`, `line_col`, `prediction_col`, feature list, validations, park-factor key.
3. Keep `src/mlb/pipeline.py` compatibility shim for strikeouts, delegating to shared core.

## Data Engineering Plan
1. Build/standardize full Statcast lake usage from `data/raw/statcast/statcast_raw_<season>.parquet` as canonical source.
2. Create reusable game-level aggregation tables:
   - pitcher-game table with multi-target labels.
   - batter-game table for future batter props (PA, hits, TB components, BB/K/contact quality, handedness splits).
3. Target derivations:
   - `outs_recorded`: event-based out accounting with double/triple-play support.
   - `hits_allowed`: singles+doubles+triples+HR allowed.
   - `bb_allowed`: walk+intentional walk allowed.
   - `earned_runs`: high-fidelity label join from pybaseball-accessible game-log/event sources (not run-allowed proxy).
4. Persist processed multi-target pitcher datasets and batter datasets for reuse across training/backtests.

## Feature Plan
1. Shared baseline features across new pitcher stats:
   - workload/rest, rolling form, handedness/live context, opponent context, rolling park context.
2. Add stat-specific opponent features:
   - opponent out tendency, run creation tendency, hit rate tendency, walk rate tendency.
3. Add contact-quality and baserunner-pressure features from Statcast aggregates:
   - hard-contact proxy, launch profile mix, on-base events allowed rate.
4. Park factors:
   - compute per-stat rolling factors (`park_factor_outs`, `park_factor_runs`, `park_factor_hits`, `park_factor_bb`, keep `park_factor_K`).
   - use recency weighting + minimum-sample fallback to neutral 1.0.

## Offseason Lines Policy
1. If lines exist:
   - run normal prediction + simulation output.
2. If lines missing and `allow_missing_lines=true`:
   - skip prediction rows
   - still train/update artifacts and run walk-forward evaluation
   - emit empty prediction frame with `run_mode=train_backtest_only`
   - log clear TODO for line ingestion enablement per stat.
3. Add lightweight placeholder/TODO tracking in code comments + logs, not fake market lines.

## Testing Plan
1. Unit tests:
   - per-target derivation correctness (outs/ER/hits/BB).
   - park-factor rolling math and fallback behavior.
   - offseason lines-missing branch behavior.
2. Integration tests:
   - `run_pipeline_with_overrides(... stat=<new_stat>)` in lines-present and lines-missing modes.
   - strikeouts regression parity.
3. Data integrity tests:
   - temporal leakage guards on rolling features and ER join.
   - duplicate/game-key collision checks.
4. Validation checks per rollout wave:
   - `.venv/bin/pytest -q`
   - `.venv/bin/ruff check .`

## Rollout Sequence
1. Wave 0 foundation:
   - shared pitcher-prop core + lines-optional control flow + park-factor engine.
   - full batter-game dataset builder scaffold.
2. Wave 1:
   - `outs_recorded` end-to-end.
3. Wave 2:
   - `earned_runs` with high-fidelity label join.
4. Wave 3:
   - `hits_allowed`.
5. Wave 4:
   - `bb_allowed`.
6. Each wave includes config, registry, tests, and backtest artifact paths.

## Acceptance Criteria
1. All four new MLB stat pipelines train and backtest offline with deterministic outputs.
2. Missing lines do not fail runs when offseason mode is enabled.
3. Internal rolling park factors are used by default for all supported stats.
4. Full Statcast-derived batter dataset pipeline exists and runs, ready for future batter props.
5. Strikeouts pipeline remains backward compatible.

## Assumptions and Defaults
1. Keep Python 3.11 and repo-local `.venv/bin/*` command policy.
2. Continue using current model stack (XGBoost + tournament framework) unless metrics force upgrades.
3. Tests remain fully offline; network calls remain optional/runtime-only.
4. Line ingestion implementation for new stats is deferred and tracked, not blocked.
