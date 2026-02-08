# sportsbalf

This project provides modular stat-line pipelines for MLB strikeouts and NFL pass attempts.
The original workflows lived in Jupyter notebooks; the code has been refactored
into reusable modules under `src/` with a sport/stat orchestration entry point.

## Setup

Use Python 3.11 for local development. Then create a virtual environment and
install dependencies:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Running the pipeline

Run the orchestration entrypoint with explicit sport/stat selection:

```bash
python -m pipeline.main --sport mlb --stat strikeouts --config config/mlb.yaml
python -m pipeline.main --sport nfl --stat pass_attempts --config config/nfl.yaml
```

The pipeline aggregates pitch-level data using the helpers in
`src/mlb/features/`, enriches games with rolling and park context, and scores
props with a persisted champion model. Champion selection is optional/config
driven and compares candidate regressors on season walk-forward splits using
lowest MAE as the primary objective (RMSE and R² tie-breakers). When enabled,
the tournament evaluates `global`, `quantile3`, and `kmeans` workload
segmentation strategies and persists the winning strategy + model artifact.
Ahead of
scoring, the pipeline fetches each team’s upcoming opponent from Baseball
Reference (via `pybaseball.schedule_and_record`), recomputes rest days, applies
the correct park factor, and normalizes player names to handle accents. The
Monte Carlo step then adds win probabilities, medians, and expected values for
each side of the bet.

Model training and the residual bootstrap now pull from every parquet listed in
`training_data_paths`, so earlier seasons (2021–2024) are folded in alongside
the current year. If you add more processed seasons, just append the parquet
paths in the config.

To retrain before scoring, pass the `--retrain` flag:

```bash
python -m pipeline.main --sport mlb --stat strikeouts --config config/mlb.yaml --retrain
```

To run the offline tournament directly and emit leaderboard/champion artifacts:

```bash
PYTHONPATH=. python scripts/backtest_mlb_strikeouts.py --config config/mlb.yaml
```

Outputs:
- CSV fold metrics (`strategy`, `model`, `test_season`, `mae`, `rmse`, `r2`)
- CSV leaderboard with fold aggregates (`mean_mae`, `median_mae`, `std_mae`, `mean_rmse`, `mean_r2`)
- JSON champion metadata with selected strategy/model and fold-level metrics

Compatibility note: legacy helpers such as `src.mlb.pipeline.run(...)` and
`src.nfl.pipeline.run(...)` still work, but engine-based invocation is the default interface.

Fetching upcoming opponents requires network access (Baseball Reference). If the
call fails, the pipeline gracefully falls back to the previous opponent context
and still produces output.

## Updating historical pitcher datasets

To rebuild the enriched pitcher game logs from raw Statcast data, use the
existing scripts in the `scripts/` directory. For a single season run:

```bash
PYTHONPATH=. python scripts/generate_pitcher_dataset_from_raw.py --season 2023
```

This will fetch the cached raw Statcast file for the season, aggregate it with
the functions in `src/mlb/features/mlb_features.py`, merge opponent strikeout
and park factor context, and write the processed parquet file under
`data/processed/`.

To regenerate multiple seasons in sequence, the helper script
`scripts/bootstrap_all_years.sh` automates fetching starters, caching the raw
pitch-level data, and producing enriched game logs for each year in the range.

## Testing

Run the unit tests with `pytest`:

```bash
pytest
```

Run lint and formatting checks:

```bash
ruff check .
black --check .
```
