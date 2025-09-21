# sportsbalf

This project provides a small demo pipeline for predicting MLB pitcher strikeouts.
The original workflows lived in Jupyter notebooks; the code has been refactored
into a set of reusable modules under `src/` with a simple pipeline entry point.

## Setup

Create a Python environment and install dependencies:

```bash
pip install -r requirements.txt
```

## Running the pipeline

The configuration file `config/mlb.yaml` specifies paths for input data, the
pretrained strikeout model, **historical training datasets**, and Monte Carlo settings. To execute the MLB
pipeline and print probabilistic predictions:

```bash
python -m pipeline.main
```

The pipeline aggregates pitch-level data using the helpers in
`src/mlb/features/`, enriches games with rolling and park context, loads the
pretrained XGBoost model (retraining on demand), and bootstraps historical
residuals to produce a discrete strikeout distribution. Ahead of scoring it
fetches each team’s upcoming opponent from Baseball Reference (via
`pybaseball.schedule_and_record`), recomputes rest days, applies the correct
park factor, and normalizes player names to handle accents. The Monte Carlo
step then adds win probabilities, medians, and expected values for each side of
the bet.

Model training and the residual bootstrap now pull from every parquet listed in
`training_data_paths`, so earlier seasons (2021–2024) are folded in alongside
the current year. If you add more processed seasons, just append the parquet
paths in the config.

To refresh the tuned XGBoost model before scoring, pass the `--retrain` flag:

```bash
python -m pipeline.main --retrain
```

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
