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

The configuration file `config/config.yaml` specifies paths for input data,
including a small sample of pitch-level Statcast data. To execute the pipeline
and print predictions:

```bash
python -m pipeline.main
```

The pipeline aggregates pitch-level data using utilities from
`features/mlb_features.py` and adds rolling and park factor features before
training a demo model.

## Updating historical pitcher datasets

To rebuild the enriched pitcher game logs from raw Statcast data, use the
existing scripts in the `scripts/` directory. For a single season run:

```bash
PYTHONPATH=. python scripts/generate_pitcher_dataset_from_raw.py --season 2023
```

This will fetch the cached raw Statcast file for the season, aggregate it with
the functions in `features/mlb_features.py`, merge opponent strikeout and park
factor context, and write the processed parquet file under `data/processed/`.

To regenerate multiple seasons in sequence, the helper script
`scripts/bootstrap_all_years.sh` automates fetching starters, caching the raw
pitch-level data, and producing enriched game logs for each year in the range.

## Testing

Run the unit tests with `pytest`:

```bash
pytest
```
