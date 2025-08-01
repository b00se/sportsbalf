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

## Testing

Run the unit tests with `pytest`:

```bash
pytest
```
