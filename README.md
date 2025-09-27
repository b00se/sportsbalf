# sportsbalf

Sportsbalf is a demo analytics stack for prop betting. It now ships two
end-to-end pipelines:

- **MLB pitcher strikeouts** (original workflow) – cleans Statcast data, trains an
  XGBoost model, and Monte Carlo simulates strikeout totals.
- **NFL quarterback pass attempts** – assembles Underdog lines with weekly team
  context, trains an XGBoost model, and simulates pass-attempt results with a
  residual bootstrap.

The code lives under `src/` with CLI entry points in `scripts/`. Both pipelines
share a common Monte Carlo module and follow the same pattern: build dataset ?
train (or load) model ? run inference ? simulate probabilities/EVs.

## Setup

Create a Python environment and install dependencies:

```bash
pip install -r requirements.txt
```

## MLB strikeout pipeline

The configuration file `config/mlb.yaml` specifies input data, pretrained model
artifacts, and Monte Carlo settings. To execute the MLB pipeline and print
probabilistic predictions:

```bash
python -m pipeline.main [--retrain]
```

The pipeline aggregates pitch-level data (`src/mlb/features/`), enriches games
with rolling and park context, loads the pretrained XGBoost model (retraining on
demand), and bootstraps historical residuals to produce a discrete strikeout
distribution. Ahead of scoring it fetches each team’s upcoming opponent via
`pybaseball.schedule_and_record`, recomputes rest days, applies park factors, and
normalizes player names to handle accents. The Monte Carlo step adds win
probabilities, medians, and expected values for each side of the bet.

To rebuild enriched pitcher game logs from raw Statcast data, use the helpers in
`scripts/` (e.g. `python scripts/generate_pitcher_dataset_from_raw.py --season 2023`).

## NFL pass-attempt pipeline

The NFL pipeline mirrors the MLB flow, using Underdog lines plus nflverse data.
Key entry points:

- **Build / refresh the dataset**
  ```bash
  python scripts/build_qb_attempts_dataset.py --start 2015 --end 2024
  ```
  This pulls weekly QB stats, schedules, play-by-play, Next Gen passing metrics,
  and Underdog lines, then writes `data/qb_attempts_dataset.parquet`.

- **Generate predictions**
  ```bash
  python scripts/predict_qb_attempts.py [--retrain] [--config config/nfl.yaml]
  ```
  The script loads the dataset, trains or reloads an XGBoost model, then runs the
  Monte Carlo simulation to produce per-line probabilities, edges, and EVs.

- **Build bet slips**
  ```bash
  python scripts/build_betslips.py --sports nfl [--combine] [--retrain]
  ```
  This wraps the NFL pipeline output in the shared slip builder, emitting
  conservative and “full send” JSON payloads under `betslips/`.

### Data providers

NFL ingestion now uses `nflreadpy` exclusively. The migration away from
`nfl_data_py` is complete, so `config/nfl.yaml` no longer needs a provider
switch; the dataset builder and pipeline always rely on `nflreadpy`.

## Bet slip generation

`scripts/build_betslips.py` combines MLB and/or NFL predictions into Underdog
slip payloads. Use `--sports mlb nfl` to build both, and `--combine` to create
mixed-sport slips. JSON outputs live under `betslips/` with a timestamped
filename.

## Testing

Run the unit tests with `pytest`:

```bash
pytest
```

Most tests run offline. Network-dependent tests (if any) are skipped unless the
required environment variables are set.

