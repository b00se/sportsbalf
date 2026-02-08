#!/bin/bash
set -e

PYTHON_BIN=".venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python"
fi

# Define year range
START_YEAR=2021
END_YEAR=2025

for YEAR in $(seq $START_YEAR $END_YEAR); do
  echo ""
  echo "🔁 Processing year $YEAR..."

  echo "📊 Generating top starters list"
  PYTHONPATH=. "$PYTHON_BIN" scripts/get_top_starters.py --season $YEAR --top 999

  echo "📥 Caching raw statcast data"
  PYTHONPATH=. "$PYTHON_BIN" scripts/fetch_statcast_raw.py --season $YEAR

  echo "🛠 Generating enriched pitcher dataset"
  PYTHONPATH=. "$PYTHON_BIN" scripts/generate_pitcher_dataset_from_raw.py --season $YEAR
done

echo ""
echo "✅ All years fully processed"
