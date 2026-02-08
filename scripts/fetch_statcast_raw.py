"""Fetch and cache raw Statcast data."""

from __future__ import annotations

import logging
import time
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from pybaseball import statcast

logger = logging.getLogger(__name__)


def _date_range(start_date: datetime, end_date: datetime) -> list[str]:
    """Return inclusive ISO date strings between start and end."""
    current = start_date
    out: list[str] = []
    while current <= end_date:
        out.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return out


def _fetch_with_warning_suppression(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch Statcast range while suppressing known upstream deprecation noise."""
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="errors='ignore' is deprecated",
            category=FutureWarning,
        )
        warnings.filterwarnings(
            "ignore",
            message=(
                "The behavior of DataFrame concatenation with empty or "
                "all-NA entries is deprecated"
            ),
            category=FutureWarning,
        )
        return statcast(start_date, end_date)


def fetch_statcast_raw(
    season: int,
    start: str = "04-01",
    end: str = "10-01",
    save_dir: str = "data/raw/statcast",
    retries_per_day: int = 2,
    retry_delay_seconds: float = 1.5,
) -> Path:
    """Fetch Statcast data day-by-day and save parquet output.

    Args:
        season: MLB season year.
        start: Start month-day (`MM-DD`) within season.
        end: End month-day (`MM-DD`) within season.
        save_dir: Output directory for cached parquet.
        retries_per_day: Number of retry attempts per day on fetch errors.
        retry_delay_seconds: Delay between retries.

    Returns:
        Path to the saved or reused parquet file.

    Raises:
        ValueError: If date inputs are invalid or start is after end.
        RuntimeError: If no data can be fetched and no cached parquet exists.
    """
    out_dir = Path(save_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"statcast_raw_{season}.parquet"

    start_date = datetime.strptime(f"{season}-{start}", "%Y-%m-%d")
    end_date = datetime.strptime(f"{season}-{end}", "%Y-%m-%d")
    if start_date > end_date:
        raise ValueError(
            "Invalid date range: "
            f"start {start_date.strftime('%Y-%m-%d')} "
            f"is after end {end_date.strftime('%Y-%m-%d')}."
        )
    days = _date_range(start_date, end_date)

    logger.info("Fetching Statcast data from %s to %s", days[0], days[-1])
    chunks: list[pd.DataFrame] = []
    failed_days: list[str] = []

    # Fast path: let pybaseball fetch the full date range in one call.
    # Fallback to day-by-day only when the full call fails.
    full_range_fetched = False
    full_range_attempt = 0
    while full_range_attempt <= retries_per_day:
        try:
            full_df = _fetch_with_warning_suppression(days[0], days[-1])
            full_range_fetched = True
            if full_df is not None and not full_df.empty:
                chunks.append(full_df)
            break
        except Exception as exc:  # pragma: no cover - network variability
            full_range_attempt += 1
            if full_range_attempt > retries_per_day:
                logger.warning(
                    "Full-range Statcast fetch failed (%s to %s): %s. "
                    "Falling back to day-by-day mode.",
                    days[0],
                    days[-1],
                    exc,
                )
            else:
                time.sleep(retry_delay_seconds)

    if not full_range_fetched:
        for day in days:
            attempt = 0
            day_df: pd.DataFrame | None = None
            while attempt <= retries_per_day:
                try:
                    day_df = _fetch_with_warning_suppression(day, day)
                    break
                except Exception as exc:  # pragma: no cover - network variability
                    attempt += 1
                    if attempt > retries_per_day:
                        logger.warning("Failed Statcast fetch for %s: %s", day, exc)
                        failed_days.append(day)
                    else:
                        time.sleep(retry_delay_seconds)
            if day_df is not None and not day_df.empty:
                chunks.append(day_df)

    if not chunks:
        if out_path.exists():
            logger.warning(
                "No fresh data fetched; using existing cached file at %s", out_path
            )
            return out_path
        raise RuntimeError(
            "Unable to fetch any Statcast data and no cached parquet is available."
        )

    df = pd.concat(chunks, ignore_index=True)
    if out_path.exists():
        existing = pd.read_parquet(out_path)
        df = pd.concat([existing, df], ignore_index=True).drop_duplicates()

    df.to_parquet(out_path, index=False)
    logger.info("Saved %s rows to %s", len(df), out_path)
    if failed_days:
        logger.warning("Skipped %s day(s) due to fetch errors", len(failed_days))
    return out_path


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--start", type=str, default="04-01")
    parser.add_argument("--end", type=str, default="10-01")
    args = parser.parse_args()

    fetch_statcast_raw(args.season, start=args.start, end=args.end)
