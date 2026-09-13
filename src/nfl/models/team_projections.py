"""Deterministic, as-of-safe team component projections for NFL modeling."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Final

import pandas as pd

_COMPONENT_COLUMNS: Final[tuple[str, ...]] = (
    "pass_attempts",
    "rush_attempts",
    "points",
    "scoring_opportunities",
)


@dataclass(frozen=True)
class TeamProjection:
    """Projected team totals for one future game, computed as of a cutoff."""

    team: str
    as_of: pd.Timestamp
    projected_pass_volume: float
    projected_rush_volume: float
    projected_total_plays: float
    projected_points: float
    projected_scoring_opportunities: float
    window: int


@dataclass(frozen=True)
class RollingOriginEvaluation:
    """Rolling-origin MAE comparison between promoted model and baseline."""

    team: str
    model_name: str
    baseline_name: str
    model_mae: float
    baseline_mae: float
    origins: int
    test_cutoffs: tuple[pd.Timestamp, ...]
    promoted: bool


def _as_timestamp(value: str | date | datetime | pd.Timestamp) -> pd.Timestamp:
    """Parse a cutoff and normalize aware values to UTC-naive timestamps."""

    parsed = pd.Timestamp(value)
    if pd.isna(parsed):
        raise ValueError("cutoff must be a valid date")
    if parsed.tz is not None:
        parsed = parsed.tz_convert("UTC").tz_localize(None)
    return parsed


def _prepare_history(history: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """Validate and normalize the historical team game frame."""

    if not isinstance(history, pd.DataFrame):
        raise TypeError("history must be a pandas DataFrame")
    date_column = "as_of" if "as_of" in history.columns else "game_date"
    required = {"team", date_column, *_COMPONENT_COLUMNS}
    missing = sorted(required.difference(history.columns))
    if missing:
        raise ValueError(f"missing required columns: {', '.join(missing)}")
    frame = history.loc[:, sorted(required)].copy()
    frame[date_column] = pd.to_datetime(
        frame[date_column], errors="coerce", utc=True, format="mixed"
    ).dt.tz_localize(None)
    if frame[date_column].isna().any():
        raise ValueError(f"{date_column} must contain valid dates")
    for column in _COMPONENT_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
        if frame[column].isna().any():
            raise ValueError(f"{column} must contain numeric values")
        if (frame[column] < 0).any():
            raise ValueError(f"{column} must be nonnegative")
    frame = frame.sort_values(
        ["team", date_column], kind="mergesort"
    ).reset_index(drop=True)
    return frame, date_column


def _prediction_from_prior(
    prior: pd.DataFrame, *, team: str, window: int, as_of: pd.Timestamp
) -> TeamProjection:
    """Create a projection from already-filtered historical rows."""

    if prior.empty:
        raise ValueError(f"no historical rows for team {team!r} before cutoff")
    sample = prior.tail(window)
    passed = float(sample["pass_attempts"].mean())
    rushed = float(sample["rush_attempts"].mean())
    total = passed + rushed
    points = max(0.0, float(sample["points"].mean()))
    opportunities = min(total, max(0.0, float(sample["scoring_opportunities"].mean())))
    return TeamProjection(
        team, as_of, passed, rushed, total, points, opportunities, window
    )


def project_team_components(
    history: pd.DataFrame,
    *,
    team: str,
    cutoff: str | date | datetime | pd.Timestamp,
    window: int = 4,
) -> TeamProjection:
    """Project a team's components from rows strictly before ``cutoff``.

    The promoted model is a transparent trailing arithmetic mean. Scoring
    opportunities are clipped to zero and total projected plays, while total
    plays is defined identically as pass plus rush volume.
    """

    if not isinstance(team, str) or not team.strip():
        raise ValueError("team must be non-empty text")
    if not isinstance(window, int) or isinstance(window, bool) or window < 1:
        raise ValueError("window must be a positive integer")
    frame, date_column = _prepare_history(history)
    as_of = _as_timestamp(cutoff)
    prior = frame[(frame["team"] == team) & (frame[date_column] < as_of)]
    return _prediction_from_prior(prior, team=team, window=window, as_of=as_of)


def evaluate_rolling_origin(
    history: pd.DataFrame,
    *,
    team: str,
    min_history: int = 3,
    window: int = 4,
) -> RollingOriginEvaluation:
    """Compare trailing-mean projections with an expanding-mean baseline.

    Each origin predicts only the immediately following chronologically
    observed game. The baseline and model use identical information sets.
    Promotion is permitted only when model MAE is no worse than baseline MAE.
    """

    if (
        not isinstance(min_history, int)
        or isinstance(min_history, bool)
        or min_history < 1
    ):
        raise ValueError("min_history must be a positive integer")
    if not isinstance(window, int) or isinstance(window, bool) or window < 1:
        raise ValueError("window must be a positive integer")
    frame, date_column = _prepare_history(history)
    team_rows = frame[frame["team"] == team].sort_values(
        date_column, kind="mergesort"
    ).reset_index(drop=True)
    if team_rows[date_column].duplicated().any():
        raise ValueError(
            "rolling-origin evaluation requires one observation per team date"
        )
    if len(team_rows) <= min_history:
        raise ValueError("history must contain a test row after min_history")
    model_errors: list[float] = []
    baseline_errors: list[float] = []
    cutoffs: list[pd.Timestamp] = []
    for index in range(min_history, len(team_rows)):
        prior = team_rows.iloc[:index]
        actual = float(team_rows.iloc[index]["points"])
        cutoff = pd.Timestamp(team_rows.iloc[index][date_column])
        if not (prior[date_column] < cutoff).all():
            raise ValueError(
                "rolling-origin training rows must precede each test cutoff"
            )
        model = _prediction_from_prior(prior, team=team, window=window, as_of=cutoff)
        baseline = float(prior["points"].mean())
        model_errors.append(abs(model.projected_points - actual))
        baseline_errors.append(abs(baseline - actual))
        cutoffs.append(cutoff)
    model_mae = float(sum(model_errors) / len(model_errors))
    baseline_mae = float(sum(baseline_errors) / len(baseline_errors))
    return RollingOriginEvaluation(
        team=team,
        model_name="trailing_mean",
        baseline_name="expanding_mean",
        model_mae=model_mae,
        baseline_mae=baseline_mae,
        origins=len(model_errors),
        test_cutoffs=tuple(cutoffs),
        promoted=model_mae <= baseline_mae,
    )
