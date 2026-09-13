"""Focused offline contract tests for NFL team projections."""

from datetime import date

import pandas as pd
import pytest
from src.nfl.models.team_projections import (
    evaluate_rolling_origin,
    project_team_components,
)


def _history() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"team": "AAA", "game_date": "2026-09-01", "pass_attempts": 30,
             "rush_attempts": 25, "points": 21, "scoring_opportunities": 8},
            {"team": "AAA", "game_date": "2026-09-08", "pass_attempts": 32,
             "rush_attempts": 24, "points": 24, "scoring_opportunities": 9},
            {"team": "AAA", "game_date": "2026-09-15", "pass_attempts": 34,
             "rush_attempts": 26, "points": 27, "scoring_opportunities": 10},
            {"team": "AAA", "game_date": "2026-09-22", "pass_attempts": 36,
             "rush_attempts": 27, "points": 30, "scoring_opportunities": 11},
            # This row must not affect an as-of projection.
            {"team": "AAA", "game_date": "2026-10-01", "pass_attempts": 99,
             "rush_attempts": 99, "points": 99, "scoring_opportunities": 99},
            {"team": "BBB", "game_date": "2026-09-22", "pass_attempts": 20,
             "rush_attempts": 30, "points": 14, "scoring_opportunities": 6},
        ]
    )


def test_projection_reconciles_volume_and_bounds_opportunities() -> None:
    result = project_team_components(
        _history(), team="AAA", cutoff=date(2026, 9, 30), window=2
    )

    assert result.projected_total_plays == pytest.approx(
        result.projected_pass_volume + result.projected_rush_volume
    )
    assert result.projected_scoring_opportunities >= 0
    assert result.projected_scoring_opportunities <= result.projected_total_plays
    assert result.projected_points == pytest.approx(28.5)


def test_cutoff_is_strict_and_future_rows_do_not_change_projection() -> None:
    history = _history()
    before = project_team_components(history, team="AAA", cutoff="2026-09-30", window=2)
    altered = history.copy()
    altered.loc[
        4, ["pass_attempts", "rush_attempts", "points", "scoring_opportunities"]
    ] = 1_000

    after = project_team_components(altered, team="AAA", cutoff="2026-09-30", window=2)
    assert after == before


def test_aware_cutoff_compares_chronologically_in_utc() -> None:
    history = _history().iloc[:4].copy()
    history.loc[len(history)] = {
        "team": "AAA",
        "game_date": "2026-09-30T00:30:00Z",
        "pass_attempts": 99,
        "rush_attempts": 99,
        "points": 99,
        "scoring_opportunities": 99,
    }

    aware = project_team_components(
        history,
        team="AAA",
        cutoff="2026-09-30T01:00:00+05:00",
        window=2,
    )
    expected = project_team_components(
        history, team="AAA", cutoff="2026-09-29T20:00:00Z", window=2
    )
    assert aware == expected


def test_rolling_origin_reports_transparent_baseline_and_promotion() -> None:
    result = evaluate_rolling_origin(
        _history().iloc[:4], team="AAA", min_history=2, window=2
    )

    assert result.baseline_name == "expanding_mean"
    assert result.model_name == "trailing_mean"
    assert result.model_mae <= result.baseline_mae
    assert result.promoted is True
    assert result.origins >= 1
    assert result.test_cutoffs == tuple(sorted(result.test_cutoffs))


def test_rejects_missing_columns() -> None:
    with pytest.raises(ValueError, match="missing required columns"):
        project_team_components(
            pd.DataFrame({"team": ["AAA"]}), team="AAA", cutoff="2026-09-30"
        )


def test_rolling_origin_rejects_same_date_observations() -> None:
    history = _history().iloc[:4].copy()
    duplicate = history.iloc[[2]].copy()
    history = pd.concat([history, duplicate], ignore_index=True)

    with pytest.raises(ValueError, match="one observation per team date"):
        evaluate_rolling_origin(history, team="AAA", min_history=2, window=2)
