"""Offline tests for the archive-compatible direct-FP candidate evaluator."""

import pandas as pd
import pytest
from src.nfl.models.direct_fp_evaluation import (
    evaluate_direct_fantasy_points,
    evaluate_season_direct_fantasy_points,
    evaluate_weekly_direct_fantasy_points,
)


def _frame() -> pd.DataFrame:
    rows = []
    for season in (2024, 2025):
        for week in (1, 2, 3, 4):
            rows.extend(
                [
                    {
                        "season": season,
                        "week": week,
                        "player_id": "a",
                        "position": "QB",
                        "fantasy_points": season - 2020 + week,
                    },
                    {
                        "season": season,
                        "week": week,
                        "player_id": "b",
                        "position": "RB",
                        "fantasy_points": week * 2,
                    },
                ]
            )
    return pd.DataFrame(rows)


def test_weekly_direct_fp_is_strict_and_window_is_explicit() -> None:
    result = evaluate_weekly_direct_fantasy_points(_frame(), window=1)
    row = result.predictions.query(
        "season == 2024 and week == 2 and player_id == 'a'"
    ).iloc[0]
    assert row.prediction == 5.0
    assert row.actual == 6.0
    assert row.baseline == "direct_fantasy_points"
    assert row.window == 1
    assert row.outer_fold == "2024-2"


def test_future_outcome_mutation_cannot_change_prior_fold_prediction() -> None:
    frame = _frame()
    result = evaluate_weekly_direct_fantasy_points(frame, window=2)
    frame.loc[(frame.season == 2025) & (frame.week == 4), "fantasy_points"] = 9999
    changed = evaluate_weekly_direct_fantasy_points(frame, window=2)
    cols = ["season", "week", "player_id", "prediction", "window"]
    pd.testing.assert_frame_equal(
        result.predictions[cols], changed.predictions[cols]
    )


def test_season_holdout_and_metrics_are_output_compatible() -> None:
    result = evaluate_season_direct_fantasy_points(_frame(), window=3)
    assert set(result.predictions.season) == {2025}
    assert result.predictions.outer_fold.nunique() == 1
    assert {
        "scope", "group", "baseline", "mae", "crps", "mean_error",
        "coverage", "spearman_rank", "top_k_recall",
    } <= set(result.metrics.columns)
    assert result.status == "inconclusive"
    assert result.status != "promoted"


def test_three_week_result_is_eligible_but_never_promoted() -> None:
    result = evaluate_weekly_direct_fantasy_points(_frame(), window=6)
    assert result.status == "eligible_for_candidate_comparison"
    assert result.status != "promoted"
    assert result.predictions.outer_fold.nunique() == 7


def test_invalid_window_fails_closed() -> None:
    with pytest.raises(ValueError, match="window must be a positive integer"):
        evaluate_direct_fantasy_points(_frame(), window=0)
