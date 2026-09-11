"""Offline tests for leakage-safe NFL baseline evaluation."""

import numpy as np
import pandas as pd
import pytest
from src.nfl.models.baseline_evaluation import (
    evaluate_baselines,
    evaluate_season_baselines,
    evaluate_weekly_baselines,
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
                        "team": "X",
                        "fantasy_points": season - 2020 + week,
                    },
                    {
                        "season": season,
                        "week": week,
                        "player_id": "b",
                        "position": "RB",
                        "team": "X",
                        "fantasy_points": week * 2,
                    },
                ]
            )
    return pd.DataFrame(rows)


def test_weekly_is_strictly_prior_and_order_invariant() -> None:
    frame = _frame()
    result = evaluate_weekly_baselines(
        frame.sample(frac=1, random_state=7), target_col="fantasy_points"
    )
    first = (
        result.predictions.query("season == 2024 and week == 2 and player_id == 'a'")
        .query("baseline == 'player_trailing_mean'")
        .prediction.iloc[0]
    )
    assert first == 5.0
    mutated = frame.copy()
    mutated.loc[
        (mutated.season == 2025) & (mutated.week == 4) & (mutated.player_id == "a"),
        "fantasy_points",
    ] = 9999
    changed = evaluate_weekly_baselines(mutated, target_col="fantasy_points")
    pd.testing.assert_frame_equal(
        result.predictions.drop(columns=["fantasy_points"]),
        changed.predictions.drop(columns=["fantasy_points"]),
    )


def test_season_holdout_has_no_within_season_training() -> None:
    result = evaluate_season_baselines(_frame(), target_col="fantasy_points")
    assert set(result.predictions.season) == {2025}
    assert (result.predictions[result.predictions.season == 2025].prediction > 0).all()


def test_unknown_player_falls_back_to_position_mean() -> None:
    frame = _frame()
    frame = pd.concat(
        [
            frame,
            pd.DataFrame(
                [
                    {
                        "season": 2025,
                        "week": 1,
                        "player_id": "new",
                        "position": "RB",
                        "team": "Y",
                        "fantasy_points": 3,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    result = evaluate_weekly_baselines(frame, target_col="fantasy_points")
    row = result.predictions.query(
        "player_id == 'new' and baseline == 'player_trailing_mean'"
    ).iloc[0]
    assert row.prediction == 5.0


def test_metrics_and_three_week_gate() -> None:
    result = evaluate_weekly_baselines(_frame(), target_col="fantasy_points")
    assert result.status == "eligible_for_candidate_comparison"
    assert result.status != "promoted"
    assert {
        "mae",
        "crps",
        "mean_error",
        "coverage",
        "spearman_rank",
        "top_k_recall",
    } <= set(result.metrics.columns)
    assert (
        evaluate_weekly_baselines(
            _frame().query("week == 1"), target_col="fantasy_points"
        ).status
        == "inconclusive"
    )


@pytest.mark.parametrize(
    "bad",
    [
        pd.DataFrame(
            {"season": [2024], "week": [1], "player_id": ["a"], "position": ["QB"]}
        ),
        pd.DataFrame(
            {
                "season": [2024, 2024],
                "week": [1, 1],
                "player_id": ["a", "a"],
                "position": ["QB", "QB"],
                "fantasy_points": [1, 2],
            }
        ),
    ],
)
def test_invalid_input_fails_closed(bad: pd.DataFrame) -> None:
    with pytest.raises(ValueError):
        evaluate_baselines(bad)


def test_nfl_week_99_is_rejected() -> None:
    bad = _frame().iloc[[0]].copy()
    bad["week"] = 99
    with pytest.raises(ValueError, match="week must be 1-22"):
        evaluate_baselines(bad)


@pytest.mark.parametrize("boolean_week", [True, np.bool_(False)])
def test_boolean_nfl_week_is_rejected(boolean_week: bool) -> None:
    bad = _frame().iloc[[0]].copy()
    bad["week"] = boolean_week
    with pytest.raises(ValueError, match="week must not contain boolean"):
        evaluate_baselines(bad)
