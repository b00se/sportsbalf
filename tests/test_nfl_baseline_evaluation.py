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
    assert result.status == "inconclusive"
    assert result.metrics.loc[
        result.metrics["aggregation"] == "aggregate", "outer_folds"
    ].eq(1).all()


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
        "scope",
        "group",
        "baseline",
        "metric",
        "value",
        "aggregation",
        "outer_folds",
        "valid_folds",
        "status",
        "definition",
    } <= set(result.metrics.columns)
    assert "coverage" not in result.metrics.columns
    aggregate = result.metrics.query(
        "scope == 'overall' and aggregation == 'aggregate'"
    )
    assert set(aggregate.metric) >= {
        "mae",
        "crps",
        "mean_error",
        "spearman_rank",
        "top_k_recall",
        "calibration",
    }
    assert aggregate.loc[aggregate.metric == "crps", "definition"].iloc[0].startswith(
        "mean absolute point-forecast error"
    )
    calibration = aggregate.loc[aggregate.metric == "calibration"].iloc[0]
    assert calibration.status == "inconclusive"
    assert calibration.value != calibration.value  # explicit NaN/unavailable
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


def test_empty_prediction_runs_keep_the_metric_schema() -> None:
    result = evaluate_season_baselines(_frame().query("season == 2024"))
    assert result.predictions.empty
    assert {"metric", "value", "status", "outer_folds"} <= set(
        result.metrics.columns
    )


@pytest.mark.parametrize("boolean_week", [True, np.bool_(False)])
def test_boolean_nfl_week_is_rejected(boolean_week: bool) -> None:
    bad = _frame().iloc[[0]].copy()
    bad["week"] = boolean_week
    with pytest.raises(ValueError, match="week must not contain boolean"):
        evaluate_baselines(bad)


def test_ranking_metrics_are_fold_local_with_deterministic_player_id_ties() -> None:
    frame = pd.DataFrame(
        [
            {
                "season": 2024,
                "week": week,
                "player_id": player,
                "position": "WR",
                "fantasy_points": actual,
            }
            for week, values in ((1, {"b": 2.0, "a": 1.0}), (2, {"a": 2.0, "b": 1.0}),
                                 (3, {"b": 2.0, "a": 1.0}))
            for player, actual in values.items()
        ]
    )
    result = evaluate_weekly_baselines(frame)
    aggregate = result.metrics.query(
        "scope == 'overall' and aggregation == 'aggregate' and "
        "baseline == 'position_historical_mean'"
    )
    top_k = aggregate.loc[aggregate.metric == "top_k_recall"].iloc[0]
    assert top_k.parameter == "k=3"
    assert top_k.outer_folds == 2
    assert top_k.valid_folds == 2
    assert top_k.status == "inconclusive"
    assert pd.isna(top_k.value)
    fold_rows = result.metrics.query(
        "scope == 'overall' and aggregation == 'fold' and "
        "baseline == 'position_historical_mean' and metric == 'top_k_recall'"
    )
    assert set(fold_rows.fold) == {"2024-2", "2024-3"}
    assert fold_rows.parameter.eq("k=3").all()
