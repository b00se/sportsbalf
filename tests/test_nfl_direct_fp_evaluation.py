"""Offline tests for the archive-compatible direct-FP candidate evaluator."""

import pandas as pd
import pytest
from src.nfl.models.direct_fp_evaluation import (
    evaluate_direct_fantasy_points,
    evaluate_season_direct_fantasy_points,
    evaluate_weekly_direct_fantasy_points,
)
from src.nfl.models.fantasy_scoring import direct_fantasy_points_benchmark


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
    assert aggregate.loc[aggregate.metric == "calibration", "value"].isna().all()
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


def test_weekly_predictions_match_canonical_benchmark() -> None:
    frame = _frame()
    result = evaluate_weekly_direct_fantasy_points(frame, window=2)
    target = frame.query("season == 2025 and week == 3")
    history = frame.query("season < 2025 or (season == 2025 and week < 3)")
    expected = direct_fantasy_points_benchmark(
        history[["player_id", "season", "week", "fantasy_points"]],
        target[["player_id", "season", "week"]],
        window=2,
    ).sort_values("player_id")
    actual = result.predictions.query("outer_fold == '2025-3'").sort_values(
        "player_id"
    )
    pd.testing.assert_series_equal(
        actual.prediction.reset_index(drop=True),
        expected.direct_fantasy_points.reset_index(drop=True),
        check_names=False,
    )


def test_realistic_archive_shape_is_evaluated_without_fold_duplication() -> None:
    frame = pd.DataFrame(
        {
            "season": [
                2021 + season for season in range(5) for _ in range(18 * 342)
            ],
            "week": [
                week
                for _ in range(5)
                for week in range(1, 19)
                for _ in range(342)
            ],
            "player_id": [
                f"p{player}"
                for _ in range(5)
                for _ in range(18)
                for player in range(342)
            ],
            "position": "RB",
            "fantasy_points": 1.0,
        }
    )
    result = evaluate_weekly_direct_fantasy_points(frame, window=6)
    assert len(frame) == 30_780
    assert result.predictions.outer_fold.nunique() == 89
    assert len(result.predictions) == 89 * 342


def test_tied_rankings_use_player_id_as_deterministic_secondary_key() -> None:
    frame = pd.DataFrame(
        [
            {
                "season": 2024,
                "week": week,
                "player_id": player,
                "position": "WR",
                "fantasy_points": actual,
            }
            for week, values in ((1, {"b": 2.0, "a": 1.0}),
                                 (2, {"a": 2.0, "b": 1.0}),
                                 (3, {"b": 2.0, "a": 1.0}),
                                 (4, {"a": 2.0, "b": 1.0}))
            for player, actual in values.items()
        ]
    )
    result = evaluate_weekly_direct_fantasy_points(frame, window=1)
    fold_rows = result.metrics.query(
        "scope == 'overall' and aggregation == 'fold' and "
        "metric == 'top_k_recall'"
    )
    assert set(fold_rows.fold) == {"2024-2", "2024-3", "2024-4"}
    assert fold_rows.parameter.eq("k=3").all()


def test_top_k_ties_are_resolved_by_ascending_player_id() -> None:
    players = ("a", "b", "c", "d")
    frame = pd.DataFrame(
        [
            {
                "season": 2024,
                "week": week,
                "player_id": player,
                "position": "WR",
                "fantasy_points": (
                    1.0
                    if week == 1
                    else {"a": 0.0, "b": 10.0, "c": 9.0, "d": 8.0}[player]
                ),
            }
            for week in (1, 2, 3, 4)
            for player in players
        ]
    )
    result = evaluate_weekly_direct_fantasy_points(frame, window=1)
    row = result.metrics.query(
        "scope == 'overall' and aggregation == 'fold' and "
        "metric == 'top_k_recall' and fold == '2024-2'"
    ).iloc[0]
    assert row.value == pytest.approx(2 / 3)
