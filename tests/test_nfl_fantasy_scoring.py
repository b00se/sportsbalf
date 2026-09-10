"""Offline contract tests for NFL component fantasy scoring."""

import numpy as np
import pandas as pd
import pytest
from src.nfl.models.fantasy_scoring import (
    FantasyScoringConfig,
    derive_fantasy_points,
    direct_fantasy_points_benchmark,
    rolling_origin_compare,
)


def _row(**overrides: object) -> pd.DataFrame:
    row = {
        "player_id": "p1",
        "position": "QB",
        "season": 2025,
        "week": 2,
        "pass_attempts": 30,
        "completions": 20,
        "passing_yards": 250,
        "passing_tds": 2,
        "interceptions": 1,
        "rushing_attempts": 4,
        "rushing_yards": 20,
        "rushing_tds": 1,
        "fumbles_lost": 1,
        "two_point_conversions": 1,
    }
    row.update(overrides)
    return pd.DataFrame([row])


def test_exact_half_ppr_qb_accounting_and_breakdown() -> None:
    result = derive_fantasy_points(_row())
    assert result.loc[0, "fantasy_points"] == pytest.approx(25.0)
    assert result.loc[0, "passing_points"] == pytest.approx(18.0)
    assert result.loc[0, "rushing_points"] == pytest.approx(8.0)
    assert result.loc[0, "turnover_points"] == pytest.approx(-3.0)
    assert result.loc[0, "bonus_points"] == pytest.approx(2.0)


def test_full_ppr_changes_only_receptions() -> None:
    rb = _row(
        position="RB",
        targets=10,
        receptions=7,
        receiving_yards=70,
        receiving_tds=1,
        rush_attempts=12,
        rushing_yards=48,
        rushing_tds=0,
        **{
            k: 0
            for k in (
                "pass_attempts",
                "completions",
                "passing_yards",
                "passing_tds",
                "interceptions",
                "fumbles_lost",
                "two_point_conversions",
            )
        },
    ).drop(columns="rushing_attempts")
    half = derive_fantasy_points(rb)
    full = derive_fantasy_points(rb, scoring="full_ppr")
    assert full.loc[0, "fantasy_points"] - half.loc[0, "fantasy_points"] == 3.5
    for column in (
        "passing_points",
        "rushing_points",
        "turnover_points",
        "bonus_points",
    ):
        assert full.loc[0, column] == half.loc[0, column]


@pytest.mark.parametrize("position", ["WR", "TE"])
def test_receiver_positions_use_receiving_components(position: str) -> None:
    frame = _row(
        position=position,
        targets=8,
        receptions=5,
        receiving_yards=65,
        receiving_tds=1,
        **{
            name: 0
            for name in (
                "pass_attempts",
                "completions",
                "passing_yards",
                "passing_tds",
                "interceptions",
                "rushing_attempts",
                "rushing_yards",
                "rushing_tds",
                "fumbles_lost",
                "two_point_conversions",
            )
        },
    )
    result = derive_fantasy_points(frame)
    assert result.loc[0, "fantasy_points"] == pytest.approx(15.0)


def test_invalid_boolean_nonfinite_and_incoherent_inputs_fail_closed() -> None:
    with pytest.raises(ValueError):
        derive_fantasy_points(_row(completions=31))
    with pytest.raises(ValueError):
        derive_fantasy_points(_row(interceptions=11))
    with pytest.raises(ValueError):
        derive_fantasy_points(_row(passing_tds=21))
    with pytest.raises(ValueError):
        derive_fantasy_points(_row(interceptions=np.inf))
    with pytest.raises(ValueError):
        derive_fantasy_points(_row(fumbles_lost=True))
    with pytest.raises(ValueError):
        derive_fantasy_points(_row(position="RB", targets=2, receptions=3))
    with pytest.raises(ValueError):
        derive_fantasy_points(_row(), scoring="turbo")
    with pytest.raises(ValueError):
        derive_fantasy_points(_row(), scoring={"reception": "not-a-number"})
    with pytest.raises(ValueError):
        derive_fantasy_points(_row(), scoring={"reception": True})
    assert FantasyScoringConfig(reception="0.75").reception == 0.75


def test_direct_benchmark_is_distinct_and_never_promoted_without_controls() -> None:
    history = pd.concat(
        [_row(week=1, passing_yards=100), _row(week=2, passing_yards=200)]
    )
    targets = _row(week=3)
    benchmark = direct_fantasy_points_benchmark(history, targets)
    assert benchmark.loc[0, "benchmark_kind"] == "direct_fantasy_points"
    assert "passing_points" not in benchmark
    comparison = rolling_origin_compare(history)
    assert comparison["promoted"] is False
    assert comparison["folds"] == 1


def test_direct_benchmark_sorts_history_and_marks_unseen_fallback() -> None:
    history = pd.DataFrame(
        [
            {"player_id": "p1", "season": 2025, "week": 2, "fantasy_points": 20},
            {"player_id": "p1", "season": 2025, "week": 1, "fantasy_points": 10},
        ]
    )
    targets = pd.DataFrame(
        [
            {"player_id": "p1", "season": 2025, "week": 3},
            {"player_id": "new", "season": 2025, "week": 3},
        ]
    )
    result = direct_fantasy_points_benchmark(history, targets)
    assert result.loc[0, "fantasy_points"] == 15
    assert bool(result.loc[0, "fallback_used"]) is False
    assert bool(result.loc[1, "fallback_used"]) is True
    assert result.loc[1, "fallback_reason"] == "no_player_history"


def test_rare_rush_is_added_to_zero_regular_rush_without_double_counting() -> None:
    frame = _row(
        position="WR",
        rushing_attempts=0,
        rushing_yards=0,
        rushing_tds=0,
        rare_rush_attempts=2,
        rare_rush_yards=14,
        rare_rush_tds=1,
        targets=4,
        receptions=2,
        receiving_yards=20,
        receiving_tds=0,
        **{
            name: 0
            for name in (
                "pass_attempts",
                "completions",
                "passing_yards",
                "passing_tds",
                "interceptions",
                "fumbles_lost",
                "two_point_conversions",
            )
        },
    )
    result = derive_fantasy_points(frame)
    assert result.loc[0, "rushing_points"] == pytest.approx(7.4)
    assert result.loc[0, "fantasy_points"] == pytest.approx(10.4)


@pytest.mark.parametrize(
    "partial",
    [
        {"rare_rush_attempts": 1},
        {"rare_rush_yards": 5},
        {"rare_rush_tds": 1},
    ],
)
def test_partial_rare_rush_stream_fails_closed(partial: dict[str, int]) -> None:
    with pytest.raises(ValueError, match="complete stream"):
        derive_fantasy_points(_row(**partial))


def test_each_rushing_stream_validates_touchdowns_before_aggregation() -> None:
    with pytest.raises(ValueError, match="canonical rushing"):
        derive_fantasy_points(
            _row(
                rushing_attempts=0,
                rushing_tds=1,
                rare_rush_attempts=2,
                rare_rush_yards=10,
                rare_rush_tds=0,
            )
        )
    with pytest.raises(ValueError, match="rare rushing"):
        derive_fantasy_points(
            _row(
                rushing_attempts=2,
                rushing_tds=0,
                rare_rush_attempts=0,
                rare_rush_yards=10,
                rare_rush_tds=1,
            )
        )


def test_calendar_rejects_python_and_numpy_booleans() -> None:
    with pytest.raises(ValueError, match="season"):
        direct_fantasy_points_benchmark(
            pd.DataFrame(
                [{"player_id": "p1", "season": True, "week": 1, "fantasy_points": 1}]
            )
        )


def test_calendar_semantic_duplicates_fail_after_normalization() -> None:
    duplicate = pd.DataFrame(
        [
            {"player_id": "p1", "season": 2025, "week": 1, "fantasy_points": 10},
            {"player_id": "p1", "season": "2025", "week": "1", "fantasy_points": 20},
        ]
    )
    with pytest.raises(ValueError, match="duplicate"):
        direct_fantasy_points_benchmark(duplicate)
    with pytest.raises(ValueError, match="duplicate"):
        rolling_origin_compare(duplicate)
    with pytest.raises(ValueError, match="week"):
        direct_fantasy_points_benchmark(
            pd.DataFrame(
                [
                    {
                        "player_id": "p1",
                        "season": 2025,
                        "week": np.bool_(True),
                        "fantasy_points": 1,
                    }
                ]
            )
        )
