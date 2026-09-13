"""Focused tests for the paired direct-FP tournament."""

import pandas as pd
import pytest
from src.nfl.models.baseline_evaluation import BaselineEvaluationResult
from src.nfl.models.direct_fp_paired_tournament import (
    tournament_direct_vs_internal_baseline,
)


def _outcomes(weeks: int = 4) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "season": 2024,
                "week": week,
                "player_id": player,
                "position": position,
                "fantasy_points": float(week + (player == "b")),
            }
            for week in range(1, weeks + 1)
            for player, position in (("a", "QB"), ("b", "RB"))
        ]
    )


def test_pairs_identical_keys_and_emits_position_fold_slices() -> None:
    result = tournament_direct_vs_internal_baseline(_outcomes(), window=2, seed=11)

    assert result.status == "unpromoted"
    assert result.outer_fold_count == 3
    assert result.sample_count == 6
    assert set(result.paired_predictions.outer_fold) == {"2024-2", "2024-3", "2024-4"}
    assert set(result.slice_results.scope) == {"overall", "position"}
    assert set(result.slice_results.query("scope == 'position'").position) == {
        "QB",
        "RB",
    }


def test_future_outcome_mutation_does_not_change_paired_predictions() -> None:
    frame = _outcomes()
    first = tournament_direct_vs_internal_baseline(frame, window=2, seed=4)
    frame.loc[frame.week == 4, "fantasy_points"] = 9999.0
    changed = tournament_direct_vs_internal_baseline(frame, window=2, seed=4)
    columns = [
        "outer_fold",
        "player_id",
        "direct_prediction",
        "baseline_prediction",
        "actual",
    ]
    prior = first.paired_predictions.query("outer_fold != '2024-4'")[columns]
    changed_prior = changed.paired_predictions.query("outer_fold != '2024-4'")[columns]
    pd.testing.assert_frame_equal(prior, changed_prior)


def test_bootstrap_is_seeded_and_requires_three_outer_folds() -> None:
    first = tournament_direct_vs_internal_baseline(
        _outcomes(), seed=17, bootstrap_replicates=300
    )
    second = tournament_direct_vs_internal_baseline(
        _outcomes(), seed=17, bootstrap_replicates=300
    )
    assert (first.ci_low, first.ci_high) == (second.ci_low, second.ci_high)
    assert first.comparison_status in {"conclusive_tie_or_better", "conclusive_worse"}
    inconclusive = tournament_direct_vs_internal_baseline(_outcomes(2), seed=17)
    assert inconclusive.outer_fold_count == 1
    assert inconclusive.comparison_status == "inconclusive"


def test_season_mode_pairs_only_prior_season_holdouts() -> None:
    result = tournament_direct_vs_internal_baseline(
        pd.concat([_outcomes(), _outcomes().assign(season=2025)], ignore_index=True),
        mode="season",
        seed=17,
    )
    assert result.mode == "season"
    assert result.outer_fold_count == 1
    assert set(result.paired_predictions.season) == {2025}
    assert result.comparison_status == "inconclusive"


def test_pairing_rejects_different_actuals(monkeypatch: pytest.MonkeyPatch) -> None:
    direct = pd.DataFrame(
        [
            {
                "season": 2024,
                "week": 2,
                "player_id": "a",
                "position": "QB",
                "actual": 4.0,
                "prediction": 3.0,
                "outer_fold": "wrong",
            }
        ]
    )
    internal = pd.DataFrame(
        [
            {
                "season": 2024,
                "week": 2,
                "player_id": "a",
                "position": "QB",
                "fantasy_points": 5.0,
                "prediction": 3.0,
                "baseline": "player_trailing_mean",
            }
        ]
    )
    monkeypatch.setattr(
        "src.nfl.models.direct_fp_paired_tournament.evaluate_weekly_direct_fantasy_points",
        lambda frame, **kwargs: BaselineEvaluationResult(
            direct, pd.DataFrame(), "eligible", "weekly"
        ),
    )
    monkeypatch.setattr(
        "src.nfl.models.direct_fp_paired_tournament.evaluate_weekly_baselines",
        lambda frame, **kwargs: BaselineEvaluationResult(
            internal, pd.DataFrame(), "eligible", "weekly"
        ),
    )
    with pytest.raises(ValueError, match="actual outcomes"):
        tournament_direct_vs_internal_baseline(_outcomes(2), min_outer_folds=1)


def test_pairing_rejects_tiny_actual_outcome_difference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    direct = pd.DataFrame(
        [
            {
                "season": 2024,
                "week": 2,
                "player_id": "a",
                "position": "QB",
                "actual": 100.0,
                "prediction": 99.0,
                "outer_fold": "2024-2",
            }
        ]
    )
    internal = direct.assign(
        actual=100.0 + 1e-9, baseline="player_trailing_mean"
    ).rename(columns={"actual": "fantasy_points"})
    monkeypatch.setattr(
        "src.nfl.models.direct_fp_paired_tournament.evaluate_weekly_direct_fantasy_points",
        lambda frame, **kwargs: BaselineEvaluationResult(
            direct, pd.DataFrame(), "eligible", "weekly"
        ),
    )
    monkeypatch.setattr(
        "src.nfl.models.direct_fp_paired_tournament.evaluate_weekly_baselines",
        lambda frame, **kwargs: BaselineEvaluationResult(
            internal, pd.DataFrame(), "eligible", "weekly"
        ),
    )
    with pytest.raises(ValueError, match="actual outcomes"):
        tournament_direct_vs_internal_baseline(_outcomes(2), min_outer_folds=1)


def test_pairing_rejects_missing_player_week_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    direct = pd.DataFrame(
        [
            {
                "season": 2024,
                "week": 2,
                "player_id": "a",
                "position": "QB",
                "actual": 4.0,
                "prediction": 3.0,
                "outer_fold": "2024-2",
            }
        ]
    )
    internal = direct.assign(baseline="player_trailing_mean").rename(
        columns={"actual": "fantasy_points"}
    )
    internal.loc[len(internal)] = [
        2024,
        3,
        "a",
        "QB",
        4.0,
        3.0,
        "2024-3",
        "player_trailing_mean",
    ]
    monkeypatch.setattr(
        "src.nfl.models.direct_fp_paired_tournament.evaluate_weekly_direct_fantasy_points",
        lambda frame, **kwargs: BaselineEvaluationResult(
            direct, pd.DataFrame(), "eligible", "weekly"
        ),
    )
    monkeypatch.setattr(
        "src.nfl.models.direct_fp_paired_tournament.evaluate_weekly_baselines",
        lambda frame, **kwargs: BaselineEvaluationResult(
            internal, pd.DataFrame(), "eligible", "weekly"
        ),
    )
    with pytest.raises(ValueError, match="key sets differ"):
        tournament_direct_vs_internal_baseline(_outcomes(2), min_outer_folds=1)


def test_direction_boundary_uses_conservative_nonpositive_upper_ci(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    direct = pd.DataFrame(
        [
            {
                "season": 2024,
                "week": week,
                "player_id": "a",
                "position": "QB",
                "actual": 5.0,
                "prediction": 4.0,
                "outer_fold": f"2024-{week}",
            }
            for week in (2, 3, 4)
        ]
    )
    internal = (
        direct.assign(prediction=4.0, baseline="player_trailing_mean")
        .drop(columns="outer_fold")
        .rename(columns={"actual": "fantasy_points"})
    )
    monkeypatch.setattr(
        "src.nfl.models.direct_fp_paired_tournament.evaluate_weekly_direct_fantasy_points",
        lambda frame, **kwargs: BaselineEvaluationResult(
            direct, pd.DataFrame(), "eligible", "weekly"
        ),
    )
    monkeypatch.setattr(
        "src.nfl.models.direct_fp_paired_tournament.evaluate_weekly_baselines",
        lambda frame, **kwargs: BaselineEvaluationResult(
            internal, pd.DataFrame(), "eligible", "weekly"
        ),
    )
    result = tournament_direct_vs_internal_baseline(
        _outcomes(4), seed=1, bootstrap_replicates=100
    )
    assert result.mean_delta == 0.0
    assert result.ci_low == result.ci_high == 0.0
    assert result.comparison_status == "conclusive_tie_or_better"
