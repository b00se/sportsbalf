import numpy as np
import pandas as pd
import pytest
from src.nfl.models.weekly_regressor_bakeoff import (
    CANDIDATE_NAMES,
    evaluate_weekly_regressor_bakeoff,
)


def _data(seasons=4):
    rows = []
    for season in range(2020, 2020 + seasons):
        for week in range(1, 5):
            for player, position, base in (
                ("p1", "WR", 8),
                ("p2", "TE", 5),
                ("p3", "RB", 4),
            ):
                rows.append(
                    {
                        "season": season,
                        "week": week,
                        "player_id": player,
                        "position": position,
                        "fantasy_points": base + week + (season - 2020) * 0.2,
                    }
                )
    return pd.DataFrame(rows)


def test_candidates_are_complete_and_deterministic():
    data = _data()
    left = evaluate_weekly_regressor_bakeoff(data)
    right = evaluate_weekly_regressor_bakeoff(data)
    assert set(left.ranking["candidate"]) == set(CANDIDATE_NAMES)
    pd.testing.assert_frame_equal(left.predictions, right.predictions)
    pd.testing.assert_frame_equal(left.ranking, right.ranking)


def test_strict_asof_is_mutation_invariant():
    data = _data()
    mutated = data.copy()
    mutated.loc[(mutated.season == 2023) & (mutated.week == 4), "fantasy_points"] = 9999
    first = evaluate_weekly_regressor_bakeoff(data).predictions
    second = evaluate_weekly_regressor_bakeoff(mutated).predictions
    key = ["season", "week", "player_id", "candidate"]
    joined = first.merge(second, on=key, suffixes=("_left", "_right"))
    target = joined.loc[
        (joined.season < 2023) | ((joined.season == 2023) & (joined.week < 4))
    ]
    np.testing.assert_allclose(target.prediction_left, target.prediction_right)


def test_insufficient_folds_is_inconclusive():
    result = evaluate_weekly_regressor_bakeoff(
        _data(seasons=1).loc[lambda frame: frame["week"] <= 3]
    )
    assert result.status == "inconclusive"
    assert set(
        result.metrics.loc[result.metrics.metric == "fantasy_point_mae", "status"]
    ) == {"inconclusive"}


def test_sparse_position_uses_its_own_fold_gate():
    data = _data().loc[lambda frame: frame["position"] != "TE"].copy()
    data = pd.concat(
        [
            data,
            pd.DataFrame(
                [
                    {
                        "season": 2023,
                        "week": 4,
                        "player_id": "sparse-te",
                        "position": "TE",
                        "fantasy_points": 7.0,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    result = evaluate_weekly_regressor_bakeoff(data)
    sparse = result.metrics.loc[
        (result.metrics["position"] == "TE")
        & (result.metrics["metric"] == "fantasy_point_mae")
    ]
    assert set(sparse["outer_folds"]) == {1}
    assert set(sparse["status"]) == {"inconclusive"}
    assert sparse["value"].isna().all()


def test_one_slate_returns_schema_valid_empty_result():
    data = _data(seasons=1).loc[lambda frame: frame["week"] == 1]
    result = evaluate_weekly_regressor_bakeoff(data)
    assert result.status == "inconclusive"
    assert result.predictions.empty
    assert list(result.predictions.columns) == [
        "fold",
        "season",
        "week",
        "player_id",
        "position",
        "candidate",
        "prediction",
        "actual",
    ]
    assert result.metrics.empty
    assert result.ranking.empty


@pytest.mark.parametrize(
    "bad", [pd.DataFrame(), _data().drop(columns=["fantasy_points"])]
)
def test_validation(bad):
    with pytest.raises(ValueError):
        evaluate_weekly_regressor_bakeoff(bad)
