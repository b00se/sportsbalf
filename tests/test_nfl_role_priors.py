"""Contract tests for R3.5 rookie/new-role priors."""

import numpy as np
import pandas as pd
import pytest
from src.nfl.models.role_priors import (
    RolePriorConfig,
    build_role_priors,
    classify_player_roles,
    rolling_origin_compare,
)


def _rb_history() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "player_id": "veteran",
                "position": "RB",
                "season": 2025,
                "week": 1,
                "rush_attempts": 10,
                "targets": 3,
                "rushing_yards": 40,
                "receptions": 2,
                "receiving_yards": 15,
                "rushing_tds": 0,
                "receiving_tds": 0,
            },
            {
                "player_id": "veteran",
                "position": "RB",
                "season": 2025,
                "week": 2,
                "rush_attempts": 12,
                "targets": 4,
                "rushing_yards": 48,
                "receptions": 3,
                "receiving_yards": 20,
                "rushing_tds": 1,
                "receiving_tds": 0,
            },
            {
                "player_id": "other",
                "position": "RB",
                "season": 2025,
                "week": 1,
                "rush_attempts": 30,
                "targets": 9,
                "rushing_yards": 150,
                "receptions": 8,
                "receiving_yards": 100,
                "rushing_tds": 2,
                "receiving_tds": 1,
            },
        ]
    )


def test_no_history_is_explicit_rookie_with_widened_uncertainty() -> None:
    target = pd.DataFrame(
        [
            {
                "player_id": "rookie",
                "position": "RB",
                "season": 2025,
                "week": 3,
                "is_rookie": True,
            }
        ]
    )
    result = build_role_priors(_rb_history(), target).iloc[0]
    assert result["classification"] == "rookie"
    assert result["prior_source"] == "supplied_rookie_context"
    assert result["uncertainty_multiplier"] > 1
    assert result["prior_rush_attempts"] < _rb_history()["rush_attempts"].max()


def test_new_role_context_is_supplied_not_future_inferred() -> None:
    target = pd.DataFrame(
        [
            {
                "player_id": "veteran",
                "position": "RB",
                "season": 2025,
                "week": 3,
                "role_context": "new_role",
            }
        ]
    )
    result = classify_player_roles(_rb_history(), target).iloc[0]
    assert result["classification"] == "new_role"
    assert result["uncertainty_multiplier"] > 1


def test_limited_history_reports_count_and_source_without_calling_it_no_history() -> (
    None
):
    target = pd.DataFrame(
        [{"player_id": "veteran", "position": "RB", "season": 2025, "week": 3}]
    )
    result = build_role_priors(
        _rb_history(), target, config=RolePriorConfig(min_history=3)
    ).iloc[0]
    assert result["classification"] == "limited_history"
    assert result["prior_source"] == "limited_player_history"
    assert result["history_count"] == 2


def test_as_of_excludes_future_history_and_target_fold_mutation() -> None:
    history = _rb_history()
    target = pd.DataFrame(
        [{"player_id": "veteran", "position": "RB", "season": 2025, "week": 3}]
    )
    before = build_role_priors(history, target, as_of=(2025, 2))
    mutated = history.copy()
    mutated.loc[mutated["week"] == 2, "rush_attempts"] = 9999
    # Week 2 is exactly the explicit cutoff and therefore excluded.
    assert build_role_priors(mutated, target, as_of=(2025, 2)).equals(before)


def test_semantic_duplicates_and_order_are_deterministic() -> None:
    targets = pd.DataFrame(
        [
            {"player_id": "veteran", "position": "RB", "season": "2025", "week": "3"},
            {"player_id": "rookie", "position": "RB", "season": 2025, "week": 3},
        ]
    )
    a = build_role_priors(_rb_history(), targets)
    b = build_role_priors(_rb_history(), targets.iloc[::-1])
    pd.testing.assert_frame_equal(a, b)
    duplicate = pd.concat([targets.iloc[[0]], targets.iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate"):
        build_role_priors(_rb_history(), duplicate)


@pytest.mark.parametrize(
    "field,value",
    [
        ("rookie_uncertainty_multiplier", np.nan),
        ("external_weight", np.inf),
        ("min_history", True),
    ],
)
def test_invalid_configuration_fails_closed(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        RolePriorConfig(**{field: value})


def test_invalid_inputs_fail_closed() -> None:
    bad = _rb_history()
    bad.loc[0, "rush_attempts"] = np.inf
    target = pd.DataFrame(
        [{"player_id": "veteran", "position": "RB", "season": 2025, "week": 3}]
    )
    with pytest.raises(ValueError, match="finite"):
        build_role_priors(bad, target)
    bad_target = target.assign(ud_projection=-1)
    with pytest.raises(ValueError, match="nonnegative"):
        build_role_priors(_rb_history(), bad_target)
    for column, value in (
        ("is_rookie", "yes"),
        ("role_context", "guess"),
        ("draft_capital", "unknown"),
    ):
        with pytest.raises(ValueError):
            build_role_priors(_rb_history(), target.assign(**{column: value}))
    with pytest.raises(ValueError):
        build_role_priors(_rb_history(), target.assign(depth_chart_role=True))
    with pytest.raises(ValueError):
        build_role_priors(_rb_history(), target.assign(depth_chart_role="third_string"))
    with pytest.raises(ValueError):
        build_role_priors(_rb_history(), target.assign(draft_year=np.inf))
    with pytest.raises(ValueError):
        build_role_priors(_rb_history(), target.assign(draft_round=-1))
    bad_bool_stat = _rb_history().astype(object)
    bad_bool_stat.loc[0, "rush_attempts"] = np.bool_(True)
    with pytest.raises(ValueError):
        build_role_priors(bad_bool_stat, target)
    with pytest.raises(ValueError):
        build_role_priors(_rb_history(), target, as_of=(2147483648, 1))


def test_uncertainty_configuration_must_widen_over_established() -> None:
    with pytest.raises(ValueError):
        RolePriorConfig(rookie_uncertainty_multiplier=1.0)
    with pytest.raises(ValueError):
        RolePriorConfig(new_role_uncertainty_multiplier=0.9)


def test_rolling_origin_promotion_and_nonpromotion_are_transparent() -> None:
    frame = _rb_history().assign(season=2025)
    promoted = rolling_origin_compare(frame)
    assert {"model_mae", "baseline_mae", "promoted", "folds"} <= promoted.keys()
    assert promoted["folds"] > 0
    assert promoted["promoted"] in {True, False}

    stable = pd.concat(
        [_rb_history().iloc[[0, 1]], _rb_history().iloc[[0]]], ignore_index=True
    )
    stable["player_id"] = "stable"
    stable.loc[:, "rush_attempts"] = [8, 8, 8]
    stable.loc[:, "week"] = [1, 2, 3]
    tie = rolling_origin_compare(stable)
    assert tie["promoted"] is True
    poor = stable.copy()
    poor.loc[poor["week"] == 2, "rush_attempts"] = 40
    nonpromotion = rolling_origin_compare(poor)
    assert nonpromotion["promoted"] is False
