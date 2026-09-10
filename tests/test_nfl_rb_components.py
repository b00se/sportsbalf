from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from src.nfl.models.rb_components import (
    RB_COMPONENTS,
    RBComponentConfig,
    project_rb_components,
    rolling_origin_compare,
)


def _history() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "rb_id": "r1",
                "season": 2024,
                "week": 1,
                "rush_attempts": 16,
                "targets": 5,
                "rushing_yards": 72,
                "receptions": 4,
                "receiving_yards": 28,
                "rushing_tds": 1,
                "receiving_tds": 0,
                "team_rush_attempts": 28,
                "team_targets": 34,
            },
            {
                "rb_id": "r1",
                "season": 2024,
                "week": 2,
                "rush_attempts": 20,
                "targets": 6,
                "rushing_yards": 90,
                "receptions": 5,
                "receiving_yards": 40,
                "rushing_tds": 0,
                "receiving_tds": 1,
                "team_rush_attempts": 30,
                "team_targets": 36,
            },
            {
                "rb_id": "r1",
                "season": 2024,
                "week": 3,
                "rush_attempts": 12,
                "targets": 4,
                "rushing_yards": 48,
                "receptions": 3,
                "receiving_yards": 20,
                "rushing_tds": 1,
                "receiving_tds": 0,
                "team_rush_attempts": 25,
                "team_targets": 32,
            },
            {
                "rb_id": "r1",
                "season": 2024,
                "week": 4,
                "rush_attempts": 18,
                "targets": 7,
                "rushing_yards": 81,
                "receptions": 6,
                "receiving_yards": 55,
                "rushing_tds": 2,
                "receiving_tds": 1,
                "team_rush_attempts": 31,
                "team_targets": 38,
            },
        ]
    )


def test_projection_is_deterministic_and_coherent() -> None:
    target = pd.DataFrame([{"rb_id": "r1", "season": 2024, "week": 5}])
    result = project_rb_components(_history(), target)
    assert set(RB_COMPONENTS) <= set(result.columns)
    assert result.equals(project_rb_components(_history(), target))
    assert (result[list(RB_COMPONENTS)] >= 0).all().all()
    assert result.loc[0, "receptions"] <= result.loc[0, "targets"]


def test_as_of_cutoff_and_target_fold_inputs_do_not_leak() -> None:
    target = pd.DataFrame([{"rb_id": "r1", "season": 2024, "week": 5}])
    baseline = project_rb_components(_history().iloc[:3], target, as_of=(2024, 3))
    changed = _history().copy()
    changed.loc[3, RB_COMPONENTS] = [100, 30, 1000, 30, 1000, 10, 10]
    assert project_rb_components(changed, target, as_of=(2024, 3))[
        list(RB_COMPONENTS)
    ].equals(baseline[list(RB_COMPONENTS)])

    frame = pd.concat(
        [_history().iloc[:3], _history().iloc[:1].assign(week=5)], ignore_index=True
    )
    mutated = frame.copy()
    mutated.loc[3, ["team_rush_attempts", "team_targets"]] = [9999, 9999]
    assert rolling_origin_compare(frame) == rolling_origin_compare(mutated)


@pytest.mark.parametrize("prob", [-0.01, 1.01, np.nan, np.inf])
def test_availability_probability_is_bounded(prob: float) -> None:
    target = pd.DataFrame(
        [{"rb_id": "r1", "season": 2025, "week": 1, "availability_probability": prob}]
    )
    with pytest.raises(ValueError, match="availability_probability"):
        project_rb_components(_history(), target)


def test_availability_status_is_conservative_and_caps_team_volume() -> None:
    target = pd.DataFrame(
        [
            {
                "rb_id": "new",
                "season": 2025,
                "week": 1,
                "availability_status": "questionable",
                "availability_probability": 0.5,
                "team_rush_attempts": 10,
                "team_targets": 3,
            }
        ]
    )
    result = project_rb_components(_history(), target)
    assert result.loc[0, "availability_probability"] == 0.5
    assert result.loc[0, "rush_attempts"] <= 10
    assert result.loc[0, "targets"] <= 3
    inactive = project_rb_components(
        _history(),
        target.assign(availability_status="inactive", availability_probability=1),
    )
    assert (inactive.loc[0, list(RB_COMPONENTS)] == 0).all()


def test_empty_history_uses_safe_priors_and_invalid_rows_fail_closed() -> None:
    target = pd.DataFrame([{"rb_id": "new", "season": 2025, "week": 1}])
    result = project_rb_components(_history().iloc[:0], target)
    assert result.loc[0, "rush_attempts"] > 0
    duplicate = pd.concat([_history().iloc[:1]] * 2, ignore_index=True)
    with pytest.raises(ValueError, match="duplicate"):
        project_rb_components(duplicate)
    invalid = _history().iloc[:1].copy()
    invalid.loc[0, "rushing_yards"] = -1
    with pytest.raises(ValueError, match="finite nonnegative"):
        project_rb_components(invalid)


def test_empty_history_needs_only_identity_and_calendar() -> None:
    history = pd.DataFrame(columns=["rb_id", "season", "week"])
    target = pd.DataFrame([{"rb_id": "rookie", "season": 2025, "week": 1}])
    result = project_rb_components(history, target)
    assert result.loc[0, "rush_attempts"] > 0


def test_historical_touchdowns_cannot_exceed_opportunity() -> None:
    invalid = _history().iloc[:1].copy()
    invalid.loc[0, "rushing_tds"] = invalid.loc[0, "rush_attempts"] + 1
    with pytest.raises(ValueError, match="rushing_tds"):
        project_rb_components(invalid)
    invalid = _history().iloc[:1].copy()
    invalid.loc[0, "receiving_tds"] = invalid.loc[0, "receptions"] + 1
    with pytest.raises(ValueError, match="receiving_tds"):
        project_rb_components(invalid)


def test_alias_only_history_materializes_canonical_components() -> None:
    aliases = {
        "rush_attempts": "rushing_attempts",
        "targets": "receiving_targets",
        "rushing_yards": "rush_yards",
        "receptions": "catches",
        "receiving_yards": "receiving_yards_gained",
        "rushing_tds": "rush_touchdowns",
        "receiving_tds": "receiving_touchdowns",
    }
    history = _history().rename(columns=aliases)
    target = pd.DataFrame([{"rb_id": "r1", "season": 2024, "week": 5}])
    result = project_rb_components(history, target)
    assert result.loc[0, "rush_attempts"] > 0


def test_conflicting_canonical_and_alias_values_fail_closed() -> None:
    history = _history().copy()
    history["rushing_attempts"] = history["rush_attempts"] + 1
    with pytest.raises(ValueError, match="conflicting"):
        project_rb_components(history)


def test_explicit_nan_availability_and_boolean_inputs_fail_closed() -> None:
    target = pd.DataFrame([{"rb_id": "r1", "season": 2025, "week": 1}])
    with pytest.raises(ValueError, match="availability_probability"):
        project_rb_components(_history(), target, availability_probability=np.nan)
    for column in ["season", "week", "team_rush_attempts", "team_targets"]:
        bad = target.copy()
        bad[column] = True
        with pytest.raises(ValueError, match=column):
            project_rb_components(_history(), bad)


def test_overflowing_finite_history_fails_closed() -> None:
    huge = _history().iloc[:1].astype({"rushing_yards": float, "rush_attempts": float})
    huge.loc[0, "rushing_yards"] = 1e308
    huge.loc[0, "rush_attempts"] = 1e-300
    huge.loc[0, "rushing_tds"] = 0
    target = pd.DataFrame([{"rb_id": "r1", "season": 2024, "week": 2}])
    with pytest.raises(ValueError, match="non-finite"):
        project_rb_components(huge, target)


def test_rolling_origin_reports_promotion_or_nonpromotion() -> None:
    frame = pd.concat(
        [_history().iloc[:3], _history().iloc[:1].assign(week=5)], ignore_index=True
    )
    result = rolling_origin_compare(frame, config=RBComponentConfig(min_history=3))
    assert {"model_mae", "baseline_mae", "promoted", "folds"} <= set(result)
    assert result["folds"] == 1
    losing = pd.concat(
        [
            frame.iloc[:1].assign(rush_attempts=10),
            frame.iloc[1:2].assign(rush_attempts=10, week=2),
            frame.iloc[2:3].assign(rush_attempts=100, week=3),
            frame.iloc[3:4].assign(rush_attempts=10, week=5),
            frame.iloc[3:4].assign(rush_attempts=0, week=6),
        ],
        ignore_index=True,
    )
    losing[["rushing_tds", "receiving_tds"]] = 0
    assert (
        rolling_origin_compare(
            losing, config=RBComponentConfig(min_history=3, window=3)
        )["promoted"]
        is False
    )
