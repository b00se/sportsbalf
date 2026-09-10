from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from src.nfl.models.qb_components import (
    COMPONENTS,
    QBComponentConfig,
    project_qb_components,
    rolling_origin_compare,
)


def _history() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "qb_id": "q1",
                "season": 2024,
                "week": 1,
                "pass_attempts": 30,
                "completions": 20,
                "passing_yards": 220,
                "passing_tds": 2,
                "interceptions": 1,
                "rushing_attempts": 4,
                "rushing_yards": 18,
                "rushing_tds": 0,
                "team_pass_attempts": 34,
            },
            {
                "qb_id": "q1",
                "season": 2024,
                "week": 2,
                "pass_attempts": 40,
                "completions": 28,
                "passing_yards": 300,
                "passing_tds": 3,
                "interceptions": 0,
                "rushing_attempts": 6,
                "rushing_yards": 31,
                "rushing_tds": 1,
                "team_pass_attempts": 44,
            },
            {
                "qb_id": "q1",
                "season": 2024,
                "week": 3,
                "pass_attempts": 35,
                "completions": 21,
                "passing_yards": 245,
                "passing_tds": 1,
                "interceptions": 2,
                "rushing_attempts": 5,
                "rushing_yards": 22,
                "rushing_tds": 0,
                "team_pass_attempts": 40,
            },
            {
                "qb_id": "q1",
                "season": 2024,
                "week": 4,
                "pass_attempts": 50,
                "completions": 45,
                "passing_yards": 600,
                "passing_tds": 8,
                "interceptions": 5,
                "rushing_attempts": 2,
                "rushing_yards": 2,
                "rushing_tds": 2,
                "team_pass_attempts": 60,
            },
        ]
    )


def test_projection_is_typed_deterministic_and_coherent() -> None:
    target = pd.DataFrame(
        [{"qb_id": "q1", "season": 2024, "week": 5, "team_pass_attempts": 48}]
    )
    result = project_qb_components(_history(), target)
    expected = {
        "pass_attempts",
        "completions",
        "passing_yards",
        "passing_tds",
        "interceptions",
        "rushing_attempts",
        "rushing_yards",
        "rushing_tds",
        "as_of_season",
        "as_of_week",
    }
    assert expected <= set(result.columns)
    assert result.equals(project_qb_components(_history(), target))
    assert (result["pass_attempts"] >= result["completions"]).all()
    assert (result[list(expected - {"as_of_season", "as_of_week"})] >= 0).all().all()


def test_as_of_cutoff_blocks_future_leakage() -> None:
    target = pd.DataFrame([{"qb_id": "q1", "season": 2024, "week": 5}])
    baseline = project_qb_components(_history().iloc[:3], target)
    mutated = _history().copy()
    mutated.loc[3, ["pass_attempts", "completions", "passing_yards", "passing_tds"]] = [
        1,
        1,
        1,
        0,
    ]
    assert project_qb_components(mutated, target, as_of=(2024, 3))[
        list(COMPONENTS)
    ].equals(baseline[list(COMPONENTS)])
    early = project_qb_components(_history(), target, as_of=(2024, 2))
    assert early.loc[0, "completions"] != baseline.loc[0, "completions"]


def test_rolling_origin_reports_model_and_declared_baseline() -> None:
    frame = _history().iloc[:3].copy()
    frame = pd.concat(
        [frame, frame.iloc[:1].assign(week=5, pass_attempts=36, completions=24)],
        ignore_index=True,
    )
    result = rolling_origin_compare(frame, config=QBComponentConfig(min_history=3))
    assert {"model_mae", "baseline_mae", "promoted", "folds"} <= set(result)
    assert result["folds"] >= 1


def test_rolling_origin_ignores_target_team_volume() -> None:
    frame = _history().iloc[:3].copy()
    frame = pd.concat(
        [frame, frame.iloc[:1].assign(week=5, pass_attempts=36, completions=24)],
        ignore_index=True,
    )
    changed = frame.copy()
    changed.loc[3, "team_pass_attempts"] = 9999
    assert rolling_origin_compare(
        frame, config=QBComponentConfig(min_history=2)
    ) == rolling_origin_compare(changed, config=QBComponentConfig(min_history=2))


def test_new_qb_opportunity_caps_fallback_attempts() -> None:
    target = pd.DataFrame(
        [{"qb_id": "new", "season": 2025, "week": 1, "team_pass_attempts": 12}]
    )
    result = project_qb_components(_history(), target)
    assert result.loc[0, "pass_attempts"] <= 12


def test_as_of_metadata_preserves_supplied_cross_season_cutoff() -> None:
    target = pd.DataFrame([{"qb_id": "q1", "season": 2025, "week": 1}])
    result = project_qb_components(_history(), target, as_of=(2024, 4))
    assert result.loc[0, ["as_of_season", "as_of_week"]].tolist() == [2024, 4]


@pytest.mark.parametrize(
    "column,value",
    [("season", 2024.5), ("week", 0), ("week", -1), ("week", 23)],
)
def test_invalid_calendar_values_are_rejected(column: str, value: float) -> None:
    frame = _history().iloc[:1].copy()
    frame.loc[frame.index[0], column] = value
    with pytest.raises(ValueError, match="season|week"):
        project_qb_components(frame)


def test_duplicate_player_week_is_rejected() -> None:
    frame = pd.concat([_history().iloc[:1], _history().iloc[:1]], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate"):
        project_qb_components(frame)


def test_semantic_duplicate_calendar_values_are_rejected_in_any_order() -> None:
    frame = _history().iloc[:1].copy()
    duplicate = frame.copy()
    duplicate["season"] = "2024"
    duplicate["week"] = "1"
    combined = pd.concat([frame, duplicate], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate"):
        project_qb_components(combined)
    with pytest.raises(ValueError, match="duplicate"):
        project_qb_components(combined.iloc[::-1].reset_index(drop=True))


def test_future_as_of_is_clamped_to_target_predecessor() -> None:
    target = pd.DataFrame([{"qb_id": "q1", "season": 2024, "week": 3}])
    result = project_qb_components(_history(), target, as_of=(2025, 1))
    assert result.loc[0, ["as_of_season", "as_of_week"]].tolist() == [2024, 2]


def test_week_one_reports_prior_season_boundary_by_default() -> None:
    target = pd.DataFrame([{"qb_id": "q1", "season": 2025, "week": 1}])
    result = project_qb_components(_history(), target)
    assert result.loc[0, ["as_of_season", "as_of_week"]].tolist() == [2024, 22]


@pytest.mark.parametrize(
    "field,value",
    [
        ("fallback_attempts", float("inf")),
        ("fallback_pass_td_rate", float("nan")),
        ("fallback_yards_per_attempt", -1.0),
    ],
)
def test_invalid_fallback_priors_fail_closed(field: str, value: float) -> None:
    with pytest.raises(ValueError, match=field):
        QBComponentConfig(**{field: value})


@pytest.mark.parametrize(
    "field,value",
    [
        ("min_history", 1.5),
        ("window", 2.5),
        ("min_history", 0),
        ("window", 0),
        ("min_history", -1),
        ("window", -1),
        ("min_history", "many"),
        ("window", "six"),
        ("fallback_attempts", "many"),
    ],
)
def test_invalid_config_types_and_ranges_fail_closed(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        QBComponentConfig(**{field: value})


def test_numeric_config_values_are_normalized_and_valid_config_is_accepted() -> None:
    config = QBComponentConfig(min_history="2", window=4.0, fallback_attempts="30")
    assert config.min_history == 2
    assert config.window == 4
    assert config.fallback_attempts == 30.0


@pytest.mark.parametrize("field", ["min_history", "window"])
def test_numpy_boolean_calendar_config_values_are_rejected(field: str) -> None:
    with pytest.raises(ValueError):
        QBComponentConfig(**{field: np.bool_(True)})


@pytest.mark.parametrize(
    "field",
    [
        "fallback_attempts",
        "fallback_completion_rate",
        "fallback_yards_per_attempt",
        "fallback_pass_td_rate",
        "fallback_interception_rate",
        "fallback_rush_attempts",
        "fallback_rush_yards_per_attempt",
        "fallback_rush_td_rate",
    ],
)
def test_numpy_boolean_fallback_values_are_rejected(field: str) -> None:
    with pytest.raises(ValueError):
        QBComponentConfig(**{field: np.bool_(True)})


def test_adversarial_negative_and_infinite_values_are_rejected() -> None:
    negative = _history().iloc[:1].copy()
    negative.loc[negative.index[0], "passing_yards"] = -1
    with pytest.raises(ValueError, match="nonnegative"):
        project_qb_components(negative)
    infinite = _history().iloc[:1].copy()
    infinite.loc[infinite.index[0], "pass_attempts"] = float("inf")
    with pytest.raises(ValueError, match="finite"):
        project_qb_components(infinite)


def test_nonpromotion_is_reported_when_model_loses_baseline() -> None:
    frame = _history().iloc[:3].copy()
    frame.loc[:, "pass_attempts"] = [10, 10, 10]
    other = frame.assign(qb_id="q2", pass_attempts=100)
    frame = pd.concat([frame, other], ignore_index=True)
    frame = pd.concat(
        [
            frame,
            frame.loc[frame["qb_id"].eq("q1")]
            .iloc[:1]
            .assign(week=5, pass_attempts=40),
        ],
        ignore_index=True,
    )
    result = rolling_origin_compare(frame, config=QBComponentConfig(min_history=3))
    assert result["promoted"] is False
