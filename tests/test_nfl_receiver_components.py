import numpy as np
import pandas as pd
import pytest
from src.nfl.models.receiver_components import (
    RECEIVER_COMPONENTS,
    ReceiverComponentConfig,
    project_receiver_components,
    rolling_origin_compare,
)


def _history() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "receiver_id": "w1",
                "position": "WR",
                "season": 2025,
                "week": 1,
                "routes": 30,
                "targets": 8,
                "receptions": 5,
                "receiving_yards": 60,
                "receiving_tds": 1,
                "rare_rush_attempts": 1,
                "rare_rush_yards": 4,
                "rare_rush_tds": 0,
                "team_pass_attempts": 34,
                "team_rush_attempts": 24,
            },
            {
                "receiver_id": "w1",
                "position": "WR",
                "season": 2025,
                "week": 2,
                "routes": 32,
                "targets": 9,
                "receptions": 6,
                "receiving_yards": 72,
                "receiving_tds": 0,
                "rare_rush_attempts": 0,
                "rare_rush_yards": 0,
                "rare_rush_tds": 0,
                "team_pass_attempts": 36,
                "team_rush_attempts": 25,
            },
            {
                "receiver_id": "w1",
                "position": "WR",
                "season": 2025,
                "week": 3,
                "routes": 31,
                "targets": 10,
                "receptions": 7,
                "receiving_yards": 80,
                "receiving_tds": 1,
                "rare_rush_attempts": 2,
                "rare_rush_yards": 9,
                "rare_rush_tds": 0,
                "team_pass_attempts": 35,
                "team_rush_attempts": 23,
            },
            {
                "receiver_id": "t1",
                "position": "TE",
                "season": 2025,
                "week": 1,
                "routes": 25,
                "targets": 7,
                "receptions": 4,
                "receiving_yards": 40,
                "receiving_tds": 0,
                "rare_rush_attempts": 0,
                "rare_rush_yards": 0,
                "rare_rush_tds": 0,
                "team_pass_attempts": 34,
                "team_rush_attempts": 24,
            },
        ]
    )


def test_projects_wr_and_te_components_with_caps_and_as_of():
    targets = pd.DataFrame(
        [
            {
                "receiver_id": "w1",
                "position": "WR",
                "season": 2025,
                "week": 4,
                "team_pass_attempts": 5,
                "team_rush_attempts": 1,
            },
            {
                "receiver_id": "t1",
                "position": "TE",
                "season": 2025,
                "week": 2,
                "team_pass_attempts": 20,
                "team_rush_attempts": 10,
            },
        ]
    )
    result = project_receiver_components(_history(), targets, as_of=(2025, 3))
    assert list(result.columns) == [
        "receiver_id",
        "position",
        "season",
        "week",
        *RECEIVER_COMPONENTS,
        "availability_probability",
        "as_of_season",
        "as_of_week",
    ]
    assert set(result["position"]) == {"WR", "TE"}
    assert result.loc[0, "routes"] <= 5
    assert result.loc[0, "targets"] <= result.loc[0, "routes"]
    assert result.loc[0, "receptions"] <= result.loc[0, "targets"]
    assert (result[list(RECEIVER_COMPONENTS)] >= 0).all().all()
    assert tuple(result.loc[0, ["as_of_season", "as_of_week"]]) == (2025, 3)


def test_future_rows_and_same_week_rows_are_excluded():
    targets = pd.DataFrame(
        [{"receiver_id": "w1", "position": "WR", "season": 2025, "week": 3}]
    )
    result = project_receiver_components(_history(), targets, as_of=(2025, 2))
    assert result.loc[0, "targets"] == pytest.approx(31 * ((8 / 30) + (9 / 32)) / 2)


def test_new_receiver_uses_bounded_priors_and_availability():
    targets = pd.DataFrame(
        [
            {
                "receiver_id": "new",
                "position": "TE",
                "season": 2025,
                "week": 4,
                "team_pass_attempts": 2,
            }
        ]
    )
    result = project_receiver_components(
        _history(),
        targets,
        config=ReceiverComponentConfig(
            fallback_routes=40, fallback_target_rate=0.5, availability_probability=0.5
        ),
    )
    assert result.loc[0, "routes"] == 1
    assert result.loc[0, "targets"] <= 1
    assert result.loc[0, "availability_probability"] == 0.5


def test_invalid_inputs_and_semantic_duplicates_fail_closed():
    with pytest.raises(ValueError):
        ReceiverComponentConfig(fallback_target_rate=np.nan)
    with pytest.raises(ValueError):
        project_receiver_components(
            _history(),
            pd.DataFrame(
                [{"receiver_id": "w1", "position": "RB", "season": 2025, "week": 4}]
            ),
        )
    duplicate = pd.concat([_history(), _history().iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate"):
        project_receiver_components(duplicate)


def test_malformed_required_stats_and_supplied_caps_fail_closed():
    bad_stats = _history().copy()
    bad_stats.loc[0, "targets"] = "bad"
    with pytest.raises(ValueError, match="targets"):
        project_receiver_components(bad_stats)
    bad_caps = pd.DataFrame(
        [
            {
                "receiver_id": "new",
                "position": "WR",
                "season": 2025,
                "week": 4,
                "team_targets": "bad",
            }
        ]
    )
    with pytest.raises(ValueError, match="team_targets"):
        project_receiver_components(_history(), bad_caps)


def test_touchdown_components_are_capped_by_opportunities():
    history = _history().copy()
    history.loc[0, "receiving_tds"] = 99
    with pytest.raises(ValueError, match="receiving_tds"):
        project_receiver_components(history)
    history = _history().copy()
    history.loc[0, "rare_rush_tds"] = 99
    with pytest.raises(ValueError, match="rare_rush_tds"):
        project_receiver_components(history)
    projected = project_receiver_components(
        _history(),
        pd.DataFrame(
            [{"receiver_id": "w1", "position": "WR", "season": 2025, "week": 4}]
        ),
        config=ReceiverComponentConfig(
            fallback_td_rate=1.0, fallback_rare_rush_td_rate=1.0
        ),
    )
    assert projected.loc[0, "receiving_tds"] <= projected.loc[0, "receptions"]
    assert projected.loc[0, "rare_rush_tds"] <= projected.loc[0, "rare_rush_attempts"]


def test_rolling_origin_excludes_target_fold_opportunity_fields_and_promotes_on_tie():
    frame = _history().query("receiver_id == 'w1'").copy()
    frame = pd.concat(
        [frame, frame.iloc[[0]].assign(week=4, targets=8, team_pass_attempts=999)],
        ignore_index=True,
    )
    first = rolling_origin_compare(
        frame, config=ReceiverComponentConfig(min_history=3, window=3)
    )
    mutated = frame.assign(
        team_pass_attempts=np.where(frame.week == 4, 0, frame.team_pass_attempts)
    )
    second = rolling_origin_compare(
        mutated, config=ReceiverComponentConfig(min_history=3, window=3)
    )
    assert first == second
    assert first["folds"] >= 1
    assert first["promoted"] is True


def test_rolling_origin_can_decline_model():
    frame = _history().query("receiver_id == 'w1'").copy()
    frame.loc[:, "targets"] = [1, 30, 1]
    frame.loc[:, "routes"] = [10, 100, 10]
    frame = pd.concat(
        [frame, frame.iloc[[0]].assign(week=4, targets=30, routes=10)],
        ignore_index=True,
    )
    result = rolling_origin_compare(
        frame, config=ReceiverComponentConfig(min_history=3, window=3)
    )
    assert result["promoted"] is False
