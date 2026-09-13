"""Focused contract tests for the archived component adapter."""

import pandas as pd
import pytest
from src.nfl.models.archived_component_adapter import (
    adapt_archived_weekly_components,
)


def _rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "player_id": "qb-1", "position": "QB", "season": 2023, "week": 2,
                "pass_attempts": 30, "completions": 20, "passing_yards": 250,
                "passing_tds": 2, "interceptions": 1, "rushing_attempts": 3,
                "rushing_yards": -4, "rushing_tds": 0,
            },
            {
                "player_id": "rb-1", "position": "RB", "season": 2023, "week": 2,
                "rushing_attempts": 15, "targets": 4, "rushing_yards": -2,
                "receptions": 3, "receiving_yards": 21, "rushing_tds": 1,
                "receiving_tds": 0,
            },
            {
                "player_id": "wr-1", "position": "WR", "season": 2023, "week": 2,
                "routes": 32, "targets": 8, "receptions": 6, "receiving_yards": -3,
                "receiving_tds": 1, "rushing_attempts": 1, "rushing_yards": -5,
                "rushing_tds": 0,
            },
            {
                "player_id": "te-1", "position": "TE", "season": 2023, "week": 2,
                "routes": 20, "targets": 5, "receptions": 4, "receiving_yards": 40,
                "receiving_tds": 0, "rushing_attempts": 0, "rushing_yards": 0,
                "rushing_tds": 0,
            },
        ]
    )


def test_each_position_maps_to_projector_schema_and_preserves_signed_yards() -> None:
    adapted = adapt_archived_weekly_components(_rows())
    assert list(adapted.qb.columns) == [
        "qb_id",
        "season",
        "week",
        "pass_attempts",
        "completions",
        "passing_yards",
        "passing_tds",
        "interceptions",
        "rushing_attempts",
        "rushing_yards",
        "rushing_tds",
    ]
    assert list(adapted.rb.columns)[0] == "rb_id"
    assert list(adapted.receivers.columns)[0:4] == [
        "receiver_id",
        "position",
        "season",
        "week",
    ]
    assert adapted.qb.loc[0, "rushing_yards"] == -4
    assert adapted.receivers.loc[0, "receiving_yards"] == -3
    assert set(adapted.receivers["position"]) == {"WR", "TE"}


def test_adapter_returns_history_only_and_target_week_stats_are_not_injected() -> None:
    source = _rows()
    adapted = adapt_archived_weekly_components(source)
    assert "fantasy_points" not in adapted.qb
    assert "target_week" not in adapted.qb
    target = pd.DataFrame([{"qb_id": "qb-1", "season": 2023, "week": 3}])
    assert set(target.columns) == {"qb_id", "season", "week"}
    assert set(adapted.qb["week"]) == {2}


@pytest.mark.parametrize(
    "mutator, message",
    [
        (lambda frame: frame.drop(columns="routes"), "routes"),
        (lambda frame: frame.assign(position="K"), "unsupported"),
        (
            lambda frame: pd.concat([frame, frame.iloc[[0]]], ignore_index=True),
            "duplicate",
        ),
        (lambda frame: frame.assign(rushing_attempts=-1), "nonnegative"),
    ],
)
def test_supported_rows_fail_closed(mutator, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        adapt_archived_weekly_components(mutator(_rows()))
