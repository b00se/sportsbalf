from __future__ import annotations

import pandas as pd
import pytest
from src.core.model_selection import SelectionPolicy, apply_metric_filters


def _leaderboard() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"model": "a", "mean_mae": 1.0, "mean_rmse": 2.0, "mean_r2": 0.50},
            {"model": "b", "mean_mae": 1.0, "mean_rmse": 1.9, "mean_r2": 0.45},
            {"model": "c", "mean_mae": 1.2, "mean_rmse": 1.7, "mean_r2": 0.60},
        ]
    )


def test_apply_metric_filters_rejects_empty_leaderboard() -> None:
    with pytest.raises(ValueError, match="empty leaderboard"):
        apply_metric_filters(pd.DataFrame(), policy=SelectionPolicy())


def test_apply_metric_filters_rejects_unknown_metric() -> None:
    board = _leaderboard()
    policy = SelectionPolicy(primary_metric="mape", tie_breakers=("rmse",))

    with pytest.raises(ValueError, match="Unsupported metric"):
        apply_metric_filters(board, policy=policy)


def test_apply_metric_filters_applies_minimize_then_tie_break() -> None:
    board = _leaderboard()
    policy = SelectionPolicy(primary_metric="mae", tie_breakers=("rmse",), epsilon=0.0)

    candidates = apply_metric_filters(board, policy=policy)

    assert list(candidates["model"]) == ["b"]


def test_apply_metric_filters_uses_maximize_for_r2() -> None:
    board = pd.DataFrame(
        [
            {"model": "a", "mean_mae": 1.0, "mean_rmse": 2.0, "mean_r2": 0.55},
            {"model": "b", "mean_mae": 1.0, "mean_rmse": 2.0, "mean_r2": 0.60},
            {"model": "c", "mean_mae": 1.0, "mean_rmse": 2.0, "mean_r2": 0.58},
        ]
    )

    candidates = apply_metric_filters(
        board,
        policy=SelectionPolicy(primary_metric="r2", tie_breakers=("mae",), epsilon=0.0),
    )

    assert list(candidates["model"]) == ["b"]


def test_apply_metric_filters_honors_epsilon_boundary() -> None:
    board = pd.DataFrame(
        [
            {"model": "tight", "mean_mae": 1.0, "mean_rmse": 2.0, "mean_r2": 0.40},
            {"model": "near", "mean_mae": 1.0000005, "mean_rmse": 1.8, "mean_r2": 0.41},
            {"model": "far", "mean_mae": 1.01, "mean_rmse": 1.7, "mean_r2": 0.42},
        ]
    )

    wide = apply_metric_filters(
        board,
        policy=SelectionPolicy(
            primary_metric="mae", tie_breakers=("rmse",), epsilon=1e-3
        ),
    )
    narrow = apply_metric_filters(
        board,
        policy=SelectionPolicy(
            primary_metric="mae", tie_breakers=("rmse",), epsilon=1e-7
        ),
    )

    assert set(wide["model"]) == {"near"}
    assert set(narrow["model"]) == {"tight"}
