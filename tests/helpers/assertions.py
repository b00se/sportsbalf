"""Shared invariant-first assertions for pipeline and adapter tests."""

from __future__ import annotations

import math
from collections.abc import Sequence

import numpy as np
import pandas as pd


def assert_probability_columns_valid(
    frame: pd.DataFrame,
    *,
    over_col: str = "prob_over",
    under_col: str = "prob_under",
    push_col: str = "prob_push",
    tol: float = 1e-6,
) -> None:
    """Assert probability columns are finite, bounded, and sum to one.

    Args:
        frame: Frame containing probability columns.
        over_col: Over probability column name.
        under_col: Under probability column name.
        push_col: Push probability column name.
        tol: Sum-to-one absolute tolerance.
    """

    required = [over_col, under_col]
    if push_col in frame.columns:
        required.append(push_col)
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise AssertionError(f"Missing probability columns: {missing}")

    for column in required:
        series = pd.to_numeric(frame[column], errors="coerce")
        assert series.notna().all(), f"{column} contains NaN values."
        assert (series >= -tol).all(), f"{column} includes values below 0."
        assert (series <= 1.0 + tol).all(), f"{column} includes values above 1."

    if push_col in frame.columns:
        summed = frame[[over_col, under_col, push_col]].sum(axis=1)
    else:
        summed = frame[[over_col, under_col]].sum(axis=1)
    assert np.allclose(
        summed.to_numpy(dtype=float),
        np.ones(len(summed), dtype=float),
        atol=tol,
        rtol=0.0,
    ), "Probabilities must sum to 1 within tolerance."


def assert_simulation_contract(
    frame: pd.DataFrame,
    *,
    required_columns: Sequence[str] | None = None,
) -> None:
    """Assert core simulation output contract columns and numeric validity.

    Args:
        frame: Simulation result frame.
        required_columns: Optional extra required columns.
    """

    core_required = {
        "prob_over",
        "prob_under",
        "prob_push",
        "ev_over",
        "ev_under",
        "edge_over",
        "edge_under",
    }
    if required_columns is not None:
        core_required = core_required.union(set(required_columns))

    missing = sorted(core_required - set(frame.columns))
    assert not missing, f"Missing simulation contract columns: {missing}"

    for column in ["prob_over", "prob_under", "prob_push"]:
        values = pd.to_numeric(frame[column], errors="coerce")
        assert values.notna().all(), f"{column} contains non-numeric or NaN values."
        assert np.isfinite(
            values.to_numpy(dtype=float)
        ).all(), f"{column} contains non-finite values."

    for column in ["ev_over", "ev_under", "edge_over", "edge_under"]:
        values = pd.to_numeric(frame[column], errors="coerce")
        non_null = values[values.notna()]
        assert np.isfinite(
            non_null.to_numpy(dtype=float)
        ).all(), f"{column} contains non-finite values."

    assert_probability_columns_valid(frame)


def assert_horizon_semantics(
    frame: pd.DataFrame,
    *,
    horizon: str,
    prediction_col: str,
    per_game_baseline_col: str | None = None,
) -> None:
    """Assert high-level horizon semantics for prediction scale.

    Args:
        frame: Prediction frame.
        horizon: Horizon label (for example: ``game`` or ``season``).
        prediction_col: Prediction column under test.
        per_game_baseline_col: Optional per-game baseline column used for
            season scaling.
    """

    predictions = pd.to_numeric(frame[prediction_col], errors="coerce")
    assert predictions.notna().all(), f"{prediction_col} contains NaN values."
    assert (predictions >= 0.0).all(), f"{prediction_col} must be non-negative."

    if horizon == "season" and per_game_baseline_col is not None:
        baseline = pd.to_numeric(frame[per_game_baseline_col], errors="coerce")
        comparable = baseline.notna() & predictions.notna()
        if comparable.any():
            assert (
                predictions[comparable] >= baseline[comparable]
            ).all(), "Season horizon predictions must be at least per-game baseline."


def assert_no_temporal_leakage(
    baseline: pd.DataFrame,
    mutated: pd.DataFrame,
    *,
    key_columns: Sequence[str],
    compare_columns: Sequence[str],
) -> None:
    """Assert mutated future rows do not alter earlier outputs.

    Args:
        baseline: Baseline output frame.
        mutated: Output frame after future-row mutation.
        key_columns: Join key columns.
        compare_columns: Prediction columns that must remain unchanged.
    """

    merged = baseline.merge(
        mutated,
        on=list(key_columns),
        how="inner",
        suffixes=("_base", "_mut"),
    )
    for column in compare_columns:
        left = pd.to_numeric(merged[f"{column}_base"], errors="coerce")
        right = pd.to_numeric(merged[f"{column}_mut"], errors="coerce")
        assert (
            left.notna().all() and right.notna().all()
        ), f"{column} contains NaN in leakage comparison."
        assert np.allclose(
            left, right, atol=1e-8, rtol=0.0
        ), f"Temporal leakage detected for {column}."


def assert_ev_edge_sign_consistency(
    frame: pd.DataFrame,
    *,
    prob_col: str,
    ev_col: str,
    edge_col: str,
    decimal_price_col: str,
) -> None:
    """Assert EV and edge signs match probability-vs-implied relationship.

    Args:
        frame: Frame with price/probability fields.
        prob_col: Win probability column.
        ev_col: EV output column.
        edge_col: Edge output column.
        decimal_price_col: Decimal price column.
    """

    required = [prob_col, ev_col, edge_col, decimal_price_col]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise AssertionError(f"Missing EV/edge columns: {missing}")

    probs = pd.to_numeric(frame[prob_col], errors="coerce")
    evs = pd.to_numeric(frame[ev_col], errors="coerce")
    edges = pd.to_numeric(frame[edge_col], errors="coerce")
    prices = pd.to_numeric(frame[decimal_price_col], errors="coerce")

    valid = (
        probs.notna() & evs.notna() & edges.notna() & prices.notna() & (prices > 1.0)
    )
    for prob, ev, edge, price in zip(
        probs[valid].to_numpy(),
        evs[valid].to_numpy(),
        edges[valid].to_numpy(),
        prices[valid].to_numpy(),
        strict=True,
    ):
        implied = 1.0 / float(price)
        if math.isclose(prob, implied, abs_tol=1e-7):
            assert math.isclose(edge, 0.0, abs_tol=1e-6)
        elif prob > implied:
            assert edge > 0.0
            assert ev > -1.0
        else:
            assert edge < 0.0
