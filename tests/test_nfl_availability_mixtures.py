"""Tests for the R3.6 availability mixture contract."""

import numpy as np
import pandas as pd
import pytest
from src.nfl.models.availability_mixtures import (
    AvailabilityMixtureConfig,
    build_availability_mixture,
    evaluate_availability_calibration,
)


def test_probability_weighted_moments_and_quantiles() -> None:
    result = build_availability_mixture([10.0], availability_probability=0.25)
    assert result.mixture_mean == pytest.approx(2.5)
    assert result.mixture_variance == pytest.approx(18.75)
    assert result.quantiles[0.5] == 0.0
    assert result.uncertainty.startswith("availability_risk")


@pytest.mark.parametrize("probability, expected", [(0.0, 0.0), (1.0, 10.0)])
def test_probability_boundaries(probability: float, expected: float) -> None:
    result = build_availability_mixture([10.0], availability_probability=probability)
    assert result.mixture_mean == expected
    assert result.mixture_variance == 0.0


def test_quantiles_handle_zero_weight_support_and_endpoints() -> None:
    active = build_availability_mixture(
        [10.0], availability_probability=1.0, quantiles=(0.0, 1.0)
    )
    inactive = build_availability_mixture(
        [10.0], availability_probability=0.0, inactive_values=[2.0, 3.0],
        quantiles=(0.0, 1.0)
    )
    assert active.quantiles == {0.0: 10.0, 1.0: 10.0}
    assert inactive.quantiles == {0.0: 2.0, 1.0: 3.0}


def test_quantile_endpoints_handle_subnormal_availability_probability() -> None:
    result = build_availability_mixture(
        [10.0],
        availability_probability=np.nextafter(0.0, 1.0),
        inactive_values=[2.0, 3.0],
        quantiles=(0.0, 1.0),
    )
    assert result.quantiles == {0.0: 2.0, 1.0: 10.0}


def test_confirmed_inactive_is_zero_and_overrides_probability() -> None:
    result = build_availability_mixture(
        [20.0], availability_probability=1.0, status="inactive"
    )
    assert result.availability_probability == 0.0
    assert result.mixture_mean == 0.0
    assert result.quantiles[0.95] == 0.0


def test_sampling_surfaces_seed_and_is_reproducible() -> None:
    first = build_availability_mixture(
        [2.0, 4.0], availability_probability=0.5, sample_size=50, seed=7
    )
    second = build_availability_mixture(
        [2.0, 4.0], availability_probability=0.5, sample_size=50, seed=7
    )
    assert first.seed == 7
    assert first.samples == second.samples
    assert set(first.samples) <= {0.0, 2.0, 4.0}


def test_config_controls_sampling_and_rejects_invalid_values() -> None:
    result = build_availability_mixture(
        [3.0],
        availability_probability=0.5,
        config=AvailabilityMixtureConfig(
            sample_size=4, seed=11, quantiles=(0.5,)
        ),
    )
    assert len(result.samples) == 4
    assert result.seed == 11
    with pytest.raises(ValueError):
        AvailabilityMixtureConfig(sample_size=-1)
    with pytest.raises(ValueError):
        AvailabilityMixtureConfig(quantiles=(np.nan,))


@pytest.mark.parametrize("kwargs", [
    {"availability_probability": np.nan},
    {"availability_probability": True},
    {"availability_probability": 1.1},
    {"active_values": [-1.0]},
    {"active_values": [np.inf]},
    {"quantiles": [1.2]},
    {"sample_size": -1},
])
def test_invalid_inputs_fail_closed(kwargs: dict[str, object]) -> None:
    args = {"active_values": [1.0], "availability_probability": 0.5}
    args.update(kwargs)
    with pytest.raises((ValueError, TypeError)):
        build_availability_mixture(**args)


def test_calibration_cutoff_is_strict_and_never_promotes() -> None:
    frame = pd.DataFrame({
        "as_of": ["2026-09-01T00:00:00Z", "2026-09-03T00:00:00Z"],
        "availability_probability": [0.9, 0.0],
        "available": [1, np.nan],
    })
    report = evaluate_availability_calibration(frame, cutoff="2026-09-03T00:00:00Z")
    assert report["rows"] == 1
    assert report["promoted"] is False


def test_calibration_skips_missing_outcomes_and_rejects_boolean_observations() -> None:
    frame = pd.DataFrame(
        {"availability_probability": [1.0, 0.0], "available": [1, np.nan]}
    )
    assert evaluate_availability_calibration(frame)["rows"] == 1
    with pytest.raises(ValueError):
        evaluate_availability_calibration(
            pd.DataFrame({"availability_probability": [1.0], "available": [True]})
        )
    with pytest.raises(ValueError):
        evaluate_availability_calibration(
            pd.DataFrame({"availability_probability": [1.0], "available": ["bogus"]})
        )
    with pytest.raises(ValueError):
        evaluate_availability_calibration(
            pd.DataFrame({"availability_probability": [True], "available": [1]})
        )


@pytest.mark.parametrize("seed", [1.5, True, np.nan, np.inf, -1])
def test_nonintegral_or_nonfinite_seed_fails_closed(seed: object) -> None:
    with pytest.raises(ValueError):
        build_availability_mixture([1.0], availability_probability=0.5, seed=seed)


def test_large_integral_seed_is_not_float_truncated() -> None:
    seed = 2**53 + 1
    result = build_availability_mixture(
        [1.0], availability_probability=0.5, seed=seed, sample_size=0
    )
    assert result.seed == seed


def test_boolean_component_observations_fail_closed() -> None:
    with pytest.raises(ValueError):
        build_availability_mixture([True], availability_probability=0.5)


def test_overflowing_finite_component_mixture_fails_closed() -> None:
    with pytest.raises(ValueError):
        build_availability_mixture(
            [np.finfo(float).max], availability_probability=0.5,
            inactive_values=[0.0]
        )
