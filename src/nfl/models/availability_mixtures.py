"""Availability-weighted component distributions for NFL projections.

The module deliberately keeps play availability separate from conditional
production.  A confirmed inactive is a point mass at the inactive outcome;
an uncertain player is a Bernoulli mixture of inactive and active outcomes.
"""

from __future__ import annotations

import operator
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd

_STATUSES: Final[frozenset[str]] = frozenset(
    {"active", "available", "questionable", "doubtful", "limited", "inactive", "out"}
)


@dataclass(frozen=True, slots=True)
class AvailabilityMixtureConfig:
    """Validated controls for quantiles and deterministic mixture sampling."""

    quantiles: tuple[float, ...] = (0.05, 0.5, 0.95)
    sample_size: int = 0
    seed: int | None = None

    def __post_init__(self) -> None:
        if any(isinstance(q, (bool, np.bool_)) for q in self.quantiles):
            raise ValueError("quantiles must be finite values in [0, 1]")
        try:
            values = tuple(float(q) for q in self.quantiles)
        except (TypeError, ValueError):
            raise ValueError("quantiles must be finite values in [0, 1]") from None
        if any(not np.isfinite(q) or not 0 <= q <= 1 for q in values):
            raise ValueError("quantiles must be finite values in [0, 1]")
        object.__setattr__(self, "quantiles", values)
        if isinstance(self.sample_size, (bool, np.bool_)):
            raise ValueError("sample_size must be a nonnegative integer")
        try:
            count = int(self.sample_size)
        except (TypeError, ValueError):
            raise ValueError("sample_size must be a nonnegative integer") from None
        if count < 0 or float(self.sample_size) != count:
            raise ValueError("sample_size must be a nonnegative integer")
        object.__setattr__(self, "sample_size", count)
        if self.seed is not None:
            seed = _integer_seed(self.seed)
            object.__setattr__(self, "seed", seed)


def _finite_values(values: float | Sequence[float], name: str) -> np.ndarray:
    if isinstance(values, (str, bytes)):
        raise ValueError(f"{name} must be numeric")
    if isinstance(values, (bool, np.bool_)):
        raise ValueError(f"{name} must contain numeric values")
    try:
        raw_values = np.asarray(values).reshape(-1)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must contain finite numeric values") from None
    if any(isinstance(value, (bool, np.bool_)) for value in raw_values):
        raise ValueError(f"{name} must contain numeric values")
    try:
        array = np.asarray(values, dtype=float).reshape(-1)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must contain finite numeric values") from None
    if array.size == 0 or not np.isfinite(array).all():
        raise ValueError(f"{name} must contain finite numeric values")
    if (array < 0).any():
        raise ValueError(f"{name} must be nonnegative")
    return array


def _probability(value: object) -> float:
    if isinstance(value, (bool, np.bool_)):
        raise ValueError("availability_probability must be finite and in [0, 1]")
    try:
        result = float(value)
    except (TypeError, ValueError):
        raise ValueError(
            "availability_probability must be finite and in [0, 1]"
        ) from None
    if not np.isfinite(result) or not 0 <= result <= 1:
        raise ValueError("availability_probability must be finite and in [0, 1]")
    return result


def _integer_seed(value: object) -> int:
    if isinstance(value, (bool, np.bool_)):
        raise ValueError("seed must be an integer or None")
    try:
        result = operator.index(value)
    except TypeError:
        raise ValueError("seed must be an integer or None") from None
    if result < 0:
        raise ValueError("seed must be a nonnegative integer or None")
    return result


@dataclass(frozen=True, slots=True)
class AvailabilityMixtureResult:
    """Moments and optional deterministic samples for one component mixture."""

    availability_probability: float
    active_mean: float
    inactive_mean: float
    active_variance: float
    inactive_variance: float
    mixture_mean: float
    mixture_variance: float
    quantiles: Mapping[float, float]
    samples: tuple[float, ...]
    seed: int | None
    status: str
    uncertainty: str
    calibrated: bool | None = None
    promoted: bool = False

    @property
    def standard_deviation(self) -> float:
        """Return total mixture standard deviation."""
        return float(np.sqrt(self.mixture_variance))


def _weighted_quantile(
    active: np.ndarray, inactive: np.ndarray, probability: float, q: float
) -> float:
    values = np.concatenate((inactive, active))
    weights = np.concatenate(
        ((1.0 - probability) / len(inactive) * np.ones(len(inactive)),
         probability / len(active) * np.ones(len(active)))
    )
    positive = weights > 0
    values, weights = values[positive], weights[positive]
    if q == 0:
        return float(values.min())
    if q == 1:
        return float(values.max())
    order = np.argsort(values, kind="mergesort")
    ordered_values, cumulative = values[order], np.cumsum(weights[order])
    cumulative[-1] = 1.0
    index = min(np.searchsorted(cumulative, q, side="left"), len(ordered_values) - 1)
    return float(ordered_values[index])


def build_availability_mixture(
    active_values: float | Sequence[float],
    *,
    availability_probability: float,
    inactive_values: float | Sequence[float] = 0.0,
    status: str | None = None,
    quantiles: Sequence[float] = (0.05, 0.5, 0.95),
    sample_size: int = 0,
    seed: int | None = None,
    config: AvailabilityMixtureConfig | None = None,
) -> AvailabilityMixtureResult:
    """Build a nonnegative empirical active/inactive availability mixture.

    ``active_values`` and ``inactive_values`` are conditional outcomes, not
    already availability-weighted values.  Confirmed inactive/out statuses
    force probability zero and therefore zero selection eligibility.
    """
    if config is not None and not isinstance(config, AvailabilityMixtureConfig):
        raise TypeError("config must be AvailabilityMixtureConfig")
    if config is not None:
        quantiles, sample_size, seed = config.quantiles, config.sample_size, config.seed
    active = _finite_values(active_values, "active_values")
    inactive = _finite_values(inactive_values, "inactive_values")
    probability = _probability(availability_probability)
    normalized_status = "uncertain" if status is None else str(status).lower().strip()
    if normalized_status not in _STATUSES and normalized_status != "uncertain":
        raise ValueError("unknown availability status")
    if normalized_status in {"inactive", "out"}:
        probability = 0.0
        inactive = np.zeros(1, dtype=float)
        normalized_status = "inactive"
    elif normalized_status in {"active", "available"}:
        probability = 1.0
    if any(isinstance(q, (bool, np.bool_)) for q in quantiles):
        raise ValueError("quantiles must be finite values in [0, 1]")
    try:
        q_values = tuple(float(q) for q in quantiles)
    except (TypeError, ValueError):
        raise ValueError("quantiles must be finite values in [0, 1]") from None
    if any(not np.isfinite(q) or not 0 <= q <= 1 for q in q_values):
        raise ValueError("quantiles must be finite values in [0, 1]")
    if isinstance(sample_size, (bool, np.bool_)):
        raise ValueError("sample_size must be a nonnegative integer")
    try:
        count = int(sample_size)
    except (TypeError, ValueError):
        raise ValueError("sample_size must be a nonnegative integer") from None
    if count < 0 or float(sample_size) != count:
        raise ValueError("sample_size must be a nonnegative integer")
    if seed is not None:
        seed = _integer_seed(seed)
    active_mean, inactive_mean = float(active.mean()), float(inactive.mean())
    active_var, inactive_var = float(active.var()), float(inactive.var())
    mean = probability * active_mean + (1 - probability) * inactive_mean
    try:
        variance = probability * (active_var + (active_mean - mean) ** 2) + (
            1 - probability
        ) * (inactive_var + (inactive_mean - mean) ** 2)
    except OverflowError:
        raise ValueError("mixture inputs produce a non-finite result") from None
    if not np.isfinite(mean) or not np.isfinite(variance) or variance < 0:
        raise ValueError("mixture inputs produce a non-finite result")
    result_quantiles = {
        q: _weighted_quantile(active, inactive, probability, q) for q in q_values
    }
    samples: tuple[float, ...] = ()
    if count:
        rng = np.random.default_rng(seed)
        available = rng.random(count) < probability
        active_indices = rng.integers(0, len(active), size=count)
        inactive_indices = rng.integers(0, len(inactive), size=count)
        draws = np.where(available, active[active_indices], inactive[inactive_indices])
        samples = tuple(float(x) for x in draws)
    return AvailabilityMixtureResult(
        availability_probability=probability,
        active_mean=active_mean,
        inactive_mean=inactive_mean,
        active_variance=active_var,
        inactive_variance=inactive_var,
        mixture_mean=float(mean),
        mixture_variance=float(max(0.0, variance)),
        quantiles=result_quantiles,
        samples=samples,
        seed=seed,
        status=normalized_status,
        uncertainty="availability_risk_plus_conditional_production_volatility",
    )


def evaluate_availability_calibration(
    history: pd.DataFrame,
    *,
    probability_column: str = "availability_probability",
    outcome_column: str = "available",
    cutoff: object | None = None,
    time_column: str = "as_of",
    bins: int = 10,
) -> dict[str, float | int | bool | None]:
    """Evaluate supplied availability forecasts without using future rows.

    Rows are scored only when an outcome is present.  When ``cutoff`` and a
    time column are supplied, rows on/after the cutoff are excluded.
    """
    if not isinstance(history, pd.DataFrame):
        raise TypeError("history must be a pandas DataFrame")
    required = {probability_column, outcome_column}
    if not required.issubset(history.columns):
        raise ValueError("history must contain probability and outcome columns")
    if isinstance(bins, (bool, np.bool_)) or int(bins) != bins or bins < 2:
        raise ValueError("bins must be an integer >= 2")
    frame = history.copy()
    if cutoff is not None:
        if time_column not in frame:
            raise ValueError("time_column is required when cutoff is supplied")
        times = pd.to_datetime(frame[time_column], utc=True, errors="coerce")
        boundary = pd.to_datetime(cutoff, utc=True, errors="coerce")
        if pd.isna(boundary) or times.isna().any():
            raise ValueError("cutoff and time values must be valid timestamps")
        frame = frame.loc[times < boundary]
    raw_probabilities = frame[probability_column]
    if raw_probabilities.map(
        lambda value: isinstance(value, (bool, np.bool_))
    ).any():
        raise ValueError("availability probabilities must be numeric")
    raw_outcomes = frame[outcome_column]
    if raw_outcomes.map(lambda value: isinstance(value, (bool, np.bool_))).any():
        raise ValueError("availability outcomes must be numeric binary values")
    probabilities = pd.to_numeric(frame[probability_column], errors="coerce")
    missing = raw_outcomes.map(pd.isna)
    parsed_outcomes = pd.to_numeric(raw_outcomes.loc[~missing], errors="coerce")
    if parsed_outcomes.isna().any():
        raise ValueError("availability outcomes must be binary")
    outcomes = pd.Series(np.nan, index=frame.index, dtype=float)
    outcomes.loc[~missing] = parsed_outcomes
    if probabilities.isna().any() or (~probabilities.between(0, 1)).any():
        raise ValueError("availability probabilities must be finite and in [0, 1]")
    known = outcomes.notna()
    if (~outcomes.loc[known].isin([0, 1])).any():
        raise ValueError("availability outcomes must be binary")
    probabilities = probabilities.loc[known]
    outcomes = outcomes.loc[known]
    n = len(outcomes)
    if not n:
        return {
            "rows": 0,
            "brier": float("nan"),
            "ece": float("nan"),
            "calibrated": None,
            "promoted": False,
        }
    brier = float(np.mean((probabilities.to_numpy() - outcomes.to_numpy()) ** 2))
    edges = np.linspace(0, 1, int(bins) + 1)
    ece = 0.0
    for left, right in zip(edges[:-1], edges[1:]):
        upper = probabilities <= right if right == 1 else probabilities < right
        mask = (probabilities >= left) & upper
        if mask.any():
            gap = abs(
                float(probabilities[mask].mean()) - float(outcomes[mask].mean())
            )
            ece += float(mask.mean()) * gap
    calibrated = bool(ece <= 0.10)
    return {
        "rows": n,
        "brier": brier,
        "ece": float(ece),
        "calibrated": calibrated,
        "promoted": False,
    }


__all__ = [
    "AvailabilityMixtureConfig",
    "AvailabilityMixtureResult",
    "build_availability_mixture",
    "evaluate_availability_calibration",
]
