"""Registry of candidate estimators for MLB strikeout modeling."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import ElasticNet, PoissonRegressor
from xgboost import XGBRegressor

DEFAULT_RANDOM_SEED = 42


@dataclass(frozen=True, slots=True)
class ModelSpec:
    """Definition for a candidate model used in tournament training.

    Attributes:
        name: Stable identifier used in config and reporting.
        factory: Callable that returns the estimator instance.
        default_params: Estimator hyperparameters merged with runtime overrides.
        preprocess_linear: Whether to apply impute+scale preprocessing.
    """

    name: str
    factory: Callable[..., Any]
    default_params: Mapping[str, Any]
    preprocess_linear: bool = False


DEFAULT_CANDIDATES: list[str] = [
    "xgboost",
    "random_forest",
    "hist_gradient_boosting",
    "elastic_net",
    "poisson",
]

# Fixed tie-break preference from simplest to most complex.
SIMPLE_MODEL_PREFERENCE: list[str] = [
    "poisson",
    "elastic_net",
    "hist_gradient_boosting",
    "random_forest",
    "xgboost",
]

_MODEL_REGISTRY: dict[str, ModelSpec] = {
    "xgboost": ModelSpec(
        name="xgboost",
        factory=XGBRegressor,
        default_params={
            "learning_rate": 0.1,
            "max_depth": 3,
            "n_estimators": 300,
            "subsample": 0.9,
            "colsample_bytree": 0.9,
            "reg_lambda": 1.0,
            "random_state": DEFAULT_RANDOM_SEED,
        },
    ),
    "random_forest": ModelSpec(
        name="random_forest",
        factory=RandomForestRegressor,
        default_params={
            "n_estimators": 400,
            "max_depth": 10,
            "min_samples_leaf": 2,
            "random_state": DEFAULT_RANDOM_SEED,
            "n_jobs": -1,
        },
    ),
    "hist_gradient_boosting": ModelSpec(
        name="hist_gradient_boosting",
        factory=HistGradientBoostingRegressor,
        default_params={
            "max_depth": 6,
            "learning_rate": 0.05,
            "max_iter": 400,
            "random_state": DEFAULT_RANDOM_SEED,
        },
    ),
    "elastic_net": ModelSpec(
        name="elastic_net",
        factory=ElasticNet,
        default_params={
            "alpha": 0.05,
            "l1_ratio": 0.4,
            "max_iter": 20_000,
            "random_state": DEFAULT_RANDOM_SEED,
        },
        preprocess_linear=True,
    ),
    "poisson": ModelSpec(
        name="poisson",
        factory=PoissonRegressor,
        default_params={
            "alpha": 0.1,
            "max_iter": 1_000,
        },
        preprocess_linear=True,
    ),
}


def get_model_spec(name: str) -> ModelSpec:
    """Return model spec by registered name.

    Args:
        name: Candidate name.

    Returns:
        Matching ``ModelSpec``.

    Raises:
        KeyError: If the model name is unknown.
    """

    return _MODEL_REGISTRY[name]


def resolve_model_specs(candidates: Sequence[str] | None = None) -> list[ModelSpec]:
    """Resolve candidate model specs from config names.

    Args:
        candidates: Candidate names; if omitted defaults are used.

    Returns:
        Ordered list of model specs.
    """

    names = list(candidates) if candidates else list(DEFAULT_CANDIDATES)
    specs: list[ModelSpec] = []
    for name in names:
        if name not in _MODEL_REGISTRY:
            raise ValueError(f"Unknown model candidate: {name}")
        specs.append(_MODEL_REGISTRY[name])
    return specs


def available_candidates() -> list[str]:
    """Return known candidate identifiers."""

    return sorted(_MODEL_REGISTRY.keys())
