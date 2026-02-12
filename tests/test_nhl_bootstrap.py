from __future__ import annotations

import numpy as np
import pandas as pd
from src.nhl.models.bootstrap import SOGResidualBootstrapper


def _residual_games() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "player_id": ["p1"] * 6 + ["p2"] * 2,
            "shots_on_goal": [3, 4, 2, 3, 5, 3, 1, 2],
            "prediction": [2.5, 3.0, 2.0, 2.7, 4.2, 2.8, 1.1, 1.9],
        }
    )


def test_player_pool_selection_and_history_threshold() -> None:
    bootstrapper = SOGResidualBootstrapper.from_games(
        _residual_games(),
        prediction_col="prediction",
        min_history=5,
        mix_global_prob=0.0,
        min_sigma=0.0,
    )

    assert bootstrapper.can_bootstrap(entity_id="p1") is True
    assert bootstrapper.can_bootstrap(entity_id="p2") is False


def test_global_fallback_for_missing_or_sparse_player_history() -> None:
    bootstrapper = SOGResidualBootstrapper.from_games(
        _residual_games(),
        prediction_col="prediction",
        min_history=5,
        mix_global_prob=0.0,
        min_sigma=0.0,
    )

    rng = np.random.default_rng(9)
    samples_sparse = bootstrapper.sample_counts(
        mean=2.8,
        entity_id="p2",
        simulations=50,
        rng=rng,
    )
    assert samples_sparse.size == 50
    assert np.isfinite(samples_sparse).all()


def test_seeded_sampling_is_deterministic() -> None:
    bootstrapper = SOGResidualBootstrapper.from_games(
        _residual_games(),
        prediction_col="prediction",
        min_history=5,
        mix_global_prob=0.25,
        min_sigma=0.1,
    )

    first = bootstrapper.sample_counts(
        mean=3.0,
        entity_id="p1",
        simulations=100,
        rng=np.random.default_rng(123),
    )
    second = bootstrapper.sample_counts(
        mean=3.0,
        entity_id="p1",
        simulations=100,
        rng=np.random.default_rng(123),
    )

    assert np.array_equal(first, second)
