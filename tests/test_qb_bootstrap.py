import numpy as np
import pandas as pd
import pytest
from src.nfl.models.bootstrap import QBResidualBootstrapper


def test_qb_bootstrapper_can_bootstrap_threshold():
    games = pd.DataFrame(
        {
            "qb_id": ["QB-A"] * 6 + ["QB-B"] * 3,
            "pass_attempts": [30, 31, 29, 35, 28, 32, 25, 27, 26],
            "prediction": [29, 30, 28, 33, 30, 31, 24, 26, 27],
        }
    )

    bootstrapper = QBResidualBootstrapper.from_games(
        games,
        prediction_col="prediction",
        min_history=5,
    )

    assert bootstrapper.can_bootstrap(entity_id="QB-A") is True
    assert bootstrapper.can_bootstrap(entity_id="QB-B") is False
    assert bootstrapper.can_bootstrap(entity_id="missing-qb") is False
    assert bootstrapper.can_bootstrap(entity_id=None) is False


def test_qb_bootstrapper_uses_neutral_entity_id():
    games = pd.DataFrame(
        {
            "qb_id": ["QB-A"] * 6,
            "pass_attempts": [30, 32, 31, 29, 35, 28],
            "prediction": [29, 31, 30, 28, 33, 27],
        }
    )

    bootstrapper = QBResidualBootstrapper.from_games(
        games,
        prediction_col="prediction",
        min_history=5,
    )

    assert bootstrapper.can_bootstrap(entity_id="QB-A") is True
    samples = bootstrapper.sample_counts(
        mean=30.0,
        entity_id="QB-A",
        simulations=100,
        rng=np.random.default_rng(0),
    )
    assert samples.size == 100


def test_qb_bootstrapper_rejects_legacy_alias_kwargs():
    games = pd.DataFrame(
        {
            "qb_id": ["QB-A"] * 6,
            "pass_attempts": [30, 32, 31, 29, 35, 28],
            "prediction": [29, 31, 30, 28, 33, 27],
        }
    )
    bootstrapper = QBResidualBootstrapper.from_games(
        games,
        prediction_col="prediction",
        min_history=5,
    )

    with pytest.raises(TypeError):
        bootstrapper.can_bootstrap(pitcher_id="QB-A")  # type: ignore[call-arg]
