from __future__ import annotations

import numpy as np
import pandas as pd
from src.models.ensemble import (
    AUTHORITATIVE_ENTRYPOINT,
    MODULE_STATUS,
    STATUS_NOTE,
    train_ensemble,
)


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "rolling_K_avg_3": 5.0,
                "rolling_pitch_count_5": 90.0,
                "park_factor_K": 1.0,
                "opponent_k_rate": 0.22,
                "strikeouts": 6.0,
            },
            {
                "rolling_K_avg_3": 4.0,
                "rolling_pitch_count_5": 88.0,
                "park_factor_K": 0.98,
                "opponent_k_rate": 0.21,
                "strikeouts": 5.0,
            },
            {
                "rolling_K_avg_3": 6.0,
                "rolling_pitch_count_5": 92.0,
                "park_factor_K": 1.02,
                "opponent_k_rate": 0.23,
                "strikeouts": 7.0,
            },
        ]
    )


def test_ensemble_module_status_contract_is_legacy() -> None:
    assert MODULE_STATUS == "legacy_non_authoritative"
    assert AUTHORITATIVE_ENTRYPOINT == "pipeline/main.py -> src/pipeline/engine.py"
    assert "Legacy training helper" in STATUS_NOTE


def test_train_ensemble_is_deterministic_with_fixed_seed() -> None:
    frame = _frame()
    model_one = train_ensemble(frame)
    model_two = train_ensemble(frame)

    preds_one = model_one.predict(frame.drop(columns=["strikeouts"]))
    preds_two = model_two.predict(frame.drop(columns=["strikeouts"]))

    assert np.allclose(preds_one, preds_two, atol=1e-10, rtol=0.0)
