from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from src.nhl.models.predict import (
    NHL_FEATURES,
    artifact_is_compatible,
    load_model,
    predict_sog,
    save_model,
    train_model,
)


def _training_frame() -> pd.DataFrame:
    rows: list[dict[str, float | int | str]] = []
    for idx in range(12):
        rows.append(
            {
                "player_id": "8478402",
                "sog_avg_last_5": 2.5 + (idx * 0.1),
                "sog_avg_last_10": 2.7 + (idx * 0.1),
                "sog_avg_season_to_date": 2.6 + (idx * 0.1),
                "toi_avg_last_5": 17.0 + (idx * 0.2),
                "toi_avg_last_10": 17.5 + (idx * 0.2),
                "games_played_to_date": 10 + idx,
                "days_since_last_game": 1 + (idx % 3),
                "team_sog_for_avg_last_5": 2.8 + (idx * 0.05),
                "opponent_sog_allowed_avg_last_5": 2.9 + (idx * 0.04),
                "shots_on_goal": 2 + (idx % 4),
            }
        )
    return pd.DataFrame(rows)


def test_train_save_load_roundtrip(tmp_path: Path) -> None:
    train_df = _training_frame()
    model = train_model(train_df)

    model_path = tmp_path / "nhl_model.joblib"
    save_model(model, model_path, feature_columns=NHL_FEATURES, model_name="xgboost")

    loaded_model, metadata = load_model(
        model_path,
        expected_feature_columns=NHL_FEATURES,
    )
    preds = predict_sog(train_df, loaded_model)

    assert len(preds) == len(train_df)
    assert preds.notna().all()
    assert metadata["model_name"] == "xgboost"
    assert artifact_is_compatible(metadata, expected_feature_columns=NHL_FEATURES)


def test_schema_hash_mismatch_reports_incompatible(tmp_path: Path) -> None:
    train_df = _training_frame()
    model = train_model(train_df)
    model_path = tmp_path / "nhl_model.joblib"
    save_model(model, model_path, feature_columns=NHL_FEATURES, model_name="xgboost")

    _, metadata = load_model(
        model_path,
        expected_feature_columns=NHL_FEATURES,
    )
    assert artifact_is_compatible(
        metadata,
        expected_feature_columns=NHL_FEATURES + ["future_feature"],
    ) is False

    with pytest.raises(ValueError, match="schema"):
        load_model(
            model_path,
            expected_feature_columns=NHL_FEATURES + ["future_feature"],
        )


def test_corrupt_artifact_raises_value_error(tmp_path: Path) -> None:
    model_path = tmp_path / "nhl_model.joblib"
    model_path.write_bytes(b"not-a-joblib")

    with pytest.raises(ValueError, match="Failed to load NHL model artifact"):
        load_model(model_path, expected_feature_columns=NHL_FEATURES)
