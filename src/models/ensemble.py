"""Legacy ensemble helper retained for non-canonical utility usage."""

import pandas as pd
from sklearn.ensemble import RandomForestRegressor

MODULE_STATUS = "legacy_non_authoritative"
AUTHORITATIVE_ENTRYPOINT = "pipeline/main.py -> src/pipeline/engine.py"
STATUS_NOTE = "Legacy training helper; authoritative orchestration lives in engine pipeline flow."


FEATURES = [
    "rolling_K_avg_3",
    "rolling_pitch_count_5",
    "park_factor_K",
    "opponent_k_rate",
]


def train_ensemble(df: pd.DataFrame) -> RandomForestRegressor:
    """Train an ensemble regression model."""
    X = df[FEATURES]
    y = df["strikeouts"]
    model = RandomForestRegressor(n_estimators=25, random_state=42)
    model.fit(X, y)
    return model
