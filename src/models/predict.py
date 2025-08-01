from typing import Iterable
import pandas as pd
from sklearn.linear_model import LinearRegression


FEATURES = ["rolling_K_avg_3", "rolling_pitch_count_5", "park_factor_K", "opponent_k_rate"]

def train_model(df: pd.DataFrame) -> LinearRegression:
    """Train a simple linear regression model."""
    X = df[FEATURES]
    y = df["strikeouts"]
    model = LinearRegression()
    model.fit(X, y)
    return model


def predict_strikeouts(df: pd.DataFrame, model: LinearRegression) -> pd.Series:
    """Predict strikeouts using the provided model."""
    return pd.Series(model.predict(df[FEATURES]), index=df.index)
