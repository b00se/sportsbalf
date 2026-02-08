import pandas as pd


def _grouped_shifted_rolling(
    games: pd.DataFrame, column: str, window: int, default: float
) -> pd.Series:
    grouped = (
        games.groupby("pitcher")[column].rolling(window=window, min_periods=1).mean()
    )
    shifted = grouped.groupby(level=0).shift(1)
    return shifted.droplevel(0).fillna(default)


def add_rolling_features(
    games: pd.DataFrame, default_k: float = 5, default_pitch_count: float = 85
) -> pd.DataFrame:
    """Append rolling strikeout and pitch count aggregates per pitcher."""

    games = games.sort_values(["pitcher", "game_date"]).reset_index(drop=True)

    games["rolling_K_avg_3"] = _grouped_shifted_rolling(
        games, "strikeouts", window=3, default=default_k
    )
    games["rolling_K_avg_5"] = _grouped_shifted_rolling(
        games, "strikeouts", window=5, default=default_k
    )
    games["rolling_pitch_count_5"] = _grouped_shifted_rolling(
        games, "pitch_count", window=5, default=default_pitch_count
    )

    rolling_k_sum = (
        games.groupby("pitcher")["strikeouts"]
        .rolling(window=3, min_periods=1)
        .sum()
        .groupby(level=0)
        .shift(1)
    )
    rolling_pitch_sum = (
        games.groupby("pitcher")["pitch_count"]
        .rolling(window=5, min_periods=1)
        .sum()
        .groupby(level=0)
        .shift(1)
    )

    rate = (rolling_k_sum / rolling_pitch_sum).droplevel(0)
    games["rolling_K_rate"] = rate.replace([float("inf"), -float("inf")], pd.NA).fillna(
        0.055
    )

    return games
