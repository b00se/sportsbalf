"""NHL shots-on-goal feature engineering utilities."""

from __future__ import annotations

import numpy as np
import pandas as pd


def _validate_rolling_windows(rolling_windows: list[int]) -> tuple[int, int]:
    if not rolling_windows:
        raise ValueError("feature_rolling_windows must be non-empty.")

    windows = sorted({int(window) for window in rolling_windows if int(window) > 0})
    if not windows:
        raise ValueError("feature_rolling_windows must contain positive integers.")

    short_window = 5 if 5 in windows else windows[0]
    long_window = 10 if 10 in windows else windows[-1]
    return short_window, long_window


def _prepare_history(skater_games: pd.DataFrame) -> pd.DataFrame:
    history = skater_games.copy()
    history["player_id"] = history["player_id"].astype("string")
    history["team"] = history["team"].astype("string")
    history["opponent"] = history["opponent"].astype("string")
    history["game_id"] = history["game_id"].astype("string")
    history["game_date"] = pd.to_datetime(history["game_date"], errors="coerce")
    history["shots_on_goal"] = pd.to_numeric(history["shots_on_goal"], errors="coerce")
    history["time_on_ice_minutes"] = pd.to_numeric(
        history.get("time_on_ice_minutes"), errors="coerce"
    )
    return history.dropna(
        subset=["player_id", "team", "opponent", "game_date", "shots_on_goal"]
    )


def _lagged_rolling_mean(
    frame: pd.DataFrame,
    *,
    group_col: str,
    value_col: str,
    window: int,
) -> pd.Series:
    ordered = frame.sort_values([group_col, "game_date", "game_id", "player_id"])
    values = pd.to_numeric(ordered[value_col], errors="coerce")
    lagged = values.groupby(ordered[group_col], dropna=False).shift(1)
    rolled = lagged.groupby(ordered[group_col], dropna=False).transform(
        lambda series: series.rolling(window, min_periods=1).mean()
    )
    return rolled.reindex(frame.index)


def _lagged_running_mean(
    frame: pd.DataFrame,
    *,
    group_col: str,
    value_col: str,
) -> pd.Series:
    ordered = frame.sort_values([group_col, "game_date", "game_id", "player_id"])
    values = pd.to_numeric(ordered[value_col], errors="coerce")
    lagged = values.groupby(ordered[group_col], dropna=False).shift(1)

    cumulative_sum = lagged.groupby(ordered[group_col], dropna=False).cumsum()
    cumulative_count = lagged.notna().groupby(ordered[group_col], dropna=False).cumsum()
    running_mean = cumulative_sum / cumulative_count.replace(0, np.nan)
    return running_mean.reindex(frame.index)


def _lagged_rolling_mean_game_level(
    frame: pd.DataFrame,
    *,
    group_col: str,
    value_col: str,
    window: int,
) -> pd.Series:
    ordered = frame.sort_values([group_col, "game_date", "game_id"])
    values = pd.to_numeric(ordered[value_col], errors="coerce")
    lagged = values.groupby(ordered[group_col], dropna=False).shift(1)
    rolled = lagged.groupby(ordered[group_col], dropna=False).transform(
        lambda series: series.rolling(window, min_periods=1).mean()
    )
    return rolled.reindex(frame.index)


def compute_baseline_prediction(
    frame: pd.DataFrame,
    *,
    fallback_prediction: float,
) -> pd.Series:
    """Compute deterministic baseline predictions from SOG rolling features."""

    required = ["sog_avg_last_5", "sog_avg_last_10", "sog_avg_season_to_date"]
    has_history = frame.loc[:, required].notna().all(axis=1)
    weighted = (
        0.5 * pd.to_numeric(frame["sog_avg_last_5"], errors="coerce")
        + 0.3 * pd.to_numeric(frame["sog_avg_last_10"], errors="coerce")
        + 0.2 * pd.to_numeric(frame["sog_avg_season_to_date"], errors="coerce")
    )
    baseline = np.where(has_history, weighted, float(fallback_prediction))
    return pd.Series(
        pd.to_numeric(baseline, errors="coerce"),
        index=frame.index,
        name="baseline_predicted_shots_on_goal",
    )


def _build_team_game_context_features(
    history: pd.DataFrame,
    *,
    short_window: int,
) -> pd.DataFrame:
    """Build lagged team/opponent context at game granularity."""

    team_games = (
        history.groupby(
            ["team", "opponent", "game_id", "game_date"],
            dropna=False,
            as_index=False,
        )["shots_on_goal"]
        .mean()
        .rename(columns={"shots_on_goal": "team_sog_for"})
    )
    team_games["team_sog_for_avg_last_5"] = _lagged_rolling_mean_game_level(
        team_games,
        group_col="team",
        value_col="team_sog_for",
        window=short_window,
    )

    opponent_allowed = team_games.rename(
        columns={
            "team": "offense_team",
            "opponent": "team",
            "team_sog_for": "team_sog_allowed",
        }
    )
    opponent_allowed["opponent_sog_allowed_avg_last_5"] = (
        _lagged_rolling_mean_game_level(
            opponent_allowed,
            group_col="team",
            value_col="team_sog_allowed",
            window=short_window,
        )
    )

    context = team_games[
        ["team", "opponent", "game_id", "team_sog_for_avg_last_5"]
    ].copy()
    context = context.merge(
        opponent_allowed[["team", "game_id", "opponent_sog_allowed_avg_last_5"]].rename(
            columns={"team": "opponent"}
        ),
        on=["opponent", "game_id"],
        how="left",
    )
    return context[
        [
            "team",
            "game_id",
            "team_sog_for_avg_last_5",
            "opponent_sog_allowed_avg_last_5",
        ]
    ]


def build_sog_training_features(
    skater_games: pd.DataFrame,
    rolling_windows: list[int],
) -> pd.DataFrame:
    """Build leakage-safe training features from skater game history."""

    short_window, long_window = _validate_rolling_windows(rolling_windows)
    history = _prepare_history(skater_games)
    history = history.sort_values(["player_id", "game_date", "game_id"]).copy()

    ordered_player = history.sort_values(["player_id", "game_date", "game_id"])
    player_shifts = ordered_player.groupby("player_id", dropna=False)

    days_since = player_shifts["game_date"].diff().dt.days
    history.loc[ordered_player.index, "days_since_last_game"] = days_since
    history.loc[ordered_player.index, "games_played_to_date"] = (
        player_shifts.cumcount().astype(float)
    )

    history["sog_avg_last_5"] = _lagged_rolling_mean(
        history,
        group_col="player_id",
        value_col="shots_on_goal",
        window=short_window,
    )
    history["sog_avg_last_10"] = _lagged_rolling_mean(
        history,
        group_col="player_id",
        value_col="shots_on_goal",
        window=long_window,
    )
    history["sog_avg_season_to_date"] = _lagged_running_mean(
        history,
        group_col="player_id",
        value_col="shots_on_goal",
    )
    history["toi_avg_last_5"] = _lagged_rolling_mean(
        history,
        group_col="player_id",
        value_col="time_on_ice_minutes",
        window=short_window,
    )
    history["toi_avg_last_10"] = _lagged_rolling_mean(
        history,
        group_col="player_id",
        value_col="time_on_ice_minutes",
        window=long_window,
    )

    context = _build_team_game_context_features(history, short_window=short_window)
    history = history.reset_index(drop=False).rename(columns={"index": "_row_id"})
    history = history.merge(context, on=["team", "game_id"], how="left")
    history = history.sort_values("_row_id").drop(columns=["_row_id"])

    return history.reset_index(drop=True)


def _tail_mean(series: pd.Series, window: int) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return float("nan")
    return float(values.tail(window).mean())


def _recent_rest_days(game_dates: pd.Series) -> float:
    ordered = pd.to_datetime(game_dates, errors="coerce").dropna().sort_values()
    if ordered.size < 2:
        return float("nan")
    return float((ordered.iloc[-1] - ordered.iloc[-2]).days)


def _build_player_inference_summary(
    history: pd.DataFrame,
    *,
    short_window: int,
    long_window: int,
) -> pd.DataFrame:
    ordered = history.sort_values(["player_id", "game_date", "game_id"])
    grouped = ordered.groupby("player_id", dropna=False)
    latest_player_rows = ordered.drop_duplicates("player_id", keep="last")

    summary = pd.DataFrame(
        {
            "player_id": latest_player_rows["player_id"].astype("string").to_numpy(),
            "history_team": latest_player_rows["team"].astype("string").to_numpy(),
            "history_opponent": latest_player_rows["opponent"]
            .astype("string")
            .to_numpy(),
            "sog_avg_last_5": grouped["shots_on_goal"]
            .apply(lambda values: _tail_mean(values, short_window))
            .to_numpy(),
            "sog_avg_last_10": grouped["shots_on_goal"]
            .apply(lambda values: _tail_mean(values, long_window))
            .to_numpy(),
            "sog_avg_season_to_date": grouped["shots_on_goal"].mean().to_numpy(),
            "toi_avg_last_5": grouped["time_on_ice_minutes"]
            .apply(lambda values: _tail_mean(values, short_window))
            .to_numpy(),
            "toi_avg_last_10": grouped["time_on_ice_minutes"]
            .apply(lambda values: _tail_mean(values, long_window))
            .to_numpy(),
            "games_played_to_date": grouped.size().astype(float).to_numpy(),
            "days_since_last_game": grouped["game_date"]
            .apply(_recent_rest_days)
            .to_numpy(),
        }
    )
    return summary.reset_index(drop=True)


def _build_team_context_inference_maps(
    history: pd.DataFrame,
    *,
    short_window: int,
) -> tuple[pd.Series, pd.Series]:
    """Build inference-time team/opponent context at game granularity."""

    team_games = (
        history.groupby(
            ["team", "opponent", "game_id", "game_date"],
            dropna=False,
            as_index=False,
        )["shots_on_goal"]
        .mean()
        .rename(columns={"shots_on_goal": "team_sog_for"})
    )
    ordered_team_games = team_games.sort_values(["team", "game_date", "game_id"])
    team_context = (
        ordered_team_games.groupby("team", dropna=False)["team_sog_for"]
        .apply(lambda values: _tail_mean(values, short_window))
        .rename("team_sog_for_avg_last_5")
    )

    opponent_allowed = ordered_team_games.rename(
        columns={
            "team": "offense_team",
            "opponent": "team",
            "team_sog_for": "team_sog_allowed",
        }
    )
    opponent_allowed = opponent_allowed.sort_values(["team", "game_date", "game_id"])
    opponent_context = (
        opponent_allowed.groupby("team", dropna=False)["team_sog_allowed"]
        .apply(lambda values: _tail_mean(values, short_window))
        .rename("opponent_sog_allowed_avg_last_5")
    )
    return team_context, opponent_context


def build_sog_inference_features(
    inference_rows: pd.DataFrame,
    skater_games: pd.DataFrame,
    rolling_windows: list[int],
    fallback_prediction: float,
) -> pd.DataFrame:
    """Build NHL inference features and deterministic baseline predictions."""

    short_window, long_window = _validate_rolling_windows(rolling_windows)
    history = _prepare_history(skater_games)

    featured = inference_rows.copy()
    featured["player_id"] = featured["player_id"].astype("string")

    summary = _build_player_inference_summary(
        history,
        short_window=short_window,
        long_window=long_window,
    )
    featured = featured.merge(summary, on="player_id", how="left")

    team_context, opponent_context = _build_team_context_inference_maps(
        history,
        short_window=short_window,
    )

    featured["team_context_key"] = featured.get("team")
    featured["team_context_key"] = featured["team_context_key"].fillna(
        featured["history_team"]
    )
    featured["opponent_context_key"] = featured.get("opponent")
    featured["opponent_context_key"] = featured["opponent_context_key"].fillna(
        featured["history_opponent"]
    )

    featured["team_sog_for_avg_last_5"] = featured["team_context_key"].map(team_context)
    featured["opponent_sog_allowed_avg_last_5"] = featured["opponent_context_key"].map(
        opponent_context
    )

    baseline = compute_baseline_prediction(
        featured,
        fallback_prediction=fallback_prediction,
    )
    featured["baseline_predicted_shots_on_goal"] = baseline
    featured["predicted_shots_on_goal"] = baseline

    global_rest_days = _recent_rest_days(history["game_date"])
    if np.isnan(global_rest_days):
        global_rest_days = 3.0
    featured["days_since_last_game"] = pd.to_numeric(
        featured["days_since_last_game"], errors="coerce"
    ).fillna(float(global_rest_days))

    featured.drop(
        columns=[
            "history_team",
            "history_opponent",
            "team_context_key",
            "opponent_context_key",
        ],
        inplace=True,
        errors="ignore",
    )
    return featured


__all__ = [
    "build_sog_inference_features",
    "build_sog_training_features",
    "compute_baseline_prediction",
]
