"""Feature engineering helpers for NFL QB attempts."""

from __future__ import annotations

import numpy as np
import pandas as pd

TEAM_TO_DIVISION: dict[str, tuple[str, str]] = {
    "ARI": ("NFC", "West"),
    "ATL": ("NFC", "South"),
    "BAL": ("AFC", "North"),
    "BUF": ("AFC", "East"),
    "CAR": ("NFC", "South"),
    "CHI": ("NFC", "North"),
    "CIN": ("AFC", "North"),
    "CLE": ("AFC", "North"),
    "DAL": ("NFC", "East"),
    "DEN": ("AFC", "West"),
    "DET": ("NFC", "North"),
    "GB": ("NFC", "North"),
    "HOU": ("AFC", "South"),
    "IND": ("AFC", "South"),
    "JAC": ("AFC", "South"),
    "JAX": ("AFC", "South"),
    "KC": ("AFC", "West"),
    "LA": ("NFC", "West"),
    "LAR": ("NFC", "West"),
    "LAC": ("AFC", "West"),
    "LV": ("AFC", "West"),
    "MIA": ("AFC", "East"),
    "MIN": ("NFC", "North"),
    "NE": ("AFC", "East"),
    "NO": ("NFC", "South"),
    "NYG": ("NFC", "East"),
    "NYJ": ("AFC", "East"),
    "PHI": ("NFC", "East"),
    "PIT": ("AFC", "North"),
    "SEA": ("NFC", "West"),
    "SF": ("NFC", "West"),
    "TB": ("NFC", "South"),
    "TEN": ("AFC", "South"),
    "WAS": ("NFC", "East"),
}


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def compute_player_passing_features(weekly: pd.DataFrame) -> pd.DataFrame:
    """Return season and career context for QB passing volume."""
    base_columns = ["season", "week", "player_id", "attempts"]
    use_game_id = "game_id" in weekly.columns
    columns = base_columns + (["game_id"] if use_game_id else [])
    if weekly is None or weekly.empty:
        return pd.DataFrame(
            columns=columns
            + [
                "season_avg_attempts",
                "career_avg_attempts",
                "season_attempts_to_date",
                "season_games_played",
                "season_avg_attempts_to_date",
            ]
        )

    missing = [col for col in base_columns if col not in weekly.columns]
    if missing:
        raise KeyError(f"weekly data missing required columns: {missing}")

    select_cols = base_columns + (["game_id"] if use_game_id else [])
    qbs = weekly.loc[weekly["position"] == "QB", select_cols].copy()
    if not use_game_id:
        qbs["game_id"] = pd.NA
    qbs["attempts"] = _to_numeric(qbs["attempts"])
    qbs["season"] = _to_numeric(qbs["season"]).astype("Int64")
    qbs["week"] = _to_numeric(qbs["week"]).astype("Int64")

    qbs["game_id"] = qbs["game_id"].astype(str)
    qbs.sort_values(["player_id", "season", "week"], inplace=True)
    qbs["season_attempts_to_date"] = (
        qbs.groupby(["player_id", "season"])["attempts"].cumsum() - qbs["attempts"]
    )
    qbs["season_games_played"] = qbs.groupby(["player_id", "season"]).cumcount()
    career_attempts_to_date = qbs.groupby("player_id")["attempts"].cumsum() - qbs[
        "attempts"
    ]
    career_games_played = qbs.groupby("player_id").cumcount()

    with np.errstate(divide="ignore", invalid="ignore"):
        season_avg_to_date = np.where(
            qbs["season_games_played"] > 0,
            qbs["season_attempts_to_date"] / qbs["season_games_played"],
            np.nan,
        )
        career_avg_to_date = np.where(
            career_games_played > 0,
            career_attempts_to_date / career_games_played,
            np.nan,
        )
    qbs["season_avg_attempts"] = season_avg_to_date
    qbs["season_avg_attempts_to_date"] = season_avg_to_date
    qbs["career_avg_attempts"] = career_avg_to_date

    columns = [
        "season",
        "week",
        "player_id",
        "season_avg_attempts",
        "career_avg_attempts",
        "season_attempts_to_date",
        "season_games_played",
        "season_avg_attempts_to_date",
    ]
    if "game_id" in qbs.columns:
        columns.insert(2, "game_id")
    return qbs[columns]


def compute_qb_pbp_metrics(pbp: pd.DataFrame) -> pd.DataFrame:
    """Compute QB-level efficiency metrics from play-by-play data."""
    columns = [
        "season",
        "week",
        "game_id",
        "qb_id",
        "qb_dropbacks",
        "avg_cpoe",
        "epa_per_dropback",
        "air_yards_per_attempt",
        "qb_rush_attempts",
    ]
    if pbp is None or pbp.empty:
        return pd.DataFrame(columns=columns)

    frame = pbp.copy()
    frame["pass_attempt"] = _to_numeric(frame.get("pass_attempt")).fillna(0)
    frame["qb_dropback"] = _to_numeric(frame.get("qb_dropback")).fillna(0)
    frame["epa"] = _to_numeric(frame.get("epa"))
    frame["cpoe"] = _to_numeric(frame.get("cpoe"))
    frame["air_yards"] = _to_numeric(frame.get("air_yards"))
    frame["rush_attempt"] = _to_numeric(frame.get("rush_attempt")).fillna(0)
    frame["season"] = _to_numeric(frame.get("season")).astype("Int64")
    frame["week"] = _to_numeric(frame.get("week")).astype("Int64")

    qb_group = (
        frame.groupby(["season", "week", "game_id", "passer_player_id"], dropna=False)
        .agg(
            qb_dropbacks=("qb_dropback", "sum"),
            pass_attempts=("pass_attempt", "sum"),
            epa_sum=("epa", "sum"),
            avg_cpoe=("cpoe", "mean"),
            air_yards_per_attempt=("air_yards", "mean"),
        )
        .reset_index()
    )

    rush_group = (
        frame.loc[frame["rusher_player_id"].notna()]
        .groupby(["season", "week", "game_id", "rusher_player_id"])["rush_attempt"]
        .sum()
        .reset_index()
    )

    merged = qb_group.merge(
        rush_group,
        left_on=["season", "week", "game_id", "passer_player_id"],
        right_on=["season", "week", "game_id", "rusher_player_id"],
        how="left",
    )
    merged.rename(columns={"rush_attempt": "qb_rush_attempts"}, inplace=True)
    merged.drop(columns=["rusher_player_id"], inplace=True)
    merged["qb_rush_attempts"] = merged["qb_rush_attempts"].fillna(0)

    merged["epa_per_dropback"] = merged["epa_sum"] / merged["qb_dropbacks"].replace(
        0, np.nan
    )

    merged["game_id"] = merged["game_id"].astype(str)
    merged.rename(columns={"passer_player_id": "qb_id"}, inplace=True)
    merged.sort_values(["qb_id", "season", "week", "game_id"], inplace=True)
    lag_columns = [
        "qb_dropbacks",
        "avg_cpoe",
        "epa_per_dropback",
        "air_yards_per_attempt",
        "qb_rush_attempts",
    ]
    for column in lag_columns:
        merged[column] = merged.groupby("qb_id", dropna=False)[column].shift(1)

    return merged[
        [
            "season",
            "week",
            "game_id",
            "qb_id",
            "qb_dropbacks",
            "avg_cpoe",
            "epa_per_dropback",
            "air_yards_per_attempt",
            "qb_rush_attempts",
        ]
    ]


def compute_team_and_opponent_features(
    pbp: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return offensive and defensive team rates derived from play-by-play."""
    columns_team = [
        "season",
        "week",
        "game_id",
        "team",
        "plays_per_game",
        "pass_rate",
        "neutral_pass_rate",
        "pass_rate_over_expected",
    ]
    columns_opp = [
        "season",
        "week",
        "game_id",
        "opponent",
        "plays_faced",
        "opponent_pass_rate_allowed",
        "opponent_neutral_pass_rate",
    ]
    if pbp is None or pbp.empty:
        return (
            pd.DataFrame(columns=columns_team),
            pd.DataFrame(columns=columns_opp),
        )

    frame = pbp.copy()
    frame["pass_attempt"] = _to_numeric(frame.get("pass_attempt")).fillna(0)
    frame["rush_attempt"] = _to_numeric(frame.get("rush_attempt")).fillna(0)
    frame["season"] = _to_numeric(frame.get("season")).astype("Int64")
    frame["week"] = _to_numeric(frame.get("week")).astype("Int64")
    frame["score_differential"] = _to_numeric(frame.get("score_differential")).fillna(0)
    frame["qtr"] = _to_numeric(frame.get("qtr")).fillna(0)

    frame["play_count"] = 1
    frame["neutral_play"] = (
        (frame["qtr"] <= 3) & (frame["score_differential"].abs() <= 7)
    ).astype(int)
    frame["neutral_pass"] = frame["pass_attempt"] * frame["neutral_play"]

    team_group = (
        frame.groupby(["season", "week", "game_id", "posteam"], dropna=False)
        .agg(
            plays_per_game=("play_count", "sum"),
            pass_attempts=("pass_attempt", "sum"),
            neutral_plays=("neutral_play", "sum"),
            neutral_pass_attempts=("neutral_pass", "sum"),
        )
        .reset_index()
    )

    team_group["pass_rate"] = team_group["pass_attempts"] / team_group[
        "plays_per_game"
    ].replace(0, np.nan)
    team_group["neutral_pass_rate"] = team_group["neutral_pass_attempts"] / team_group[
        "neutral_plays"
    ].replace(0, np.nan)

    league = (
        frame.groupby("season")
        .agg(
            league_neutral_pass_attempts=("neutral_pass", "sum"),
            league_neutral_plays=("neutral_play", "sum"),
        )
        .reset_index()
    )
    league["league_neutral_pass_rate"] = league[
        "league_neutral_pass_attempts"
    ] / league["league_neutral_plays"].replace(0, np.nan)

    team_group = team_group.merge(
        league[["season", "league_neutral_pass_rate"]], on="season", how="left"
    )
    team_group["pass_rate_over_expected"] = (
        team_group["neutral_pass_rate"] - team_group["league_neutral_pass_rate"]
    )

    team_group.rename(columns={"posteam": "team"}, inplace=True)
    team_group["game_id"] = team_group["game_id"].astype(str)
    team_group.sort_values(["team", "season", "week", "game_id"], inplace=True)
    for column in [
        "plays_per_game",
        "pass_rate",
        "neutral_pass_rate",
        "pass_rate_over_expected",
    ]:
        team_group[column] = team_group.groupby("team", dropna=False)[column].shift(1)
    team_features = team_group[
        [
            "season",
            "week",
            "game_id",
            "team",
            "plays_per_game",
            "pass_rate",
            "neutral_pass_rate",
            "pass_rate_over_expected",
        ]
    ]

    opponent_group = (
        frame.groupby(["season", "week", "game_id", "defteam"], dropna=False)
        .agg(
            plays_faced=("play_count", "sum"),
            opponent_pass_attempts=("pass_attempt", "sum"),
            neutral_plays=("neutral_play", "sum"),
            neutral_pass_attempts=("neutral_pass", "sum"),
        )
        .reset_index()
    )

    opponent_group["opponent_pass_rate_allowed"] = opponent_group[
        "opponent_pass_attempts"
    ] / opponent_group["plays_faced"].replace(0, np.nan)
    opponent_group["opponent_neutral_pass_rate"] = opponent_group[
        "neutral_pass_attempts"
    ] / opponent_group["neutral_plays"].replace(0, np.nan)

    opponent_group.rename(columns={"defteam": "opponent"}, inplace=True)
    opponent_group["game_id"] = opponent_group["game_id"].astype(str)
    opponent_group.sort_values(
        ["opponent", "season", "week", "game_id"], inplace=True
    )
    for column in [
        "plays_faced",
        "opponent_pass_rate_allowed",
        "opponent_neutral_pass_rate",
    ]:
        opponent_group[column] = opponent_group.groupby("opponent", dropna=False)[
            column
        ].shift(1)
    opponent_features = opponent_group[
        [
            "season",
            "week",
            "game_id",
            "opponent",
            "plays_faced",
            "opponent_pass_rate_allowed",
            "opponent_neutral_pass_rate",
        ]
    ]

    return team_features, opponent_features


def compute_ngs_passing_features(ngs: pd.DataFrame) -> pd.DataFrame:
    """Return Next Gen Stats passing metrics keyed by player and game."""
    columns = [
        "season",
        "week",
        "qb_id",
        "ngs_avg_time_to_throw",
        "ngs_avg_air_yards",
        "ngs_cpoe",
    ]
    if ngs is None or ngs.empty:
        return pd.DataFrame(columns=columns)

    frame = ngs.copy()
    frame["season"] = _to_numeric(frame.get("season")).astype("Int64")
    frame["week"] = _to_numeric(frame.get("week")).astype("Int64")

    frame.rename(
        columns={
            "player_gsis_id": "qb_id",
            "avg_time_to_throw": "ngs_avg_time_to_throw",
            "avg_intended_air_yards": "ngs_avg_air_yards",
            "completion_percentage_above_expectation": "ngs_cpoe",
        },
        inplace=True,
    )

    if "qb_id" not in frame.columns or frame["qb_id"].isna().all():
        frame["qb_id"] = frame.get("player_display_name", "").astype(str)

    return frame[
        [
            "season",
            "week",
            "qb_id",
            "ngs_avg_time_to_throw",
            "ngs_avg_air_yards",
            "ngs_cpoe",
        ]
    ].copy()


def compute_game_context_features(schedule: pd.DataFrame) -> pd.DataFrame:
    """Derive rest and divisional context from the NFL schedule."""
    columns = [
        "season",
        "week",
        "game_id",
        "team",
        "rest_days",
        "short_week",
        "is_divisional",
    ]
    if schedule is None or schedule.empty:
        return pd.DataFrame(columns=columns)

    required = [
        "game_id",
        "season",
        "week",
        "gameday",
        "home_team",
        "away_team",
        "div_game",
    ]
    missing = [col for col in required if col not in schedule.columns]
    if missing:
        raise KeyError(f"schedule data missing required columns: {missing}")

    frame = schedule[required].copy()
    frame["gameday"] = pd.to_datetime(frame["gameday"], errors="coerce")
    frame["season"] = _to_numeric(frame["season"]).astype("Int64")
    frame["week"] = _to_numeric(frame["week"]).astype("Int64")
    div_game = pd.to_numeric(frame["div_game"], errors="coerce")
    div_game = div_game.where(div_game.notna(), 0).astype(int)
    frame["div_game"] = div_game

    home = frame[
        ["season", "week", "game_id", "home_team", "away_team", "gameday", "div_game"]
    ].copy()
    home.rename(columns={"home_team": "team", "away_team": "opponent"}, inplace=True)

    away = frame[
        ["season", "week", "game_id", "away_team", "home_team", "gameday", "div_game"]
    ].copy()
    away.rename(columns={"away_team": "team", "home_team": "opponent"}, inplace=True)

    combined = pd.concat([home, away], ignore_index=True)
    combined.sort_values(["team", "gameday"], inplace=True)

    combined["rest_days"] = (
        combined["gameday"] - combined.groupby("team")["gameday"].shift(1)
    ).dt.days
    rest_days = combined["rest_days"].where(combined["rest_days"].notna(), 7)
    combined["rest_days"] = rest_days
    combined["short_week"] = combined["rest_days"] <= 4
    combined["is_divisional"] = combined["div_game"] > 0

    combined["game_id"] = combined["game_id"].astype(str)
    return combined[
        [
            "season",
            "week",
            "game_id",
            "team",
            "rest_days",
            "short_week",
            "is_divisional",
        ]
    ]
