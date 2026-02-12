from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from src.nhl.data.shot_snapshot import (
    aggregate_shot_events_to_skater_games,
    build_skater_snapshot_from_shots_csv,
)


def _shot_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "season": 2024,
                "game_id": 20001,
                "isHomeTeam": 1,
                "homeTeamCode": "NYR",
                "awayTeamCode": "BOS",
                "teamCode": "NYR",
                "shooterPlayerId": 8478402,
                "shooterName": "Player One",
                "shotWasOnGoal": 1,
                "shooterTimeOnIce": 60,
            },
            {
                "season": 2024,
                "game_id": 20001,
                "isHomeTeam": 1,
                "homeTeamCode": "NYR",
                "awayTeamCode": "BOS",
                "teamCode": "NYR",
                "shooterPlayerId": 8478402,
                "shooterName": "Player One",
                "shotWasOnGoal": 0,
                "shooterTimeOnIce": 120,
            },
            {
                "season": 2024,
                "game_id": 20001,
                "isHomeTeam": 0,
                "homeTeamCode": "NYR",
                "awayTeamCode": "BOS",
                "teamCode": "BOS",
                "shooterPlayerId": 8471214,
                "shooterName": "Player Two",
                "shotWasOnGoal": 1,
                "shooterTimeOnIce": 30,
            },
        ]
    )


def test_aggregate_shot_events_to_skater_games_sums_sog_and_sets_opponent() -> None:
    snapshot = aggregate_shot_events_to_skater_games(_shot_rows())

    one = snapshot.loc[snapshot["player_id"] == "8478402"].iloc[0]
    two = snapshot.loc[snapshot["player_id"] == "8471214"].iloc[0]

    assert one["shots_on_goal"] == 1.0
    assert one["time_on_ice_minutes"] == 2.0
    assert one["team"] == "NYR"
    assert one["opponent"] == "BOS"

    assert two["shots_on_goal"] == 1.0
    assert two["team"] == "BOS"
    assert two["opponent"] == "NYR"


def test_aggregate_shot_events_to_skater_games_rejects_missing_columns() -> None:
    with pytest.raises(ValueError, match="missing required columns"):
        aggregate_shot_events_to_skater_games(pd.DataFrame([{"season": 2024}]))


def test_build_skater_snapshot_from_shots_csv_writes_output(tmp_path: Path) -> None:
    input_path = tmp_path / "shots.csv"
    output_path = tmp_path / "snapshot.csv"
    _shot_rows().to_csv(input_path, index=False)

    snapshot = build_skater_snapshot_from_shots_csv(
        input_path=input_path,
        output_path=output_path,
        chunk_size=2,
    )

    assert output_path.exists()
    loaded = pd.read_csv(output_path)
    assert len(snapshot) == len(loaded)
    assert set(loaded.columns) == {
        "season",
        "game_id",
        "game_date",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "shots_on_goal",
        "time_on_ice_minutes",
    }


def test_aggregate_shot_events_to_skater_games_drops_missing_player_name() -> None:
    frame = _shot_rows()
    frame.loc[frame["shooterPlayerId"] == 8478402, "shooterName"] = pd.NA

    snapshot = aggregate_shot_events_to_skater_games(frame)
    assert "8478402" not in set(snapshot["player_id"].astype(str).tolist())
