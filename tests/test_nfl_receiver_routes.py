"""Offline tests for conservative NFLverse receiver route materialization."""

import pandas as pd
import pytest
from src.nfl.features.receiver_routes import (
    ReceiverRouteSchemaError,
    materialize_identified_receiver_route_history,
    materialize_identified_receiver_routes,
)


def _inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    participation = pd.DataFrame(
        {
            "nflverse_game_id": ["g1", "g1", "g1", "g1", "g1"],
            "play_id": [1, 2, 3, 4, 5],
            "offense_players": [
                "wr1;te1;rb1",
                "wr1;te1;rb1",
                "rb1;te1",
                "wr1;te1",
                "wr1;te1",
            ],
            "offense_positions": ["WR;TE;RB", "WR;TE;RB", "RB;TE", "WR;TE", "WR;TE"],
            "route": ["GO", "OUT", "POST", "CROSS", ""],
        }
    )
    pbp = pd.DataFrame(
        {
            "game_id": ["g1"] * 5,
            "play_id": [1, 2, 3, 4, 5],
            "season": [2024] * 5,
            "week": [2, 2, 2, 2, 2],
            "play_type": ["pass"] * 5,
            "pass_attempt": [1] * 5,
            "receiver_player_id": ["wr1", "missing", "rb1", "te1", "te1"],
            "posteam": ["AAA"] * 5,
            "season_type": ["REG"] * 5,
        }
    )
    return participation, pbp


def test_valid_mapping_excludes_unlinked_and_non_wr_te() -> None:
    participation, pbp = _inputs()
    result = materialize_identified_receiver_routes(participation, pbp)

    assert result.frame.to_dict("records") == [
        {
            "player_id": "te1",
            "season": 2024,
            "week": 2,
            "team": "AAA",
            "position": "TE",
            "identified_receiver_routes": 1,
        },
        {
            "player_id": "wr1",
            "season": 2024,
            "week": 2,
            "team": "AAA",
            "position": "WR",
            "identified_receiver_routes": 1,
        },
    ]
    assert result.diagnostics.source_pass_route_rows == 4
    assert result.diagnostics.linked_receiver_rows == 3
    assert result.diagnostics.unlinked_receiver_rows == 1
    assert result.diagnostics.excluded_non_wr_te_rows == 1
    assert result.diagnostics.unmatched_participation_route_rows == 0


def test_postseason_is_not_proven_regular_season() -> None:
    participation, pbp = _inputs()
    pbp.loc[0, "season_type"] = "POST"

    result = materialize_identified_receiver_routes(participation, pbp)

    assert result.frame["identified_receiver_routes"].sum() == 1


def test_unmatched_participation_route_rows_are_audited() -> None:
    participation, pbp = _inputs()
    extra = participation.iloc[[0]].copy()
    extra["nflverse_game_id"] = "missing-game"
    extra["play_id"] = 99

    result = materialize_identified_receiver_routes(
        pd.concat([participation, extra], ignore_index=True), pbp
    )

    assert result.diagnostics.unmatched_participation_route_rows == 1


def test_malformed_alignment_fails_closed() -> None:
    participation, pbp = _inputs()
    participation.loc[0, "offense_positions"] = "WR"

    with pytest.raises(ReceiverRouteSchemaError, match="alignment mismatch"):
        materialize_identified_receiver_routes(participation, pbp)

    participation, pbp = _inputs()
    participation.loc[0, "offense_players"] = "wr1;wr1;te1;rb1"
    participation.loc[0, "offense_positions"] = "WR;WR;TE;RB"
    with pytest.raises(ReceiverRouteSchemaError, match="duplicate offense"):
        materialize_identified_receiver_routes(participation, pbp)


def test_as_of_history_excludes_target_week_and_later() -> None:
    participation, pbp = _inputs()
    prior_participation = participation.iloc[[0]].copy()
    prior_participation["nflverse_game_id"] = "g0"
    prior_participation["play_id"] = 1
    prior_pbp = pbp.iloc[[0]].copy()
    prior_pbp["game_id"] = "g0"
    prior_pbp["season"] = 2023
    history = materialize_identified_receiver_route_history(
        pd.concat([prior_participation, participation], ignore_index=True),
        pd.concat([prior_pbp, pbp], ignore_index=True),
        as_of=(2024, 2),
    )

    assert history.frame[["season", "week"]].to_dict("records") == [
        {"season": 2023, "week": 2}
    ]
    assert (history.frame["week"] < 2).all() or (history.frame["season"] < 2024).all()


def test_route_schema_and_key_errors_are_not_silently_repaired() -> None:
    participation, pbp = _inputs()
    participation.loc[0, "nflverse_game_id"] = ""
    with pytest.raises(ReceiverRouteSchemaError, match="nflverse_game_id"):
        materialize_identified_receiver_routes(participation, pbp)

    participation, pbp = _inputs()
    participation["route"] = [1, 2, 3, 4, 5]
    with pytest.raises(ReceiverRouteSchemaError, match="categorical"):
        materialize_identified_receiver_routes(participation, pbp)

    for column, value, message, source in [
        ("season", True, "season.*boolean", "pbp"),
        ("week", 2.5, "week.*integers", "pbp"),
        ("play_id", -1, "play_id.*between", "participation"),
    ]:
        participation, pbp = _inputs()
        target = pbp if source == "pbp" else participation
        target[column] = target[column].astype(object)
        target.loc[0, column] = value
        with pytest.raises(ReceiverRouteSchemaError, match=message):
            materialize_identified_receiver_routes(participation, pbp)

    participation, pbp = _inputs()
    pbp.loc[0, "posteam"] = ""
    with pytest.raises(ReceiverRouteSchemaError, match="posteam"):
        materialize_identified_receiver_routes(participation, pbp)

    participation, pbp = _inputs()
    pbp.loc[0, "season_type"] = pd.NA
    with pytest.raises(ReceiverRouteSchemaError, match="season_type"):
        materialize_identified_receiver_routes(participation, pbp)


def test_as_of_diagnostics_describe_only_the_returned_history() -> None:
    participation, pbp = _inputs()
    prior_participation = participation.iloc[[0]].copy()
    prior_participation["nflverse_game_id"] = "g0"
    prior_participation["play_id"] = 1
    prior_pbp = pbp.iloc[[0]].copy()
    prior_pbp["game_id"] = "g0"
    prior_pbp["season"] = 2023
    extra = participation.iloc[[0]].copy()
    extra["nflverse_game_id"] = "not-in-pbp"
    extra["play_id"] = 99
    history = materialize_identified_receiver_route_history(
        pd.concat([prior_participation, participation, extra], ignore_index=True),
        pd.concat([prior_pbp, pbp], ignore_index=True),
        as_of=(2024, 2),
    )

    assert history.diagnostics.source_pass_route_rows == 1
    assert history.diagnostics.unmatched_participation_route_rows == 0
