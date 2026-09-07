"""Regression tests for canonical and scoped NFL identity semantics."""

import json
from pathlib import Path

import pytest
from src.fantasy.adapters.nfl.rankings import (
    export_unattended_rankings_csv,
    parse_rankings_csv,
)
from src.nfl.data.identity import IdentityStatus, build_identity_graph


@pytest.fixture
def scoped_graph():
    payload = json.loads(
        Path("tests/testdata/nfl_identity_scoped_fixture.json").read_text()
    )
    return build_identity_graph(**payload)


def test_appearance_ids_change_per_game_but_share_canonical_player(scoped_graph):
    first = scoped_graph.resolve_appearance("app-week-1", 2026, game_id="game-1")
    second = scoped_graph.resolve_appearance("app-week-2", 2026, game_id="game-2")
    assert first.status is second.status is IdentityStatus.RESOLVED
    assert first.nflverse_id == second.nflverse_id == "mahomes"


def test_ud_reference_is_scoped_to_slate_and_trade_date(scoped_graph):
    assert (
        scoped_graph.resolve_player(
            "ud-trade",
            2026,
            source="ud_player_id",
            slate_id="slate-1",
            as_of="2026-09-10",
        ).nflverse_id
        == "trade-player"
    )
    assert (
        scoped_graph.resolve_player(
            "ud-trade",
            2026,
            source="ud_player_id",
            slate_id="slate-2",
            as_of="2026-09-11",
        ).nflverse_id
        == "trade-player"
    )


def test_ambiguous_material_row_blocks_without_writing(tmp_path, scoped_graph):
    table = parse_rankings_csv(Path("tests/testdata/fantasy/nfl/rankings.csv"))
    destination = tmp_path / "blocked.csv"
    with pytest.raises(Exception, match="unattended export blocked"):
        export_unattended_rankings_csv(
            table, destination, identity_graph=scoped_graph, season=2026
        )
    assert not destination.exists()
