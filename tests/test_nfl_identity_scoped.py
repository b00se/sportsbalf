"""Regression tests for canonical and scoped NFL identity semantics."""

import json
from pathlib import Path

import pytest
from src.fantasy.adapters.nfl.rankings import (
    RankingsTable,
    export_unattended_rankings_csv,
    parse_rankings_csv,
)
from src.nfl.data.identity import (
    IdentityIngestionError,
    IdentityStatus,
    build_identity_graph,
)


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


def test_wrong_scope_preserves_existing_destination(tmp_path):
    """A blocked scoped lookup must not overwrite an existing export."""
    graph = build_identity_graph(
        players=[
            {"nflverse_id": "nfl-other", "ud_id": "b", "season": 2026}
        ],
        teams=[],
        games=[],
        appearances=[
            {
                "nflverse_id": "nfl-scoped",
                "ud_player_id": "scoped-row",
                "appearance_id": "appearance-1",
                "slate_id": "slate-1",
                "season": 2026,
            }
        ],
    )
    table = parse_rankings_csv(Path("tests/testdata/fantasy/nfl/rankings.csv"))
    # The fixture's first row is deliberately replaced with the scoped ID;
    # export rows carry no slate, so validation must fail closed.
    rows = list(table.rows)
    rows[0] = {**rows[0], "id": "scoped-row"}
    table = RankingsTable(table.columns, tuple(rows), table.source_path)
    destination = tmp_path / "existing.csv"
    sentinel = b"do-not-overwrite\n"
    destination.write_bytes(sentinel)
    with pytest.raises(Exception, match="unattended export blocked"):
        export_unattended_rankings_csv(
            table, destination, identity_graph=graph, season=2026
        )
    assert destination.read_bytes() == sentinel


def test_scoped_ud_player_resolves_only_with_matching_slate():
    graph = build_identity_graph(
        players=[],
        teams=[],
        games=[],
        appearances=[
            {
                "nflverse_id": "nfl-scoped",
                "ud_player_id": "scoped-row",
                "slate_id": "slate-1",
                "season": 2026,
            }
        ],
    )
    resolved = graph.resolve_player(
        "scoped-row", 2026, source="ud_player_id", slate_id="slate-1"
    )
    assert resolved.status is IdentityStatus.RESOLVED
    assert resolved.nflverse_id == "nfl-scoped"
    wrong_scope = graph.resolve_player(
        "scoped-row", 2026, source="ud_player_id", slate_id="slate-2"
    )
    assert wrong_scope.status is IdentityStatus.UNRESOLVED


def test_appearance_requires_matching_scope(scoped_graph):
    result = scoped_graph.resolve_appearance("app-week-1", 2026)
    assert result.status is IdentityStatus.UNRESOLVED
    assert "scope" in result.reason


def test_explicit_team_and_game_entities_resolve(scoped_graph):
    assert scoped_graph.resolve_team("ud-team-kc", 2026).gsis_id == "KC"
    assert scoped_graph.resolve_game("ud-game-1", 2026).gsis_id == "2026_01_KC_BUF"


@pytest.mark.parametrize("bad_week", [True, False, 1.5, 1.1, "1.5", "two"])
def test_malformed_week_is_rejected(bad_week):
    with pytest.raises(IdentityIngestionError, match="invalid week"):
        build_identity_graph(
            players=[
                {
                    "nflverse_id": "nfl-player",
                    "ud_player_id": "ud-player",
                    "season": 2026,
                    "week": bad_week,
                }
            ],
            teams=[],
            games=[],
        )


def test_overlapping_scoped_reference_is_rejected():
    with pytest.raises(IdentityIngestionError, match="overlapping"):
        build_identity_graph(
            players=[
                {
                    "nflverse_id": "nfl-player",
                    "ud_player_id": "ud-player",
                    "slate_id": "slate-1",
                    "season": 2026,
                    "effective_from": "2026-09-01",
                    "effective_to": "2026-09-10",
                },
                {
                    "nflverse_id": "nfl-player",
                    "ud_player_id": "ud-player",
                    "slate_id": "slate-1",
                    "season": 2026,
                    "effective_from": "2026-09-10",
                    "effective_to": "2026-09-20",
                },
            ],
            teams=[],
            games=[],
        )


def test_conflicting_aliases_are_ambiguous(scoped_graph):
    report = scoped_graph.check_material_players(
        [
            {
                "nflverse_id": "mahomes",
                "appearance_id": "app-week-1",
                "game_id": "game-1",
                "slate_id": "slate-1",
            },
            {
                "nflverse_id": "trade-player",
                "appearance_id": "app-week-1",
                "game_id": "game-1",
                "slate_id": "slate-1",
            },
        ],
        2026,
    )
    assert report.ambiguous == ("trade-player",)
