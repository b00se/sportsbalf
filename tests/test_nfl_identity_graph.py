"""Offline contract tests for the Underdog-to-GSIS identity graph."""

import hashlib
import json
from pathlib import Path

import pytest
from src.nfl.data.identity import (
    IdentityGraph,
    IdentityStatus,
    build_identity_graph,
)


@pytest.fixture
def graph() -> IdentityGraph:
    return build_identity_graph(
        players=[
            {
                "ud_id": "ud-p1",
                "gsis_id": "00-001",
                "name": "A. Runner",
                "season": 2026,
            },
            {
                "ud_id": "ud-p2",
                "gsis_id": "00-002",
                "name": "B. Catcher",
                "season": 2026,
            },
            {
                "ud_id": "ud-old",
                "gsis_id": "00-001",
                "name": "A. Runner",
                "season": 2025,
            },
        ],
        teams=[{"ud_id": "ud-t1", "gsis_id": "KC", "season": 2026}],
        games=[{"ud_id": "ud-g1", "gsis_id": "2026_01_KC_BUF", "season": 2026}],
    )


def test_frozen_fixture_hash_and_shape() -> None:
    path = Path("tests/testdata/nfl_identity_graph_fixture.json")
    assert hashlib.sha256(path.read_bytes()).hexdigest() == (
        "db699cc9f5811e6dbd79c40e67abbb521ccbfa4cfe90afa07e21650596cdc3ed"
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    fixture_graph = build_identity_graph(**payload)
    assert fixture_graph.resolve_player("ud-p1", 2026).gsis_id == "00-001"


def test_id_resolution_is_stable_and_season_scoped(graph: IdentityGraph) -> None:
    result = graph.resolve_player(ud_id="ud-p1", season=2026)
    assert result.status is IdentityStatus.RESOLVED
    assert result.gsis_id == "00-001"
    assert (
        graph.resolve_player(ud_id="ud-p1", season=2025).status
        is IdentityStatus.UNRESOLVED
    )
    assert graph.resolve_team(ud_id="ud-t1", season=2026).gsis_id == "KC"
    assert graph.resolve_game(ud_id="ud-g1", season=2026).gsis_id == "2026_01_KC_BUF"


def test_missing_or_name_only_inputs_fail_closed(graph: IdentityGraph) -> None:
    assert (
        graph.resolve_player(ud_id="unknown", season=2026).status
        is IdentityStatus.UNRESOLVED
    )
    result = graph.resolve_player(name="A. Runner", season=2026)
    assert result.status is IdentityStatus.UNRESOLVED
    assert "name-only" in result.reason


def test_duplicate_ids_are_ambiguous(graph: IdentityGraph) -> None:
    graph = build_identity_graph(
        players=[
            {"ud_id": "ud-x", "gsis_id": "00-010", "season": 2026},
            {"ud_id": "ud-x", "gsis_id": "00-011", "season": 2026},
        ],
        teams=[],
        games=[],
    )
    result = graph.resolve_player(ud_id="ud-x", season=2026)
    assert result.status is IdentityStatus.AMBIGUOUS
    assert result.gsis_id is None


def test_material_resolution_report_counts_unresolved_and_ambiguous(
    graph: IdentityGraph,
) -> None:
    report = graph.check_material_players(
        [{"ud_id": "ud-p1"}, {"ud_id": "missing"}, {"ud_id": "ud-p2"}], season=2026
    )
    assert report.total == 3
    assert report.resolved == 2
    assert report.unresolved == ("missing",)
    assert report.ready is False
