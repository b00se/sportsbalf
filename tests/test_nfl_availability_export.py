from datetime import UTC, datetime
from io import StringIO
from pathlib import Path

import pytest
from src.fantasy.adapters.nfl.rankings import (
    RANKINGS_COLUMNS,
    RankingsSchemaError,
    RankingsTable,
    export_unattended_rankings_csv,
    normalize_rankings,
    parse_rankings_csv,
)
from src.nfl.data.availability import (
    AvailabilityDecision,
    AvailabilityStatus,
)
from src.nfl.data.identity import build_identity_graph

AS_OF = datetime(2026, 9, 7, 12, tzinfo=UTC)


def _table(*rows: tuple[str, str, str]) -> RankingsTable:
    values = []
    for player_id, adp, material in rows:
        values.append(
            {
                **dict(
                    zip(
                        RANKINGS_COLUMNS,
                        [
                            player_id,
                            player_id,
                            "P",
                            "Player",
                            adp,
                            "10",
                            "1000",
                            "1",
                            "QB",
                            "KC",
                            "",
                            "9",
                        ],
                    )
                ),
                "is_material": material,
            }
        )
    return RankingsTable(RANKINGS_COLUMNS, tuple(values))


def _graph(*player_ids: str):
    return build_identity_graph(
        players=[
            {"nflverse_id": player_id, "season": 2026}
            for player_id in player_ids
        ],
        teams=[], games=[],
    )


def _decision(player_id: str, *, as_of=AS_OF, status=AvailabilityStatus.ACTIVE):
    return AvailabilityDecision(
        player_id, status, 1.0, ("ud",), (), False, 2026, as_of
    )


def test_unattended_export_blocks_availability_before_writing(tmp_path: Path) -> None:
    row = "p1,ud-1,Pat,Player,1,10,1000,1,QB,KC,,9"
    csv_text = ",".join(RANKINGS_COLUMNS) + "\n" + row + "\n"
    table = normalize_rankings(parse_rankings_csv(StringIO(csv_text)))
    graph = build_identity_graph(
        players=[
            {"nflverse_id": "p1", "ud_id": "ud-1", "gsis_id": "g1", "season": 2026}
        ],
        teams=[], games=[],
    )
    destination = tmp_path / "rankings.csv"
    destination.write_bytes(b"sentinel")
    decisions = {"p1": AvailabilityDecision(
        "p1", AvailabilityStatus.UNKNOWN, 0.2, ("ud",), ("missing",), False,
        2026, datetime(2026, 9, 7, tzinfo=UTC),
    )}
    with pytest.raises(RankingsSchemaError, match="availability"):
        export_unattended_rankings_csv(
            table, destination, identity_graph=graph, season=2026,
            availability_decisions=decisions,
            availability_player_ids=("p1",), high_impact_players=("p1",),
        )
    assert destination.read_bytes() == b"sentinel"


def test_coverage_is_derived_from_material_table_rows(tmp_path: Path) -> None:
    table = _table(("p1", "1", "true"), ("p2", "2", "true"))
    with pytest.raises(RankingsSchemaError, match="exactly cover"):
        export_unattended_rankings_csv(
            table, tmp_path / "blocked.csv", identity_graph=_graph("p1", "p2"),
            season=2026, availability_decisions={"p1": _decision("p1")},
            high_impact_players=("p1",), availability_as_of_utc=AS_OF,
        )


def test_nonmaterial_rows_do_not_require_availability(tmp_path: Path) -> None:
    table = _table(("p1", "1", "true"), ("p2", "2", "false"))
    export_unattended_rankings_csv(
        table, tmp_path / "ok.csv", identity_graph=_graph("p1", "p2"), season=2026,
        availability_decisions={"p1": _decision("p1")}, high_impact_players=(),
        availability_as_of_utc=AS_OF,
    )


def test_explicit_material_flag_overrides_rank_cutoff(tmp_path: Path) -> None:
    table = _table(("p1", "100", "true"), ("p2", "1", "false"))
    export_unattended_rankings_csv(
        table, tmp_path / "ok.csv", identity_graph=_graph("p1", "p2"), season=2026,
        availability_decisions={"p1": _decision("p1")}, high_impact_players=(),
        availability_as_of_utc=AS_OF,
    )


@pytest.mark.parametrize(
    "bad_as_of",
    [None, "2026-09-07T12:00:00Z", 123, datetime(2026, 9, 7, 12)],
)
def test_export_rejects_bad_availability_as_of_before_writing(
    tmp_path: Path, bad_as_of
) -> None:
    table = _table(("p1", "1", "true"))
    destination = tmp_path / "existing.csv"
    destination.write_bytes(b"sentinel")
    with pytest.raises(RankingsSchemaError, match="timezone-aware"):
        export_unattended_rankings_csv(
            table, destination, identity_graph=_graph("p1"), season=2026,
            availability_decisions={"p1": _decision("p1")}, high_impact_players=(),
            availability_as_of_utc=bad_as_of,
        )
    assert destination.read_bytes() == b"sentinel"


def test_export_rejects_decision_timestamp_mismatch_before_writing(
    tmp_path: Path,
) -> None:
    table = _table(("p1", "1", "true"))
    destination = tmp_path / "existing.csv"
    destination.write_bytes(b"sentinel")
    with pytest.raises(RankingsSchemaError, match="as_of mismatch"):
        export_unattended_rankings_csv(
            table, destination, identity_graph=_graph("p1"), season=2026,
            availability_decisions={
                "p1": _decision("p1", as_of=AS_OF.replace(hour=13))
            },
            high_impact_players=(), availability_as_of_utc=AS_OF,
        )
    assert destination.read_bytes() == b"sentinel"
