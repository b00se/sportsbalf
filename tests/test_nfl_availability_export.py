from io import StringIO
from pathlib import Path

import pytest
from src.fantasy.adapters.nfl.rankings import (
    RANKINGS_COLUMNS,
    RankingsSchemaError,
    export_unattended_rankings_csv,
    normalize_rankings,
    parse_rankings_csv,
)
from src.nfl.data.availability import (
    AvailabilityDecision,
    AvailabilityStatus,
)
from src.nfl.data.identity import build_identity_graph


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
        "p1", AvailabilityStatus.UNKNOWN, 0.2, ("ud",), ("missing",), False
    )}
    with pytest.raises(RankingsSchemaError, match="availability"):
        export_unattended_rankings_csv(
            table, destination, identity_graph=graph, season=2026,
            availability_decisions=decisions,
            availability_player_ids=("p1",), high_impact_players=("p1",),
        )
    assert destination.read_bytes() == b"sentinel"
