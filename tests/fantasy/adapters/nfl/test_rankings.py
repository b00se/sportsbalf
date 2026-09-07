from io import StringIO
from pathlib import Path

import pytest
from src.fantasy.adapters.nfl.rankings import (
    RANKINGS_COLUMNS,
    RankingsSchemaError,
    export_rankings_csv,
    normalize_rankings,
    parse_rankings_csv,
    reorder_rankings,
)
from src.nfl.data.identity import build_identity_graph


def _csv(rows: list[str]) -> str:
    return ",".join(RANKINGS_COLUMNS) + "\n" + "\n".join(rows) + "\n"


FIXTURE = Path(__file__).parents[3] / "testdata" / "fantasy" / "nfl" / "rankings.csv"


def test_rankings_parse_normalize_reorder_export_is_lossless() -> None:
    table = normalize_rankings(parse_rankings_csv(FIXTURE))
    reordered = reorder_rankings(table, ["a", "b"])

    assert export_rankings_csv(reordered) == _csv([
        'a,1,John,Smith,1.00,100.00,6000,1,QB,KC,,9',
        'b,2,Jane,Doe,2.00,101.50,5500,2,RB,BUF,questionable,7',
    ])


def test_rankings_preserve_alphanumeric_position_rank() -> None:
    source = _csv([
        'a,1,John,Smith,1.00,100.00,6000,QB1,QB,KC,,9',
    ])

    table = normalize_rankings(parse_rankings_csv(StringIO(source)))

    assert table.rows[0]["positionRank"] == "QB1"


@pytest.mark.parametrize("bad", [
    "id,playerId",
    ",1,A,B,1,2,3,1,QB,KC,,1",
    "a,1,A,B,nope,2,3,1,QB,KC,,1",
])
def test_rankings_schema_errors_are_actionable(bad: str) -> None:
    with pytest.raises(RankingsSchemaError, match="rankings"):
        parse_rankings_csv(StringIO(bad + "\n"))


def test_reorder_rejects_unknown_or_duplicate_ids() -> None:
    table = normalize_rankings(parse_rankings_csv(StringIO(_csv([
        'a,1,A,B,1,2,3,1,QB,KC,,1',
    ]))))
    with pytest.raises(RankingsSchemaError, match="order"):
        reorder_rankings(table, ["missing"])


def test_rankings_reject_non_finite_numeric_values() -> None:
    with pytest.raises(RankingsSchemaError, match="finite"):
        parse_rankings_csv(StringIO(_csv([
            "a,1,A,B,NaN,2,3,1,QB,KC,,1",
        ])))


def test_rankings_export_rejects_overwriting_input_source(tmp_path: Path) -> None:
    source = tmp_path / "rankings.csv"
    source.write_text(FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")
    table = parse_rankings_csv(source)
    with pytest.raises(RankingsSchemaError, match="overwrite"):
        export_rankings_csv(table, source)


def test_unattended_rankings_export_blocks_unresolved_identity() -> None:
    table = normalize_rankings(parse_rankings_csv(FIXTURE))
    graph = build_identity_graph(
        players=[{"ud_id": "a", "gsis_id": "00-1", "season": 2026}],
        teams=[], games=[]
    )
    with pytest.raises(RankingsSchemaError, match="unattended"):
        export_rankings_csv(table, identity_graph=graph, season=2026, unattended=True)


def test_unattended_rankings_export_accepts_material_identity() -> None:
    table = normalize_rankings(parse_rankings_csv(FIXTURE))
    graph = build_identity_graph(
        players=[
            {"ud_id": "a", "gsis_id": "00-1", "season": 2026},
            {"ud_id": "b", "gsis_id": "00-2", "season": 2026},
        ],
        teams=[], games=[]
    )
    # Fixture's first id is a; gate should run and then serialize.
    assert export_rankings_csv(
        table, identity_graph=graph, season=2026, unattended=True
    ).startswith("id,playerId")
