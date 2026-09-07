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
