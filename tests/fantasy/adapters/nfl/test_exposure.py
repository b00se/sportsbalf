from io import StringIO
from pathlib import Path

import pytest
from src.fantasy.adapters.nfl.exposure import (
    EXPOSURE_COLUMNS,
    ExposureSchemaError,
    parse_exposure_csv,
    reconstruct_user_entries,
)


def _csv(rows: list[str]) -> str:
    return ",".join(EXPOSURE_COLUMNS) + "\n" + "\n".join(rows) + "\n"


FIXTURE = Path(__file__).parents[3] / "testdata" / "fantasy" / "nfl" / "exposure.csv"


def test_exposure_reconstructs_entries_in_pick_order_and_preserves_nullable_fields(
) -> None:
    export = parse_exposure_csv(FIXTURE)
    entries = reconstruct_user_entries(export)
    assert [p.pick_number for p in entries["E1"].picks] == [1, 2, 3, 4, 5, 6]
    assert entries["E1"].picks[0].player_name == "Player E11"
    assert entries["E1"].picks[0].raw["Payout"] is None
    assert len(entries) == 6
    assert len({entry.draft_id for entry in entries.values()}) == 6
    assert {len(entry.picks) for entry in entries.values()} == {6}


def test_exposure_rejects_duplicate_pick_and_mixed_pool() -> None:
    values = [""] * len(EXPOSURE_COLUMNS)
    fields = {
        "Draft Entry": "E1",
        "Pick Number": "1",
        "Pool ID": "P1",
        "Draft ID": "D1",
        "Player Name": "A",
    }
    for key, value in fields.items():
        values[EXPOSURE_COLUMNS.index(key)] = value
    duplicate = ",".join(values)
    values[EXPOSURE_COLUMNS.index("Pool ID")] = "P2"
    mixed = ",".join(values)
    with pytest.raises(ExposureSchemaError, match="duplicate|pool"):
        reconstruct_user_entries(parse_exposure_csv(StringIO(_csv([duplicate, mixed]))))
