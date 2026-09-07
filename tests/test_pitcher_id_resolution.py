from __future__ import annotations

from pathlib import Path

import pandas as pd
import scripts.generate_pitcher_dataset_from_raw as gen_script
import scripts.update_pitcher_dataset_from_raw as upd_script


def _raw_pitcher_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "player_name": [
                "Schlittler, Cam",
                "Burrows, Mike",
                "Rodr\u00edguez, Yariel",
                "Martin, Chris",
                "Martin, Cody",
            ],
            "pitcher": [693645, 681347, 684320, 111111, 222222],
        }
    )


def _fake_reverse_lookup(_ids: list[int], key_type: str = "fangraphs") -> pd.DataFrame:
    assert key_type == "fangraphs"
    return pd.DataFrame(
        {
            "key_fangraphs": [24728],
            "key_mlbam": [681347],
        }
    )


def _assert_resolution(module, fixture_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(module, "playerid_reverse_lookup", _fake_reverse_lookup)
    resolved = module.load_pitcher_ids(str(fixture_path), _raw_pitcher_df())
    resolved_map = {name: pid for name, pid in resolved}

    assert resolved_map["Cam Schlittler"] == 693645
    assert resolved_map["Mike Burrows"] == 681347
    assert resolved_map["Yariel Rodriguez"] == 684320
    # Ambiguous by last name + first initial; should not resolve.
    assert "Cam Martin" not in resolved_map


def test_generate_script_load_pitcher_ids_fallback(monkeypatch) -> None:
    fixture_path = Path("tests/testdata/top_starters_fixture.csv")
    _assert_resolution(gen_script, fixture_path, monkeypatch)


def test_update_script_load_pitcher_ids_fallback(monkeypatch) -> None:
    fixture_path = Path("tests/testdata/top_starters_fixture.csv")
    _assert_resolution(upd_script, fixture_path, monkeypatch)


def test_generate_script_load_pitcher_ids_derives_from_raw_when_csv_missing(
    monkeypatch,
) -> None:
    monkeypatch.setattr(gen_script, "playerid_reverse_lookup", _fake_reverse_lookup)
    raw_df = pd.DataFrame(
        {
            "player_name": [
                "Cole, Gerrit",
                "Cole, Gerrit",
                "Wheeler, Zack",
                "Hader, Josh",
            ],
            "pitcher": [1, 1, 2, 3],
            "inning": [1, 2, 1, 9],
            "game_date": [
                "2026-04-01",
                "2026-04-01",
                "2026-04-02",
                "2026-04-02",
            ],
            "game_pk": [100, 100, 200, 200],
        }
    )

    resolved = gen_script.load_pitcher_ids("missing.csv", raw_df)

    assert resolved == [("Gerrit Cole", 1), ("Zack Wheeler", 2)]


def test_update_script_load_pitcher_ids_derives_from_raw_when_csv_missing(
    monkeypatch,
) -> None:
    monkeypatch.setattr(upd_script, "playerid_reverse_lookup", _fake_reverse_lookup)
    raw_df = pd.DataFrame(
        {
            "player_name": [
                "Cole, Gerrit",
                "Cole, Gerrit",
                "Wheeler, Zack",
                "Hader, Josh",
            ],
            "pitcher": [1, 1, 2, 3],
            "inning": [1, 2, 1, 9],
            "game_date": [
                "2026-04-01",
                "2026-04-01",
                "2026-04-02",
                "2026-04-02",
            ],
            "game_pk": [100, 100, 200, 200],
        }
    )

    resolved = upd_script.load_pitcher_ids("missing.csv", raw_df)

    assert resolved == [("Gerrit Cole", 1), ("Zack Wheeler", 2)]
