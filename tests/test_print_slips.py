"""Tests for the slip-printing CLI."""

from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pytest


def _slip_payload(
    player: str, stat_id: str, play: str, line: float
) -> dict[str, object]:
    """Return a minimal slip payload for formatting tests.

    Args:
        player: Player name shown in the report.
        stat_id: Market stat identifier.
        play: Pick direction.
        line: Sportsbook line.

    Returns:
        Slip payload compatible with the live MLB artifact format.
    """

    return {
        "slips": [
            {
                "slip_size": 1,
                "units": 1,
                "p_win": 0.62,
                "total_ev": 0.25,
                "legs": [
                    {
                        "player": player,
                        "team": "NYY",
                        "opponent": "BOS",
                        "stat_id": stat_id,
                        "play": play,
                        "line": line,
                        "prob": 0.62,
                        "ev": 0.25,
                        f"predicted_{stat_id}": line + 1.0,
                    }
                ],
            }
        ]
    }


def _write_summary_fixture(tmp_path: Path) -> Path:
    """Write a summary plus referenced slip files for tests.

    Args:
        tmp_path: Temporary test directory.

    Returns:
        Path to the generated summary artifact.
    """

    conservative_path = tmp_path / "mlb_live_2026-03-25_conservative.json"
    conservative_path.write_text(
        json.dumps(_slip_payload("Gerrit Cole", "strikeouts", "over", 8.5)),
        encoding="utf-8",
    )

    fullsend_path = tmp_path / "mlb_live_2026-03-25_fullsend.json"
    fullsend_path.write_text(
        json.dumps(_slip_payload("Aaron Nola", "hits_allowed", "under", 6.5)),
        encoding="utf-8",
    )

    summary_path = tmp_path / "mlb_live_2026-03-25_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "target_date": "2026-03-25",
                "slip_files": {
                    "conservative": str(conservative_path),
                    "fullsend": str(fullsend_path),
                },
            }
        ),
        encoding="utf-8",
    )
    return summary_path


def test_print_slips_renders_all_summary_sections(
    monkeypatch: object, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The script prints all slip sections when no tag filter is provided."""

    from scripts import print_slips as cli

    summary_path = _write_summary_fixture(tmp_path)
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda: Namespace(summary_path=summary_path, tag=None),
    )

    cli.main()
    captured = capsys.readouterr()

    assert "CONSERVATIVE SLIPS" in captured.out
    assert "FULLSEND SLIPS" in captured.out
    assert "Gerrit Cole" in captured.out
    assert "Aaron Nola" in captured.out


def test_print_slips_limits_output_to_selected_tag(
    monkeypatch: object, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The script filters output to a single slip section when requested."""

    from scripts import print_slips as cli

    summary_path = _write_summary_fixture(tmp_path)
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda: Namespace(summary_path=summary_path, tag="fullsend"),
    )

    cli.main()
    captured = capsys.readouterr()

    assert "CONSERVATIVE SLIPS" not in captured.out
    assert "FULLSEND SLIPS" in captured.out
    assert "Aaron Nola" in captured.out
    assert "Gerrit Cole" not in captured.out


def test_print_slips_raises_for_missing_summary_tag(
    monkeypatch: object, tmp_path: Path
) -> None:
    """The script fails clearly when the requested tag is unavailable."""

    from scripts import print_slips as cli

    summary_path = _write_summary_fixture(tmp_path)
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda: Namespace(summary_path=summary_path, tag="nonexistent"),
    )

    with pytest.raises(ValueError, match="Requested slip tag"):
        cli.main()
