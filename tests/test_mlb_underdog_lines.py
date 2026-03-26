"""Tests for MLB Underdog line ingestion helpers."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest
from src.mlb.data.underdog import _extract_lines, import_ud_mlb_lines


def _payload(stat_id: str) -> dict[str, Any]:
    return {
        "appearances": [
            {
                "id": "appearance-1",
                "player_id": "player-1",
                "match_id": "game-1",
                "team_id": "team-home",
            }
        ],
        "games": [
            {
                "id": "game-1",
                "scheduled_at": "2026-03-25T19:05:00Z",
                "season_type": "regular",
                "home_team_id": "team-home",
                "away_team_id": "team-away",
            }
        ],
        "players": [
            {
                "id": "player-1",
                "first_name": "Gerrit",
                "last_name": "Cole",
            }
        ],
        "over_under_lines": [
            {
                "stat_value": 8.5,
                "over_under": {
                    "appearance_stat": {
                        "pickem_stat_id": stat_id,
                        "appearance_id": "appearance-1",
                    }
                },
                "options": [
                    {
                        "choice": "higher",
                        "decimal_price": 1.92,
                        "payout_multiplier": 0.92,
                        "american_price": -110,
                    },
                    {
                        "choice": "lower",
                        "decimal_price": 1.88,
                        "payout_multiplier": 0.88,
                        "american_price": -115,
                    },
                ],
            }
        ],
    }


def test_extract_lines_normalizes_matching_stat() -> None:
    frame = _extract_lines(_payload("strikeouts"), "strikeouts")

    expected = pd.DataFrame(
        [
            {
                "appearance_id": "appearance-1",
                "player_ud_id": "player-1",
                "player_name": "Gerrit Cole",
                "game_id": "game-1",
                "team_id": "team-home",
                "line": 8.5,
                "book": "Underdog",
                "scheduled_at": "2026-03-25T19:05:00Z",
                "season_type": "regular",
                "stat_id": "strikeouts",
                "over_decimal_price": 1.92,
                "over_payout_multiplier": 0.92,
                "over_american_price": -110,
                "under_decimal_price": 1.88,
                "under_payout_multiplier": 0.88,
                "under_american_price": -115,
            }
        ]
    )

    pd.testing.assert_frame_equal(frame.reset_index(drop=True), expected)


def test_extract_lines_returns_empty_frame_for_non_matching_stat() -> None:
    frame = _extract_lines(_payload("strikeouts"), "bb_allowed")

    assert frame.empty


def test_import_ud_mlb_lines_uses_stat_id_from_algolia_object_id(
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}

    def fake_fetch_payload(algolia_object_id: str) -> dict[str, Any]:
        captured["algolia_object_id"] = algolia_object_id
        return _payload("earned_runs")

    monkeypatch.setattr(
        "src.mlb.data.underdog._fetch_payload",
        fake_fetch_payload,
    )

    frame = import_ud_mlb_lines(
        algolia_object_id="PickemStat_earned_runs",
    )

    assert captured["algolia_object_id"] == "PickemStat_earned_runs"
    assert frame.loc[0, "stat_id"] == "earned_runs"


def test_import_ud_mlb_lines_rejects_malformed_algolia_object_id(
    monkeypatch: Any,
) -> None:
    def fake_fetch_payload(algolia_object_id: str) -> dict[str, Any]:
        raise AssertionError(f"unexpected fetch: {algolia_object_id}")

    monkeypatch.setattr(
        "src.mlb.data.underdog._fetch_payload",
        fake_fetch_payload,
    )

    with pytest.raises(ValueError, match="PickemStat_<stat-id>"):
        import_ud_mlb_lines(algolia_object_id="earned_runs")
