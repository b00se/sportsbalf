from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.mlb import pipeline


def _make_latest_games(names: list[str]) -> pd.DataFrame:
    games = pd.DataFrame(
        {
            "pitcher_id": list(range(1, len(names) + 1)),
            "pitcher_name": names,
            "pitcher_team": ["WSH"] * len(names),
            "game_date": pd.to_datetime(["2025-09-19"] * len(names)),
            "opponent_team": ["NYM"] * len(names),
            "opponent_k_pct": [0.25] * len(names),
            "opponent_k_rate": [0.25] * len(names),
            "park_factor_K": [1.0] * len(names),
        }
    )
    return pipeline._latest_games(games)


def test_build_prediction_rows_matches_short_first_name(monkeypatch) -> None:
    latest_games = _make_latest_games(["Cameron Schlittler"])
    lines = pd.DataFrame({"player": ["Cam Schlittler"]})
    lines["name_key"] = lines["player"].map(pipeline._normalize_name)

    monkeypatch.setattr(pipeline, "_next_opponent", lambda *_args, **_kwargs: None)

    rows = pipeline._build_prediction_rows(
        lines=lines,
        latest_games=latest_games,
        target_date=datetime(2025, 9, 20),
        park_lookup={"WSH": 1.0},
        opponent_lookup={"NYM": {"opponent_k_pct": 0.25, "opponent_k_rate": 0.25}},
        fallback_opponent={"opponent_k_pct": 0.25, "opponent_k_rate": 0.25},
    )

    assert len(rows) == 1
    assert rows.iloc[0]["player"] == "Cam Schlittler"
    assert rows.iloc[0]["pitcher_name"] == "Cameron Schlittler"


def test_build_prediction_rows_keeps_ambiguous_aliases_missing(monkeypatch) -> None:
    latest_games = _make_latest_games(["Chris Martin", "Cody Martin"])
    lines = pd.DataFrame({"player": ["Cam Martin"]})
    lines["name_key"] = lines["player"].map(pipeline._normalize_name)

    monkeypatch.setattr(pipeline, "_next_opponent", lambda *_args, **_kwargs: None)

    rows = pipeline._build_prediction_rows(
        lines=lines,
        latest_games=latest_games,
        target_date=datetime(2025, 9, 20),
        park_lookup={"WSH": 1.0},
        opponent_lookup={"NYM": {"opponent_k_pct": 0.25, "opponent_k_rate": 0.25}},
        fallback_opponent={"opponent_k_pct": 0.25, "opponent_k_rate": 0.25},
    )

    assert rows.empty
