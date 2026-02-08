"""Underdog ingestion helpers for NFL QB pass attempts."""

from __future__ import annotations

import http.client
import json
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlencode

import pandas as pd

PASS_ATTEMPTS_ALGOLIA_ID = "PickemStat_de868934-c920-405c-b827-693c15aa47a1"
UNDERDOG_HOST = "api.underdogfantasy.com"
UNDERDOG_PATH = "/v2/pickem_search/search_results"


def _fetch_payload(algolia_object_id: str) -> dict[str, Any]:
    """Fetch the raw Underdog payload for the given Algolia object id."""
    query = urlencode({"sport_id": "HOME", "algolia_object_id": algolia_object_id})
    connection = http.client.HTTPSConnection(UNDERDOG_HOST, timeout=15)
    try:
        connection.request(
            "GET",
            f"{UNDERDOG_PATH}?{query}",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        response = connection.getresponse()
        if response.status >= 400:
            raise RuntimeError(
                f"Underdog API request failed with status {response.status}."
            )
        payload = response.read()
        return json.loads(payload.decode("utf-8"))
    finally:
        connection.close()


def _extract_lines(payload: dict[str, Any], stat_id: str) -> pd.DataFrame:
    """Parse Over/Under lines for the specified pick'em stat id."""
    appearances = {
        appearance.get("id"): appearance
        for appearance in payload.get("appearances", [])
        if appearance.get("id")
    }
    games = {
        game.get("id"): game for game in payload.get("games", []) if game.get("id")
    }
    players = {
        player.get("id"): player
        for player in payload.get("players", [])
        if player.get("id")
    }

    rows: list[dict[str, Any]] = []
    for line in payload.get("over_under_lines", []):
        over_under = line.get("over_under", {})
        appearance_stat = over_under.get("appearance_stat") or {}
        if appearance_stat.get("pickem_stat_id") != stat_id:
            continue

        appearance_id = appearance_stat.get("appearance_id")
        if not appearance_id:
            continue
        appearance = appearances.get(appearance_id)
        if appearance is None:
            continue

        player = players.get(appearance.get("player_id"))
        first = (player or {}).get("first_name") or ""
        last = (player or {}).get("last_name") or ""
        player_name = f"{first} {last}".strip()
        if not player_name:
            higher_option = next(
                (
                    opt
                    for opt in line.get("options", [])
                    if opt.get("choice") == "higher"
                ),
                None,
            )
            player_name = (higher_option or {}).get("selection_header")
        if not player_name:
            continue

        options = line.get("options", [])
        higher_option = next(
            (opt for opt in options if opt.get("choice") == "higher"), None
        )
        lower_option = next(
            (opt for opt in options if opt.get("choice") == "lower"), None
        )

        game = games.get(appearance.get("match_id"))
        rows.append(
            {
                "appearance_id": appearance_id,
                "player_ud_id": appearance.get("player_id"),
                "player_name": player_name,
                "game_id": appearance.get("match_id"),
                "team_id": appearance.get("team_id"),
                "line": line.get("stat_value"),
                "book": "Underdog",
                "scheduled_at": (game or {}).get("scheduled_at"),
                "season_type": (game or {}).get("season_type"),
                "over_decimal_price": (higher_option or {}).get("decimal_price"),
                "over_payout_multiplier": (higher_option or {}).get(
                    "payout_multiplier"
                ),
                "over_american_price": (higher_option or {}).get("american_price"),
                "under_decimal_price": (lower_option or {}).get("decimal_price"),
                "under_payout_multiplier": (lower_option or {}).get(
                    "payout_multiplier"
                ),
                "under_american_price": (lower_option or {}).get("american_price"),
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame

    numeric_cols = [
        "line",
        "over_decimal_price",
        "over_payout_multiplier",
        "under_decimal_price",
        "under_payout_multiplier",
    ]
    for col in numeric_cols:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame["game_id"] = frame["game_id"].astype(str)
    return frame


def import_ud_pass_attempt_lines(
    years: Sequence[int] | None = None,
    algolia_object_id: str = PASS_ATTEMPTS_ALGOLIA_ID,
) -> pd.DataFrame:
    """Return Underdog pass attempt lines as a tidy DataFrame."""
    _ = years  # years are not used yet but kept for interface symmetry
    stat_id = algolia_object_id.split("_", 1)[-1]
    payload = _fetch_payload(algolia_object_id)
    return _extract_lines(payload, stat_id)
