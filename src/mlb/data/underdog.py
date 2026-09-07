"""Underdog ingestion helpers for MLB pitcher props."""

from __future__ import annotations

import http.client
import json
import re
from typing import Any
from urllib.parse import urlencode

import pandas as pd

UNDERDOG_HOST = "api.underdogfantasy.com"
UNDERDOG_PATH = "/v2/pickem_search/search_results"
_ALGOLIA_OBJECT_ID_PATTERN = re.compile(r"^PickemStat_[^\s]+$")
_LINE_COLUMNS: tuple[str, ...] = (
    "appearance_id",
    "player_ud_id",
    "player_name",
    "game_id",
    "team_id",
    "line",
    "book",
    "scheduled_at",
    "season_type",
    "stat_id",
    "over_decimal_price",
    "over_payout_multiplier",
    "over_american_price",
    "under_decimal_price",
    "under_payout_multiplier",
    "under_american_price",
)
_NUMERIC_COLUMNS: tuple[str, ...] = (
    "line",
    "over_decimal_price",
    "over_payout_multiplier",
    "under_decimal_price",
    "under_payout_multiplier",
)


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


def _stat_id_from_algolia_object_id(algolia_object_id: str) -> str:
    """Return the pick'em stat id encoded in the Algolia object id."""

    if not _ALGOLIA_OBJECT_ID_PATTERN.fullmatch(algolia_object_id):
        raise ValueError(
            "Invalid Underdog Algolia object id: expected 'PickemStat_<stat-id>'."
        )

    return algolia_object_id.split("_", maxsplit=1)[-1]


def _selection_value(options: list[dict[str, Any]], choice: str) -> dict[str, Any]:
    """Return the first matching choice option from an Underdog line payload."""

    return next((opt for opt in options if opt.get("choice") == choice), {})


def _player_full_name(player: dict[str, Any]) -> str:
    """Return the normalized full name for a player payload row."""

    first = str(player.get("first_name") or "").strip()
    last = str(player.get("last_name") or "").strip()
    return f"{first} {last}".strip()


def _resolve_appearance(
    *,
    line: dict[str, Any],
    appearance_stat: dict[str, Any],
    appearances: dict[str, dict[str, Any]],
    appearance_by_player_id: dict[str, dict[str, Any]],
    player_id_by_name: dict[str, str],
) -> tuple[str | None, dict[str, Any] | None]:
    """Resolve the best available appearance id and appearance payload."""

    appearance_id = appearance_stat.get("appearance_id") or line.get("appearance_id")
    appearance = appearances.get(str(appearance_id)) if appearance_id is not None else None
    if appearance is not None:
        return str(appearance_id), appearance

    selection_header = str(
        (line.get("options") or [{}])[0].get("selection_header") or ""
    ).strip()
    if not selection_header:
        return (
            str(appearance_id) if appearance_id is not None else None,
            None,
        )

    player_id = player_id_by_name.get(selection_header)
    if not player_id:
        return (
            str(appearance_id) if appearance_id is not None else None,
            None,
        )

    fallback_appearance = appearance_by_player_id.get(player_id)
    if fallback_appearance is None:
        return (
            str(appearance_id) if appearance_id is not None else None,
            None,
        )
    return str(fallback_appearance.get("id") or appearance_id), fallback_appearance


def _extract_lines(payload: dict[str, Any], stat_id: str) -> pd.DataFrame:
    """Parse Over/Under lines for a specific MLB pick'em stat id."""

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
    appearance_by_player_id = {
        str(appearance.get("player_id")): appearance
        for appearance in payload.get("appearances", [])
        if appearance.get("player_id")
    }
    player_id_by_name = {
        name: str(player_id)
        for player_id, player in players.items()
        if (name := _player_full_name(player))
    }

    rows: list[dict[str, Any]] = []
    for line in payload.get("over_under_lines", []):
        over_under = line.get("over_under") or {}
        appearance_stat = over_under.get("appearance_stat") or {}
        if appearance_stat.get("pickem_stat_id") != stat_id:
            continue

        appearance_id, appearance = _resolve_appearance(
            line=line,
            appearance_stat=appearance_stat,
            appearances=appearances,
            appearance_by_player_id=appearance_by_player_id,
            player_id_by_name=player_id_by_name,
        )
        if appearance is None:
            continue

        player = players.get(appearance.get("player_id")) or {}
        player_name = _player_full_name(player)
        if not player_name:
            player_name = str(
                (line.get("options") or [{}])[0].get("selection_header") or ""
            ).strip()
        if not player_name:
            continue

        options = list(line.get("options") or [])
        higher_option = _selection_value(options, "higher")
        lower_option = _selection_value(options, "lower")
        game = games.get(appearance.get("match_id")) or {}

        rows.append(
            {
                "appearance_id": str(appearance_id),
                "player_ud_id": str(appearance.get("player_id") or ""),
                "player_name": player_name,
                "game_id": str(appearance.get("match_id") or ""),
                "team_id": str(appearance.get("team_id") or ""),
                "line": line.get("stat_value"),
                "book": "Underdog",
                "scheduled_at": game.get("scheduled_at"),
                "season_type": game.get("season_type"),
                "stat_id": stat_id,
                "over_decimal_price": higher_option.get("decimal_price"),
                "over_payout_multiplier": higher_option.get("payout_multiplier"),
                "over_american_price": higher_option.get("american_price"),
                "under_decimal_price": lower_option.get("decimal_price"),
                "under_payout_multiplier": lower_option.get("payout_multiplier"),
                "under_american_price": lower_option.get("american_price"),
            }
        )

    frame = pd.DataFrame(rows, columns=_LINE_COLUMNS)
    if frame.empty:
        return frame

    for column in _NUMERIC_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def import_ud_mlb_lines(algolia_object_id: str) -> pd.DataFrame:
    """Return MLB Underdog lines as a tidy DataFrame."""

    stat_id = _stat_id_from_algolia_object_id(algolia_object_id)
    payload = _fetch_payload(algolia_object_id)
    return _extract_lines(payload, stat_id)
