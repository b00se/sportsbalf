"""Tests for fantasy provider-player mapping resolution."""

from __future__ import annotations

import pandas as pd
from src.fantasy.core.mapping import resolve_provider_player_ids


def test_mapping_resolver_statuses() -> None:
    mapping_frame = pd.DataFrame(
        [
            {
                "provider": "underdog",
                "sport": "mlb",
                "provider_player_id": "1",
                "internal_player_id": "mlb-1",
                "provider_player_name": "Alpha",
                "canonical_name": "Alpha",
                "is_active": True,
                "source": "fixture",
                "updated_at_utc": "2026-02-12T00:00:00Z",
            },
            {
                "provider": "underdog",
                "sport": "mlb",
                "provider_player_id": "2",
                "internal_player_id": "mlb-2a",
                "provider_player_name": "Beta",
                "canonical_name": "Beta",
                "is_active": True,
                "source": "fixture",
                "updated_at_utc": "2026-02-12T00:00:00Z",
            },
            {
                "provider": "underdog",
                "sport": "mlb",
                "provider_player_id": "2",
                "internal_player_id": "mlb-2b",
                "provider_player_name": "Beta",
                "canonical_name": "Beta",
                "is_active": True,
                "source": "fixture",
                "updated_at_utc": "2026-02-12T00:00:00Z",
            },
        ]
    )

    resolved = resolve_provider_player_ids(
        provider="underdog",
        sport="mlb",
        provider_player_ids=("1", "2", "3"),
        mapping_frame=mapping_frame,
    )

    statuses = dict(zip(resolved["provider_player_id"], resolved["status"]))
    internal_ids = dict(
        zip(resolved["provider_player_id"], resolved["internal_player_id"])
    )

    assert statuses["1"] == "mapped"
    assert statuses["2"] == "duplicate_provider_id"
    assert statuses["3"] == "unmapped"
    assert internal_ids["1"] == "mlb-1"
    assert pd.isna(internal_ids["2"])
    assert pd.isna(internal_ids["3"])
