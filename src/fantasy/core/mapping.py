"""Player-id mapping loading and resolution helpers."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from src.utils.io import read_csv

REQUIRED_MAPPING_COLUMNS: tuple[str, ...] = (
    "provider",
    "sport",
    "provider_player_id",
    "internal_player_id",
    "provider_player_name",
    "canonical_name",
    "is_active",
    "source",
    "updated_at_utc",
)


def load_provider_player_mapping(path: str) -> pd.DataFrame:
    """Load provider-player mapping data from CSV/Parquet path."""

    frame = read_csv(path)
    missing = [
        column for column in REQUIRED_MAPPING_COLUMNS if column not in frame.columns
    ]
    if missing:
        raise ValueError(
            "Mapping file is missing required columns: " f"{', '.join(sorted(missing))}"
        )
    return frame.copy()


def resolve_provider_player_ids(
    *,
    provider: str,
    sport: str,
    provider_player_ids: Sequence[str],
    mapping_frame: pd.DataFrame,
) -> pd.DataFrame:
    """Resolve provider player ids to internal ids with stable status values.

    Status values:
    - `mapped`: exactly one active mapping exists.
    - `unmapped`: no active mapping exists.
    - `duplicate_provider_id`: multiple active mappings exist for one provider id.
    """

    normalized = mapping_frame.copy()
    for column in REQUIRED_MAPPING_COLUMNS:
        if column not in normalized.columns:
            raise ValueError(
                "Mapping frame is missing required columns: "
                f"{', '.join(REQUIRED_MAPPING_COLUMNS)}"
            )

    provider_key = provider.strip().lower()
    sport_key = sport.strip().lower()
    normalized["provider"] = normalized["provider"].astype(str).str.strip().str.lower()
    normalized["sport"] = normalized["sport"].astype(str).str.strip().str.lower()
    normalized["provider_player_id"] = (
        normalized["provider_player_id"].astype(str).str.strip()
    )

    scoped = normalized[
        (normalized["provider"] == provider_key)
        & (normalized["sport"] == sport_key)
        & (normalized["is_active"].astype(bool))
    ]

    grouped = scoped.groupby("provider_player_id")["internal_player_id"].agg(list)
    duplicates = {
        player_id
        for player_id, ids in grouped.items()
        if len({str(value) for value in ids}) > 1
    }
    single_map = {
        player_id: str(values[0])
        for player_id, values in grouped.items()
        if len({str(value) for value in values}) == 1
    }

    rows: list[dict[str, object]] = []
    for provider_player_id in provider_player_ids:
        player_id = str(provider_player_id).strip()
        if player_id in duplicates:
            status = "duplicate_provider_id"
            internal_id: str | None = None
        elif player_id in single_map:
            status = "mapped"
            internal_id = single_map[player_id]
        else:
            status = "unmapped"
            internal_id = None

        rows.append(
            {
                "provider": provider_key,
                "sport": sport_key,
                "provider_player_id": player_id,
                "internal_player_id": internal_id,
                "status": status,
            }
        )

    return pd.DataFrame(rows)
