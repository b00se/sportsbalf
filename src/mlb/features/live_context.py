"""Live context service for MLB pregame feature fetching and cache fallback."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd

from src.mlb.features.feature_store import (
    LIVE_CONTEXT_FEATURE_COLUMNS,
    ensure_live_feature_defaults,
)
from src.mlb.features.venue import normalize_venue_payload
from src.mlb.features.weather import normalize_weather_payload

try:  # pragma: no cover - optional dependency
    from pybaseball import schedule_and_record
except Exception:  # pragma: no cover - optional dependency
    schedule_and_record = None

logger = logging.getLogger(__name__)


def _coerce_schedule_date(value: object, year: int) -> pd.Timestamp:
    """Parse pybaseball schedule date strings into timestamps."""

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.notna(parsed):
        return parsed
    return pd.to_datetime(f"{value} {year}", errors="coerce")


def _extract_wind_components(raw_wind: object) -> tuple[object, object]:
    """Split free-form wind text into speed and direction hints."""

    if not isinstance(raw_wind, str):
        return raw_wind, None
    tokens = [part.strip() for part in raw_wind.split(",") if part.strip()]
    if len(tokens) == 1:
        return tokens[0], None
    return tokens[0], tokens[1]


@dataclass(slots=True)
class LiveFeatureFetchResult:
    """Container for fetched live context frame and provenance metadata."""

    frame: pd.DataFrame
    metadata: dict[str, Any]


class LiveContextService:
    """Fetch/cache normalized live MLB context features for inference."""

    def __init__(
        self,
        config: dict[str, Any] | None = None,
    ) -> None:
        settings = config or {}
        weather_cfg = settings.get("weather") if isinstance(settings, dict) else {}
        if not isinstance(weather_cfg, dict):
            weather_cfg = {}

        self.enabled = bool(settings.get("enabled", False))
        self.source_policy = str(settings.get("source_policy", "pybaseball_first"))
        self.fallback_policy = str(settings.get("fallback_policy", "stale_cache"))
        self.cache_path = Path(
            str(settings.get("cache_path", "data/cache/mlb_live_features.parquet"))
        )
        self.cache_ttl_hours = int(settings.get("cache_ttl_hours", 24))
        self.weather_enabled = bool(weather_cfg.get("enabled", True))
        self.primary_weather_source = str(
            weather_cfg.get("primary_source", "pybaseball_team_game_logs")
        )
        self.secondary_weather_source = str(
            weather_cfg.get("secondary_source", "statsapi_game_feed")
        )

    def fetch(
        self, rows: pd.DataFrame, target_date: datetime
    ) -> LiveFeatureFetchResult:
        """Fetch live context features for inference rows.

        Args:
            rows: Prediction rows containing pitcher/team context.
            target_date: Inference slate date.

        Returns:
            Normalized feature frame and run metadata.
        """

        keys = ["pitcher_id", "opponent_team", "game_pk"]
        base_frame = rows[[column for column in keys if column in rows.columns]].copy()
        if base_frame.empty:
            return LiveFeatureFetchResult(
                frame=ensure_live_feature_defaults(base_frame),
                metadata={
                    "live_feature_set_version": "v1",
                    "live_feature_sources": ["neutral_defaults"],
                    "live_fetch_timestamp": datetime.now(UTC).isoformat(),
                    "cache_age_hours": None,
                    "cache_status": "empty_input",
                    "stale_cache_usage_pct": 0.0,
                },
            )

        if not self.enabled:
            return LiveFeatureFetchResult(
                frame=ensure_live_feature_defaults(base_frame),
                metadata={
                    "live_feature_set_version": "v1",
                    "live_feature_sources": ["disabled"],
                    "live_fetch_timestamp": datetime.now(UTC).isoformat(),
                    "cache_age_hours": None,
                    "cache_status": "disabled",
                    "stale_cache_usage_pct": 0.0,
                },
            )

        requested_key_columns = [
            col
            for col in ("pitcher_id", "opponent_team", "game_pk")
            if col in base_frame.columns
        ]

        def _lookup_cached_rows(
            requested: pd.DataFrame, cached: pd.DataFrame
        ) -> tuple[pd.DataFrame, float]:
            """Match cached rows to requested keys and return coverage ratio."""

            if requested.empty or cached.empty:
                return pd.DataFrame(), 0.0
            cache_keys = [col for col in requested.columns if col in cached.columns]
            if not cache_keys:
                return pd.DataFrame(), 0.0

            stale_lookup = cached.copy()
            if "fetched_at" in stale_lookup.columns:
                parsed_fetched_at = pd.to_datetime(
                    stale_lookup["fetched_at"], errors="coerce"
                )
                stale_lookup = stale_lookup.assign(
                    _fetched_sort=parsed_fetched_at
                ).sort_values("_fetched_sort")

            stale_lookup = stale_lookup.drop_duplicates(subset=cache_keys, keep="last")
            matched_rows = requested.merge(
                stale_lookup,
                on=cache_keys,
                how="left",
                indicator=True,
            )
            matched_rows = matched_rows.loc[matched_rows["_merge"] == "both"].drop(
                columns=["_merge", "_fetched_sort"],
                errors="ignore",
            )
            usage_pct = float(len(matched_rows) / max(len(base_frame), 1))
            return matched_rows, usage_pct

        fetched = self._fetch_primary(base_frame, target_date)
        used_secondary = False
        used_cache = False
        cache_age_hours: float | None = None
        cache_status = "fresh"
        stale_cache_usage_pct = 0.0
        if self.weather_enabled:
            secondary_rows = pd.DataFrame()
            if fetched.empty:
                secondary_request = base_frame[
                    [
                        col
                        for col in ("pitcher_id", "opponent_team")
                        if col in base_frame.columns
                    ]
                ]
                if not secondary_request.empty:
                    secondary_rows = self._fetch_secondary(
                        secondary_request, target_date
                    )
                if not secondary_rows.empty:
                    used_secondary = True
                    fetched = secondary_rows.copy()
            else:
                missing_weather = fetched[
                    [
                        col
                        for col in ("game_temp_f", "humidity_pct", "wind_speed_mph")
                        if col in fetched.columns
                    ]
                ].isna().any(axis=1)
                needs_roof_fill = (
                    fetched["roof_state"].isna()
                    | fetched["roof_state"]
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .isin({"", "unknown", "none", "nan"})
                ) if "roof_state" in fetched.columns else pd.Series(
                    False, index=fetched.index
                )
                needs_secondary = missing_weather | needs_roof_fill
                if needs_secondary.any():
                    secondary_rows = self._fetch_secondary(
                        fetched.loc[
                            needs_secondary,
                            [
                                col
                                for col in ("pitcher_id", "opponent_team")
                                if col in fetched.columns
                            ],
                        ],
                        target_date,
                    )
                if not secondary_rows.empty:
                    used_secondary = True
                    merge_keys = [
                        col
                        for col in ("pitcher_id", "opponent_team")
                        if col in fetched.columns and col in secondary_rows.columns
                    ]
                    fetched = fetched.merge(
                        secondary_rows,
                        on=merge_keys,
                        how="left",
                        suffixes=("", "_secondary"),
                    )
                    for col in [
                        "game_temp_f",
                        "humidity_pct",
                        "wind_speed_mph",
                        "wind_out_to_cf_flag",
                    ]:
                        secondary_col = f"{col}_secondary"
                        if secondary_col in fetched.columns:
                            fetched[col] = fetched[col].fillna(fetched[secondary_col])
                            fetched.drop(columns=[secondary_col], inplace=True)
                    roof_secondary_col = "roof_state_secondary"
                    if roof_secondary_col in fetched.columns:
                        roof_fill_mask = fetched["roof_state"].isna() | fetched[
                            "roof_state"
                        ].astype(str).str.strip().str.lower().isin(
                            {"", "unknown", "none", "nan"}
                        )
                        fetched.loc[roof_fill_mask, "roof_state"] = fetched.loc[
                            roof_fill_mask, roof_secondary_col
                        ]
                        fetched.drop(columns=[roof_secondary_col], inplace=True)
                    fetched["weather_known_flag"] = (
                        fetched[["game_temp_f", "humidity_pct", "wind_speed_mph"]]
                        .notna()
                        .all(axis=1)
                        .astype(int)
                    )

        if not fetched.empty:
            key_columns = [
                col for col in requested_key_columns if col in fetched.columns
            ]
            if key_columns:
                requested_keys = base_frame[key_columns].drop_duplicates()
                fetched_keys = fetched[key_columns].drop_duplicates()
                missing_keys = requested_keys.merge(
                    fetched_keys,
                    on=key_columns,
                    how="left",
                    indicator=True,
                )
                missing_keys = missing_keys.loc[
                    missing_keys["_merge"] == "left_only", key_columns
                ]
                if not missing_keys.empty:
                    cached = self._load_cache()
                    if not cached.empty:
                        cache_age_hours = self._cache_age_hours(cached)
                        if (
                            cache_age_hours is not None
                            and cache_age_hours <= self.cache_ttl_hours
                        ):
                            cached_rows, cached_usage_pct = _lookup_cached_rows(
                                missing_keys, cached
                            )
                            if not cached_rows.empty:
                                used_cache = True
                                cache_status = "partial_stale_fallback"
                                cached_rows["is_stale"] = 1
                                cached_rows["source_used"] = "cache"
                                fetched = pd.concat(
                                    [fetched, cached_rows],
                                    ignore_index=True,
                                    sort=False,
                                )
                                stale_cache_usage_pct = cached_usage_pct

        if fetched.empty:
            cached = self._load_cache()
            if not cached.empty:
                cache_age_hours = self._cache_age_hours(cached)
                if (
                    cache_age_hours is not None
                    and cache_age_hours <= self.cache_ttl_hours
                ):
                    logger.warning("Using stale cache fallback for live features.")
                    requested = base_frame[requested_key_columns].drop_duplicates()
                    stale, stale_usage_pct = _lookup_cached_rows(requested, cached)
                    if not stale.empty:
                        stale["is_stale"] = 1
                        return LiveFeatureFetchResult(
                            frame=ensure_live_feature_defaults(stale),
                            metadata={
                                "live_feature_set_version": "v1",
                                "live_feature_sources": ["cache"],
                                "live_fetch_timestamp": datetime.now(UTC).isoformat(),
                                "cache_age_hours": cache_age_hours,
                                "cache_status": "stale_fallback",
                                "stale_cache_usage_pct": stale_usage_pct,
                            },
                        )

            return LiveFeatureFetchResult(
                frame=ensure_live_feature_defaults(base_frame),
                metadata={
                    "live_feature_set_version": "v1",
                    "live_feature_sources": ["neutral_defaults"],
                    "live_fetch_timestamp": datetime.now(UTC).isoformat(),
                    "cache_age_hours": None,
                    "cache_status": "neutral_fallback",
                    "stale_cache_usage_pct": 0.0,
                },
            )

        default_source = "secondary" if used_secondary else "primary"
        if "source_used" not in fetched.columns:
            fetched["source_used"] = default_source
        else:
            fetched["source_used"] = (
                fetched["source_used"].fillna(default_source).astype(str)
            )

        if "is_stale" not in fetched.columns:
            fetched["is_stale"] = 0
        else:
            fetched["is_stale"] = (
                pd.to_numeric(fetched["is_stale"], errors="coerce")
                .fillna(0)
                .astype(int)
            )

        stale_mask = fetched["is_stale"].eq(1)
        fetched_at = datetime.now(UTC).isoformat()
        if "fetched_at" not in fetched.columns:
            fetched["fetched_at"] = pd.NA
        fetched.loc[~stale_mask, "fetched_at"] = fetched_at
        if cache_status == "fresh":
            cache_status = "fresh_with_cache" if used_cache else "fresh"
        self._save_cache(fetched)

        return LiveFeatureFetchResult(
            frame=ensure_live_feature_defaults(fetched),
            metadata={
                "live_feature_set_version": "v1",
                "live_feature_sources": [
                    source
                    for source in [
                        self.primary_weather_source,
                        self.secondary_weather_source if used_secondary else None,
                        "cache" if used_cache else None,
                    ]
                    if source is not None
                ],
                "live_fetch_timestamp": datetime.now(UTC).isoformat(),
                "cache_age_hours": cache_age_hours if used_cache else 0.0,
                "cache_status": cache_status,
                "stale_cache_usage_pct": stale_cache_usage_pct,
            },
        )

    def _fetch_primary(self, rows: pd.DataFrame, target_date: datetime) -> pd.DataFrame:
        """Fetch pybaseball-first daily context with robust failure fallback."""

        if schedule_and_record is None:
            return pd.DataFrame()

        records: list[dict[str, Any]] = []
        for row in rows.itertuples(index=False):
            opponent = str(getattr(row, "opponent_team", "")).strip()
            if not opponent:
                continue
            payload: dict[str, Any] = {
                "pitcher_id": getattr(row, "pitcher_id", None),
                "opponent_team": opponent,
            }
            try:
                schedule = schedule_and_record(target_date.year, opponent)
                if schedule is not None and not schedule.empty:
                    schedule = schedule.copy()
                    if "Date" not in schedule.columns:
                        continue
                    schedule["game_date"] = schedule["Date"].map(
                        lambda value: _coerce_schedule_date(value, target_date.year)
                    )
                    target_day = pd.Timestamp(target_date).normalize()
                    same_day = schedule[
                        schedule["game_date"].dt.normalize() == target_day
                    ]
                    if same_day.empty:
                        continue
                    weather_row = same_day.iloc[0].to_dict()
                    wind_speed, wind_direction = _extract_wind_components(
                        weather_row.get("Wind")
                    )
                    weather = normalize_weather_payload(
                        {
                            "game_temp_f": weather_row.get("Temp")
                            or weather_row.get("temperature"),
                            "wind_speed_mph": wind_speed
                            or weather_row.get("wind_speed_mph"),
                            "humidity_pct": weather_row.get("Humidity")
                            or weather_row.get("humidity_pct"),
                            "wind_direction": weather_row.get("WindDir")
                            or wind_direction
                            or weather_row.get("wind_direction"),
                        },
                        use_defaults=False,
                    )
                    payload.update(weather)
                else:
                    payload.update(normalize_weather_payload({}, use_defaults=False))

                venue = normalize_venue_payload({"roof_state": None})
                payload.update(venue)
                weather_values = [
                    payload.get("game_temp_f"),
                    payload.get("humidity_pct"),
                    payload.get("wind_speed_mph"),
                ]
                payload["weather_known_flag"] = int(
                    all(pd.notna(value) for value in weather_values)
                )
            except Exception:
                logger.debug("Primary live feature fetch failed for team=%s", opponent)
                continue
            records.append(payload)

        if not records:
            return pd.DataFrame()

        frame = pd.DataFrame(records)
        for col in LIVE_CONTEXT_FEATURE_COLUMNS:
            if col not in frame.columns:
                frame[col] = pd.NA
        return frame

    def _fetch_secondary(
        self, rows: pd.DataFrame, target_date: datetime
    ) -> pd.DataFrame:
        """Fetch supplemental weather/roof context from MLB StatsAPI game feed."""

        query = urlencode(
            {
                "sportId": 1,
                "date": pd.Timestamp(target_date).date().isoformat(),
            }
        )
        schedule_payload: dict[str, Any] | None = None
        try:
            with urlopen(  # noqa: S310
                f"https://statsapi.mlb.com/api/v1/schedule?{query}"
            ) as resp:
                loaded = json.loads(resp.read().decode("utf-8"))
                if isinstance(loaded, dict):
                    schedule_payload = loaded
        except (URLError, TimeoutError, ValueError, OSError):
            return pd.DataFrame()

        if not schedule_payload:
            return pd.DataFrame()

        team_to_game: dict[str, dict[str, Any]] = {}
        dates = schedule_payload.get("dates")
        if isinstance(dates, list):
            for date_blob in dates:
                games = date_blob.get("games") if isinstance(date_blob, dict) else []
                if not isinstance(games, list):
                    continue
                for game in games:
                    if not isinstance(game, dict):
                        continue
                    teams_blob = game.get("teams")
                    if not isinstance(teams_blob, dict):
                        continue
                    for side in ("home", "away"):
                        side_blob = teams_blob.get(side)
                        if not isinstance(side_blob, dict):
                            continue
                        team_blob = side_blob.get("team")
                        if not isinstance(team_blob, dict):
                            continue
                        abbr = team_blob.get("abbreviation")
                        if isinstance(abbr, str) and abbr.strip():
                            team_to_game[abbr.strip().upper()] = game

        records: list[dict[str, Any]] = []
        for row in rows.itertuples(index=False):
            team = str(getattr(row, "opponent_team", "")).strip()
            if not team:
                continue
            game = team_to_game.get(team.upper())
            if game is None:
                continue

            roof_state = "unknown"
            weather: dict[str, Any] = {}
            weather_blob = game.get("weather") if isinstance(game, dict) else {}
            if isinstance(weather_blob, dict):
                weather = {
                    "game_temp_f": weather_blob.get("temp"),
                    "wind_speed_mph": weather_blob.get("wind"),
                    "humidity_pct": weather_blob.get("humidity"),
                    "wind_direction": weather_blob.get("windDirection"),
                }
            venue_blob = game.get("venue") if isinstance(game, dict) else {}
            if isinstance(venue_blob, dict):
                roof_state = str(venue_blob.get("roofType") or "unknown")

            normalized_weather = normalize_weather_payload(weather, use_defaults=False)
            normalized_venue = normalize_venue_payload({"roof_state": roof_state})
            records.append(
                {
                    "pitcher_id": getattr(row, "pitcher_id", None),
                    "opponent_team": team,
                    **normalized_weather,
                    **normalized_venue,
                }
            )

        return pd.DataFrame(records)

    def _load_cache(self) -> pd.DataFrame:
        if not self.cache_path.exists():
            return pd.DataFrame()
        try:
            return pd.read_parquet(self.cache_path)
        except Exception:
            logger.warning("Failed to read live feature cache at %s", self.cache_path)
            return pd.DataFrame()

    def _save_cache(self, frame: pd.DataFrame) -> None:
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            frame.to_parquet(self.cache_path, index=False)
        except Exception:
            logger.warning("Failed to write live feature cache at %s", self.cache_path)

    def _cache_age_hours(self, frame: pd.DataFrame) -> float | None:
        if "fetched_at" not in frame.columns or frame.empty:
            return None
        fetched = pd.to_datetime(frame["fetched_at"], errors="coerce", utc=True)
        latest = fetched.max()
        if pd.isna(latest):
            return None
        delta = datetime.now(UTC) - latest.to_pydatetime()
        return max(delta / timedelta(hours=1), 0.0)
