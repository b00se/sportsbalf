from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
import pytest
import src.mlb.features.live_context as live_context_module
from src.mlb.features.feature_store import (
    LIVE_CONTEXT_FEATURE_COLUMNS,
    build_historical_live_features,
    ensure_live_feature_defaults,
)
from src.mlb.features.live_context import LiveContextService
from src.mlb.features.venue import normalize_roof_state
from src.mlb.models.predict import FEATURES


def test_live_feature_defaults_include_all_columns() -> None:
    frame = pd.DataFrame([{"pitcher_id": 1, "opponent_team": "NYY"}])
    enriched = ensure_live_feature_defaults(frame)

    assert set(LIVE_CONTEXT_FEATURE_COLUMNS).issubset(enriched.columns)
    assert enriched.loc[0, "roof_state"] == "unknown"
    assert float(enriched.loc[0, "game_temp_f"]) == 72.0


def test_model_feature_list_includes_live_context_columns() -> None:
    assert set(LIVE_CONTEXT_FEATURE_COLUMNS) - {"roof_state"} <= set(FEATURES)


def test_historical_umpire_features_are_shifted() -> None:
    frame = pd.DataFrame(
        [
            {"pitcher": 1, "game_date": "2024-04-01", "strikeouts": 7, "umpire": "A"},
            {"pitcher": 2, "game_date": "2024-04-02", "strikeouts": 5, "umpire": "A"},
            {"pitcher": 3, "game_date": "2024-04-03", "strikeouts": 9, "umpire": "A"},
        ]
    )
    frame["game_date"] = pd.to_datetime(frame["game_date"])

    enriched = build_historical_live_features(frame)

    # second game prior sample size should be 1; third should be 2
    assert float(enriched.loc[1, "umpire_sample_size"]) == 1.0
    assert float(enriched.loc[2, "umpire_sample_size"]) == 2.0


def test_normalize_roof_state_mapping() -> None:
    assert normalize_roof_state("Retractable Roof (Open)") == "retractable_open"
    assert normalize_roof_state("Dome") == "closed"
    assert normalize_roof_state(None) == "unknown"


def test_live_context_uses_stale_cache_fallback(tmp_path) -> None:
    cache_path = tmp_path / "live_cache.parquet"
    now = datetime.now(UTC)
    cached = pd.DataFrame(
        [
            {
                "pitcher_id": 123,
                "opponent_team": "LAD",
                "game_temp_f": 70.0,
                "humidity_pct": 55.0,
                "wind_speed_mph": 9.0,
                "wind_out_to_cf_flag": 1,
                "roof_state": "open",
                "fetched_at": now.isoformat(),
            }
        ]
    )
    cached.to_parquet(cache_path, index=False)

    service = LiveContextService(
        {
            "enabled": True,
            "cache_path": str(cache_path),
            "cache_ttl_hours": 24,
            "weather": {"enabled": True},
        }
    )

    service._fetch_primary = lambda *_args, **_kwargs: pd.DataFrame()  # type: ignore[method-assign]

    rows = pd.DataFrame([{"pitcher_id": 123, "opponent_team": "LAD"}])
    result = service.fetch(rows, datetime.now())

    assert result.metadata["cache_status"] == "stale_fallback"
    assert float(result.frame.loc[0, "humidity_pct"]) == 55.0


def test_live_context_uses_secondary_only_for_missing_keys(tmp_path) -> None:
    cache_path = tmp_path / "live_cache.parquet"
    service = LiveContextService(
        {
            "enabled": True,
            "cache_path": str(cache_path),
            "cache_ttl_hours": 24,
            "weather": {
                "enabled": True,
                "primary_source": "pybaseball_team_game_logs",
                "secondary_source": "statsapi_game_feed",
            },
        }
    )

    primary = pd.DataFrame(
        [
            {
                "pitcher_id": 7,
                "opponent_team": "BOS",
                "game_temp_f": 68.0,
                "humidity_pct": pd.NA,
                "wind_speed_mph": 8.0,
                "wind_out_to_cf_flag": 0,
                "roof_state": "unknown",
                "weather_known_flag": 0,
            }
        ]
    )
    secondary = pd.DataFrame(
        [
            {
                "pitcher_id": 7,
                "opponent_team": "BOS",
                "humidity_pct": 61.0,
                "roof_state": "closed",
            }
        ]
    )

    service._fetch_primary = lambda *_args, **_kwargs: primary.copy()  # type: ignore[method-assign]
    service._fetch_secondary = lambda *_args, **_kwargs: secondary.copy()  # type: ignore[method-assign]

    rows = pd.DataFrame([{"pitcher_id": 7, "opponent_team": "BOS"}])
    result = service.fetch(rows, datetime.now())

    assert "statsapi_game_feed" in result.metadata["live_feature_sources"]
    assert float(result.frame.loc[0, "humidity_pct"]) == 61.0


def test_primary_fetch_uses_target_date_weather(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = LiveContextService({"enabled": True})
    schedule = pd.DataFrame(
        [
            {"Date": "Sun, Apr 13", "Temp": 50, "Wind": "3, In"},
            {"Date": "Mon, Apr 14", "Temp": 70, "Wind": "9, Out to CF"},
        ]
    )
    monkeypatch.setattr(
        live_context_module,
        "schedule_and_record",
        lambda _year, _team: schedule.copy(),
    )

    rows = pd.DataFrame([{"pitcher_id": 101, "opponent_team": "BOS"}])
    result = service._fetch_primary(rows, datetime(2025, 4, 14))

    assert not result.empty
    assert float(result.loc[0, "game_temp_f"]) == 70.0
    assert int(result.loc[0, "wind_out_to_cf_flag"]) == 1


def test_secondary_fetch_matches_team_and_date(monkeypatch: pytest.MonkeyPatch) -> None:
    service = LiveContextService({"enabled": True})
    payload = {
        "dates": [
            {
                "games": [
                    {
                        "teams": {
                            "home": {"team": {"abbreviation": "BOS"}},
                            "away": {"team": {"abbreviation": "NYY"}},
                        },
                        "weather": {
                            "temp": 63,
                            "wind": 11,
                            "humidity": 58,
                            "windDirection": "Out to CF",
                        },
                        "venue": {"roofType": "Open"},
                    },
                    {
                        "teams": {
                            "home": {"team": {"abbreviation": "LAD"}},
                            "away": {"team": {"abbreviation": "SF"}},
                        },
                        "weather": {
                            "temp": 71,
                            "wind": 5,
                            "humidity": 44,
                            "windDirection": "In from CF",
                        },
                        "venue": {"roofType": "Closed"},
                    },
                ]
            }
        ]
    }

    class _DummyResponse:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self) -> bytes:
            import json

            return json.dumps(payload).encode("utf-8")

    monkeypatch.setattr(
        live_context_module,
        "urlopen",
        lambda _url: _DummyResponse(),
    )

    rows = pd.DataFrame([{"pitcher_id": 7, "opponent_team": "LAD"}])
    result = service._fetch_secondary(rows, datetime(2025, 4, 14))

    assert not result.empty
    assert float(result.loc[0, "game_temp_f"]) == 71.0
    assert str(result.loc[0, "roof_state"]).lower() == "closed"
