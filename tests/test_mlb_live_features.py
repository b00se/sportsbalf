from __future__ import annotations

import sys
from datetime import UTC, datetime

import pandas as pd
import pytest
import src.mlb.features.live_context as live_context_module
from src.mlb.features.feature_store import (
    LIVE_CONTEXT_FEATURE_COLUMNS,
    build_historical_live_features,
    ensure_live_feature_defaults,
    merge_live_feature_frame,
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


def test_merge_live_feature_frame_overwrites_existing_live_defaults() -> None:
    predictions = pd.DataFrame(
        [
            {
                "pitcher_id": 99,
                "opponent_team": "SEA",
                "humidity_pct": 50.0,
                "roof_state": "unknown",
                "weather_known_flag": 0,
            }
        ]
    )
    live = pd.DataFrame(
        [
            {
                "pitcher_id": 99,
                "opponent_team": "SEA",
                "humidity_pct": 80.0,
                "roof_state": "closed",
                "weather_known_flag": 1,
            }
        ]
    )

    merged = merge_live_feature_frame(predictions, live)

    assert float(merged.loc[0, "humidity_pct"]) == 80.0
    assert str(merged.loc[0, "roof_state"]).lower() == "closed"
    assert int(merged.loc[0, "weather_known_flag"]) == 1
    assert "humidity_pct_live" not in merged.columns
    assert "roof_state_live" not in merged.columns


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
    service._fetch_secondary = lambda *_args, **_kwargs: pd.DataFrame()  # type: ignore[method-assign]

    rows = pd.DataFrame([{"pitcher_id": 123, "opponent_team": "LAD"}])
    result = service.fetch(rows, datetime.now())

    assert result.metadata["cache_status"] == "stale_fallback"
    assert float(result.frame.loc[0, "humidity_pct"]) == 55.0


def test_live_context_uses_secondary_when_primary_returns_empty(tmp_path) -> None:
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

    secondary = pd.DataFrame(
        [
            {
                "pitcher_id": 31,
                "opponent_team": "ATL",
                "game_temp_f": 67.0,
                "humidity_pct": 49.0,
                "wind_speed_mph": 10.0,
                "wind_out_to_cf_flag": 1,
                "roof_state": "open",
            }
        ]
    )

    service._fetch_primary = lambda *_args, **_kwargs: pd.DataFrame()  # type: ignore[method-assign]
    service._fetch_secondary = lambda *_args, **_kwargs: secondary.copy()  # type: ignore[method-assign]

    rows = pd.DataFrame([{"pitcher_id": 31, "opponent_team": "ATL"}])
    result = service.fetch(rows, datetime.now())

    assert "statsapi_game_feed" in result.metadata["live_feature_sources"]
    assert float(result.frame.loc[0, "humidity_pct"]) == 49.0
    assert result.metadata["cache_status"] == "fresh"


def test_live_context_uses_cache_for_missing_rows_in_partial_fetch(tmp_path) -> None:
    cache_path = tmp_path / "live_cache.parquet"
    now = datetime.now(UTC)
    cached = pd.DataFrame(
        [
            {
                "pitcher_id": 22,
                "opponent_team": "LAD",
                "game_temp_f": 61.0,
                "humidity_pct": 66.0,
                "wind_speed_mph": 12.0,
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

    primary = pd.DataFrame(
        [
            {
                "pitcher_id": 11,
                "opponent_team": "BOS",
                "game_temp_f": 70.0,
                "humidity_pct": 40.0,
                "wind_speed_mph": 5.0,
                "wind_out_to_cf_flag": 0,
                "roof_state": "open",
                "weather_known_flag": 1,
            }
        ]
    )
    service._fetch_primary = lambda *_args, **_kwargs: primary.copy()  # type: ignore[method-assign]
    service._fetch_secondary = lambda *_args, **_kwargs: pd.DataFrame()  # type: ignore[method-assign]

    rows = pd.DataFrame(
        [
            {"pitcher_id": 11, "opponent_team": "BOS"},
            {"pitcher_id": 22, "opponent_team": "LAD"},
        ]
    )
    result = service.fetch(rows, datetime.now())
    keyed = result.frame.set_index(["pitcher_id", "opponent_team"])

    assert result.metadata["cache_status"] == "partial_stale_fallback"
    assert "cache" in result.metadata["live_feature_sources"]
    assert float(result.metadata["stale_cache_usage_pct"]) == 0.5
    assert float(keyed.loc[(22, "LAD"), "humidity_pct"]) == 66.0
    assert int(keyed.loc[(22, "LAD"), "is_stale"]) == 1


def test_stale_cache_fallback_only_returns_requested_key_matches(tmp_path) -> None:
    cache_path = tmp_path / "live_cache.parquet"
    now = datetime.now(UTC)
    cached = pd.DataFrame(
        [
            {
                "pitcher_id": 501,
                "opponent_team": "BAL",
                "game_temp_f": 64.0,
                "humidity_pct": 62.0,
                "wind_speed_mph": 8.0,
                "wind_out_to_cf_flag": 0,
                "roof_state": "open",
                "fetched_at": now.isoformat(),
            },
            {
                "pitcher_id": 999,
                "opponent_team": "XXX",
                "game_temp_f": 77.0,
                "humidity_pct": 10.0,
                "wind_speed_mph": 1.0,
                "wind_out_to_cf_flag": 0,
                "roof_state": "closed",
                "fetched_at": now.isoformat(),
            },
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
    service._fetch_secondary = lambda *_args, **_kwargs: pd.DataFrame()  # type: ignore[method-assign]

    rows = pd.DataFrame(
        [
            {"pitcher_id": 501, "opponent_team": "BAL"},
            {"pitcher_id": 502, "opponent_team": "BOS"},
        ]
    )
    result = service.fetch(rows, datetime.now())

    assert result.metadata["cache_status"] == "stale_fallback"
    assert float(result.metadata["stale_cache_usage_pct"]) == 0.5
    assert len(result.frame) == 1
    assert int(result.frame.loc[0, "pitcher_id"]) == 501
    assert str(result.frame.loc[0, "opponent_team"]) == "BAL"


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
    assert int(result.frame.loc[0, "weather_known_flag"]) == 1


def test_live_context_uses_secondary_when_only_roof_state_missing(tmp_path) -> None:
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
                "pitcher_id": 21,
                "opponent_team": "TEX",
                "game_temp_f": 72.0,
                "humidity_pct": 45.0,
                "wind_speed_mph": 7.0,
                "wind_out_to_cf_flag": 0,
                "roof_state": "unknown",
                "weather_known_flag": 1,
            }
        ]
    )
    secondary = pd.DataFrame(
        [
            {
                "pitcher_id": 21,
                "opponent_team": "TEX",
                "roof_state": "closed",
            }
        ]
    )

    service._fetch_primary = lambda *_args, **_kwargs: primary.copy()  # type: ignore[method-assign]
    service._fetch_secondary = lambda *_args, **_kwargs: secondary.copy()  # type: ignore[method-assign]

    rows = pd.DataFrame([{"pitcher_id": 21, "opponent_team": "TEX"}])
    result = service.fetch(rows, datetime.now())

    assert "statsapi_game_feed" in result.metadata["live_feature_sources"]
    assert str(result.frame.loc[0, "roof_state"]).lower() == "closed"
    assert float(result.frame.loc[0, "game_temp_f"]) == 72.0


def test_primary_missing_weather_stays_nan_for_secondary_fill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = LiveContextService({"enabled": True})
    schedule = pd.DataFrame([{"Date": "Mon, Apr 14"}])
    monkeypatch.setattr(
        live_context_module,
        "schedule_and_record",
        lambda _year, _team: schedule.copy(),
    )
    rows = pd.DataFrame([{"pitcher_id": 200, "opponent_team": "BOS"}])

    primary = service._fetch_primary(rows, datetime(2025, 4, 14))
    assert primary["humidity_pct"].isna().all()
    assert primary["wind_speed_mph"].isna().all()
    assert int(primary.loc[0, "weather_known_flag"]) == 0


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


def test_primary_fetch_deduplicates_schedule_lookups_by_opponent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = LiveContextService({"enabled": True})
    schedule = pd.DataFrame([{"Date": "Mon, Apr 14", "Temp": 70, "Wind": "9, Out"}])
    calls: list[tuple[int, str]] = []

    def fake_schedule_and_record(year: int, team: str) -> pd.DataFrame:
        calls.append((year, team))
        return schedule.copy()

    monkeypatch.setattr(
        live_context_module,
        "schedule_and_record",
        fake_schedule_and_record,
    )

    rows = pd.DataFrame(
        [
            {"pitcher_id": 101, "opponent_team": "BOS"},
            {"pitcher_id": 102, "opponent_team": "BOS"},
            {"pitcher_id": 103, "opponent_team": "NYY"},
        ]
    )
    result = service._fetch_primary(rows, datetime(2025, 4, 14))

    assert len(result) == 3
    assert calls == [(2025, "BOS"), (2025, "NYY")]


def test_primary_fetch_reuses_service_schedule_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = LiveContextService({"enabled": True})
    schedule = pd.DataFrame([{"Date": "Mon, Apr 14", "Temp": 70, "Wind": "9, Out"}])
    calls: list[tuple[int, str]] = []

    def fake_schedule_and_record(year: int, team: str) -> pd.DataFrame:
        calls.append((year, team))
        return schedule.copy()

    monkeypatch.setattr(
        live_context_module,
        "schedule_and_record",
        fake_schedule_and_record,
    )

    rows = pd.DataFrame([{"pitcher_id": 101, "opponent_team": "BOS"}])
    first = service._fetch_primary(rows, datetime(2025, 4, 14))
    second = service._fetch_primary(rows, datetime(2025, 4, 14))

    assert not first.empty
    assert not second.empty
    assert calls == [(2025, "BOS")]


def test_primary_fetch_does_not_mutate_process_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = LiveContextService({"enabled": True})
    schedule = pd.DataFrame([{"Date": "Mon, Apr 14", "Temp": 70, "Wind": "9, Out"}])

    monkeypatch.setattr(
        live_context_module,
        "schedule_and_record",
        lambda _year, _team: schedule.copy(),
    )

    original_stdout = sys.stdout
    rows = pd.DataFrame(
        [
            {"pitcher_id": 101, "opponent_team": "BOS"},
            {"pitcher_id": 102, "opponent_team": "NYY"},
        ]
    )
    _ = service._fetch_primary(rows, datetime(2025, 4, 14))

    assert sys.stdout is original_stdout


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
