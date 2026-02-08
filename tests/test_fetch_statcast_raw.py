from __future__ import annotations

import pandas as pd
import pytest
import scripts.fetch_statcast_raw as fetch_script


def test_fetch_statcast_raw_rejects_invalid_date_window(tmp_path) -> None:
    with pytest.raises(ValueError, match="Invalid date range"):
        fetch_script.fetch_statcast_raw(
            season=2025,
            start="10-01",
            end="04-01",
            save_dir=str(tmp_path),
        )


def test_fetch_statcast_raw_uses_cached_file_when_network_fails(
    monkeypatch, tmp_path
) -> None:
    out_path = tmp_path / "statcast_raw_2025.parquet"
    pd.DataFrame({"pitcher": [1], "game_date": ["2025-04-01"]}).to_parquet(
        out_path, index=False
    )

    def _always_fail(_start: str, _end: str) -> pd.DataFrame:
        raise RuntimeError("network unavailable")

    monkeypatch.setattr(fetch_script, "_fetch_with_warning_suppression", _always_fail)

    result = fetch_script.fetch_statcast_raw(
        season=2025,
        start="04-01",
        end="04-01",
        save_dir=str(tmp_path),
        retries_per_day=0,
        retry_delay_seconds=0.0,
    )

    assert result == out_path
