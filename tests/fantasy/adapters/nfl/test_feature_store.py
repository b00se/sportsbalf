"""Acceptance tests for snapshot-backed NFL feature construction."""

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest
from src.fantasy.adapters.nfl.feature_store import (
    FeatureStoreError,
    SnapshotEvidence,
    build_nfl_feature_store,
)

AS_OF = datetime(2026, 9, 10, 12, tzinfo=UTC)


def _source() -> SnapshotEvidence:
    frame = pd.DataFrame(
        {
            "nflverse_id": ["p1", "p1", "p2"],
            "game_id": ["g0", "g1", "g0"],
            "source_timestamp_utc": [
                AS_OF - timedelta(days=14),
                AS_OF + timedelta(days=1),
                AS_OF - timedelta(days=10),
            ],
            "plays": [60, 999, 50],
            "pace": [65, 999, 55],
            "neutral_pass_rate": [0.6, 0.99, 0.5],
            "opportunity_share": [0.2, 0.99, 0.3],
            "routes": [30, 999, 25],
            "snaps": [40, 999, 35],
            "opponent_strength": [0.1, 9.9, 0.2],
            "rest_days": [7, 1, 6],
            "weather": ["clear", "storm", "clear"],
            "spread": [-3, 99, 2],
            "total": [44, 99, 42],
            "role": ["starter", "bench", "starter"],
            "availability": ["active", "inactive", "active"],
            "projection": [15, 999, 12],
            "provenance": ["fixture:prior", "fixture:future", "fixture:prior"],
        }
    )
    return SnapshotEvidence(frame, "snap-1", "sha-1", "manifest-1")


def _targets() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "nflverse_id": ["p1", "p2"],
            "game_id": ["g1", "g1"],
            "season": [2026, 2026],
            "week": [2, 2],
            "as_of_utc": [AS_OF, AS_OF],
        }
    )


def test_features_are_pre_target_and_snapshot_audited() -> None:
    result = build_nfl_feature_store(_targets(), {"stats": _source()})
    assert result.loc[0, "plays"] == 60
    assert result.loc[0, "projection"] == 15
    assert result.loc[0, "source_snapshot_id"] == "snap-1"
    assert result.loc[0, "source_manifest_ref"] == "manifest-1"
    assert not result.loc[0, "plays_fallback"]


def test_future_same_player_perturbation_is_inert() -> None:
    before = build_nfl_feature_store(_targets(), {"stats": _source()})
    changed = _source().frame.copy()
    changed.loc[1, "plays"] = -10000
    changed.loc[1, "projection"] = -10000
    changed.loc[1, "availability"] = "inactive"
    after = build_nfl_feature_store(
        _targets(),
        {"stats": SnapshotEvidence(changed, "snap-1", "sha-1", "manifest-1")},
    )
    pd.testing.assert_frame_equal(before, after)


def test_unscoped_source_requires_explicit_global_safe() -> None:
    source = _source().frame.drop(columns=["game_id"])
    with pytest.raises(FeatureStoreError, match="unscoped"):
        build_nfl_feature_store(
            _targets(), {"stats": SnapshotEvidence(source, "s", "h", "m")}
        )


@pytest.mark.parametrize(
    "value",
    [123, "2026-09-10T12:00:00", datetime(2026, 9, 10, 12)],
)
def test_timestamp_types_fail_closed(value) -> None:
    targets = _targets()
    targets["as_of_utc"] = value
    with pytest.raises(FeatureStoreError, match="timestamp"):
        build_nfl_feature_store(targets, {"stats": _source()})


def test_missing_snapshot_metadata_fails_closed() -> None:
    with pytest.raises(FeatureStoreError, match="snapshot"):
        build_nfl_feature_store(_targets(), {"stats": _source().frame})
