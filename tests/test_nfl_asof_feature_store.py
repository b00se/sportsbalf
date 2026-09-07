"""Offline leakage and provenance contracts for the NFL as-of feature store."""

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest
from src.nfl.features.asof_store import AsOfFeatureError, build_as_of_feature_store

AS_OF = datetime(2026, 9, 10, 12, tzinfo=UTC)


def _targets() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "nflverse_id": ["p1", "p2"],
            "event_id": ["g1", "g1"],
            "as_of_utc": [AS_OF, AS_OF],
        }
    )


def _projection() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "nflverse_id": ["p1", "p1", "p2"],
            "source_timestamp_utc": [
                AS_OF - timedelta(days=2),
                AS_OF + timedelta(days=1),
                AS_OF - timedelta(hours=1),
            ],
            "projection": [10.0, 999.0, 20.0],
            "provenance": ["fixture:prior", "fixture:future", "fixture:prior"],
        }
    )


def test_store_uses_latest_prior_evidence_and_records_audit_metadata() -> None:
    result = build_as_of_feature_store(
        _targets(), {"projection": _projection()}
    ).to_frame()
    assert result["projection_projection"].tolist() == [10.0, 20.0]
    assert result["projection_provenance"].tolist() == [
        "fixture:prior",
        "fixture:prior",
    ]
    assert result.loc[0, "projection_freshness_seconds"] == 172800.0


def test_future_same_player_perturbation_cannot_change_historical_features() -> None:
    baseline = build_as_of_feature_store(
        _targets(), {"projection": _projection()}
    ).to_frame()
    changed = _projection().copy()
    changed.loc[1, "projection"] = -12345.0
    changed.loc[1, "provenance"] = "fixture:future-mutated"
    result = build_as_of_feature_store(_targets(), {"projection": changed}).to_frame()
    pd.testing.assert_frame_equal(baseline, result)


def test_future_availability_and_source_rows_cannot_change_historical_features(
) -> None:
    availability = pd.DataFrame(
        {
            "nflverse_id": ["p1", "p2"],
            "source_timestamp_utc": [AS_OF - timedelta(hours=2)] * 2,
            "status": ["active", "active"],
            "provenance": ["fixture:availability"] * 2,
        }
    )
    baseline = build_as_of_feature_store(
        _targets(), {"availability": availability}, require_evidence=True
    ).to_frame()
    future = pd.concat(
        [
            availability,
            pd.DataFrame(
                {
                    "nflverse_id": ["p1"],
                    "source_timestamp_utc": [AS_OF + timedelta(minutes=1)],
                    "status": ["inactive"],
                    "provenance": ["fixture:future"],
                }
            ),
        ],
        ignore_index=True,
    )
    result = build_as_of_feature_store(
        _targets(), {"availability": future}, require_evidence=True
    ).to_frame()
    pd.testing.assert_frame_equal(baseline, result)


@pytest.mark.parametrize("column", ["as_of_utc", "source_timestamp_utc"])
def test_missing_or_naive_timestamps_fail_closed(column: str) -> None:
    if column == "as_of_utc":
        targets = _targets()
        targets.loc[0, column] = None
        with pytest.raises(AsOfFeatureError, match="timestamp"):
            build_as_of_feature_store(targets, {})
    else:
        source = _projection()
        if column == "source_timestamp_utc":
            source["source_timestamp_utc"] = pd.to_datetime(
                ["2026-09-10T10:00:00"] * len(source)
            )
        with pytest.raises(AsOfFeatureError, match="naive"):
            build_as_of_feature_store(_targets(), {"projection": source})


def test_unresolved_canonical_identity_fails_closed() -> None:
    source = _projection()
    source.loc[0, "nflverse_id"] = None
    with pytest.raises(AsOfFeatureError, match="canonical"):
        build_as_of_feature_store(_targets(), {"projection": source})
