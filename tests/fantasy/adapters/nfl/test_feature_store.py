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


def _evidence(frame: pd.DataFrame, *, global_safe: bool = False) -> SnapshotEvidence:
    """Build valid, deliberately unhashed fixture evidence."""
    return SnapshotEvidence(
        frame=frame,
        snapshot_id="stats-fixture",
        retrieved_at=AS_OF - timedelta(hours=1),
        row_count=len(frame),
        manifest_ref="fixtures/stats.csv",
        global_safe=global_safe,
    )


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
    return _evidence(frame)


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


def test_features_are_pre_target_and_snapshot_audited_without_hash() -> None:
    result = build_nfl_feature_store(_targets(), {"stats": _source()})
    assert result.loc[0, "plays"] == 60
    assert result.loc[0, "projection"] == 15
    assert result.loc[0, "source_snapshot_id"] == "stats-fixture"
    assert result.loc[0, "source_manifest_ref"] == "fixtures/stats.csv"
    assert result.loc[0, "source_row_count"] == 3
    assert not result.loc[0, "plays_fallback"]


def test_future_same_player_perturbation_is_inert() -> None:
    before = build_nfl_feature_store(_targets(), {"stats": _source()})
    changed = _source().frame.copy()
    changed.loc[1, "plays"] = -10000
    changed.loc[1, "projection"] = -10000
    changed.loc[1, "availability"] = "inactive"
    after = build_nfl_feature_store(_targets(), {"stats": _evidence(changed)})
    pd.testing.assert_frame_equal(before, after)


def test_snapshot_retains_copy_when_caller_mutates_frame() -> None:
    frame = _source().frame.copy()
    evidence = _evidence(frame)
    frame.loc[0, "plays"] = -10000
    result = build_nfl_feature_store(_targets(), {"stats": evidence})
    assert result.loc[0, "plays"] == 60


def test_unscoped_source_requires_explicit_global_safe() -> None:
    source = _source().frame.drop(columns=["game_id"])
    with pytest.raises(FeatureStoreError, match="unscoped"):
        build_nfl_feature_store(_targets(), {"stats": _evidence(source)})


@pytest.mark.parametrize(
    "value",
    [123, "2026-09-10T12:00:00", datetime(2026, 9, 10, 12)],
)
def test_timestamp_types_fail_closed(value: object) -> None:
    targets = _targets()
    targets["as_of_utc"] = value
    with pytest.raises(FeatureStoreError, match="timestamp"):
        build_nfl_feature_store(targets, {"stats": _source()})


def test_missing_snapshot_metadata_fails_closed() -> None:
    with pytest.raises(FeatureStoreError, match="snapshot"):
        build_nfl_feature_store(_targets(), {"stats": _source().frame})


def test_duplicate_observation_fails_closed() -> None:
    frame = pd.concat([_source().frame.iloc[[0]], _source().frame.iloc[[0]]])
    frame.iloc[1, frame.columns.get_loc("plays")] = 99
    with pytest.raises(FeatureStoreError, match="duplicate"):
        build_nfl_feature_store(_targets().iloc[[0]], {"stats": _evidence(frame)})


def test_categorical_timestamp_tie_across_sources_fails_closed() -> None:
    first = _source().frame.iloc[[0]].copy()
    second = first.copy()
    second["role"] = "backup"
    with pytest.raises(FeatureStoreError, match="unresolved categorical tie"):
        build_nfl_feature_store(
            _targets().iloc[[0]],
            {"a": _evidence(first), "z": _evidence(second)},
        )


def test_blank_target_or_source_scope_fails_closed() -> None:
    targets = _targets().iloc[[0]].copy()
    targets["game_id"] = " "
    with pytest.raises(FeatureStoreError, match="blank game_id"):
        build_nfl_feature_store(targets, {"stats": _source()})
    source = _source().frame.copy()
    source.loc[0, "game_id"] = " "
    with pytest.raises(FeatureStoreError, match="blank game_id"):
        build_nfl_feature_store(_targets().iloc[[0]], {"stats": _evidence(source)})


def test_no_evidence_bypass_and_week_one_prior_requires_snapshot() -> None:
    target = _targets().iloc[[0]].copy()
    target["nflverse_id"] = "rookie"
    target["week"] = 1
    with pytest.raises(FeatureStoreError, match="source snapshot"):
        build_nfl_feature_store(target, {})

    prior = _source().frame.iloc[[2]].copy()
    prior["season"] = 2025
    result = build_nfl_feature_store(
        target, {"prior": _evidence(prior, global_safe=True)}
    )
    assert result.loc[target.index[0], "plays"] == 50
    assert result.loc[target.index[0], "plays_fallback"]
