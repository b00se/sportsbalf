"""Offline tests for the NFL projection-source audit contract."""

import csv
from datetime import UTC, datetime, timedelta

import pytest
from src.fantasy.adapters.nfl.projection_sources import (
    ProjectionSource,
    SourceAuditError,
    audit_projection_source,
    load_projection_sources,
    run_projection_source_tournament,
    score_rolling_origin_snapshot,
)


def _source(**overrides: object) -> ProjectionSource:
    values: dict[str, object] = {
        "source_id": "fixture-consensus",
        "publisher": "Example Research",
        "access_url": "https://example.test/consensus.csv",
        "license_name": "CC BY 4.0",
        "license_url": "https://creativecommons.org/licenses/by/4.0/",
        "cost_usd": 0.0,
        "commercial_use": True,
        "redistribution_allowed": True,
        "snapshot_format": "csv",
        "version": "2026.09.01",
        "retrieval_method": "manual_download",
        "source_timestamp_utc": "2026-09-01T12:00:00Z",
        "content_sha256": (
            "3cc634e802d868801a77b6cc94fa2b72a5a19191fbe39b168328fe0d66707e59"
        ),
        "coverage_score": 0.9,
        "baseline_role": "public_projection",
        "snapshot_path": "tests/testdata/fantasy/nfl/projections/nflverse_week01.csv",
        "snapshot_schema": (
            "player_id",
            "game_id",
            "season",
            "week",
            "projection",
            "actual",
            "as_of_utc",
            "target_cutoff_utc",
            "historical_player_mean",
            "public_projection",
            "consensus_projection",
        ),
    }
    values.update(overrides)
    return ProjectionSource(**values)


def test_audit_accepts_licensed_reproducible_free_source() -> None:
    result = audit_projection_source(
        _source(), as_of_utc=datetime(2026, 9, 2, 12, tzinfo=UTC)
    )

    assert result.eligible is True
    assert result.failures == ()
    assert result.freshness_hours == 24.0


def test_audit_rejects_missing_license_and_unreproducible_snapshot() -> None:
    source = _source(
        license_name="",
        license_url="",
        snapshot_format="",
        content_sha256="",
    )

    result = audit_projection_source(
        source, as_of_utc=datetime(2026, 9, 2, 12, tzinfo=UTC)
    )

    assert result.eligible is False
    assert "license_missing" in result.failures
    assert "snapshot_format_missing" in result.failures
    assert "content_hash_missing" in result.failures


def test_tournament_is_deterministic_and_excludes_stale_or_paid_sources() -> None:
    as_of = datetime(2026, 9, 2, 12, tzinfo=UTC)
    winner = _source(source_id="winner", coverage_score=0.8)
    stale = _source(
        source_id="stale",
        source_timestamp_utc=(as_of - timedelta(days=10))
        .isoformat()
        .replace("+00:00", "Z"),
    )
    paid = _source(source_id="paid", cost_usd=1.0)

    consensus = _source(
        source_id="consensus",
        publisher="Other Publisher",
        access_url="https://other.test/consensus.csv",
        baseline_role="consensus_reference",
    )
    tournament = run_projection_source_tournament(
        (paid, stale, winner, consensus), as_of_utc=as_of, max_age_hours=72
    )

    assert [item.source.source_id for item in tournament] == ["consensus", "winner"]
    assert tournament[0].rank == 1


def test_tournament_requires_two_baseline_roles() -> None:
    with pytest.raises(SourceAuditError, match="two independent"):
        run_projection_source_tournament(
            (_source(),), as_of_utc=datetime(2026, 9, 2, 12, tzinfo=UTC)
        )


def test_rolling_origin_fixture_scores_required_schema() -> None:
    result = score_rolling_origin_snapshot(
        "tests/testdata/fantasy/nfl/projections/nflverse_week01.csv"
    )
    assert result.rows == 2
    assert result.mean_absolute_error == 0.75
    assert result.coverage == 1.0


def test_checked_in_registry_has_two_real_reproducible_baselines() -> None:
    sources = load_projection_sources("config/fantasy/nfl_projection_sources_2026.yaml")
    result = run_projection_source_tournament(
        sources, as_of_utc=datetime(2026, 9, 2, 12, tzinfo=UTC)
    )
    assert {entry.source.baseline_role for entry in result} == {
        "public_projection",
        "consensus_reference",
    }
    with pytest.raises(SourceAuditError, match="commercial"):
        run_projection_source_tournament(
            sources,
            as_of_utc=datetime(2026, 9, 2, 12, tzinfo=UTC),
            commercial_mode=True,
        )


def test_invalid_timestamp_fails_closed() -> None:
    with pytest.raises(SourceAuditError, match="UTC"):
        audit_projection_source(
            _source(source_timestamp_utc="2026-09-01"),
            as_of_utc=datetime(2026, 9, 2, 12, tzinfo=UTC),
        )


def test_version_digest_and_boolean_types_fail_closed() -> None:
    as_of = datetime(2026, 9, 2, 12, tzinfo=UTC)
    assert (
        "version_missing"
        in audit_projection_source(_source(version=""), as_of_utc=as_of).failures
    )
    assert (
        "content_hash_missing"
        in audit_projection_source(
            _source(content_sha256="A" * 64), as_of_utc=as_of
        ).failures
    )
    assert (
        "commercial_use_invalid"
        in audit_projection_source(
            _source(commercial_use="true"), as_of_utc=as_of
        ).failures
    )


def test_config_loader_rejects_string_booleans(tmp_path) -> None:
    path = tmp_path / "sources.yaml"
    path.write_text(
        "projection_sources:\n"
        "  - source_id: x\n"
        "    publisher: p\n"
        "    access_url: https://example.test\n"
        "    license_name: CC0\n"
        "    license_url: https://creativecommons.org/publicdomain/zero/1.0/\n"
        "    cost_usd: 0\n"
        "    commercial_use: 'true'\n"
        "    redistribution_allowed: true\n"
        "    snapshot_format: csv\n"
        "    version: v1\n"
        "    retrieval_method: local_fixture\n"
        "    source_timestamp_utc: 2026-09-01T00:00:00Z\n"
        f"    content_sha256: {'a' * 64}\n"
        "    coverage_score: 0.5\n",
        encoding="utf-8",
    )
    with pytest.raises(SourceAuditError, match="boolean"):
        load_projection_sources(path)


def _write_eval(path, rows):
    fields = [
        "player_id",
        "game_id",
        "season",
        "week",
        "projection",
        "actual",
        "as_of_utc",
        "target_cutoff_utc",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def test_scoring_includes_first_row_and_uses_separate_denominators(tmp_path) -> None:
    target = tmp_path / "target.csv"
    public = tmp_path / "public.csv"
    consensus = tmp_path / "consensus.csv"
    base = {
        "game_id": "g",
        "season": "2026",
        "as_of_utc": "2026-08-01T00:00:00Z",
        "target_cutoff_utc": "2026-08-01T00:00:00Z",
    }
    _write_eval(
        target,
        [
            {
                **base,
                "player_id": " P1 ",
                "week": "1",
                "projection": "10",
                "actual": "12",
            },
            {
                **base,
                "player_id": "P1",
                "week": "2",
                "projection": "10",
                "actual": "13",
                "target_cutoff_utc": "2026-08-08T00:00:00Z",
                "as_of_utc": "2026-08-08T00:00:00Z",
            },
        ],
    )
    _write_eval(
        public,
        [
            {**base, "player_id": "P1", "week": "1", "projection": "11", "actual": "0"},
            {
                **base,
                "player_id": "P1",
                "week": "2",
                "projection": "15",
                "actual": "0",
                "target_cutoff_utc": "2026-08-08T00:00:00Z",
                "as_of_utc": "2026-08-08T00:00:00Z",
            },
        ],
    )
    _write_eval(
        consensus,
        [
            {**base, "player_id": "P1", "week": "1", "projection": "10", "actual": "0"},
            {
                **base,
                "player_id": "P1",
                "week": "2",
                "projection": "14",
                "actual": "0",
                "target_cutoff_utc": "2026-08-08T00:00:00Z",
                "as_of_utc": "2026-08-08T00:00:00Z",
            },
        ],
    )
    result = score_rolling_origin_snapshot(
        target, public_path=public, consensus_path=consensus
    )
    assert result.public_baseline_mae == 1.5
    assert result.consensus_baseline_mae == 1.5
    assert result.historical_mean_mae == 1.0


def test_scoring_normalizes_ids_and_clamps_unique_coverage(tmp_path) -> None:
    target = tmp_path / "target.csv"
    rows = [
        {
            "player_id": p,
            "game_id": "g",
            "season": "2026",
            "week": "1",
            "projection": proj,
            "actual": "1",
            "as_of_utc": "2026-08-03T00:00:00Z",
            "target_cutoff_utc": "2026-08-03T00:00:00Z",
        }
        for p, proj in [(" p1 ", "1"), ("P1", "nan"), ("", "2"), ("P2", "inf")]
    ]
    _write_eval(target, rows)
    with pytest.raises(SourceAuditError, match="finite"):
        score_rolling_origin_snapshot(target, material_player_ids=["p1", "P2"])


def test_scoring_rejects_future_baseline_and_malformed_fields(tmp_path) -> None:
    target = tmp_path / "target.csv"
    rows = [
        {
            "player_id": "p",
            "game_id": "g",
            "season": "bad",
            "week": "1",
            "projection": "1",
            "actual": "1",
            "as_of_utc": "2026-08-03T00:00:00Z",
            "target_cutoff_utc": "2026-08-03T00:00:00Z",
        }
    ]
    _write_eval(target, rows)
    with pytest.raises(SourceAuditError, match="season"):
        score_rolling_origin_snapshot(target)


def test_tournament_checks_publisher_and_domain_independently() -> None:
    public = _source(publisher="Same", access_url="https://one.example.com/a")
    consensus = _source(
        source_id="c",
        baseline_role="consensus_reference",
        publisher="Same",
        access_url="https://two.other.net/a",
    )
    with pytest.raises(SourceAuditError, match="independent"):
        run_projection_source_tournament(
            (public, consensus), as_of_utc=datetime(2026, 9, 2, 12, tzinfo=UTC)
        )


def test_tournament_does_not_use_unrelated_source_to_prove_pair_independence() -> None:
    public = _source(publisher="Same", access_url="https://one.example.test/a")
    consensus = _source(
        source_id="c",
        baseline_role="consensus_reference",
        publisher="Same",
        access_url="https://two.example.test/a",
    )
    unrelated = _source(
        source_id="u",
        baseline_role="public_projection",
        publisher="Different",
        access_url="https://third.other.test/a",
    )
    with pytest.raises(SourceAuditError, match="independent"):
        run_projection_source_tournament(
            (public, consensus, unrelated),
            as_of_utc=datetime(2026, 9, 2, 12, tzinfo=UTC),
        )


def test_empty_material_ids_have_stable_zero_coverage(tmp_path) -> None:
    target = tmp_path / "target.csv"
    rows = [
        {
            "player_id": "p",
            "game_id": "g",
            "season": "2026",
            "week": str(week),
            "projection": "1",
            "actual": "1",
            "as_of_utc": f"2026-08-{week:02d}T00:00:00Z",
            "target_cutoff_utc": f"2026-08-{week:02d}T00:00:00Z",
        }
        for week in (1, 2)
    ]
    _write_eval(target, rows)
    assert score_rolling_origin_snapshot(target, material_player_ids=[]).coverage == 0.0


def test_historical_mean_excludes_prior_as_of_after_target_cutoff(tmp_path) -> None:
    target = tmp_path / "target.csv"
    rows = [
        {
            "player_id": "p",
            "game_id": "g",
            "season": "2026",
            "week": "1",
            "projection": "1",
            "actual": "10",
            "as_of_utc": "2026-08-09T00:00:00Z",
            "target_cutoff_utc": "2026-08-09T00:00:00Z",
        },
        {
            "player_id": "p",
            "game_id": "g",
            "season": "2026",
            "week": "2",
            "projection": "1",
            "actual": "4",
            "as_of_utc": "2026-08-08T00:00:00Z",
            "target_cutoff_utc": "2026-08-08T00:00:00Z",
        },
        {
            "player_id": "p",
            "game_id": "g",
            "season": "2026",
            "week": "3",
            "projection": "1",
            "actual": "6",
            "as_of_utc": "2026-08-10T00:00:00Z",
            "target_cutoff_utc": "2026-08-10T00:00:00Z",
        },
    ]
    _write_eval(target, rows)
    result = score_rolling_origin_snapshot(target)
    assert result.historical_mean_mae == 1.0


def test_target_as_of_after_its_cutoff_is_rejected(tmp_path) -> None:
    target = tmp_path / "target.csv"
    _write_eval(
        target,
        [
            {
                "player_id": "p",
                "game_id": "g",
                "season": "2026",
                "week": "1",
                "projection": "1",
                "actual": "1",
                "as_of_utc": "2026-08-02T00:00:00Z",
                "target_cutoff_utc": "2026-08-01T00:00:00Z",
            }
        ],
    )
    with pytest.raises(SourceAuditError, match="as_of"):
        score_rolling_origin_snapshot(target)


def test_baseline_uses_newest_candidate_before_target_cutoff(tmp_path) -> None:
    target = tmp_path / "target.csv"
    public = tmp_path / "public.csv"
    consensus = tmp_path / "consensus.csv"
    row = {
        "player_id": "p",
        "game_id": "g",
        "season": "2026",
        "week": "1",
        "projection": "1",
        "actual": "3",
        "as_of_utc": "2026-08-01T00:00:00Z",
        "target_cutoff_utc": "2026-08-03T00:00:00Z",
    }
    _write_eval(target, [row])
    old = {**row, "projection": "99", "as_of_utc": "2026-08-01T00:00:00Z"}
    new = {**row, "projection": "4", "as_of_utc": "2026-08-02T00:00:00Z"}
    future = {**row, "projection": "100", "as_of_utc": "2026-08-04T00:00:00Z"}
    _write_eval(public, [new, future, old])
    _write_eval(consensus, [new, future, old])
    assert (
        score_rolling_origin_snapshot(
            target, public_path=public, consensus_path=consensus
        ).public_baseline_mae
        == 1.0
    )


def test_fantasypros_family_is_not_independent_from_dynastyprocess() -> None:
    public = _source(
        publisher="DynastyProcess", access_url="https://dynastyprocess.com/a"
    )
    consensus = _source(
        source_id="c",
        baseline_role="consensus_reference",
        publisher="FantasyPros",
        access_url="https://fantasypros.com/a",
    )
    with pytest.raises(SourceAuditError, match="independent"):
        run_projection_source_tournament(
            (public, consensus), as_of_utc=datetime(2026, 9, 2, 12, tzinfo=UTC)
        )
