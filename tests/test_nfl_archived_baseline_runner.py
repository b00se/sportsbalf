"""Offline tests for the archive-backed NFL baseline runner."""

from pathlib import Path

import pandas as pd
import pytest
from src.nfl.data.archive import ArchiveAdapter
from src.nfl.data.providers.base import LoadResult, reconcile_seasons
from src.nfl.models.archived_baseline_runner import (
    run_archived_baseline_backtest,
)


def _weekly() -> pd.DataFrame:
    rows = []
    for season in (2023, 2024):
        for week in (1, 2, 3):
            rows.extend(
                [
                    {
                        "season": season,
                        "week": week,
                        "player_id": "qb-a",
                        "position": "QB",
                        "passing_yards": 200 + week,
                        "attempts": 30,
                        "completions": 20,
                        "passing_tds": 2,
                        "interceptions": 0,
                        "passing_2pt_conversions": 1,
                        "rushing_yards": 10,
                        "rushing_tds": 0,
                    },
                    {
                        "season": season,
                        "week": week,
                        "player_id": "rb-a",
                        "position": "RB",
                        "rushing_yards": 80 + week,
                        "rushing_attempts": 15,
                        "rushing_tds": 1,
                        "receptions": 2,
                        "targets": 3,
                        "receiving_yards": 15,
                        "receiving_tds": 0,
                        "rushing_fumbles_lost": 1,
                        "receiving_fumbles_lost": 1,
                        "sack_fumbles_lost": 1,
                    },
                ]
            )
    return pd.DataFrame(rows)


class _FixtureProvider:
    name = "fixture"

    def __init__(self, frame: pd.DataFrame | None = None) -> None:
        self.frame = _weekly() if frame is None else frame

    def load_weekly(self, years: list[int]) -> LoadResult:
        return reconcile_seasons(self.frame, years)

    def load_weekly_raw(self, years: list[int]) -> pd.DataFrame:
        return self.frame.copy()


def test_runner_fetches_archives_and_evaluates_both_modes(tmp_path: Path) -> None:
    result = run_archived_baseline_backtest(
        _FixtureProvider(), [2023, 2024], cache_dir=tmp_path, fetch=True,
        output_dir=tmp_path / "results"
    )

    assert result.archive.payload_path.exists()
    assert result.archive.manifest_path.exists()
    assert (tmp_path / "results" / "outcomes.csv").exists()
    assert (tmp_path / "results" / "weekly_metrics.csv").exists()
    assert (tmp_path / "results" / "season_metrics.csv").exists()
    assert (tmp_path / "results" / "weekly_predictions.csv").exists()
    assert (tmp_path / "results" / "season_predictions.csv").exists()
    assert result.weekly.mode == "weekly"
    assert result.season.mode == "season"
    assert result.weekly.status == "eligible_for_candidate_comparison"
    assert result.season.status == "eligible_for_candidate_comparison"
    points = result.outcomes.query(
        "player_id == 'rb-a' and season == 2024 and week == 1"
    )
    assert points.fantasy_points.iloc[0] == pytest.approx(10.6)


def test_runner_archive_mode_verifies_immutable_manifest(tmp_path: Path) -> None:
    archive = ArchiveAdapter(tmp_path).write(
        "nflverse_weekly", _weekly(), requested_seasons=[2023, 2024]
    )
    result = run_archived_baseline_backtest(
        None,
        [2023, 2024],
        cache_dir=tmp_path,
        fetch=False,
        archive_path=archive.payload_path,
    )
    assert result.archive.manifest_path == archive.manifest_path
    assert len(result.outcomes) == len(_weekly())


def test_runner_rejects_duplicate_player_weeks(tmp_path: Path) -> None:
    duplicate = pd.concat([_weekly(), _weekly().iloc[[0]]], ignore_index=True)
    duplicate.loc[len(duplicate) - 1, "passing_yards"] = 999
    with pytest.raises(ValueError, match="duplicate player-week"):
        run_archived_baseline_backtest(
            _FixtureProvider(duplicate), [2023, 2024], cache_dir=tmp_path, fetch=True
        )


def test_runner_discards_non_player_rows_but_validates_supported_identity(
    tmp_path: Path,
) -> None:
    aggregate = pd.DataFrame(
        [{"season": 2023, "week": 1, "player_id": pd.NA, "position": pd.NA}]
    )
    with_aggregate = pd.concat([_weekly(), aggregate], ignore_index=True)
    result = run_archived_baseline_backtest(
        _FixtureProvider(with_aggregate), [2023, 2024], cache_dir=tmp_path, fetch=True
    )
    assert len(result.outcomes) == len(_weekly())

    invalid_supported = _weekly()
    invalid_supported.loc[0, "player_id"] = pd.NA
    with pytest.raises(ValueError, match="missing identity"):
        run_archived_baseline_backtest(
            _FixtureProvider(invalid_supported), [2023, 2024],
            cache_dir=tmp_path / "supported-invalid", fetch=True
        )


def test_runner_rejects_conflicting_aliases_and_sums_sacks(tmp_path: Path) -> None:
    bad = _weekly()
    bad["pass_yards"] = bad["passing_yards"] + 1
    with pytest.raises(ValueError, match="conflicting alternate"):
        run_archived_baseline_backtest(
            _FixtureProvider(bad), [2023, 2024], cache_dir=tmp_path, fetch=True
        )


def test_runner_manifest_contains_reproduction_digests(tmp_path: Path) -> None:
    output = tmp_path / "results"
    result = run_archived_baseline_backtest(
        _FixtureProvider(), [2023, 2024], cache_dir=tmp_path, fetch=True,
        output_dir=output,
    )
    manifest = (output / "run_manifest.json").read_text(encoding="utf-8")
    assert result.archive.manifest_path.name in manifest
    assert "archive_sha256" in manifest
    assert "archive_manifest_sha256" in manifest
    assert '"outcomes.csv"' in manifest


def test_runner_manifest_audits_folds_and_provider_provenance(tmp_path: Path) -> None:
    output = tmp_path / "results"
    run_archived_baseline_backtest(
        _FixtureProvider(), [2023, 2024], cache_dir=tmp_path, fetch=True,
        output_dir=output,
    )
    parsed = __import__("json").loads(
        (output / "run_manifest.json").read_text(encoding="utf-8")
    )
    assert parsed["provider"]["name"] == "fixture"
    assert parsed["provider"]["distribution"]["name"] == "fixture"
    assert parsed["provider"]["distribution"]["version"] == "unknown"
    assert parsed["reports"]["weekly"]["prediction_file"] == "weekly_predictions.csv"
    assert parsed["reports"]["season"]["prediction_file"] == "season_predictions.csv"
    assert parsed["reports"]["weekly"]["outer_fold_count"] >= 3
    assert parsed["reports"]["season"]["outer_fold_count"] == 1
    for report in parsed["reports"].values():
        assert report["prediction_sha256"]
        assert report["metrics_sha256"]
        assert report["prediction_rows"] > 0


@pytest.mark.parametrize("bad_seasons", [[True], [2024.5], [0]])
def test_runner_rejects_invalid_requested_seasons(
    tmp_path: Path, bad_seasons: list[object]
) -> None:
    with pytest.raises(ValueError, match="requested seasons"):
        run_archived_baseline_backtest(
            _FixtureProvider(), bad_seasons, cache_dir=tmp_path, fetch=True
        )


def test_runner_rejects_malformed_stats(tmp_path: Path) -> None:
    bad = _weekly()
    bad["passing_yards"] = bad["passing_yards"].astype(object)
    bad.loc[0, "passing_yards"] = "not-a-number"
    with pytest.raises(ValueError, match="passing_yards"):
        run_archived_baseline_backtest(
            _FixtureProvider(bad), [2023, 2024], cache_dir=tmp_path, fetch=True
        )


def test_runner_rejects_partial_provider(tmp_path: Path) -> None:
    class PartialProvider(_FixtureProvider):
        def load_weekly(self, years: list[int]) -> LoadResult:
            return LoadResult(
                self.frame[self.frame.season == 2023].copy(), skipped_years=[2024]
            )

    with pytest.raises(ValueError, match="every requested season"):
        run_archived_baseline_backtest(
            PartialProvider(), [2023, 2024], cache_dir=tmp_path, fetch=True
        )
