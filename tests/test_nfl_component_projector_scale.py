"""Realistic-scale acceptance coverage for archived QB/RB projections."""

import time

import pandas as pd
from src.nfl.models.component_evaluation import evaluate_archived_components
from src.nfl.models.qb_components import project_qb_components
from src.nfl.models.rb_components import project_rb_components


def test_team_share_clips_each_prior_observation_before_averaging() -> None:
    """Preserve historical per-week clipping for QB and RB team shares."""
    qb_history = pd.DataFrame(
        [
            {
                "qb_id": "qb",
                "season": 2024,
                "week": 1,
                "pass_attempts": 20,
                "team_pass_attempts": 10,
            },
            {
                "qb_id": "qb",
                "season": 2024,
                "week": 2,
                "pass_attempts": 0,
                "team_pass_attempts": 10,
            },
        ]
    )
    qb_target = pd.DataFrame(
        [{"qb_id": "qb", "season": 2024, "week": 3, "team_pass_attempts": 100}]
    )
    assert project_qb_components(qb_history, qb_target).loc[0, "pass_attempts"] == 50.0

    rb_history = pd.DataFrame(
        [
            {
                "rb_id": "rb",
                "season": 2024,
                "week": 1,
                "rush_attempts": 20,
                "targets": 20,
                "team_rush_attempts": 10,
                "team_targets": 10,
                "rushing_yards": 0,
                "receptions": 0,
                "receiving_yards": 0,
                "rushing_tds": 0,
                "receiving_tds": 0,
            },
            {
                "rb_id": "rb",
                "season": 2024,
                "week": 2,
                "rush_attempts": 0,
                "targets": 0,
                "team_rush_attempts": 10,
                "team_targets": 10,
                "rushing_yards": 0,
                "receptions": 0,
                "receiving_yards": 0,
                "rushing_tds": 0,
                "receiving_tds": 0,
            },
        ]
    )
    rb_target = pd.DataFrame(
        [
            {
                "rb_id": "rb",
                "season": 2024,
                "week": 3,
                "team_rush_attempts": 100,
                "team_targets": 100,
            }
        ]
    )
    rb = project_rb_components(rb_history, rb_target)
    assert rb.loc[0, "rush_attempts"] == 50.0
    assert rb.loc[0, "targets"] == 50.0


def test_optimized_projectors_match_independent_reference_calculation() -> None:
    """Check optimized outputs against explicit prior-only QB/RB calculations."""
    qb_history = pd.DataFrame(
        [
            {
                "qb_id": "q1",
                "season": 2024,
                "week": 1,
                "pass_attempts": 20,
                "completions": 10,
                "passing_yards": 100,
                "passing_tds": 1,
                "interceptions": 0,
                "rushing_attempts": 4,
                "rushing_yards": -4,
                "rushing_tds": 0,
                "team_pass_attempts": 10,
            },
            {
                "qb_id": "q1",
                "season": 2024,
                "week": 2,
                "pass_attempts": 0,
                "completions": 0,
                "passing_yards": 0,
                "passing_tds": 0,
                "interceptions": 0,
                "rushing_attempts": 0,
                "rushing_yards": 0,
                "rushing_tds": 0,
                "team_pass_attempts": 10,
            },
        ]
    )
    qb_targets = pd.DataFrame(
        [
            {"qb_id": "q1", "season": 2024, "week": 3, "team_pass_attempts": 50},
            {"qb_id": "q2", "season": 2024, "week": 3},
        ]
    )
    qb = project_qb_components(qb_history, qb_targets)
    qb_expected = pd.DataFrame(
        [
            [25.0, 12.5, 125.0, 1.25, 0.0, 2.0, 0.0, 0.0],
            [30.0, 18.6, 210.0, 1.2, 0.75, 3.0, 12.0, 0.06],
        ],
        columns=[
            "pass_attempts",
            "completions",
            "passing_yards",
            "passing_tds",
            "interceptions",
            "rushing_attempts",
            "rushing_yards",
            "rushing_tds",
        ],
    )
    pd.testing.assert_frame_equal(
        qb[qb_expected.columns].reset_index(drop=True), qb_expected
    )

    rb_history = pd.DataFrame(
        [
            {
                "rb_id": "r1",
                "season": 2024,
                "week": 1,
                "rush_attempts": 20,
                "targets": 6,
                "rushing_yards": -5,
                "receptions": 3,
                "receiving_yards": -8,
                "rushing_tds": 1,
                "receiving_tds": 0,
                "team_rush_attempts": 10,
                "team_targets": 10,
            },
            {
                "rb_id": "r1",
                "season": 2024,
                "week": 2,
                "rush_attempts": 0,
                "targets": 0,
                "rushing_yards": 0,
                "receptions": 0,
                "receiving_yards": 0,
                "rushing_tds": 0,
                "receiving_tds": 0,
                "team_rush_attempts": 10,
                "team_targets": 10,
            },
        ]
    )
    rb_targets = pd.DataFrame(
        [
            {
                "rb_id": "r1",
                "season": 2024,
                "week": 3,
                "team_rush_attempts": 50,
                "team_targets": 20,
            },
            {"rb_id": "r2", "season": 2024, "week": 3},
        ]
    )
    rb = project_rb_components(rb_history, rb_targets)
    rb_expected = pd.DataFrame(
        [
            [25.0, 6.0, 0.0, 3.0, 0.0, 1.25, 0.0],
            [12.0, 4.0, 50.4, 2.8, 22.4, 0.48, 0.056],
        ],
        columns=[
            "rush_attempts",
            "targets",
            "rushing_yards",
            "receptions",
            "receiving_yards",
            "rushing_tds",
            "receiving_tds",
        ],
    )
    pd.testing.assert_frame_equal(
        rb[rb_expected.columns].reset_index(drop=True), rb_expected
    )


def _scale_frame() -> pd.DataFrame:
    """Create 109 weekly folds and 11,468 deterministic player-week rows."""
    rows: list[dict[str, object]] = []
    for fold in range(109):
        season = 2010 + fold // 17
        week = fold % 17 + 1
        for player in range(52):
            rows.extend(
                [
                    {
                        "player_id": f"qb-{player}",
                        "position": "QB",
                        "season": season,
                        "week": week,
                        "pass_attempts": 28 + player % 5,
                        "completions": 18 + player % 4,
                        "passing_yards": 205 + player,
                        "passing_tds": 1,
                        "interceptions": 1,
                        "rushing_attempts": 3,
                        "rushing_yards": 12,
                        "rushing_tds": 0,
                        "targets": 0,
                        "receptions": 0,
                        "receiving_yards": 0,
                        "receiving_tds": 0,
                    },
                    {
                        "player_id": f"rb-{player}",
                        "position": "RB",
                        "season": season,
                        "week": week,
                        "pass_attempts": 0,
                        "completions": 0,
                        "passing_yards": 0,
                        "passing_tds": 0,
                        "interceptions": 0,
                        "rushing_attempts": 13 + player % 4,
                        "rushing_yards": 65 + player,
                        "rushing_tds": 1,
                        "targets": 4,
                        "receptions": 3,
                        "receiving_yards": 22,
                        "receiving_tds": 0,
                    },
                ]
            )
        rows.append(
            {
                "player_id": f"qb-extra-{fold}",
                "position": "QB",
                "season": season,
                "week": week,
                "pass_attempts": 30,
                "completions": 19,
                "passing_yards": 220,
                "passing_tds": 2,
                "interceptions": 0,
                "rushing_attempts": 2,
                "rushing_yards": 8,
                "rushing_tds": 0,
                "targets": 0,
                "receptions": 0,
                "receiving_yards": 0,
                "receiving_tds": 0,
            }
        )
    # Add 23 rows to an existing fold, producing the exact archived workload
    # size used by the R3 completion measurement without changing fold count.
    for extra in range(23):
        rows.append(
            {
                "player_id": f"qb-extra-final-{extra}",
                "position": "QB",
                "season": 2016,
                "week": 7,
                "pass_attempts": 29,
                "completions": 18,
                "passing_yards": 210,
                "passing_tds": 1,
                "interceptions": 0,
                "rushing_attempts": 2,
                "rushing_yards": 7,
                "rushing_tds": 0,
                "targets": 0,
                "receptions": 0,
                "receiving_yards": 0,
                "receiving_tds": 0,
            }
        )
    frame = pd.DataFrame(rows)
    assert len(frame) == 11_468
    assert frame[["season", "week"]].drop_duplicates().shape[0] == 109
    return frame


def test_archived_component_projection_completes_at_r3_scale() -> None:
    """Run projection and scoring work on the measured 11,468-row workload."""
    frame = _scale_frame()
    started = time.perf_counter()
    result = evaluate_archived_components(frame, mode="weekly")
    elapsed = time.perf_counter() - started

    assert result.status == "eligible_for_candidate_comparison"
    assert len(result.predictions) > 10_000
    assert set(result.predictions["position"]) == {"QB", "RB"}
    # The bound is intentionally broad for shared CI hosts; it catches the
    # pre-optimization multi-minute regression while avoiding timing flakes.
    assert elapsed < 90.0
