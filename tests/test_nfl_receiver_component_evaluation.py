"""Offline contract tests for the archived WR/TE component evaluator."""

import pandas as pd
import pytest
import src.nfl.models.receiver_component_evaluation as evaluation
from src.nfl.models.receiver_component_evaluation import (
    evaluate_archived_receiver_components,
)


def _outcomes() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for season in (2021, 2022, 2023):
        for week in (1, 2, 3):
            rows.extend(
                [
                    {
                        "player_id": "wr1",
                        "position": "WR",
                        "season": season,
                        "week": week,
                        "targets": 8 + week,
                        "receptions": 5 + week % 2,
                        "receiving_yards": 60 + 10 * week,
                        "receiving_tds": int(week == 3),
                        "rushing_attempts": 1,
                        "rushing_yards": 4,
                        "rushing_tds": 0,
                    },
                    {
                        "player_id": "te1",
                        "position": "TE",
                        "season": season,
                        "week": week,
                        "targets": 6 + week,
                        "receptions": 4,
                        "receiving_yards": 40 + 5 * week,
                        "receiving_tds": 0,
                        "rushing_attempts": 0,
                        "rushing_yards": 0,
                        "rushing_tds": 0,
                    },
                    # The evaluator accepts an all-position archive but must
                    # not let unsupported positions affect receiver folds.
                    {
                        "player_id": "qb1",
                        "position": "QB",
                        "season": season,
                        "week": week,
                        "pass_attempts": 20,
                        "completions": 12,
                        "passing_yards": 180,
                        "passing_tds": 1,
                        "interceptions": 0,
                        "rushing_attempts": 2,
                        "rushing_yards": 5,
                        "rushing_tds": 0,
                    },
                ]
            )
    return pd.DataFrame(rows)


def _routes() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "player_id": "wr1",
                "position": "WR",
                "season": 2021,
                "week": 1,
                "team": "A",
                "identified_receiver_routes": 20,
            },
            {
                "player_id": "wr1",
                "position": "WR",
                "season": 2021,
                "week": 1,
                "team": "B",
                "identified_receiver_routes": 4,
            },
            {
                "player_id": "te1",
                "position": "TE",
                "season": 2021,
                "week": 1,
                "team": "A",
                "identified_receiver_routes": 18,
            },
            {
                "player_id": "wr1",
                "position": "WR",
                "season": 2021,
                "week": 2,
                "team": "A",
                "identified_receiver_routes": 22,
            },
            {
                "player_id": "te1",
                "position": "TE",
                "season": 2021,
                "week": 2,
                "team": "A",
                "identified_receiver_routes": 19,
            },
            {
                "player_id": "wr1",
                "position": "WR",
                "season": 2021,
                "week": 3,
                "team": "A",
                "identified_receiver_routes": 25,
            },
            {
                "player_id": "te1",
                "position": "TE",
                "season": 2021,
                "week": 3,
                "team": "A",
                "identified_receiver_routes": 20,
            },
        ]
    )


def test_receiver_walk_forward_uses_all_targets_and_shared_metric_contract() -> None:
    result = evaluate_archived_receiver_components(_outcomes(), _routes())

    assert result.status == "eligible_for_candidate_comparison"
    assert set(result.predictions["position"]) == {"WR", "TE"}
    assert set(result.predictions["player_id"]) == {"wr1", "te1"}
    assert {"component_mae", "fantasy_point_mae", "fantasy_point_crps"} <= set(
        result.metrics["metric"]
    )
    aggregate = result.metrics.query("aggregation == 'aggregate'")
    assert aggregate["outer_folds"].ge(3).all()
    assert {"spearman_rank", "top_k_recall", "calibration"} <= set(
        aggregate["metric"]
    )
    assert result.matched_history_rows > 0
    assert result.target_rows == len(_outcomes().query("position in ['WR', 'TE']"))


def test_multi_team_routes_are_aggregated_and_duplicate_keys_rejected() -> None:
    result = evaluate_archived_receiver_components(_outcomes(), _routes())
    history = result.predictions.query(
        "season == 2021 and week == 2 and player_id == 'wr1'"
    )
    assert len(history) == 1

    duplicate = pd.concat([_routes(), _routes().iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate route key"):
        evaluate_archived_receiver_components(_outcomes(), duplicate)

    conflict = _routes().copy()
    conflict.loc[0, "position"] = "TE"
    with pytest.raises(ValueError, match="conflicting positions"):
        evaluate_archived_receiver_components(_outcomes(), conflict)


def test_target_week_route_and_stats_mutation_cannot_change_prior_predictions() -> None:
    original = evaluate_archived_receiver_components(_outcomes(), _routes())
    changed_outcomes = _outcomes()
    target = (changed_outcomes["season"] == 2021) & (changed_outcomes["week"] == 3)
    changed_outcomes.loc[target, "receiving_yards"] = 9999
    changed_routes = _routes().copy()
    changed_routes.loc[
        (changed_routes["season"] == 2021) & (changed_routes["week"] == 3),
        "identified_receiver_routes",
    ] = 9999
    changed = evaluate_archived_receiver_components(changed_outcomes, changed_routes)
    columns = ["player_id", "position", "season", "week", "predicted_fantasy_points"]
    left = original.predictions.query("season == 2021 and week < 3")[columns]
    right = changed.predictions.query("season == 2021 and week < 3")[columns]
    pd.testing.assert_frame_equal(
        left.reset_index(drop=True), right.reset_index(drop=True)
    )


def test_projector_receives_identity_only_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[pd.DataFrame] = []
    original = evaluation.project_receiver_components

    def spy(
        history: pd.DataFrame,
        targets: pd.DataFrame,
        **kwargs: object,
    ) -> pd.DataFrame:
        seen.append(targets.copy(deep=True))
        return original(history, targets, **kwargs)

    monkeypatch.setattr(evaluation, "project_receiver_components", spy)
    evaluate_archived_receiver_components(_outcomes(), _routes())
    assert seen
    assert all(
        set(target.columns) == {"receiver_id", "position", "season", "week"}
        for target in seen
    )


def test_season_mode_and_optional_scoring_streams_are_supported() -> None:
    outcomes = _outcomes()
    outcomes["fumbles_lost"] = 0.0
    outcomes["two_point_conversions"] = 0.0
    outcomes.loc[
        (outcomes["position"] == "WR") & (outcomes["season"] == 2023),
        "fumbles_lost",
    ] = 1.0
    result = evaluate_archived_receiver_components(
        outcomes, _routes(), mode="season"
    )

    assert result.mode == "season"
    assert result.status == "eligible_for_candidate_comparison"
    assert set(result.predictions["outer_fold"]) == {"2021", "2022", "2023"}
    assert result.predictions["actual_fantasy_points"].notna().all()


def test_conflicting_component_aliases_fail_closed() -> None:
    outcomes = _outcomes()
    outcomes["rush_attempts"] = outcomes["rushing_attempts"] + 1
    with pytest.raises(ValueError, match="conflicting aliases"):
        evaluate_archived_receiver_components(outcomes, _routes())
