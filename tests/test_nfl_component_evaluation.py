import pandas as pd
from src.nfl.models.component_evaluation import evaluate_archived_components


def _rows() -> pd.DataFrame:
    rows = []
    for season in (2021, 2022, 2023):
        for week in (1, 2, 3):
            rows.extend(
                [
                    {
                        "player_id": "qb1",
                        "position": "QB",
                        "season": season,
                        "week": week,
                        "pass_attempts": 20 + week,
                        "completions": 12 + week,
                        "passing_yards": 180 + 10 * week,
                        "passing_tds": 1,
                        "interceptions": 0,
                        "rushing_attempts": 3,
                        "rushing_yards": 10 + week,
                        "rushing_tds": 0,
                        "targets": 0,
                        "receptions": 0,
                        "receiving_yards": 0,
                        "receiving_tds": 0,
                    },
                    {
                        "player_id": "rb1",
                        "position": "RB",
                        "season": season,
                        "week": week,
                        "pass_attempts": 0,
                        "completions": 0,
                        "passing_yards": 0,
                        "passing_tds": 0,
                        "interceptions": 0,
                        "rushing_attempts": 14 + week,
                        "rushing_yards": 65 + 4 * week,
                        "rushing_tds": 1,
                        "targets": 4,
                        "receptions": 3,
                        "receiving_yards": 20 + week,
                        "receiving_tds": 0,
                    },
                ]
            )
    return pd.DataFrame(rows)


def test_weekly_component_evaluation_has_as_of_folds_and_metrics():
    result = evaluate_archived_components(_rows(), mode="weekly")

    assert result.status == "eligible_for_candidate_comparison"
    assert result.predictions[["season", "week"]].drop_duplicates().shape[0] == 8
    assert {"QB", "RB"} == set(result.predictions["position"])
    assert {"component_mae", "fantasy_point_mae", "fantasy_point_crps"} <= set(
        result.metrics["metric"]
    )
    assert {"WR", "TE"} == set(result.unsupported_positions)


def test_target_week_mutation_cannot_change_prior_projection():
    original = evaluate_archived_components(_rows(), mode="weekly")
    changed = _rows()
    changed.loc[
        (changed["season"] == 2023) & (changed["week"] == 3), "passing_yards"
    ] = 9999
    mutated = evaluate_archived_components(changed, mode="weekly")
    columns = ["player_id", "position", "season", "week", "predicted_fantasy_points"]
    left = original.predictions.loc[
        original.predictions["week"] < 3, columns
    ].reset_index(drop=True)
    right = mutated.predictions.loc[
        mutated.predictions["week"] < 3, columns
    ].reset_index(drop=True)
    pd.testing.assert_frame_equal(left, right)


def test_season_held_out_uses_prior_seasons_only_and_distinct_paths():
    result = evaluate_archived_components(_rows(), mode="season")

    assert result.status == "inconclusive"  # only two seasons have prior history
    assert set(result.predictions["season"]) == {2022, 2023}
    qb = result.predictions.query("position == 'QB'").iloc[0]
    rb = result.predictions.query("position == 'RB'").iloc[0]
    assert qb["predicted_pass_attempts"] > 0
    assert rb["predicted_rush_attempts"] > 0
    assert qb["predicted_pass_attempts"] != rb["predicted_rush_attempts"]


def test_source_shaped_signed_yardage_is_accepted_but_counts_stay_nonnegative():
    signed = _rows()
    signed.loc[
        (signed["season"] == 2021)
        & (signed["week"] == 1)
        & (signed["position"] == "RB"),
        "receiving_yards",
    ] = -4
    result = evaluate_archived_components(signed, mode="weekly")
    assert not result.predictions.empty

    invalid = _rows()
    invalid.loc[0, "receptions"] = -1
    try:
        evaluate_archived_components(invalid, mode="weekly")
    except ValueError as error:
        assert "nonnegative" in str(error)
    else:
        raise AssertionError("negative event counts must be rejected")
