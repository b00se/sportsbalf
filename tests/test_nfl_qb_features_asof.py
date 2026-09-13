"""As-of and materialization contracts for NFL QB team features."""

import pandas as pd
import pytest
from src.nfl.features.qb_features import compute_team_and_opponent_features


def _pbp() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    play_id = 0
    for week, a_passes, c_passes in ((1, 1, 0), (2, 0, 1), (3, 1, 1), (4, 0, 0)):
        for game_id, offense, defense, passes in (
            (f"a{week}", "A", "B", a_passes),
            (f"c{week}", "C", "D", c_passes),
        ):
            for pass_attempt in (1,) * passes + (0,):
                play_id += 1
                rows.append(
                    {
                        "season": 2024,
                        "week": week,
                        "game_id": game_id,
                        "posteam": offense,
                        "defteam": defense,
                        "pass_attempt": pass_attempt,
                        "rush_attempt": 1 - pass_attempt,
                        "score_differential": 0,
                        "qtr": 1,
                        "play_id": play_id,
                    }
                )
    return pd.DataFrame(rows)


def test_future_row_perturbation_does_not_change_any_earlier_feature() -> None:
    source = _pbp()
    baseline = compute_team_and_opponent_features(source)
    changed = source.copy()
    changed.loc[changed["week"].eq(4), "pass_attempt"] = 1
    changed.loc[changed["week"].eq(4), "rush_attempt"] = 0
    perturbed = compute_team_and_opponent_features(changed)

    for before, after in zip(baseline, perturbed, strict=True):
        pd.testing.assert_frame_equal(
            before.loc[before["week"] < 4].reset_index(drop=True),
            after.loc[after["week"] < 4].reset_index(drop=True),
            check_dtype=False,
        )


def test_materialization_is_invariant_to_source_order() -> None:
    source = _pbp()
    expected = compute_team_and_opponent_features(source)
    shuffled = compute_team_and_opponent_features(source.sample(frac=1, random_state=7))
    for before, after in zip(expected, shuffled, strict=True):
        pd.testing.assert_frame_equal(
            before.reset_index(drop=True),
            after.reset_index(drop=True),
            check_dtype=False,
        )


def test_target_features_use_completed_prior_week_and_retain_archive_reference() -> (
    None
):
    source = _pbp()
    source.attrs["archive_reference"] = "fixture:nflverse:2024"
    team, opponent = compute_team_and_opponent_features(source)

    week_two = team.loc[(team["team"] == "A") & (team["week"] == 2)].iloc[0]
    assert week_two["plays_per_game"] == 2
    assert week_two["pass_rate"] == 0.5
    assert week_two["neutral_pass_rate"] == 0.5
    assert week_two["pass_rate_over_expected"] == pytest.approx(1 / 6)
    week_three = team.loc[(team["team"] == "A") & (team["week"] == 3)].iloc[0]
    assert week_three["pass_rate_over_expected"] == pytest.approx(-1 / 3)
    assert (
        opponent.loc[
            (opponent["opponent"] == "B") & (opponent["week"] == 2), "plays_faced"
        ].iloc[0]
        == 2
    )
    assert team.attrs["archive_reference"] == "fixture:nflverse:2024"
    assert opponent.attrs["archive_reference"] == "fixture:nflverse:2024"


def test_malformed_and_duplicate_canonical_keys_fail_closed() -> None:
    source = _pbp()
    missing = source.drop(columns="game_id")
    with pytest.raises(KeyError, match="game_id"):
        compute_team_and_opponent_features(missing)

    duplicate = pd.concat([source, source.iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate"):
        compute_team_and_opponent_features(duplicate)

    cross_team_duplicate = source.copy()
    cross_team_duplicate.loc[0, "posteam"] = "Z"
    with pytest.raises(ValueError, match="duplicate"):
        compute_team_and_opponent_features(
            pd.concat([source, cross_team_duplicate.iloc[[0]]], ignore_index=True)
        )

    malformed = source.copy()
    malformed.loc[0, "week"] = 0
    with pytest.raises(ValueError, match="week"):
        compute_team_and_opponent_features(malformed)
