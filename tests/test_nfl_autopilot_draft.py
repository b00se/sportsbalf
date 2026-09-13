from dataclasses import replace

import pytest
from src.fantasy.adapters.nfl.contest import (
    DEFAULT_ROSTER,
    DraftConfig,
    FieldConfig,
    NflContestConfig,
    PayoutConfig,
    PositionCaps,
    ScoringConfig,
)
from src.nfl.models.autopilot_draft import (
    DraftPlayer,
    DraftValidationError,
    simulate_autopilot_draft,
)


def config(entrants=4):
    return NflContestConfig(
        contest_id="test",
        provenance=None,
        scoring=ScoringConfig(),
        roster=DEFAULT_ROSTER,
        draft=DraftConfig(entrants=entrants, rounds=6),
        field=FieldConfig(entrants, 1, 1, 0, None),
        position_caps=PositionCaps(
            qb=entrants, rb=entrants + 1, wr=entrants + 2, te=entrants + 1
        ),
        payouts=PayoutConfig((), 0),
        objective="expected_net_payout",
    )


def pool(n=40):
    positions = ["QB", "RB", "WR", "WR", "TE"] * (n // 5 + 1)
    return [
        DraftPlayer(f"p{i:02d}", positions[i], rank=i + 1, projection=100 - i)
        for i in range(n)
    ]


def test_four_entrant_snake_order_and_reproducibility():
    first = simulate_autopilot_draft(config(4), pool(), seed=7)
    second = simulate_autopilot_draft(config(4), pool(), seed=7)
    assert first == second
    assert [(p.round, p.entrant) for p in first.selections] == [
        (1, 0),
        (1, 1),
        (1, 2),
        (1, 3),
        (2, 3),
        (2, 2),
        (2, 1),
        (2, 0),
        (3, 0),
        (3, 1),
        (3, 2),
        (3, 3),
        (4, 3),
        (4, 2),
        (4, 1),
        (4, 0),
        (5, 0),
        (5, 1),
        (5, 2),
        (5, 3),
        (6, 3),
        (6, 2),
        (6, 1),
        (6, 0),
    ]


def test_highest_ranked_legal_pick_and_position_caps():
    result = simulate_autopilot_draft(config(4), pool(), seed=7)
    assert [selection.player.player_id for selection in result.selections[:4]] == [
        "p00",
        "p01",
        "p02",
        "p03",
    ]
    for roster in result.rosters:
        assert sum(player.position == "QB" for player in roster.players) <= 1

    capped = replace(
        config(4),
        position_caps=PositionCaps(qb=4, rb=1, wr=6, te=5),
    )
    capped_result = simulate_autopilot_draft(capped, pool(), seed=7)
    assert [s.player.player_id for s in capped_result.selections[:4]] == [
        "p00",
        "p01",
        "p02",
        "p03",
    ]
    for roster in capped_result.rosters:
        assert sum(player.position == "RB" for player in roster.players) <= 1


def test_globally_feasible_pool_completes():
    positions = [
        "TE", "WR", "TE", "WR", "TE", "WR", "RB", "WR", "RB", "RB",
        "QB", "RB", "QB", "TE", "WR", "WR", "TE", "WR", "WR", "QB",
        "RB", "QB", "WR", "RB",
    ]
    players = [
        DraftPlayer(str(i), position, rank=i) for i, position in enumerate(positions)
    ]
    result = simulate_autopilot_draft(config(4), players, seed=0)
    assert len(result.selections) == 24


def test_six_entrant_snake_order():
    result = simulate_autopilot_draft(config(6), pool(), seed=1)
    assert [(p.round, p.entrant) for p in result.selections] == [
        (r, e) for r in range(1, 7) for e in (range(6) if r % 2 else range(5, -1, -1))
    ]


def test_rosters_are_legal_and_flex_is_explicit():
    result = simulate_autopilot_draft(config(), pool(), seed=2)
    for roster in result.rosters:
        assert len(roster.players) == 6
        assert len(roster.by_slot["QB"]) == 1
        assert len(roster.by_slot["RB"]) == 1
        assert len(roster.by_slot["WR"]) == 2
        assert len(roster.by_slot["TE"]) == 1
        assert len(roster.by_slot["FLEX"]) == 1
        assert roster.by_slot["FLEX"][0].position in {"RB", "WR", "TE"}


def test_no_duplicates_and_ties_are_deterministic():
    tied = [
        DraftPlayer(
            f"z{i}", ["QB", "RB", "WR", "WR", "TE"][i % 5], rank=1, projection=10
        )
        for i in range(30)
    ]
    a = simulate_autopilot_draft(config(), tied, seed=99)
    b = simulate_autopilot_draft(config(), tied, seed=99)
    assert a == b
    assert len({p.player.player_id for p in a.selections}) == 24


@pytest.mark.parametrize(
    "bad_config", [config(5), replace(config(), draft=DraftConfig(4, 5))]
)
def test_invalid_draft_config_fails_closed(bad_config):
    with pytest.raises(DraftValidationError):
        simulate_autopilot_draft(bad_config, pool(), seed=1)


def test_infeasible_pool_and_duplicate_ids_fail_closed():
    with pytest.raises(DraftValidationError):
        simulate_autopilot_draft(config(), pool(5), seed=1)
    with pytest.raises(DraftValidationError):
        simulate_autopilot_draft(
            config(),
            [DraftPlayer("x", "QB", 1, 1), DraftPlayer("x", "RB", 2, 1)],
            seed=1,
        )
