from decimal import Decimal
from pathlib import Path

import pytest
from src.fantasy.adapters.nfl.contest import (
    DEFAULT_ROSTER,
    UnknownScoringError,
    load_nfl_contest_config,
)

FIXTURES = Path(__file__).parents[3] / "testdata" / "fantasy" / "nfl"


def test_half_ppr_fixture_loads_and_reconciles_payouts() -> None:
    config = load_nfl_contest_config(FIXTURES / "half_ppr_standard.yaml")

    assert config.scoring.ruleset == "half_ppr"
    assert config.roster == DEFAULT_ROSTER
    assert config.draft.entrants == 4
    assert config.draft.rounds == 6
    assert config.field.size == 500
    assert config.position_caps.qb == 2
    assert config.payouts.prize_pool == 5000
    assert config.objective == "expected_net_payout"


def test_explicit_ppr_override_loads() -> None:
    config = load_nfl_contest_config(FIXTURES / "ppr_standard.yaml")
    assert config.scoring.ruleset == "ppr"
    assert config.scoring.points_per_reception == 1


def test_admin_royale_fixture_preserves_terms_and_reconciles_exactly() -> None:
    config = load_nfl_contest_config(FIXTURES / "ud_week2_royale_2025.yaml")

    assert config.provenance.label == "UD Week 2 Royale"
    assert config.provenance.source_live_field_size == 486
    assert config.field.field_size == 492
    assert config.field.entry_fee * config.field.field_size == Decimal("1476.00")
    assert config.field.prize_pool == Decimal("1476.00")
    assert config.field.rake == Decimal("0.00")
    assert config.field.max_entries == 15
    assert config.draft.entrants == 6
    assert config.draft.rounds == 6
    assert config.scoring.reception == 0.5
    assert len(config.payouts.ladder) == 125
    payouts_by_rank = {payout.rank: payout.amount for payout in config.payouts.ladder}
    assert payouts_by_rank[1] == Decimal("100.00")
    assert payouts_by_rank[7] == Decimal("30.00")
    assert payouts_by_rank[10] == Decimal("30.00")
    assert payouts_by_rank[46] == Decimal("6.00")
    assert payouts_by_rank[125] == Decimal("6.00")
    assert config.payouts.prize_pool == Decimal("1476.00")


def test_unknown_scoring_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "unknown.yaml"
    path.write_text("scoring:\n  ruleset: turbo_ppr\n", encoding="utf-8")
    with pytest.raises(UnknownScoringError):
        load_nfl_contest_config(path)


def test_invalid_caps_and_payouts_fail(tmp_path: Path) -> None:
    path = tmp_path / "invalid.yaml"
    path.write_text(
        """
scoring: {ruleset: half_ppr}
roster: {qb: 1, rb: 1, wr: 2, flex: 1, te: 1}
draft: {entrants: 4, rounds: 6}
field: {field_size: 10, entry_fee: 10, prize_pool: 100, rake: 0.00, max_entries: 15}
position_caps: {qb: 0, rb: 1, wr: 2, te: 1}
payouts: [{rank: 1, amount: 1}]
objective: expected_net_payout
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="position_caps.qb"):
        load_nfl_contest_config(path)


def test_flex_requires_allowed_capacity(tmp_path: Path) -> None:
    path = tmp_path / "infeasible-flex.yaml"
    path.write_text(
        """
scoring: {ruleset: half_ppr}
roster: {qb: 1, rb: 1, wr: 2, flex: 1, te: 1, flex_positions: []}
draft: {entrants: 4, rounds: 6}
field: {field_size: 10, entry_fee: 10, prize_pool: 100, rake: 0.00, max_entries: 15}
position_caps: {qb: 2, rb: 1, wr: 2, te: 1}
payouts: [{rank: 1, amount: 100}]
objective: expected_net_payout
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="flex"):
        load_nfl_contest_config(path)


def test_payout_rank_must_be_in_field(tmp_path: Path) -> None:
    path = tmp_path / "out-of-field-payout.yaml"
    path.write_text(
        """
scoring: {ruleset: half_ppr}
roster: {qb: 1, rb: 1, wr: 2, flex: 1, te: 1}
draft: {entrants: 4, rounds: 6}
field: {field_size: 10, entry_fee: 10, prize_pool: 100, rake: 0.00, max_entries: 15}
position_caps: {qb: 2, rb: 4, wr: 6, te: 3}
payouts: [{rank: 11, amount: 100}]
objective: expected_net_payout
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="field.size"):
        load_nfl_contest_config(path)
