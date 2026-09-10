import pandas as pd
import pytest
from src.nfl.models.projection_ensemble import (
    EnsembleInputError,
    run_nested_ensemble_tournament,
)


def _frame(values):
    rows = []
    for week, values_for_week in enumerate(values, 1):
        for player, value in values_for_week.items():
            rows.append(
                {"player_id": player, "season": 2026, "week": week, "prediction": value}
            )
    return pd.DataFrame(rows)


def _actual(values):
    rows = []
    for week, values_for_week in enumerate(values, 1):
        for player, value in values_for_week.items():
            rows.append(
                {"player_id": player, "season": 2026, "week": week, "actual": value}
            )
    return pd.DataFrame(rows)


def test_nested_selection_is_prior_only_and_target_mutation_invariant():
    internal = _frame([{"a": 10}] * 4)
    underdog = _frame([{"a": 8}] * 4)
    consensus = _frame([{"a": 12}] * 4)
    actual = _actual([{"a": 8}, {"a": 8}, {"a": 12}, {"a": 12}])
    first = run_nested_ensemble_tournament(internal, underdog, consensus, actual)
    mutated = actual.copy()
    mutated.loc[mutated.week == 4, "actual"] = 1000
    second = run_nested_ensemble_tournament(internal, underdog, consensus, mutated)
    assert first.predictions.iloc[0].to_dict() == second.predictions.iloc[0].to_dict()
    assert (
        first.predictions.iloc[1][["prediction", "selected_model"]].to_dict()
        == second.predictions.iloc[1][["prediction", "selected_model"]].to_dict()
    )
    assert all(
        row["selection_scope"] == "prior_calendar_only"
        for row in first.predictions.to_dict("records")
    )


def test_tie_promotes_and_result_preserves_source_provenance():
    source = _frame([{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}])
    actual = _actual([{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}])
    result = run_nested_ensemble_tournament(source, source, source, actual)
    assert result.promoted is True
    assert result.promotion_status == "promoted_tie_or_better"
    assert set(result.predictions["source_identity"]) == {"internal+underdog+consensus"}


def test_missing_or_semantic_duplicate_source_fails_closed():
    source = _frame([{"a": 1}, {"a": 2}, {"a": 3}])
    with pytest.raises(EnsembleInputError):
        run_nested_ensemble_tournament(
            source, None, source, _actual([{"a": 1}, {"a": 2}, {"a": 3}])
        )
    duplicate = pd.concat([source, source.iloc[[0]].assign(season="2026", week="1")])
    with pytest.raises(EnsembleInputError, match="duplicate"):
        run_nested_ensemble_tournament(
            duplicate, source, source, _actual([{"a": 1}, {"a": 2}, {"a": 3}])
        )


def test_inconclusive_uncertainty_never_claims_edge():
    source = _frame([{"a": 1}, {"a": 2}, {"a": 3}])
    result = run_nested_ensemble_tournament(
        source, source, source, _actual([{"a": 1}, {"a": 2}, {"a": 3}])
    )
    assert result.uncertainty["status"] == "inconclusive"
    assert result.uncertainty["edge_claim"] is False
