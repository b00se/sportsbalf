"""Phase 1.5 pybaseball priors tests."""

from __future__ import annotations

import pandas as pd

PRIOR_COLUMNS = ["prior_pa", "prior_bb_rate", "prior_k_rate"]


def test_join_priors_uses_league_median_fallback_for_missing_ids() -> None:
    from src.fantasy.adapters.mlb.priors import attach_priors_to_snapshots

    snapshots = pd.DataFrame(
        {
            "entity_id": ["101", "202"],
            "fg_id": [11, 99],
            "season": [2025, 2025],
        }
    )
    priors = pd.DataFrame(
        {
            "fg_id": [11],
            "season": [2025],
            "prior_pa": [600.0],
            "prior_bb_rate": [0.09],
            "prior_k_rate": [0.21],
        }
    )

    enriched = attach_priors_to_snapshots(
        snapshots=snapshots,
        priors=priors,
        prior_columns=tuple(PRIOR_COLUMNS),
    )

    present = enriched[enriched["fg_id"] == 11].iloc[0]
    missing = enriched[enriched["fg_id"] == 99].iloc[0]

    assert float(present["prior_pa"]) == 600.0
    assert int(present["prior_imputed_flag"]) == 0
    assert float(missing["prior_pa"]) == 600.0
    assert int(missing["prior_imputed_flag"]) == 1


def test_load_cached_priors_reads_parquet_without_network(tmp_path) -> None:
    from src.fantasy.adapters.mlb.priors import load_cached_priors

    expected = pd.DataFrame(
        {
            "fg_id": [11],
            "season": [2025],
            "prior_pa": [620.0],
        }
    )
    cache_path = tmp_path / "priors.parquet"
    expected.to_parquet(cache_path, index=False)

    loaded = load_cached_priors(str(cache_path))

    pd.testing.assert_frame_equal(loaded, expected)


def test_join_priors_without_fg_id_does_not_duplicate_rows() -> None:
    from src.fantasy.adapters.mlb.priors import attach_priors_to_snapshots

    snapshots = pd.DataFrame(
        {
            "entity_id": ["a", "b"],
            "season": [2025, 2025],
        }
    )
    priors = pd.DataFrame(
        {
            "fg_id": [11, 12],
            "season": [2025, 2025],
            "prior_pa": [500.0, 600.0],
            "prior_bb_rate": [0.08, 0.10],
            "prior_k_rate": [0.20, 0.22],
        }
    )

    enriched = attach_priors_to_snapshots(
        snapshots=snapshots,
        priors=priors,
        prior_columns=tuple(PRIOR_COLUMNS),
    )

    assert len(enriched) == len(snapshots)
    assert (enriched["prior_imputed_flag"] == 1).all()
