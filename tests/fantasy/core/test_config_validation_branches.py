"""Branch-focused tests for fantasy config validation helpers."""

from __future__ import annotations

import pytest
from src.fantasy.core.validation import (
    FantasyConfigValidationError,
    normalize_mode,
    parse_base_metrics,
    parse_derived_metrics,
    parse_market_definitions,
    validate_mapping_section,
    validate_mode_config_shape,
)


def test_normalize_mode_trims_and_lowercases() -> None:
    assert normalize_mode("  SINGLE_GAME_PICKEM  ", path="contest.mode") == (
        "single_game_pickem"
    )


def test_normalize_mode_rejects_invalid_mode() -> None:
    with pytest.raises(FantasyConfigValidationError, match="Invalid mode"):
        normalize_mode("unknown_mode", path="contest.mode")


def test_parse_base_metrics_rejects_non_mapping_entry() -> None:
    with pytest.raises(FantasyConfigValidationError, match="expected mapping"):
        parse_base_metrics({"base_metrics": ["bad-row"]})


def test_parse_derived_metrics_rejects_bad_transform_params() -> None:
    payload = {
        "derived_metrics": [
            {
                "derived_metric_id": "fantasy_points",
                "input_metric_ids": ["hits"],
                "transform_id": "mlb_points",
                "transform_params": {"hits": "not-a-number"},
            }
        ]
    }

    with pytest.raises(FantasyConfigValidationError, match="expected numeric value"):
        parse_derived_metrics(payload)


def test_validate_mapping_section_rejects_unresolved_policy() -> None:
    with pytest.raises(FantasyConfigValidationError, match="unresolved_policy"):
        validate_mapping_section(
            {
                "player_id_map_path": "tests/testdata/fantasy_player_mapping.csv",
                "unresolved_policy": "explode",
            }
        )


def test_parse_market_definitions_rejects_mode_mismatch() -> None:
    with pytest.raises(FantasyConfigValidationError, match="must match"):
        parse_market_definitions(
            raw_markets={"mode": "short_slate_fantasy", "definitions": []},
            provider="underdog",
            sport="mlb",
            mode="season_long_tournament",
        )


def test_parse_market_definitions_rejects_line_without_operator() -> None:
    with pytest.raises(FantasyConfigValidationError, match="operator is required"):
        parse_market_definitions(
            raw_markets={
                "mode": "single_game_pickem",
                "definitions": [
                    {
                        "market_id": "m1",
                        "metric_id": "hits",
                        "horizon": "game",
                        "line_value": 1.5,
                        "window_start": "2026-06-01",
                        "window_end": "2026-06-01",
                    }
                ],
            },
            provider="underdog",
            sport="mlb",
            mode="single_game_pickem",
        )


def test_validate_mode_config_shape_rejects_bad_type() -> None:
    with pytest.raises(FantasyConfigValidationError, match="mode_config.roster"):
        validate_mode_config_shape("season_long_tournament", {"roster": 18})
