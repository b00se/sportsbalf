"""Tests for unified fantasy Phase 0 config validation."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
import yaml
from src.fantasy.core.config import (
    FantasyConfigValidationError,
    load_unified_fantasy_config,
)

BASE_CONFIG = {
    "contest": {
        "contest_id": "ud-mlb-2026-season",
        "provider": "underdog",
        "sport": "mlb",
        "mode": "season_long_tournament",
        "scoring_ruleset_id": "ud_mlb_classic",
        "mode_config": {
            "roster": {"slots": 18},
            "advancement": {"rounds": [12, 2]},
            "payouts": {"first": 100000},
        },
        "metadata": {"season": 2026},
    },
    "metrics": {
        "base_metrics": [
            {
                "metric_id": "hits",
                "horizon": "season",
                "adapter_key": "mlb_hits_season",
            },
            {
                "metric_id": "runs",
                "horizon": "season",
                "adapter_key": "mlb_runs_season",
            },
            {"metric_id": "hits", "horizon": "game", "adapter_key": "mlb_hits_game"},
        ],
        "derived_metrics": [
            {
                "derived_metric_id": "fantasy_points",
                "input_metric_ids": ["hits", "runs"],
                "transform_id": "mlb_points",
                "transform_params": {"hits": 3.0, "runs": 2.0},
            }
        ],
    },
    "mapping": {
        "player_id_map_path": "tests/testdata/fantasy_player_mapping.csv",
        "unresolved_policy": "fail",
    },
    "markets": {
        "mode": "season_long_tournament",
        "definitions": [
            {
                "market_id": "season_points",
                "metric_id": "fantasy_points",
                "horizon": "season",
                "window_start": "2026-03-01",
                "window_end": "2026-10-01",
            }
        ],
    },
}


def _write_config(tmp_path: Path, payload: dict) -> str:
    path = tmp_path / "fantasy.yaml"
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return str(path)


def test_season_long_fantasy_config_loads(tmp_path: Path) -> None:
    config = load_unified_fantasy_config(_write_config(tmp_path, BASE_CONFIG))

    assert config.contest.mode == "season_long_tournament"
    assert config.market_mode == "season_long_tournament"


def test_short_slate_fantasy_config_loads(tmp_path: Path) -> None:
    payload = deepcopy(BASE_CONFIG)
    payload["contest"]["mode"] = "short_slate_fantasy"
    payload["markets"]["mode"] = "short_slate_fantasy"
    payload["contest"]["mode_config"] = {
        "roster": {"slots": 6},
        "slate_selection_rule": "main_only",
    }

    config = load_unified_fantasy_config(_write_config(tmp_path, payload))
    assert config.contest.mode == "short_slate_fantasy"


def test_single_game_pickem_config_loads_with_line_operator(tmp_path: Path) -> None:
    payload = deepcopy(BASE_CONFIG)
    payload["markets"] = {
        "mode": "single_game_pickem",
        "definitions": [
            {
                "market_id": "game_hits_over",
                "metric_id": "hits",
                "horizon": "game",
                "operator": "over",
                "line_value": 1.5,
                "window_start": "2026-06-01",
                "window_end": "2026-06-01",
                "game_id": "game-123",
            }
        ],
    }
    payload["contest"]["mode"] = "single_game_pickem"
    payload["contest"]["scoring_ruleset_id"] = None
    payload["contest"]["mode_config"] = {
        "slip_constraints": {"min_legs": 2},
        "payout_ladder": {"2": 3.0},
    }

    config = load_unified_fantasy_config(_write_config(tmp_path, payload))
    assert config.markets[0].operator == "over"


def test_season_long_stat_pickem_config_loads(tmp_path: Path) -> None:
    payload = deepcopy(BASE_CONFIG)
    payload["markets"] = {
        "mode": "season_long_stat_pickem",
        "definitions": [
            {
                "market_id": "season_hits_over",
                "metric_id": "hits",
                "horizon": "season",
                "operator": "over",
                "line_value": 149.5,
                "window_start": "2026-03-01",
                "window_end": "2026-10-01",
            }
        ],
    }
    payload["contest"]["mode"] = "season_long_stat_pickem"
    payload["contest"]["scoring_ruleset_id"] = None
    payload["contest"]["mode_config"] = {
        "slip_constraints": {"min_legs": 2},
        "payout_ladder": {"2": 3.0},
    }

    config = load_unified_fantasy_config(_write_config(tmp_path, payload))
    assert config.market_mode == "season_long_stat_pickem"


def test_invalid_metric_horizon_reference_fails(tmp_path: Path) -> None:
    payload = deepcopy(BASE_CONFIG)
    payload["markets"] = {
        "mode": "season_long_tournament",
        "definitions": [
            {
                "market_id": "bad",
                "metric_id": "hits",
                "horizon": "week",
                "window_start": "2026-03-01",
                "window_end": "2026-10-01",
            }
        ],
    }

    with pytest.raises(FantasyConfigValidationError, match="metric/horizon"):
        load_unified_fantasy_config(_write_config(tmp_path, payload))


def test_missing_base_metric_for_derived_fails(tmp_path: Path) -> None:
    payload = deepcopy(BASE_CONFIG)
    payload["metrics"] = {
        "base_metrics": [
            {"metric_id": "hits", "horizon": "season", "adapter_key": "mlb_hits_season"}
        ],
        "derived_metrics": [
            {
                "derived_metric_id": "fantasy_points",
                "input_metric_ids": ["hits", "runs"],
                "transform_id": "mlb_points",
                "transform_params": {"hits": 3.0, "runs": 2.0},
            }
        ],
    }

    with pytest.raises(FantasyConfigValidationError, match="derived metric"):
        load_unified_fantasy_config(_write_config(tmp_path, payload))


def test_line_value_requires_operator(tmp_path: Path) -> None:
    payload = deepcopy(BASE_CONFIG)
    payload["markets"] = {
        "mode": "single_game_pickem",
        "definitions": [
            {
                "market_id": "bad_line",
                "metric_id": "hits",
                "horizon": "game",
                "line_value": 1.5,
                "window_start": "2026-06-01",
                "window_end": "2026-06-01",
                "game_id": "game-123",
            }
        ],
    }
    payload["contest"]["mode"] = "single_game_pickem"
    payload["contest"]["scoring_ruleset_id"] = None
    payload["contest"]["mode_config"] = {
        "slip_constraints": {"min_legs": 2},
        "payout_ladder": {"2": 3.0},
    }

    with pytest.raises(FantasyConfigValidationError, match="operator"):
        load_unified_fantasy_config(_write_config(tmp_path, payload))
