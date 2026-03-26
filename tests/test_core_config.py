from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from src.core.config import (
    ConfigValidationError,
    extract_mlb_live_underdog_config,
    load_pipeline_config,
)

MLB_IMPLEMENTED_STATS: tuple[str, ...] = (
    "strikeouts",
    "outs_recorded",
    "earned_runs",
    "hits_allowed",
    "bb_allowed",
)
EXPECTED_MLB_LIVE_UNDERDOG_STAT_IDS: dict[str, str] = {
    "strikeouts": "PickemStat_de868934-c920-405c-b827-693c15aa47a1",
    "outs_recorded": "PickemStat_0f4a1b3d-62d9-47f8-9f45-7c2ddf6c8d8e",
    "earned_runs": "PickemStat_a2f0d1e5-4c4a-4f4c-98f2-8f0f88f7a3d1",
    "hits_allowed": "PickemStat_9e7e7cb2-58a3-4a3f-86f2-6f07d7dd4d55",
    "bb_allowed": "PickemStat_7c1f6a0d-0f25-4e1d-8ce8-0c82ff1b0f44",
}
EXPECTED_MLB_LIVE_UNDERDOG_DEFAULTS: dict[str, Any] = {
    "same_pitcher_stacking": True,
    "min_players_per_slip": 2,
    "min_teams_per_slip": 2,
    "json_only": True,
}
_MIGRATION_MESSAGE_PATTERN = (
    "sectioned schema.*pipeline\\.sport.*pipeline\\.stat.*\\{sport\\}\\.\\{stat\\}"
)


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")


def test_rejects_legacy_flat_schema_with_migration_message(tmp_path: Path) -> None:
    config_path = tmp_path / "legacy.yaml"
    _write_yaml(config_path, {"pitch_data_path": "x.csv"})

    with pytest.raises(
        ConfigValidationError,
        match=_MIGRATION_MESSAGE_PATTERN,
    ):
        load_pipeline_config(str(config_path))


def test_rejects_non_mapping_pipeline_with_migration_message(tmp_path: Path) -> None:
    config_path = tmp_path / "bad.yaml"
    _write_yaml(config_path, {"pipeline": "mlb"})

    with pytest.raises(
        ConfigValidationError,
        match=_MIGRATION_MESSAGE_PATTERN,
    ):
        load_pipeline_config(str(config_path))


@pytest.mark.parametrize(
    ("pipeline_section", "expected_path"),
    [
        ({}, "pipeline.sport"),
        ({"sport": "mlb"}, "pipeline.stat"),
        ({"sport": "", "stat": "strikeouts"}, "pipeline.sport"),
        ({"sport": "mlb", "stat": ""}, "pipeline.stat"),
    ],
)
def test_rejects_missing_pipeline_identity_fields(
    tmp_path: Path,
    pipeline_section: dict[str, Any],
    expected_path: str,
) -> None:
    config_path = tmp_path / "bad.yaml"
    _write_yaml(config_path, {"pipeline": pipeline_section})

    with pytest.raises(ConfigValidationError, match=expected_path):
        load_pipeline_config(str(config_path))


def test_rejects_missing_sport_section(tmp_path: Path) -> None:
    config_path = tmp_path / "missing_sport.yaml"
    _write_yaml(
        config_path,
        {
            "pipeline": {"sport": "mlb", "stat": "strikeouts"},
            "nfl": {"pass_attempts": {"training_years": [2023]}},
        },
    )

    with pytest.raises(ConfigValidationError, match="Missing sport section 'mlb'"):
        load_pipeline_config(str(config_path))


def test_rejects_missing_stat_section(tmp_path: Path) -> None:
    config_path = tmp_path / "missing_stat.yaml"
    _write_yaml(
        config_path,
        {"pipeline": {"sport": "mlb", "stat": "strikeouts"}, "mlb": {}},
    )

    with pytest.raises(
        ConfigValidationError, match="Missing stat section 'mlb\\.strikeouts'"
    ):
        load_pipeline_config(str(config_path))


@pytest.mark.parametrize("stat", MLB_IMPLEMENTED_STATS)
@pytest.mark.parametrize("missing_key", ["pitch_data_path", "model_path", "lines_path"])
def test_mlb_required_keys_must_exist(
    tmp_path: Path,
    stat: str,
    missing_key: str,
) -> None:
    section = {
        "pitch_data_path": "data/pitch.csv",
        "model_path": "models/model.joblib",
        "lines_path": "data/lines.csv",
    }
    section.pop(missing_key)
    config_path = tmp_path / f"mlb_{stat}_missing_{missing_key}.yaml"
    _write_yaml(
        config_path,
        {"pipeline": {"sport": "mlb", "stat": stat}, "mlb": {stat: section}},
    )

    with pytest.raises(ConfigValidationError, match=f"mlb\\.{stat}\\.{missing_key}"):
        load_pipeline_config(str(config_path))


@pytest.mark.parametrize("stat", MLB_IMPLEMENTED_STATS)
@pytest.mark.parametrize("bad_value", [123, True, ["x"]])
def test_mlb_required_keys_must_be_strings(
    tmp_path: Path,
    stat: str,
    bad_value: Any,
) -> None:
    section = {
        "pitch_data_path": bad_value,
        "model_path": "models/model.joblib",
        "lines_path": "data/lines.csv",
    }
    config_path = tmp_path / f"mlb_{stat}_bad_type.yaml"
    _write_yaml(
        config_path,
        {"pipeline": {"sport": "mlb", "stat": stat}, "mlb": {stat: section}},
    )

    with pytest.raises(
        ConfigValidationError, match=f"mlb\\.{stat}\\.pitch_data_path.*string"
    ):
        load_pipeline_config(str(config_path))


@pytest.mark.parametrize(
    ("training_years", "expected_path"),
    [
        (None, "nfl.pass_attempts.training_years"),
        ([], "nfl.pass_attempts.training_years"),
        ("2023", "nfl.pass_attempts.training_years"),
        ([2023, "2024"], "nfl.pass_attempts.training_years\\[1\\]"),
        ([2023, True], "nfl.pass_attempts.training_years\\[1\\]"),
    ],
)
def test_nfl_training_years_validation(
    tmp_path: Path,
    training_years: Any,
    expected_path: str,
) -> None:
    section: dict[str, Any] = {}
    if training_years is not None:
        section["training_years"] = training_years

    config_path = tmp_path / "nfl_bad.yaml"
    _write_yaml(
        config_path,
        {
            "pipeline": {"sport": "nfl", "stat": "pass_attempts"},
            "nfl": {"pass_attempts": section},
        },
    )

    with pytest.raises(ConfigValidationError, match=expected_path):
        load_pipeline_config(str(config_path))


def test_overrides_do_not_bypass_required_pipeline_section(tmp_path: Path) -> None:
    config_path = tmp_path / "legacy.yaml"
    _write_yaml(
        config_path,
        {
            "mlb": {
                "strikeouts": {
                    "pitch_data_path": "data/pitch.csv",
                    "model_path": "models/model.joblib",
                    "lines_path": "data/lines.csv",
                }
            }
        },
    )

    with pytest.raises(ConfigValidationError, match="sectioned schema"):
        load_pipeline_config(
            str(config_path),
            sport_override="mlb",
            stat_override="strikeouts",
        )


@pytest.mark.parametrize("stat", MLB_IMPLEMENTED_STATS)
def test_mlb_config_loads_for_all_implemented_stats(stat: str) -> None:
    config = load_pipeline_config(
        "config/mlb.yaml",
        sport_override="mlb",
        stat_override=stat,
    )

    assert config.sport == "mlb"
    assert config.stat == stat
    assert isinstance(config.section, dict)


def test_mlb_live_underdog_config_loads_from_repo_config() -> None:
    config = load_pipeline_config(
        "config/mlb.yaml",
        sport_override="mlb",
        stat_override="strikeouts",
    )

    live_underdog = extract_mlb_live_underdog_config(config)

    assert live_underdog.stat_ids == EXPECTED_MLB_LIVE_UNDERDOG_STAT_IDS
    assert live_underdog.snapshot_output_dir == "data/lines"
    assert live_underdog.snapshot_filename_template == "{stat}_{date}.csv"
    assert live_underdog.orchestration_defaults == EXPECTED_MLB_LIVE_UNDERDOG_DEFAULTS


def test_mlb_live_underdog_config_defaults_missing_stat_ids(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "mlb_live_underdog_partial.yaml"
    _write_yaml(
        config_path,
        {
            "pipeline": {"sport": "mlb", "stat": "strikeouts"},
            "mlb": {
                "strikeouts": {
                    "pitch_data_path": "data/pitch.csv",
                    "model_path": "models/model.joblib",
                    "lines_path": "data/lines.csv",
                },
                "live_underdog": {
                    "stat_ids": {
                        "strikeouts": "PickemStat_de868934-c920-405c-b827-693c15aa47a1",
                        "outs_recorded": (
                            "PickemStat_0f4a1b3d-62d9-47f8-9f45-7c2ddf6c8d8e"
                        ),
                    }
                },
            },
        },
    )

    config = load_pipeline_config(
        str(config_path),
        sport_override="mlb",
        stat_override="strikeouts",
    )
    live_underdog = extract_mlb_live_underdog_config(config)

    assert live_underdog.stat_ids == {
        "strikeouts": "PickemStat_de868934-c920-405c-b827-693c15aa47a1",
        "outs_recorded": "PickemStat_0f4a1b3d-62d9-47f8-9f45-7c2ddf6c8d8e",
    }
    assert live_underdog.snapshot_output_dir == "data/lines"
    assert live_underdog.snapshot_filename_template == "{stat}_{date}.csv"
    assert live_underdog.orchestration_defaults == EXPECTED_MLB_LIVE_UNDERDOG_DEFAULTS


def test_mlb_live_underdog_config_rejects_unsupported_stat_id_key(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "mlb_live_underdog_bad_stat_key.yaml"
    _write_yaml(
        config_path,
        {
            "pipeline": {"sport": "mlb", "stat": "strikeouts"},
            "mlb": {
                "strikeouts": {
                    "pitch_data_path": "data/pitch.csv",
                    "model_path": "models/model.joblib",
                    "lines_path": "data/lines.csv",
                },
                "live_underdog": {
                    "stat_ids": {
                        "strikeouts": "PickemStat_de868934-c920-405c-b827-693c15aa47a1",
                        "strikeoutz": "PickemStat_bad",
                    }
                },
            },
        },
    )

    config = load_pipeline_config(
        str(config_path),
        sport_override="mlb",
        stat_override="strikeouts",
    )

    with pytest.raises(ConfigValidationError, match="mlb\\.live_underdog\\.stat_ids"):
        extract_mlb_live_underdog_config(config)


def test_mlb_live_underdog_config_rejects_mis_typed_orchestration_defaults(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "mlb_live_underdog_bad_defaults.yaml"
    _write_yaml(
        config_path,
        {
            "pipeline": {"sport": "mlb", "stat": "strikeouts"},
            "mlb": {
                "strikeouts": {
                    "pitch_data_path": "data/pitch.csv",
                    "model_path": "models/model.joblib",
                    "lines_path": "data/lines.csv",
                },
                "live_underdog": {
                    "stat_ids": {
                        "strikeouts": "PickemStat_de868934-c920-405c-b827-693c15aa47a1",
                    },
                    "orchestration_defaults": {
                        "same_pitcher_stacking": "true",
                        "min_players_per_slip": "2",
                    },
                },
            },
        },
    )

    config = load_pipeline_config(
        str(config_path),
        sport_override="mlb",
        stat_override="strikeouts",
    )

    with pytest.raises(
        ConfigValidationError, match="mlb\\.live_underdog\\.orchestration_defaults"
    ):
        extract_mlb_live_underdog_config(config)


def test_nfl_config_loads_for_pass_attempts() -> None:
    config = load_pipeline_config("config/nfl.yaml")

    assert config.sport == "nfl"
    assert config.stat == "pass_attempts"
    assert isinstance(config.section, dict)


def _base_nhl_shots_on_goal_section() -> dict[str, Any]:
    return {
        "provider": "moneypuck_snapshot",
        "inference_input_path": "tests/testdata/nhl_shots_on_goal_input.csv",
        "model_path": "models/nhl_shots_on_goal_model.joblib",
        "provider_seasons": [2024],
        "moneypuck_skater_games_snapshot_path": (
            "tests/testdata/nhl/moneypuck/skater_games_full_snapshot_sample.csv"
        ),
        "moneypuck_skater_games_curated_cache_path": (
            "tests/testdata/nhl/moneypuck/skater_games_curated.parquet"
        ),
        "feature_rolling_windows": [5, 10],
        "auto_refresh_snapshot": False,
        "fail_on_provider_error": True,
    }


@pytest.mark.parametrize(
    ("missing_key", "expected_path"),
    [
        ("provider", "nhl.shots_on_goal.provider"),
        ("inference_input_path", "nhl.shots_on_goal.inference_input_path"),
        ("model_path", "nhl.shots_on_goal.model_path"),
        ("provider_seasons", "nhl.shots_on_goal.provider_seasons"),
        (
            "moneypuck_skater_games_snapshot_path",
            "nhl.shots_on_goal.moneypuck_skater_games_snapshot_path",
        ),
        (
            "moneypuck_skater_games_curated_cache_path",
            "nhl.shots_on_goal.moneypuck_skater_games_curated_cache_path",
        ),
        ("feature_rolling_windows", "nhl.shots_on_goal.feature_rolling_windows"),
        ("auto_refresh_snapshot", "nhl.shots_on_goal.auto_refresh_snapshot"),
        ("fail_on_provider_error", "nhl.shots_on_goal.fail_on_provider_error"),
    ],
)
def test_nhl_shots_on_goal_required_keys_must_exist(
    tmp_path: Path,
    missing_key: str,
    expected_path: str,
) -> None:
    section = _base_nhl_shots_on_goal_section()
    section.pop(missing_key)
    config_path = tmp_path / f"nhl_shots_missing_{missing_key}.yaml"
    _write_yaml(
        config_path,
        {
            "pipeline": {"sport": "nhl", "stat": "shots_on_goal"},
            "nhl": {"shots_on_goal": section},
        },
    )

    with pytest.raises(ConfigValidationError, match=expected_path):
        load_pipeline_config(str(config_path))


@pytest.mark.parametrize(
    ("key", "bad_value", "expected_path"),
    [
        ("provider", 1, "nhl.shots_on_goal.provider"),
        ("inference_input_path", True, "nhl.shots_on_goal.inference_input_path"),
        ("model_path", True, "nhl.shots_on_goal.model_path"),
        ("provider_seasons", [], "nhl.shots_on_goal.provider_seasons"),
        (
            "provider_seasons",
            [2024, "2025"],
            "nhl.shots_on_goal.provider_seasons\\[1\\]",
        ),
        ("provider_seasons", [2024, True], "nhl.shots_on_goal.provider_seasons\\[1\\]"),
        ("feature_rolling_windows", [], "nhl.shots_on_goal.feature_rolling_windows"),
        (
            "feature_rolling_windows",
            [5, "10"],
            "nhl.shots_on_goal.feature_rolling_windows\\[1\\]",
        ),
        ("auto_refresh_snapshot", "false", "nhl.shots_on_goal.auto_refresh_snapshot"),
        (
            "fail_on_provider_error",
            "true",
            "nhl.shots_on_goal.fail_on_provider_error",
        ),
    ],
)
def test_nhl_shots_on_goal_required_keys_must_have_valid_types(
    tmp_path: Path,
    key: str,
    bad_value: Any,
    expected_path: str,
) -> None:
    section = _base_nhl_shots_on_goal_section()
    section[key] = bad_value
    config_path = tmp_path / f"nhl_shots_bad_{key}.yaml"
    _write_yaml(
        config_path,
        {
            "pipeline": {"sport": "nhl", "stat": "shots_on_goal"},
            "nhl": {"shots_on_goal": section},
        },
    )

    with pytest.raises(ConfigValidationError, match=expected_path):
        load_pipeline_config(str(config_path))


def test_nhl_shots_on_goal_fail_on_provider_error_must_be_true(tmp_path: Path) -> None:
    section = _base_nhl_shots_on_goal_section()
    section["fail_on_provider_error"] = False
    config_path = tmp_path / "nhl_fail_soft.yaml"
    _write_yaml(
        config_path,
        {
            "pipeline": {"sport": "nhl", "stat": "shots_on_goal"},
            "nhl": {"shots_on_goal": section},
        },
    )

    with pytest.raises(
        ConfigValidationError, match="nhl.shots_on_goal.fail_on_provider_error"
    ):
        load_pipeline_config(str(config_path))


@pytest.mark.parametrize(
    ("key", "value", "expected_path"),
    [
        ("training_seasons", [], "nhl.shots_on_goal.training_seasons"),
        (
            "training_seasons",
            [2024, "2025"],
            "nhl.shots_on_goal.training_seasons\\[1\\]",
        ),
        (
            "min_training_games_per_player",
            0,
            "nhl.shots_on_goal.min_training_games_per_player",
        ),
        ("sigma_min_history", 0, "nhl.shots_on_goal.sigma_min_history"),
        ("min_sigma", -0.1, "nhl.shots_on_goal.min_sigma"),
        ("bootstrap_min_sigma", -0.1, "nhl.shots_on_goal.bootstrap_min_sigma"),
        (
            "bootstrap_mix_global_prob",
            -0.1,
            "nhl.shots_on_goal.bootstrap_mix_global_prob",
        ),
        (
            "bootstrap_mix_global_prob",
            1.1,
            "nhl.shots_on_goal.bootstrap_mix_global_prob",
        ),
    ],
)
def test_nhl_shots_on_goal_optional_model_keys_validate_constraints(
    tmp_path: Path,
    key: str,
    value: Any,
    expected_path: str,
) -> None:
    section = _base_nhl_shots_on_goal_section()
    section[key] = value
    config_path = tmp_path / "nhl_optional_invalid.yaml"
    _write_yaml(
        config_path,
        {
            "pipeline": {"sport": "nhl", "stat": "shots_on_goal"},
            "nhl": {"shots_on_goal": section},
        },
    )

    with pytest.raises(ConfigValidationError, match=expected_path):
        load_pipeline_config(str(config_path))


def test_nhl_config_loads_for_shots_on_goal() -> None:
    config = load_pipeline_config("config/nhl.yaml")

    assert config.sport == "nhl"
    assert config.stat == "shots_on_goal"
    assert isinstance(config.section, dict)


def test_unknown_sport_stat_without_registered_validator_still_loads(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "unknown.yaml"
    _write_yaml(
        config_path,
        {
            "pipeline": {"sport": "nhl", "stat": "shots"},
            "nhl": {"shots": {"foo": "bar"}},
        },
    )

    config = load_pipeline_config(str(config_path))
    assert config.sport == "nhl"
    assert config.stat == "shots"
    assert config.section == {"foo": "bar"}
