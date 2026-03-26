"""Typed config loading and validation utilities for modular pipelines."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from numbers import Integral
from pathlib import Path
from typing import Any

from src.core.contracts import PipelineConfig
from src.utils.io import load_config


class ConfigValidationError(ValueError):
    """Raised when a config file is missing required sections."""


_MLB_RUNTIME_CRITICAL_STATS: frozenset[str] = frozenset(
    {
        "strikeouts",
        "outs_recorded",
        "earned_runs",
        "hits_allowed",
        "bb_allowed",
    }
)
_MLB_REQUIRED_KEYS: tuple[str, ...] = ("pitch_data_path", "model_path", "lines_path")
_MLB_LIVE_UNDERDOG_DEFAULT_SNAPSHOT_OUTPUT_DIR = "data/lines"
_MLB_LIVE_UNDERDOG_DEFAULT_SNAPSHOT_FILENAME_TEMPLATE = "{stat}_{date}.csv"
_MLB_LIVE_UNDERDOG_DEFAULT_ORCHESTRATION_DEFAULTS: dict[str, Any] = {
    "same_pitcher_stacking": True,
    "min_players_per_slip": 2,
    "min_teams_per_slip": 2,
    "json_only": True,
}
_SECTIONED_SCHEMA_MESSAGE = (
    "Config must use sectioned schema. Required fields: 'pipeline.sport', "
    "'pipeline.stat', and '{sport}.{stat}'. Legacy flat config is not supported."
)


@dataclass(slots=True)
class MlbLiveUnderdogConfig:
    """Normalized MLB live Underdog orchestration config.

    Attributes:
        stat_ids: Mapping of MLB stat name to Underdog ``PickemStat_*`` id.
        snapshot_output_dir: Directory where dated line snapshots are written.
        snapshot_filename_template: Filename template for dated snapshots.
        orchestration_defaults: Shared slip-generation defaults.
    """

    stat_ids: dict[str, str]
    snapshot_output_dir: str
    snapshot_filename_template: str
    orchestration_defaults: dict[str, Any]


def _validate_sectioned_schema_root(raw_config: dict[str, Any]) -> dict[str, Any]:
    """Validate root schema and return the required `pipeline` mapping.

    Args:
        raw_config: Loaded config root mapping.

    Returns:
        The validated `pipeline` subsection.

    Raises:
        ConfigValidationError: If `pipeline` is missing or not a mapping.
    """

    pipeline_section = raw_config.get("pipeline")
    if not isinstance(pipeline_section, dict):
        raise ConfigValidationError(_SECTIONED_SCHEMA_MESSAGE)
    return pipeline_section


def _validate_required_str(section: dict[str, Any], path: str) -> str:
    """Validate a required non-empty string field from a mapping.

    Args:
        section: Parent mapping containing the field.
        path: Full dotted path to the field.

    Returns:
        Normalized (trimmed) field value.

    Raises:
        ConfigValidationError: If the field is missing, not a string, or blank.
    """

    key = path.rsplit(".", maxsplit=1)[-1]
    value = section.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigValidationError(
            f"Invalid required field '{path}': expected non-empty string."
        )
    return value.strip()


def _validate_required_str_key(section: dict[str, Any], path: str) -> None:
    """Validate a required string key for runtime-critical config fields.

    Args:
        section: Parent mapping containing the key.
        path: Full dotted path to the key.

    Raises:
        ConfigValidationError: If key is missing or not a non-empty string.
    """

    _validate_required_str(section, path)


def _validate_required_non_empty_int_list(section: dict[str, Any], path: str) -> None:
    """Validate a required non-empty list of integers (excluding bool).

    Args:
        section: Parent mapping containing the list.
        path: Full dotted path to the list.

    Raises:
        ConfigValidationError: If the value is missing/empty/not a list, or contains
            non-integer values.
    """

    key = path.rsplit(".", maxsplit=1)[-1]
    value = section.get(key)
    if not isinstance(value, list):
        raise ConfigValidationError(
            f"Invalid required field '{path}': expected non-empty list[int]."
        )
    if not value:
        raise ConfigValidationError(
            f"Invalid required field '{path}': expected non-empty list[int]."
        )

    for idx, item in enumerate(value):
        if isinstance(item, bool) or not isinstance(item, Integral):
            raise ConfigValidationError(
                f"Invalid required field '{path}[{idx}]': expected integer."
            )


def _validate_optional_non_empty_int_list(section: dict[str, Any], path: str) -> None:
    """Validate an optional non-empty list of integers (excluding bool)."""

    key = path.rsplit(".", maxsplit=1)[-1]
    if key not in section:
        return

    value = section.get(key)
    if not isinstance(value, list) or not value:
        raise ConfigValidationError(
            f"Invalid optional field '{path}': expected non-empty list[int]."
        )
    for idx, item in enumerate(value):
        if isinstance(item, bool) or not isinstance(item, Integral):
            raise ConfigValidationError(
                f"Invalid optional field '{path}[{idx}]': expected integer."
            )


def _validate_required_bool_key(section: dict[str, Any], path: str) -> None:
    """Validate a required boolean key for runtime-critical config fields.

    Args:
        section: Parent mapping containing the key.
        path: Full dotted path to the key.

    Raises:
        ConfigValidationError: If key is missing or not a boolean.
    """

    key = path.rsplit(".", maxsplit=1)[-1]
    value = section.get(key)
    if not isinstance(value, bool):
        raise ConfigValidationError(
            f"Invalid required field '{path}': expected boolean."
        )


def _validate_mlb_stat_section(section: dict[str, Any], path: str) -> None:
    """Validate MLB runtime-critical keys for implemented stats.

    Args:
        section: Active stat config subsection.
        path: Full dotted section path, such as `mlb.strikeouts`.
    """

    for key in _MLB_REQUIRED_KEYS:
        _validate_required_str_key(section, f"{path}.{key}")


def _validate_nfl_pass_attempts_section(section: dict[str, Any], path: str) -> None:
    """Validate NFL pass-attempt runtime-critical keys.

    Args:
        section: Active stat config subsection.
        path: Full dotted section path, such as `nfl.pass_attempts`.
    """

    _validate_required_non_empty_int_list(section, f"{path}.training_years")


def _validate_nhl_shots_on_goal_section(section: dict[str, Any], path: str) -> None:
    """Validate NHL shots-on-goal runtime-critical keys for PR#9 flow.

    Args:
        section: Active stat config subsection.
        path: Full dotted section path, such as `nhl.shots_on_goal`.
    """

    for key in (
        "provider",
        "inference_input_path",
        "model_path",
        "moneypuck_skater_games_snapshot_path",
        "moneypuck_skater_games_curated_cache_path",
    ):
        _validate_required_str_key(section, f"{path}.{key}")

    for key in ("provider_seasons", "feature_rolling_windows"):
        _validate_required_non_empty_int_list(section, f"{path}.{key}")

    _validate_required_bool_key(section, f"{path}.auto_refresh_snapshot")
    _validate_required_bool_key(section, f"{path}.fail_on_provider_error")
    if section.get("fail_on_provider_error") is not True:
        raise ConfigValidationError(
            f"Invalid required field '{path}.fail_on_provider_error': expected true."
        )

    _validate_optional_non_empty_int_list(section, f"{path}.training_seasons")

    min_training_games = section.get("min_training_games_per_player")
    if min_training_games is not None:
        if isinstance(min_training_games, bool) or not isinstance(
            min_training_games, Integral
        ):
            raise ConfigValidationError(
                f"Invalid optional field '{path}.min_training_games_per_player': "
                "expected integer >= 1."
            )
        if int(min_training_games) < 1:
            raise ConfigValidationError(
                f"Invalid optional field '{path}.min_training_games_per_player': "
                "expected integer >= 1."
            )

    sigma_min_history = section.get("sigma_min_history")
    if sigma_min_history is not None:
        if isinstance(sigma_min_history, bool) or not isinstance(
            sigma_min_history, Integral
        ):
            raise ConfigValidationError(
                f"Invalid optional field '{path}.sigma_min_history': "
                "expected integer >= 1."
            )
        if int(sigma_min_history) < 1:
            raise ConfigValidationError(
                f"Invalid optional field '{path}.sigma_min_history': "
                "expected integer >= 1."
            )

    min_sigma = section.get("min_sigma")
    if min_sigma is not None:
        try:
            numeric_min_sigma = float(min_sigma)
        except (TypeError, ValueError) as exc:
            raise ConfigValidationError(
                f"Invalid optional field '{path}.min_sigma': expected float >= 0."
            ) from exc
        if numeric_min_sigma < 0:
            raise ConfigValidationError(
                f"Invalid optional field '{path}.min_sigma': expected float >= 0."
            )

    bootstrap_mix_prob = section.get("bootstrap_mix_global_prob")
    if bootstrap_mix_prob is not None:
        try:
            numeric_prob = float(bootstrap_mix_prob)
        except (TypeError, ValueError) as exc:
            raise ConfigValidationError(
                f"Invalid optional field '{path}.bootstrap_mix_global_prob': "
                "expected float in [0, 1]."
            ) from exc
        if numeric_prob < 0 or numeric_prob > 1:
            raise ConfigValidationError(
                f"Invalid optional field '{path}.bootstrap_mix_global_prob': "
                "expected float in [0, 1]."
            )

    bootstrap_min_sigma = section.get("bootstrap_min_sigma")
    if bootstrap_min_sigma is not None:
        try:
            numeric_bootstrap_min_sigma = float(bootstrap_min_sigma)
        except (TypeError, ValueError) as exc:
            raise ConfigValidationError(
                f"Invalid optional field '{path}.bootstrap_min_sigma': "
                "expected float >= 0."
            ) from exc
        if numeric_bootstrap_min_sigma < 0:
            raise ConfigValidationError(
                f"Invalid optional field '{path}.bootstrap_min_sigma': "
                "expected float >= 0."
            )


def _normalize_mlb_live_underdog_defaults(
    section: dict[str, Any] | None,
) -> MlbLiveUnderdogConfig:
    """Normalize the optional MLB live Underdog config block.

    Args:
        section: Optional `mlb.live_underdog` mapping.

    Returns:
        Normalized MLB live Underdog config payload.

    Raises:
        ConfigValidationError: If a provided live Underdog subsection has an
            invalid type.
    """

    if section is None:
        return MlbLiveUnderdogConfig(
            stat_ids={},
            snapshot_output_dir=_MLB_LIVE_UNDERDOG_DEFAULT_SNAPSHOT_OUTPUT_DIR,
            snapshot_filename_template=(
                _MLB_LIVE_UNDERDOG_DEFAULT_SNAPSHOT_FILENAME_TEMPLATE
            ),
            orchestration_defaults=dict(
                _MLB_LIVE_UNDERDOG_DEFAULT_ORCHESTRATION_DEFAULTS
            ),
        )

    stat_ids_raw = section.get("stat_ids", {})
    if not isinstance(stat_ids_raw, dict):
        raise ConfigValidationError(
            "Invalid optional field 'mlb.live_underdog.stat_ids': expected mapping."
        )

    stat_ids: dict[str, str] = {}
    for stat, raw_object_id in stat_ids_raw.items():
        if not isinstance(stat, str) or not stat.strip():
            raise ConfigValidationError(
                "Invalid optional field 'mlb.live_underdog.stat_ids': "
                "expected non-empty stat names."
            )
        stat_name = stat.strip().lower()
        if stat_name not in _MLB_RUNTIME_CRITICAL_STATS:
            raise ConfigValidationError(
                "Invalid optional field 'mlb.live_underdog.stat_ids."
                f"{stat_name}': unsupported MLB stat."
            )
        if not isinstance(raw_object_id, str) or not raw_object_id.strip():
            raise ConfigValidationError(
                "Invalid optional field 'mlb.live_underdog.stat_ids."
                f"{stat_name}': expected non-empty string."
            )
        stat_ids[stat_name] = raw_object_id.strip()

    snapshot_output_dir_raw = section.get("snapshot_output_dir")
    if snapshot_output_dir_raw is None:
        snapshot_output_dir = _MLB_LIVE_UNDERDOG_DEFAULT_SNAPSHOT_OUTPUT_DIR
    elif not isinstance(snapshot_output_dir_raw, str) or not (
        snapshot_output_dir_raw.strip()
    ):
        raise ConfigValidationError(
            "Invalid optional field 'mlb.live_underdog.snapshot_output_dir': "
            "expected non-empty string."
        )
    else:
        snapshot_output_dir = snapshot_output_dir_raw.strip()

    snapshot_filename_template_raw = section.get("snapshot_filename_template")
    if snapshot_filename_template_raw is None:
        snapshot_filename_template = (
            _MLB_LIVE_UNDERDOG_DEFAULT_SNAPSHOT_FILENAME_TEMPLATE
        )
    elif (
        not isinstance(snapshot_filename_template_raw, str)
        or not snapshot_filename_template_raw.strip()
    ):
        raise ConfigValidationError(
            "Invalid optional field 'mlb.live_underdog.snapshot_filename_template': "
            "expected non-empty string."
        )
    else:
        snapshot_filename_template = snapshot_filename_template_raw.strip()

    orchestration_defaults_raw = section.get("orchestration_defaults", {})
    if not isinstance(orchestration_defaults_raw, dict):
        raise ConfigValidationError(
            "Invalid optional field 'mlb.live_underdog.orchestration_defaults': "
            "expected mapping."
        )

    orchestration_defaults = _validate_mlb_live_underdog_orchestration_defaults(
        orchestration_defaults_raw
    )

    return MlbLiveUnderdogConfig(
        stat_ids=stat_ids,
        snapshot_output_dir=snapshot_output_dir,
        snapshot_filename_template=snapshot_filename_template,
        orchestration_defaults=orchestration_defaults,
    )


def _validate_mlb_live_underdog_orchestration_defaults(
    section: dict[str, Any],
) -> dict[str, Any]:
    """Validate live Underdog orchestration defaults and apply fallback values."""

    defaults = dict(_MLB_LIVE_UNDERDOG_DEFAULT_ORCHESTRATION_DEFAULTS)

    same_pitcher_stacking = section.get("same_pitcher_stacking")
    if same_pitcher_stacking is not None:
        if not isinstance(same_pitcher_stacking, bool):
            raise ConfigValidationError(
                "Invalid optional field "
                "'mlb.live_underdog.orchestration_defaults.same_pitcher_stacking': "
                "expected boolean."
            )
        defaults["same_pitcher_stacking"] = same_pitcher_stacking

    min_players_per_slip = section.get("min_players_per_slip")
    if min_players_per_slip is not None:
        if isinstance(min_players_per_slip, bool) or not isinstance(
            min_players_per_slip, Integral
        ):
            raise ConfigValidationError(
                "Invalid optional field "
                "'mlb.live_underdog.orchestration_defaults.min_players_per_slip': "
                "expected integer >= 1."
            )
        if int(min_players_per_slip) < 1:
            raise ConfigValidationError(
                "Invalid optional field "
                "'mlb.live_underdog.orchestration_defaults.min_players_per_slip': "
                "expected integer >= 1."
            )
        defaults["min_players_per_slip"] = int(min_players_per_slip)

    min_teams_per_slip = section.get("min_teams_per_slip")
    if min_teams_per_slip is not None:
        if isinstance(min_teams_per_slip, bool) or not isinstance(
            min_teams_per_slip, Integral
        ):
            raise ConfigValidationError(
                "Invalid optional field "
                "'mlb.live_underdog.orchestration_defaults.min_teams_per_slip': "
                "expected integer >= 1."
            )
        if int(min_teams_per_slip) < 1:
            raise ConfigValidationError(
                "Invalid optional field "
                "'mlb.live_underdog.orchestration_defaults.min_teams_per_slip': "
                "expected integer >= 1."
            )
        defaults["min_teams_per_slip"] = int(min_teams_per_slip)

    json_only = section.get("json_only")
    if json_only is not None:
        if not isinstance(json_only, bool):
            raise ConfigValidationError(
                "Invalid optional field "
                "'mlb.live_underdog.orchestration_defaults.json_only': "
                "expected boolean."
            )
        defaults["json_only"] = json_only

    return defaults


_VALIDATORS: dict[tuple[str, str], Callable[[dict[str, Any], str], None]] = {
    **{
        ("mlb", stat): _validate_mlb_stat_section
        for stat in _MLB_RUNTIME_CRITICAL_STATS
    },
    ("nfl", "pass_attempts"): _validate_nfl_pass_attempts_section,
    ("nhl", "shots_on_goal"): _validate_nhl_shots_on_goal_section,
}


def extract_stat_section(
    raw_config: dict[str, Any],
    sport: str,
    stat: str,
) -> dict[str, Any]:
    """Return the resolved sport/stat section from a config payload.

    Requires sectioned schema and resolves the active sport/stat section.
    """

    normalized_sport = sport.strip().lower()
    normalized_stat = stat.strip().lower()

    sport_section = raw_config.get(normalized_sport)
    if not isinstance(sport_section, dict):
        raise ConfigValidationError(
            f"Missing sport section '{normalized_sport}' in config."
        )

    stat_section = sport_section.get(normalized_stat)
    if not isinstance(stat_section, dict):
        raise ConfigValidationError(
            "Missing stat section " f"'{normalized_sport}.{normalized_stat}' in config."
        )

    return dict(stat_section)


def _resolve_pipeline_identity(
    raw_config: dict[str, Any],
    sport_override: str | None,
    stat_override: str | None,
) -> tuple[str, str]:
    pipeline_section = _validate_sectioned_schema_root(raw_config)
    pipeline_sport = _validate_required_str(pipeline_section, "pipeline.sport")
    pipeline_stat = _validate_required_str(pipeline_section, "pipeline.stat")

    sport = sport_override or pipeline_sport
    stat = stat_override or pipeline_stat

    if not isinstance(sport, str) or not sport.strip():
        raise ConfigValidationError(
            "Invalid required field 'pipeline.sport': expected non-empty string."
        )
    if not isinstance(stat, str) or not stat.strip():
        raise ConfigValidationError(
            "Invalid required field 'pipeline.stat': expected non-empty string."
        )

    return sport.strip().lower(), stat.strip().lower()


def load_pipeline_config(
    config_path: str,
    *,
    sport_override: str | None = None,
    stat_override: str | None = None,
) -> PipelineConfig:
    """Load and validate YAML config for the requested sport/stat pipeline."""

    path = Path(config_path)
    raw = load_config(str(path))

    if not isinstance(raw, dict):
        raise ConfigValidationError("Config root must be a mapping.")

    sport, stat = _resolve_pipeline_identity(raw, sport_override, stat_override)
    section = extract_stat_section(raw, sport, stat)
    validator = _VALIDATORS.get((sport, stat))
    if validator is not None:
        validator(section, f"{sport}.{stat}")

    return PipelineConfig(
        config_path=path,
        sport=sport,
        stat=stat,
        raw=raw,
        section=section,
    )


def extract_mlb_live_underdog_config(
    config: PipelineConfig | dict[str, Any],
) -> MlbLiveUnderdogConfig:
    """Return normalized MLB live Underdog config from a pipeline config.

    Args:
        config: Validated pipeline config or raw config mapping.

    Returns:
        Normalized live Underdog configuration with defaults applied.
    """

    raw_config = config.raw if isinstance(config, PipelineConfig) else config
    if not isinstance(raw_config, dict):
        raise ConfigValidationError("Config root must be a mapping.")

    mlb_section = raw_config.get("mlb")
    if not isinstance(mlb_section, dict):
        return _normalize_mlb_live_underdog_defaults(None)

    live_underdog_section = mlb_section.get("live_underdog")
    if live_underdog_section is None:
        return _normalize_mlb_live_underdog_defaults(None)
    if not isinstance(live_underdog_section, dict):
        raise ConfigValidationError(
            "Invalid optional field 'mlb.live_underdog': expected mapping."
        )

    return _normalize_mlb_live_underdog_defaults(live_underdog_section)
