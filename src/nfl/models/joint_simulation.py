"""Deterministic correlated simulation of NFL player fantasy-point outcomes.

R4.1 intentionally consumes the direct fantasy-point reference projection as
an input.  It is a simulation device, not a promoted projection model.  A
zero-mean game factor and a zero-mean game/team factor are shared by players
in the corresponding groups; independent player noise supplies the remaining
uncertainty.  Consequently, the projection remains the population mean while
same-group draws are positively correlated.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd

_PROJECTION_ALIASES: Final[tuple[str, ...]] = (
    "projection",
    "direct_fp_projection",
    "prediction",
)
_PROVENANCE_ALIASES: Final[tuple[str, ...]] = (
    "provenance",
    "projection_source",
    "baseline",
)
_STATUS_ALIASES: Final[tuple[str, ...]] = (
    "promotion_status",
    "status",
)


@dataclass(frozen=True, slots=True)
class JointSimulationConfig:
    """Parameters for a fixed-seed joint player simulation.

    Attributes:
        simulations: Number of outcome draws to produce.
        seed: Explicit seed for a private NumPy generator.  ``None`` is
            rejected by :func:`simulate_joint_player_outcomes`.
        game_factor_std: Standard deviation of the shared game environment
            factor, expressed as a multiplicative fraction of projection.
        team_factor_std: Standard deviation of the shared game/team factor,
            expressed as a multiplicative fraction of projection.
        idiosyncratic_std: Standard deviation of independent player noise in
            fantasy-point units.
    """

    simulations: int = 10_000
    seed: int | None = None
    game_factor_std: float = 0.10
    team_factor_std: float = 0.25
    idiosyncratic_std: float = 1.50

    def __post_init__(self) -> None:
        """Validate simulation controls without touching global RNG state."""
        if (
            isinstance(self.simulations, (bool, np.bool_))
            or not isinstance(self.simulations, (int, np.integer))
            or self.simulations < 1
        ):
            raise ValueError("simulations must be a positive integer")
        if self.seed is not None and (
            isinstance(self.seed, (bool, np.bool_))
            or not isinstance(self.seed, (int, np.integer))
        ):
            raise ValueError("seed must be an integer or None")
        for name in ("game_factor_std", "team_factor_std", "idiosyncratic_std"):
            value = getattr(self, name)
            if (
                isinstance(value, (bool, np.bool_))
                or not np.isscalar(value)
                or not np.isfinite(float(value))
                or float(value) < 0
            ):
                raise ValueError(f"{name} must be finite and nonnegative")


def _first_present(frame: pd.DataFrame, aliases: tuple[str, ...], label: str) -> str:
    """Return one schema column, rejecting conflicting aliases."""
    present = [column for column in aliases if column in frame.columns]
    if not present:
        raise ValueError(f"missing required {label} column; expected one of {aliases}")
    if len(present) > 1:
        first = frame[present[0]]
        for other_name in present[1:]:
            if not first.equals(frame[other_name]):
                raise ValueError(f"conflicting aliases for {label}: {present}")
    return present[0]


def _non_empty(values: pd.Series, label: str) -> pd.Series:
    """Validate and return string keys without changing source metadata."""
    if values.isna().any():
        raise ValueError(f"{label} must be non-empty")
    normalized = values.astype(str).str.strip()
    if normalized.eq("").any():
        raise ValueError(f"{label} must be non-empty")
    return normalized


def _validate_provenance(frame: pd.DataFrame) -> tuple[str, str | None]:
    """Validate direct-FP and non-promotion provenance fields."""
    provenance_col = _first_present(frame, _PROVENANCE_ALIASES, "provenance")
    source = _non_empty(frame[provenance_col], "provenance").str.lower()
    direct_fp = source.map(
        lambda value: "direct" in value
        and ("fantasy" in value or "fp" in value or "points" in value)
    )
    if not direct_fp.all():
        raise ValueError("projection provenance must identify the direct-FP baseline")

    status_cols = [column for column in _STATUS_ALIASES if column in frame.columns]
    status_col = status_cols[0] if status_cols else None
    status: pd.Series
    if status_col is None:
        status = source
        if not status.map(_is_non_promoted).all():
            raise ValueError("direct-FP projections require non-promoted provenance")
    else:
        status = _non_empty(frame[status_col], "promotion status").str.lower()
        for alias in status_cols[1:]:
            other_status = _non_empty(frame[alias], "promotion status").str.lower()
            if not status.equals(other_status):
                raise ValueError(
                    f"conflicting aliases for promotion status: {status_cols}"
                )
        if not status.map(_is_non_promoted).all():
            raise ValueError("direct-FP projection status must be non-promoted")
    return provenance_col, status_col


def _is_non_promoted(value: str) -> bool:
    """Return whether a provenance/status token explicitly remains unpromoted."""
    token = value.lower().replace("-", "_").replace(" ", "_")
    if "promot" in token:
        return any(
            marker in token for marker in ("unpromot", "not_promot", "non_promot")
        )
    return any(
        marker in token
        for marker in ("research", "candidate", "inconclusive", "baseline")
    )


def _validate_projection_table(frame: pd.DataFrame) -> tuple[pd.DataFrame, str, str]:
    """Validate input and return private normalized keys and projection name."""
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("projections must be a pandas DataFrame")
    if frame.empty:
        raise ValueError("projections must contain at least one player row")
    projection_col = _first_present(frame, _PROJECTION_ALIASES, "projection")
    if "player_id" not in frame.columns:
        raise ValueError("missing required columns: ['player_id']")
    if "team" not in frame.columns:
        raise ValueError("missing required columns: ['team']")
    game_col = "game_id" if "game_id" in frame.columns else "game"
    if game_col not in frame.columns:
        raise ValueError("missing required game_id column")
    reserved = {"simulation_id", "simulated_fantasy_points"}.intersection(frame)
    if reserved:
        raise ValueError(
            "reserved output columns cannot be input metadata: "
            f"{sorted(reserved)}"
        )
    _validate_provenance(frame)

    work = frame.copy(deep=True)
    work["_player_key"] = _non_empty(work["player_id"], "player_id")
    work["_team_key"] = _non_empty(work["team"], "team")
    work["_game_key"] = _non_empty(work[game_col], game_col)
    raw_projection = work[projection_col]
    if raw_projection.map(lambda value: isinstance(value, (bool, np.bool_))).any():
        raise ValueError("projection must contain finite numeric values")
    projection = pd.to_numeric(raw_projection, errors="coerce")
    if projection.isna().any() or (~np.isfinite(projection)).any():
        raise ValueError("projection must contain finite numeric values")
    work["_projection"] = projection.astype(float)
    if work.duplicated(["_game_key", "_player_key"]).any():
        raise ValueError("duplicate game/player projection rows")
    return work, projection_col, game_col


def simulate_joint_player_outcomes(
    projections: pd.DataFrame,
    *,
    config: JointSimulationConfig | None = None,
    seed: int | None = None,
    simulations: int | None = None,
) -> pd.DataFrame:
    """Generate correlated player fantasy-point outcomes in long form.

    Args:
        projections: Player-level direct-FP table.  Required columns are
            ``player_id``, ``game_id`` (or ``game``), ``team``, one projection
            alias (prefer ``projection``), one direct-FP provenance alias, and
            an explicit non-promoted status (or a provenance token containing
            that status).  All other columns are retained in every draw.
        config: Optional validated simulation configuration.
        seed: Explicit seed override.  A seed must be supplied here or in
            ``config``.
        simulations: Optional count override, useful for small offline runs.

    Returns:
        A long ``DataFrame`` with one row per simulation/player pair.  It
        retains input metadata and adds ``simulation_id`` and
        ``simulated_fantasy_points``.  The random generator is private and
        never mutates NumPy's global RNG.

    Raises:
        TypeError: If ``projections`` is not a DataFrame.
        ValueError: If schema, values, provenance, or controls are invalid.
    """
    base_config = config if config is not None else JointSimulationConfig()
    if not isinstance(base_config, JointSimulationConfig):
        raise TypeError("config must be a JointSimulationConfig")
    if seed is not None and base_config.seed is not None and seed != base_config.seed:
        raise ValueError("seed conflicts with config.seed")
    resolved_seed = base_config.seed if seed is None else seed
    resolved_count = base_config.simulations if simulations is None else simulations
    effective = JointSimulationConfig(
        simulations=resolved_count,
        seed=resolved_seed,
        game_factor_std=base_config.game_factor_std,
        team_factor_std=base_config.team_factor_std,
        idiosyncratic_std=base_config.idiosyncratic_std,
    )
    if effective.seed is None:
        raise ValueError("seed is required for deterministic joint simulation")
    work, _, _ = _validate_projection_table(projections)

    player_count = len(work)
    game_codes, _ = pd.factorize(work["_game_key"], sort=False)
    team_group = work["_game_key"] + "\x1f" + work["_team_key"]
    team_codes, _ = pd.factorize(team_group, sort=False)
    rng = np.random.default_rng(effective.seed)
    game_factor = rng.normal(
        0.0,
        effective.game_factor_std,
        size=(effective.simulations, game_codes.max() + 1),
    )
    team_factor = rng.normal(
        0.0,
        effective.team_factor_std,
        size=(effective.simulations, team_codes.max() + 1),
    )
    independent = rng.normal(
        0.0, effective.idiosyncratic_std, size=(effective.simulations, player_count)
    )
    means = work["_projection"].to_numpy(dtype=float)
    samples = means + means * (
        game_factor[:, game_codes] + team_factor[:, team_codes]
    ) + independent

    row_index = np.tile(np.arange(player_count), effective.simulations)
    output = projections.iloc[row_index].reset_index(drop=True).copy()
    output["simulation_id"] = np.repeat(
        np.arange(effective.simulations, dtype=int), player_count
    )
    output["simulated_fantasy_points"] = samples.reshape(-1)
    output.attrs["simulation_config"] = effective
    output.attrs["provenance_status"] = "direct_fantasy_points_unpromoted"
    return output


simulate_joint_outcomes = simulate_joint_player_outcomes
simulate_joint_fantasy_points = simulate_joint_player_outcomes


__all__ = [
    "JointSimulationConfig",
    "simulate_joint_fantasy_points",
    "simulate_joint_outcomes",
    "simulate_joint_player_outcomes",
]
