"""Paired, leakage-safe tournament for the direct-FP benchmark.

The tournament deliberately treats the two existing evaluators as the source
of truth for predictions.  It only joins their eligible player-week outputs,
so the comparison cannot accidentally score different slates or fold sets.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

from .baseline_evaluation import (
    BaselineEvaluationResult,
    evaluate_season_baselines,
    evaluate_weekly_baselines,
)
from .direct_fp_evaluation import (
    evaluate_season_direct_fantasy_points,
    evaluate_weekly_direct_fantasy_points,
)

Mode = Literal["weekly", "season"]
Alternative = Literal["one-sided", "two-sided"]


@dataclass(frozen=True, slots=True)
class PairedTournamentResult:
    """Auditable direct-FP versus internal-baseline comparison."""

    mode: Mode
    baseline: str
    paired_predictions: pd.DataFrame
    fold_results: pd.DataFrame
    slice_results: pd.DataFrame
    mean_delta: float
    ci_low: float
    ci_high: float
    confidence_level: float
    seed: int
    bootstrap_replicates: int
    outer_fold_count: int
    sample_count: int
    status: Literal["unpromoted"]
    comparison_status: Literal[
        "conclusive_tie_or_better", "conclusive_worse", "inconclusive"
    ]


_KEYS = ["season", "week", "player_id", "position", "outer_fold"]


def _fold_column(predictions: pd.DataFrame, mode: Mode) -> pd.Series:
    """Return canonical fold identifiers without trusting a model label."""
    if mode == "weekly":
        return predictions["season"].astype(str) + "-" + predictions["week"].astype(str)
    return predictions["season"].astype(str)


def _prepare(
    result: BaselineEvaluationResult,
    *,
    mode: Mode,
    prediction_name: str,
    baseline: str | None = None,
) -> pd.DataFrame:
    """Validate and normalize one evaluator's player-week predictions."""
    frame = result.predictions.copy(deep=True)
    required = {"season", "week", "player_id", "position", "prediction"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{prediction_name} predictions missing columns: {missing}")
    if baseline is not None:
        if "baseline" not in frame.columns:
            raise ValueError("internal baseline predictions missing baseline column")
        frame = frame.loc[frame["baseline"].eq(baseline)].copy()
    frame["outer_fold"] = _fold_column(frame, mode)
    key_columns = ["season", "week", "player_id", "position", "outer_fold"]
    if frame.duplicated(key_columns).any():
        raise ValueError(f"duplicate {prediction_name} player-week predictions")
    frame["prediction"] = pd.to_numeric(frame["prediction"], errors="coerce")
    if frame["prediction"].isna().any() or (~np.isfinite(frame["prediction"])).any():
        raise ValueError(f"{prediction_name} predictions must be finite")
    actual = "actual" if "actual" in frame.columns else "fantasy_points"
    if actual not in frame.columns:
        raise ValueError(f"{prediction_name} predictions missing actual outcome")
    frame["actual"] = pd.to_numeric(frame[actual], errors="coerce")
    if frame["actual"].isna().any() or (~np.isfinite(frame["actual"])).any():
        raise ValueError(f"{prediction_name} outcomes must be finite")
    return frame[key_columns + ["prediction", "actual"]].rename(
        columns={
            "prediction": f"{prediction_name}_prediction",
            "actual": f"{prediction_name}_actual",
        }
    )


def _bootstrap_interval(
    deltas: np.ndarray,
    *,
    confidence_level: float,
    seed: int,
    replicates: int,
    alternative: Alternative,
) -> tuple[float, float]:
    """Calculate a deterministic percentile interval over outer-fold deltas."""
    if len(deltas) == 0:
        return float("nan"), float("nan")
    if replicates < 1:
        raise ValueError("bootstrap_replicates must be positive")
    rng = np.random.default_rng(seed)
    samples = rng.choice(deltas, size=(replicates, len(deltas)), replace=True).mean(
        axis=1
    )
    alpha = 1.0 - confidence_level
    low_q = 0.0 if alternative == "one-sided" else alpha / 2.0
    high_q = 1.0 - alpha
    return float(np.quantile(samples, low_q)), float(np.quantile(samples, high_q))


def _result_rows(paired: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for values, group in paired.groupby(group_columns, sort=True, dropna=False):
        if not isinstance(values, tuple):
            values = (values,)
        delta = group["direct_abs_error"].mean() - group["baseline_abs_error"].mean()
        row = {column: value for column, value in zip(group_columns, values)}
        row.update(
            rows_count=len(group),
            direct_mae=float(group["direct_abs_error"].mean()),
            baseline_mae=float(group["baseline_abs_error"].mean()),
            delta=float(delta),
        )
        rows.append(row)
    return pd.DataFrame(rows)


def _slice_results(paired: pd.DataFrame) -> pd.DataFrame:
    """Return overall and position-by-fold paired results."""
    overall = _result_rows(paired, ["outer_fold"])
    overall.insert(0, "scope", "overall")
    position = _result_rows(paired, ["position", "outer_fold"])
    position.insert(0, "scope", "position")
    return pd.concat([overall, position], ignore_index=True, sort=False)


def tournament_direct_vs_internal_baseline(
    frame: pd.DataFrame,
    *,
    mode: Mode = "weekly",
    target_col: str = "fantasy_points",
    window: int = 6,
    baseline: str = "player_trailing_mean",
    confidence_level: float = 0.95,
    seed: int = 0,
    bootstrap_replicates: int = 2000,
    alternative: Alternative = "two-sided",
    min_outer_folds: int = 3,
) -> PairedTournamentResult:
    """Compare direct FP and the declared internal baseline on paired folds.

    Both evaluators receive the same deep-copied frame and retain their own
    strict as-of logic.  A comparison is conclusive only with at least
    ``min_outer_folds`` and an interval whose upper bound is non-positive;
    promotion is never performed by this function.
    """
    if mode not in ("weekly", "season"):
        raise ValueError("mode must be 'weekly' or 'season'")
    if alternative not in ("one-sided", "two-sided"):
        raise ValueError("alternative must be 'one-sided' or 'two-sided'")
    if not 0 < confidence_level < 1:
        raise ValueError("confidence_level must be between 0 and 1")
    if (
        isinstance(min_outer_folds, bool)
        or not isinstance(min_outer_folds, int)
        or min_outer_folds < 1
    ):
        raise ValueError("min_outer_folds must be a positive integer")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ValueError("seed must be an integer")

    source = frame.copy(deep=True)
    if mode == "weekly":
        direct = evaluate_weekly_direct_fantasy_points(
            source, target_col=target_col, window=window
        )
        internal = evaluate_weekly_baselines(source, target_col=target_col)
    else:
        direct = evaluate_season_direct_fantasy_points(
            source, target_col=target_col, window=window
        )
        internal = evaluate_season_baselines(source, target_col=target_col)
    left = _prepare(direct, mode=mode, prediction_name="direct")
    right = _prepare(internal, mode=mode, prediction_name="baseline", baseline=baseline)
    left_keys = set(map(tuple, left[_KEYS].itertuples(index=False, name=None)))
    right_keys = set(map(tuple, right[_KEYS].itertuples(index=False, name=None)))
    if left_keys != right_keys:
        raise ValueError("paired evaluator key sets differ")
    if left.empty or right.empty:
        paired = pd.DataFrame(
            columns=_KEYS + ["direct_prediction", "baseline_prediction", "actual"]
        )
    else:
        paired = left.merge(right, on=_KEYS, how="inner", validate="one_to_one")
        if not np.array_equal(
            paired["direct_actual"].to_numpy(dtype=float),
            paired["baseline_actual"].to_numpy(dtype=float),
        ):
            raise ValueError("paired evaluators disagree on actual outcomes")
        paired = paired.drop(columns=["baseline_actual"]).rename(
            columns={"direct_actual": "actual"}
        )
        paired["direct_abs_error"] = (
            paired["direct_prediction"] - paired["actual"]
        ).abs()
        paired["baseline_abs_error"] = (
            paired["baseline_prediction"] - paired["actual"]
        ).abs()
    folds = _result_rows(paired, ["outer_fold"]) if not paired.empty else pd.DataFrame()
    deltas = (
        folds["delta"].to_numpy(dtype=float)
        if not folds.empty
        else np.array([], dtype=float)
    )
    ci_low, ci_high = _bootstrap_interval(
        deltas,
        confidence_level=confidence_level,
        seed=seed,
        replicates=bootstrap_replicates,
        alternative=alternative,
    )
    fold_count = int(folds["outer_fold"].nunique()) if not folds.empty else 0
    mean_delta = float(deltas.mean()) if len(deltas) else float("nan")
    conclusive = (
        fold_count >= min_outer_folds and np.isfinite(ci_high) and ci_high <= 0.0
    )
    if fold_count < min_outer_folds or not np.isfinite(ci_high):
        comparison_status = "inconclusive"
    elif conclusive:
        comparison_status = "conclusive_tie_or_better"
    else:
        comparison_status = "conclusive_worse"
    return PairedTournamentResult(
        mode=mode,
        baseline=baseline,
        paired_predictions=paired,
        fold_results=folds,
        slice_results=_slice_results(paired) if not paired.empty else pd.DataFrame(),
        mean_delta=mean_delta,
        ci_low=ci_low,
        ci_high=ci_high,
        confidence_level=confidence_level,
        seed=seed,
        bootstrap_replicates=bootstrap_replicates,
        outer_fold_count=fold_count,
        sample_count=len(paired),
        status="unpromoted",
        comparison_status=comparison_status,
    )


__all__ = ["PairedTournamentResult", "tournament_direct_vs_internal_baseline"]
