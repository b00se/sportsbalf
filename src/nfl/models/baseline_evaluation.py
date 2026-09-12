"""Leakage-safe baseline evaluation for canonical NFL player-week outcomes.

The evaluator deliberately contains no model fitting.  It provides useful,
reproducible reference points before more elaborate component models are
compared with the same walk-forward protocol.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

Mode = Literal["weekly", "season"]
RANKING_TOP_K = 3
METRIC_COLUMNS = [
    "scope",
    "group",
    "baseline",
    "metric",
    "component",
    "fold",
    "aggregation",
    "rows",
    "outer_folds",
    "valid_folds",
    "folds",
    "eligible",
    "parameter",
    "definition",
    "valid",
    "status",
    "reason",
    "value",
]
METRIC_DEFINITIONS = {
    "mae": "mean absolute point-forecast error",
    "crps": (
        "mean absolute point-forecast error; deterministic point-forecast "
        "CRPS equals MAE"
    ),
    "mean_error": "mean signed point-forecast error (prediction minus actual)",
    "spearman_rank": (
        "mean per-fold Spearman correlation of predicted and actual values; "
        "folds with fewer than two distinct values are ineligible"
    ),
    "top_k_recall": (
        "mean per-fold recall of the actual top-k values in the predicted "
        "top-k, where k=min(3,n); ties are resolved by player_id ascending"
    ),
    "calibration": (
        "unavailable: deterministic point forecasts expose neither predictive "
        "intervals nor probabilities, so empirical calibration cannot be computed"
    ),
}


@dataclass(frozen=True, slots=True)
class BaselineEvaluationResult:
    """Predictions, metrics, and promotion status for one evaluation run."""

    predictions: pd.DataFrame
    metrics: pd.DataFrame
    status: str
    mode: Mode


def _validate(frame: pd.DataFrame, target_col: str) -> pd.DataFrame:
    required = {"season", "week", "player_id", "position", target_col}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"missing required columns: {missing}")
    work = frame.copy(deep=True)
    for col in ("season", "week"):
        if work[col].map(lambda value: isinstance(value, (bool, np.bool_))).any():
            raise ValueError(f"{col} must not contain boolean values")
        vals = pd.to_numeric(work[col], errors="coerce")
        if vals.isna().any() or (vals % 1 != 0).any():
            raise ValueError(f"{col} must contain integer calendar values")
        work[col] = vals.astype(int)
    if (
        (work["season"] < 1900).any()
        or (work["week"] < 1).any()
        or (work["week"] > 22).any()
    ):
        raise ValueError("invalid season/week calendar values; NFL week must be 1-22")
    for col in ("player_id", "position"):
        if work[col].isna().any() or work[col].astype(str).str.strip().eq("").any():
            raise ValueError(f"{col} must be non-empty")
    work["player_id"] = work["player_id"].astype(str)
    work["position"] = work["position"].astype(str).str.upper().str.strip()
    target = pd.to_numeric(work[target_col], errors="coerce")
    if target.isna().any() or (~np.isfinite(target)).any():
        raise ValueError("outcomes must be finite numeric values")
    work[target_col] = target.astype(float)
    if work.duplicated(["season", "week", "player_id"]).any():
        raise ValueError("duplicate player-week keys")
    return work.sort_values(
        ["season", "week", "player_id"], kind="mergesort"
    ).reset_index(drop=True)


def _spearman(pred: pd.Series, actual: pd.Series) -> float:
    if len(pred) < 2 or pred.nunique() < 2 or actual.nunique() < 2:
        return float("nan")
    return float(pred.rank(method="average").corr(actual.rank(method="average")))


def _top_recall(
    pred: pd.Series, actual: pd.Series, ids: pd.Series, k: int = RANKING_TOP_K
) -> float:
    if len(pred) == 0:
        return float("nan")
    ranking = pd.DataFrame(
        {
            "prediction": pred.to_numpy(),
            "actual": actual.to_numpy(),
            "player_id": ids.astype(str).to_numpy(),
        }
    )
    n = min(k, len(ranking))
    predicted_top = set(
        ranking.sort_values(
            ["prediction", "player_id"],
            ascending=[False, True],
            kind="mergesort",
        ).head(n)["player_id"]
    )
    actual_top = set(
        ranking.sort_values(
            ["actual", "player_id"], ascending=[False, True], kind="mergesort"
        ).head(n)["player_id"]
    )
    return float(len(predicted_top & actual_top) / n)


def _build_metrics(pred: pd.DataFrame, target_col: str) -> pd.DataFrame:
    """Build the shared honest metrics contract for deterministic forecasts.

    Errors are aggregated over complete outer folds, while rank metrics are
    calculated within each fold and then averaged.  The fold count gate is
    applied to every aggregate result, and point forecasts truthfully report
    calibration as unavailable.
    """
    rows: list[dict[str, object]] = []
    if pred.empty:
        return pd.DataFrame(columns=METRIC_COLUMNS)
    work = pred.copy(deep=True)
    if "outer_fold" not in work:
        work["outer_fold"] = (
            work["season"].astype(str) + "-" + work["week"].astype(str)
        )
    groups = [("overall", None, work)] + [
        ("position", p, g) for p, g in work.groupby("position", sort=True)
    ]
    for scope, key, group in groups:
        for baseline, b in group.groupby("baseline", sort=True):
            err = b["prediction"] - b[target_col]
            outer_folds = int(b["outer_fold"].nunique())
            aggregate_eligible = outer_folds >= 3
            aggregate_status = "eligible" if aggregate_eligible else "inconclusive"
            aggregate_reason = (
                "" if aggregate_eligible else "fewer than three distinct outer folds"
            )

            def add_aggregate(
                metric: str, value: float, definition: str
            ) -> None:
                rows.append(
                    {
                        "scope": scope,
                        "group": key,
                        "baseline": baseline,
                        "metric": metric,
                        "component": target_col,
                        "fold": None,
                        "aggregation": "aggregate",
                        "rows": len(b),
                        "outer_folds": outer_folds,
                        "valid_folds": outer_folds,
                        "folds": outer_folds,
                        "eligible": aggregate_eligible,
                        "parameter": None,
                        "definition": definition,
                        "valid": aggregate_eligible,
                        "status": aggregate_status,
                        "reason": aggregate_reason,
                        "value": value if aggregate_eligible else float("nan"),
                    }
                )

            mean_abs_error = float(err.abs().mean())
            add_aggregate("mae", mean_abs_error, METRIC_DEFINITIONS["mae"])
            add_aggregate("crps", mean_abs_error, METRIC_DEFINITIONS["crps"])
            add_aggregate(
                "mean_error", float(err.mean()), METRIC_DEFINITIONS["mean_error"]
            )

            rank_values: dict[str, list[float]] = {
                "spearman_rank": [],
                "top_k_recall": [],
            }
            for fold, fold_group in b.groupby("outer_fold", sort=True):
                fold_pred = pd.to_numeric(fold_group["prediction"], errors="coerce")
                fold_actual = pd.to_numeric(
                    fold_group[target_col], errors="coerce"
                )
                if (
                    len(fold_group) >= 2
                    and fold_pred.nunique(dropna=True) >= 2
                    and fold_actual.nunique(dropna=True) >= 2
                ):
                    correlation = fold_pred.corr(fold_actual, method="spearman")
                    spearman_valid = pd.notna(correlation)
                    spearman_value = (
                        float(correlation) if spearman_valid else float("nan")
                    )
                    spearman_reason = (
                        "" if spearman_valid else "undefined Spearman correlation"
                    )
                else:
                    spearman_valid = False
                    spearman_value = float("nan")
                    spearman_reason = (
                        "requires at least two rows and two distinct predicted and "
                        "actual values"
                    )
                if spearman_valid:
                    rank_values["spearman_rank"].append(spearman_value)
                rows.append(
                    {
                        "scope": scope,
                        "group": key,
                        "baseline": baseline,
                        "metric": "spearman_rank",
                        "component": target_col,
                        "fold": fold,
                        "aggregation": "fold",
                        "rows": len(fold_group),
                        "outer_folds": 1,
                        "valid_folds": int(spearman_valid),
                        "folds": int(spearman_valid),
                        "eligible": False,
                        "parameter": None,
                        "definition": METRIC_DEFINITIONS["spearman_rank"],
                        "valid": spearman_valid,
                        "status": "inconclusive",
                        "reason": spearman_reason,
                        "value": spearman_value,
                    }
                )

                top_k_value = _top_recall(
                    fold_group["prediction"],
                    fold_group[target_col],
                    fold_group["player_id"],
                    k=RANKING_TOP_K,
                )
                top_k_valid = pd.notna(top_k_value)
                if top_k_valid:
                    rank_values["top_k_recall"].append(float(top_k_value))
                rows.append(
                    {
                        "scope": scope,
                        "group": key,
                        "baseline": baseline,
                        "metric": "top_k_recall",
                        "component": target_col,
                        "fold": fold,
                        "aggregation": "fold",
                        "rows": len(fold_group),
                        "outer_folds": 1,
                        "valid_folds": int(top_k_valid),
                        "folds": int(top_k_valid),
                        "eligible": False,
                        "parameter": f"k={RANKING_TOP_K}",
                        "definition": METRIC_DEFINITIONS["top_k_recall"],
                        "valid": top_k_valid,
                        "status": "inconclusive",
                        "reason": "" if top_k_valid else "requires at least one row",
                        "value": float(top_k_value)
                        if top_k_valid
                        else float("nan"),
                    }
                )

            for metric, values in rank_values.items():
                eligible = outer_folds >= 3 and len(values) >= 3
                rows.append(
                    {
                        "scope": scope,
                        "group": key,
                        "baseline": baseline,
                        "metric": metric,
                        "component": target_col,
                        "fold": None,
                        "aggregation": "aggregate",
                        "rows": len(b),
                        "outer_folds": outer_folds,
                        "valid_folds": len(values),
                        "folds": len(values),
                        "eligible": eligible,
                        "parameter": (
                            f"k={RANKING_TOP_K}" if metric == "top_k_recall" else None
                        ),
                        "definition": METRIC_DEFINITIONS[metric],
                        "valid": bool(values),
                        "status": "eligible" if eligible else "inconclusive",
                        "reason": (
                            "" if eligible else "fewer than three valid outer folds"
                        ),
                        "value": float(np.mean(values)) if eligible else float("nan"),
                    }
                )
            rows.append(
                {
                    "scope": scope,
                    "group": key,
                    "baseline": baseline,
                    "metric": "calibration",
                    "component": target_col,
                    "fold": None,
                    "aggregation": "aggregate",
                    "rows": len(b),
                    "outer_folds": outer_folds,
                    "valid_folds": 0,
                    "folds": 0,
                    "eligible": False,
                    "parameter": "unavailable",
                    "definition": METRIC_DEFINITIONS["calibration"],
                    "valid": False,
                    "status": "inconclusive",
                    "reason": METRIC_DEFINITIONS["calibration"],
                    "value": float("nan"),
                }
            )
    return pd.DataFrame(rows, columns=METRIC_COLUMNS)


def evaluate_baselines(
    frame: pd.DataFrame, *, target_col: str = "fantasy_points", mode: Mode = "weekly"
) -> BaselineEvaluationResult:
    """Evaluate fixed naive baselines using only strictly earlier observations.

    ``weekly`` uses expanding rolling-origin folds (one calendar week at a
    time); ``season`` holds each season out and trains only on earlier seasons.
    """
    if mode not in ("weekly", "season"):
        raise ValueError("mode must be 'weekly' or 'season'")
    work = _validate(frame, target_col)
    keys = list(
        work[["season", "week"]].drop_duplicates().itertuples(index=False, name=None)
    )
    seasons = sorted(work["season"].unique())
    preds: list[pd.DataFrame] = []
    for key in keys if mode == "weekly" else [(s, None) for s in seasons]:
        if mode == "weekly":
            season, week = key
            train = work[
                (work["season"] < season)
                | ((work["season"] == season) & (work["week"] < week))
            ]
            test = work[(work["season"] == season) & (work["week"] == week)].copy()
        else:
            season = key[0]
            train = work[work["season"] < season]
            test = work[work["season"] == season].copy()
        if train.empty or test.empty:
            continue
        player = train.groupby("player_id")[target_col].mean()
        pos = train.groupby("position")[target_col].mean()
        global_mean = float(train[target_col].mean())
        test["player_trailing_mean"] = (
            test["player_id"]
            .map(player)
            .fillna(test["position"].map(pos))
            .fillna(global_mean)
        )
        test["position_historical_mean"] = test["position"].map(pos).fillna(global_mean)
        long = test.melt(
            id_vars=["season", "week", "player_id", "position", target_col],
            value_vars=["player_trailing_mean", "position_historical_mean"],
            var_name="baseline",
            value_name="prediction",
        )
        long["outer_fold"] = (
            f"{season}-{week}" if mode == "weekly" else str(season)
        )
        preds.append(long)
    prediction_frame = (
        pd.concat(preds, ignore_index=True)
        if preds
        else pd.DataFrame(
            columns=[
                "season",
                "week",
                "player_id",
                "position",
                target_col,
                "baseline",
                "prediction",
                "outer_fold",
            ]
        )
    )
    outer_folds = (
        prediction_frame["outer_fold"].nunique()
        if not prediction_frame.empty
        else 0
    )
    status = (
        "eligible_for_candidate_comparison"
        if outer_folds >= 3
        else "inconclusive"
    )
    return BaselineEvaluationResult(
        prediction_frame,
        _build_metrics(prediction_frame, target_col),
        status,
        mode,
    )


def evaluate_weekly_baselines(
    frame: pd.DataFrame, *, target_col: str = "fantasy_points"
) -> BaselineEvaluationResult:
    """Evaluate expanding weekly baselines."""
    return evaluate_baselines(frame, target_col=target_col, mode="weekly")


def evaluate_season_baselines(
    frame: pd.DataFrame, *, target_col: str = "fantasy_points"
) -> BaselineEvaluationResult:
    """Evaluate season-held-out baselines."""
    return evaluate_baselines(frame, target_col=target_col, mode="season")


__all__ = [
    "BaselineEvaluationResult",
    "evaluate_baselines",
    "evaluate_weekly_baselines",
    "evaluate_season_baselines",
]
