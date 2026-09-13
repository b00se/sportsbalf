"""Leakage-safe weekly regressor bakeoff for archived NFL fantasy points."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd
from sklearn.ensemble import (
    ExtraTreesRegressor,
    GradientBoostingRegressor,
    RandomForestRegressor,
)
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {"season", "week", "player_id", "position", "fantasy_points"}
)
CANDIDATE_NAMES: Final[tuple[str, ...]] = (
    "ridge",
    "random_forest",
    "extra_trees",
    "gradient_boosting",
)
FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "prior_mean",
    "recent_mean",
    "prior_count",
    "season_mean",
)
METRIC_COLUMNS: Final[tuple[str, ...]] = (
    "candidate",
    "position",
    "metric",
    "value",
    "outer_folds",
    "valid_folds",
    "eligible",
    "status",
)
PREDICTION_COLUMNS: Final[tuple[str, ...]] = (
    "fold",
    "season",
    "week",
    "player_id",
    "position",
    "candidate",
    "prediction",
    "actual",
)
RANKING_COLUMNS: Final[tuple[str, ...]] = (
    "candidate",
    "mean_fold_mae",
    "outer_folds",
    "eligible",
    "status",
    "selection",
)


@dataclass(frozen=True, slots=True)
class WeeklyBakeoffResult:
    """Fold predictions, metrics, and research-only candidate ranking."""

    predictions: pd.DataFrame
    metrics: pd.DataFrame
    ranking: pd.DataFrame
    status: str


def _validate_outcomes(outcomes: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(outcomes, pd.DataFrame):
        raise TypeError("outcomes must be a pandas DataFrame")
    missing = sorted(REQUIRED_COLUMNS - set(outcomes.columns))
    if missing:
        raise ValueError(f"outcomes missing required columns: {missing}")
    work = outcomes.copy(deep=True)
    work["player_id"] = work["player_id"].astype("string").str.strip()
    work["position"] = work["position"].astype("string").str.strip().str.upper()
    if work["player_id"].isna().any() or work["player_id"].eq("").any():
        raise ValueError("player_id must be non-empty")
    if work["position"].isna().any() or work["position"].eq("").any():
        raise ValueError("position must be non-empty")
    for col, low, high in (("season", 1920, 9999), ("week", 1, 22)):
        raw = work[col]
        if raw.map(lambda value: isinstance(value, (bool, np.bool_))).any():
            raise ValueError(f"{col} must not contain booleans")
        values = pd.to_numeric(raw, errors="coerce")
        if (
            values.isna().any()
            or (~np.isfinite(values)).any()
            or (values % 1 != 0).any()
            or (values < low).any()
            or (values > high).any()
        ):
            raise ValueError(f"{col} must be a finite integer in range")
        work[col] = values.astype(int)
    raw = work["fantasy_points"]
    if raw.map(lambda value: isinstance(value, (bool, np.bool_))).any():
        raise ValueError("fantasy_points must not contain booleans")
    work["fantasy_points"] = pd.to_numeric(raw, errors="coerce")
    if (
        work["fantasy_points"].isna().any()
        or (~np.isfinite(work["fantasy_points"])).any()
    ):
        raise ValueError("fantasy_points must be finite numeric values")
    if work.duplicated(["player_id", "season", "week"]).any():
        raise ValueError("duplicate player/calendar rows")
    return work.sort_values(
        ["season", "week", "player_id"], kind="mergesort"
    ).reset_index(drop=True)


def _calendar(frame: pd.DataFrame) -> pd.Series:
    return frame["season"] * 100 + frame["week"]


def _history_features(history: pd.DataFrame, rows: pd.DataFrame) -> pd.DataFrame:
    """Compute features using observations strictly before each row."""
    ordered = history.sort_values(
        ["player_id", "season", "week"], kind="mergesort"
    ).copy()
    grouped = ordered.groupby("player_id", sort=False)["fantasy_points"]
    prior = grouped.shift(1)
    count = grouped.cumcount().astype(float)
    cumulative = prior.groupby(ordered["player_id"], sort=False).cumsum()
    ordered["prior_count"] = count
    ordered["prior_mean"] = cumulative.div(count.replace(0.0, np.nan)).fillna(0.0)
    ordered["recent_mean"] = (
        prior.groupby(ordered["player_id"], sort=False)
        .rolling(4, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
        .fillna(0.0)
        .to_numpy()
    )
    season_group = ordered.groupby(["player_id", "season"], sort=False)[
        "fantasy_points"
    ]
    season_prior = season_group.shift(1)
    season_cumulative = season_prior.groupby(
        [ordered["player_id"], ordered["season"]], sort=False
    ).cumsum()
    season_count = season_group.cumcount().astype(float)
    ordered["season_mean"] = season_cumulative.div(
        season_count.replace(0.0, np.nan)
    ).fillna(ordered["prior_mean"])
    key = ["player_id", "season", "week"]
    feature_table = ordered.set_index(key)[list(FEATURE_COLUMNS)]
    result = feature_table.reindex(rows.set_index(key).index).fillna(0.0)
    result.index = rows.index
    return result


def _candidates() -> dict[str, object]:
    return {
        "ridge": make_pipeline(StandardScaler(), Ridge(alpha=10.0)),
        "random_forest": RandomForestRegressor(
            n_estimators=80, min_samples_leaf=3, random_state=42, n_jobs=1
        ),
        "extra_trees": ExtraTreesRegressor(
            n_estimators=80, min_samples_leaf=3, random_state=42, n_jobs=1
        ),
        "gradient_boosting": GradientBoostingRegressor(
            n_estimators=80,
            learning_rate=0.05,
            max_depth=3,
            min_samples_leaf=3,
            random_state=42,
        ),
    }


def _spearman(predicted: pd.Series, actual: pd.Series) -> float:
    if predicted.nunique() < 2 or actual.nunique() < 2:
        return float("nan")
    return float(predicted.rank(method="average").corr(actual.rank(method="average")))


def _top_k(predicted: pd.Series, actual: pd.Series) -> float:
    k = min(3, len(actual))
    if k == 0:
        return float("nan")
    ids = pd.Series(actual.index, index=actual.index).astype(str)
    order = pd.DataFrame({"pred": predicted, "actual": actual, "id": ids}).sort_values(
        ["pred", "id"], ascending=[False, True]
    )
    pred_ids = set(order.head(k).index)
    actual_ids = set(
        order.sort_values(["actual", "id"], ascending=[False, True]).head(k).index
    )
    return len(pred_ids & actual_ids) / k


def _metric_rows(predictions: pd.DataFrame) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for candidate, group in predictions.groupby("candidate", sort=True):
        fold_values: list[float] = []
        spearman_values: list[float] = []
        top_values: list[float] = []
        for _, fold in group.groupby("fold", sort=True):
            fold_values.append(
                float(np.mean(np.abs(fold["prediction"] - fold["actual"])))
            )
            spearman_values.append(
                _spearman(
                    fold.set_index("player_id")["prediction"],
                    fold.set_index("player_id")["actual"],
                )
            )
            top_values.append(
                _top_k(
                    fold.set_index("player_id")["prediction"],
                    fold.set_index("player_id")["actual"],
                )
            )
        valid_s = [v for v in spearman_values if np.isfinite(v)]
        valid_t = [v for v in top_values if np.isfinite(v)]
        outer = group["fold"].nunique()
        eligible = outer >= 3
        mae = float(np.mean(fold_values)) if fold_values else float("nan")
        rows.extend(
            [
                {
                    "candidate": candidate,
                    "metric": "fantasy_point_mae",
                    "value": mae,
                    "outer_folds": int(outer),
                    "valid_folds": int(outer),
                    "eligible": bool(eligible),
                    "status": "eligible" if eligible else "inconclusive",
                },
                {
                    "candidate": candidate,
                    "metric": "fantasy_point_crps",
                    "value": mae,
                    "outer_folds": int(outer),
                    "valid_folds": int(outer),
                    "eligible": bool(eligible),
                    "status": "eligible" if eligible else "inconclusive",
                },
                {
                    "candidate": candidate,
                    "metric": "spearman_rank",
                    "value": float(np.mean(valid_s))
                    if eligible and valid_s
                    else float("nan"),
                    "outer_folds": int(outer),
                    "valid_folds": len(valid_s),
                    "eligible": bool(eligible and len(valid_s) >= 3),
                    "status": "eligible"
                    if eligible and len(valid_s) >= 3
                    else "inconclusive",
                },
                {
                    "candidate": candidate,
                    "metric": "top_k_recall",
                    "value": float(np.mean(valid_t))
                    if eligible and valid_t
                    else float("nan"),
                    "outer_folds": int(outer),
                    "valid_folds": len(valid_t),
                    "eligible": bool(eligible and len(valid_t) >= 3),
                    "status": "eligible"
                    if eligible and len(valid_t) >= 3
                    else "inconclusive",
                },
                {
                    "candidate": candidate,
                    "metric": "calibration",
                    "value": float("nan"),
                    "outer_folds": int(outer),
                    "valid_folds": 0,
                    "eligible": False,
                    "status": "unavailable",
                },
            ]
        )
        for position, position_group in group.groupby("position", sort=True):
            position_outer = position_group["fold"].nunique()
            position_eligible = position_outer >= 3
            position_folds = [
                float(np.mean(np.abs(f["prediction"] - f["actual"])))
                for _, f in position_group.groupby("fold", sort=True)
            ]
            rows.append(
                {
                    "candidate": candidate,
                    "position": position,
                    "metric": "fantasy_point_mae",
                    "value": float(np.mean(position_folds))
                    if position_eligible
                    else float("nan"),
                    "outer_folds": int(position_outer),
                    "valid_folds": len(position_folds),
                    "eligible": bool(position_eligible),
                    "status": "eligible" if position_eligible else "inconclusive",
                }
            )
    return rows


def evaluate_weekly_regressor_bakeoff(outcomes: pd.DataFrame) -> WeeklyBakeoffResult:
    """Fit fixed candidate regressors in weekly rolling-origin folds."""
    work = _validate_outcomes(outcomes)
    keys = work[["season", "week"]].drop_duplicates().sort_values(["season", "week"])
    prediction_frames: list[pd.DataFrame] = []
    candidates = _candidates()
    for fold_no, key in enumerate(keys.itertuples(index=False), start=1):
        boundary = int(key.season) * 100 + int(key.week)
        test = work.loc[
            (work["season"] == key.season) & (work["week"] == key.week)
        ].copy()
        train = work.loc[_calendar(work) < boundary].copy()
        if train.empty:
            continue
        x_train = _history_features(work, train)
        x_test = _history_features(work, test)
        y_train = train["fantasy_points"]
        for name, estimator in candidates.items():
            estimator.fit(x_train, y_train)
            predicted = np.asarray(estimator.predict(x_test), dtype=float)
            prediction_frames.append(
                pd.DataFrame(
                    {
                        "fold": fold_no,
                        "season": test["season"].to_numpy(),
                        "week": test["week"].to_numpy(),
                        "player_id": test["player_id"].astype(str).to_numpy(),
                        "position": test["position"].astype(str).to_numpy(),
                        "candidate": name,
                        "prediction": predicted,
                        "actual": test["fantasy_points"].to_numpy(),
                    }
                )
            )
    predictions = (
        pd.concat(prediction_frames, ignore_index=True)
        if prediction_frames
        else pd.DataFrame(columns=PREDICTION_COLUMNS)
    )
    metrics = pd.DataFrame(_metric_rows(predictions), columns=METRIC_COLUMNS)
    ranking = (
        metrics.loc[
            metrics["metric"].eq("fantasy_point_mae") & metrics["position"].isna(),
            ["candidate", "value", "outer_folds", "eligible", "status"],
        ]
        .rename(columns={"value": "mean_fold_mae"})
        .sort_values(["mean_fold_mae", "candidate"], kind="mergesort")
        .reset_index(drop=True)
    )
    if ranking.empty:
        ranking = pd.DataFrame(columns=RANKING_COLUMNS)
    ranking["selection"] = "research_only_same_outer_folds"
    core = metrics.loc[
        metrics["metric"].isin(
            ["fantasy_point_mae", "fantasy_point_crps", "spearman_rank", "top_k_recall"]
        )
    ]
    status = (
        "eligible"
        if not core.empty and bool(core["eligible"].all())
        else "inconclusive"
    )
    return WeeklyBakeoffResult(
        predictions=predictions, metrics=metrics, ranking=ranking, status=status
    )
