"""Tests for unified MLB pitcher-prop slate orchestration."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from src.core.contracts import PipelineConfig
from src.mlb.pitcher_props.slate import run_mlb_pitcher_prop_slate


def _pipeline_config(stat: str) -> PipelineConfig:
    return PipelineConfig(
        config_path=Path("config/mlb.yaml"),
        sport="mlb",
        stat=stat,
        raw={"pipeline": {"sport": "mlb", "stat": stat}},
        section={"stat": stat},
    )


def _scored_frame(stat: str, player: str, line: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "player": player,
                "predicted_value": line + 1.0,
                "prob_over": 0.61,
                "prob_under": 0.39,
                "ev_over": 0.12,
                "ev_under": -0.05,
                "run_mode": "prediction",
                "lines_status": "present",
            }
        ]
    )


def test_run_mlb_pitcher_prop_slate_combines_successful_stats_and_tracks_skips() -> (
    None
):
    completed = {
        "strikeouts": _pipeline_config("strikeouts"),
        "outs_recorded": _pipeline_config("outs_recorded"),
        "earned_runs": None,
        "hits_allowed": _pipeline_config("hits_allowed"),
        "bb_allowed": _pipeline_config("bb_allowed"),
    }

    def scorer(config: PipelineConfig, retrain: bool = False) -> pd.DataFrame:
        del retrain
        if config.stat == "strikeouts":
            return _scored_frame("strikeouts", "Gerrit Cole", 7.5)
        if config.stat == "outs_recorded":
            return _scored_frame("outs_recorded", "Zack Wheeler", 17.5)
        if config.stat == "hits_allowed":
            raise FileNotFoundError(
                "Pitcher prop lines file not found: /tmp/hits_allowed_2026-03-25.csv"
            )
        if config.stat == "bb_allowed":
            raise FileNotFoundError("could not open /tmp/model.joblib")
        return pd.DataFrame()

    result = run_mlb_pitcher_prop_slate(completed, scorer=scorer)

    assert list(result.completed_stats) == ["strikeouts", "outs_recorded"]
    assert result.skipped_stats == {
        "earned_runs": "no config provided",
        "hits_allowed": (
            "Pitcher prop lines file not found: /tmp/hits_allowed_2026-03-25.csv"
        ),
    }
    assert result.failed_stats == {"bb_allowed": "could not open /tmp/model.joblib"}
    assert list(result.combined_frame["stat_id"]) == ["strikeouts", "outs_recorded"]
    assert list(result.combined_frame["player"]) == ["Gerrit Cole", "Zack Wheeler"]


def test_run_mlb_pitcher_prop_slate_returns_stable_empty_schema_when_no_rows() -> None:
    config = {"strikeouts": _pipeline_config("strikeouts")}

    def scorer(config: PipelineConfig, retrain: bool = False) -> pd.DataFrame:
        del config, retrain
        return pd.DataFrame()

    result = run_mlb_pitcher_prop_slate(config, scorer=scorer, stats=["strikeouts"])

    assert result.completed_stats == ()
    assert result.skipped_stats == {"strikeouts": "no scored frame returned"}
    assert result.failed_stats == {}
    assert result.combined_frame.empty
    assert list(result.combined_frame.columns) == [
        "stat_id",
        "player",
        "predicted_value",
        "prob_over",
        "prob_under",
        "ev_over",
        "ev_under",
        "run_mode",
        "lines_status",
    ]


def test_run_mlb_pitcher_prop_slate_rejects_unsupported_explicit_stats() -> None:
    config = {"strikeouts": _pipeline_config("strikeouts")}

    def scorer(config: PipelineConfig, retrain: bool = False) -> pd.DataFrame:
        del config, retrain
        return _scored_frame("strikeouts", "Gerrit Cole", 7.5)

    try:
        run_mlb_pitcher_prop_slate(config, scorer=scorer, stats=["strikouts"])
    except ValueError as exc:
        message = str(exc)
    else:  # pragma: no cover - defensive branch
        raise AssertionError("Expected unsupported stat to raise ValueError")

    assert "Unsupported MLB pitcher-prop stat(s): ['strikouts']" in message
    assert "strikeouts" in message
