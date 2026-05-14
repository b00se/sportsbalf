"""CLI tests for MLB live Underdog shadow-run bet slips."""

from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pandas as pd
import pytest
from src.core.config import ConfigValidationError
from src.core.contracts import PipelineConfig
from src.mlb.pitcher_props.slate import MlbPitcherPropSlateResult


def _live_rows(stat: str, player: str, team: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "appearance_id": f"{stat}-appearance",
                "player_ud_id": f"{stat}-player",
                "player_name": player,
                "game_id": f"{stat}-game",
                "team_id": team,
                "line": 7.5,
                "book": "Underdog",
                "scheduled_at": "2026-03-25T19:05:00Z",
                "season_type": "regular",
                "stat_id": stat,
                "over_decimal_price": 1.92,
                "over_payout_multiplier": 0.92,
                "over_american_price": -110,
                "under_decimal_price": 1.88,
                "under_payout_multiplier": 0.88,
                "under_american_price": -115,
            }
        ]
    )


def _slate_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "player": "Gerrit Cole",
                "player_id": "cole-1",
                "team": "NYY",
                "opponent": "BOS",
                "game_date": pd.Timestamp("2026-03-25"),
                "rest_days": 4,
                "park_factor": 1.02,
                "stat_id": "strikeouts",
                "line": 8.5,
                "play": "over",
                "prob": 0.62,
                "ev": 0.16,
                "payout": 1.92,
                "payout_multiplier": 0.92,
                "sport": "MLB",
                "market": "strikeouts",
                "run_mode": "prediction",
                "lines_status": "present",
            },
            {
                "player": "Gerrit Cole",
                "player_id": "cole-1",
                "team": "NYY",
                "opponent": "BOS",
                "game_date": pd.Timestamp("2026-03-25"),
                "rest_days": 4,
                "park_factor": 1.02,
                "stat_id": "outs_recorded",
                "line": 18.5,
                "play": "over",
                "prob": 0.58,
                "ev": 0.11,
                "payout": 1.9,
                "payout_multiplier": 0.9,
                "sport": "MLB",
                "market": "outs_recorded",
                "run_mode": "prediction",
                "lines_status": "present",
            },
            {
                "player": "Aaron Nola",
                "player_id": "nola-1",
                "team": "PHI",
                "opponent": "ATL",
                "game_date": pd.Timestamp("2026-03-25"),
                "rest_days": 5,
                "park_factor": 0.97,
                "stat_id": "hits_allowed",
                "line": 6.5,
                "play": "under",
                "prob": 0.57,
                "ev": 0.13,
                "payout": 2.05,
                "payout_multiplier": 1.05,
                "sport": "MLB",
                "market": "hits_allowed",
                "run_mode": "prediction",
                "lines_status": "present",
            },
        ]
    )


def _config(stat: str) -> PipelineConfig:
    return PipelineConfig(
        config_path=Path("config/mlb.yaml"),
        sport="mlb",
        stat=stat,
        raw={"pipeline": {"sport": "mlb", "stat": stat}},
        section={
            "pitch_data_path": f"tests/{stat}_pitch.csv",
            "model_path": f"tests/{stat}_model.joblib",
            "lines_path": f"tests/{stat}_lines.csv",
        },
    )


def test_build_mlb_live_betslips_writes_snapshots_and_json_slips(
    monkeypatch: object, tmp_path: Path, capsys: object
) -> None:
    from scripts import build_mlb_live_betslips as cli

    args = Namespace(
        config="config/mlb.yaml",
        mode="proof",
        retrain=False,
        output_dir=tmp_path / "betslips",
        snapshot_dir=tmp_path / "lines",
        prefix="mlb_live",
        top_n=10,
        conservative_count=0,
        fullsend_count=1,
        stat_ids=[
            "strikeouts=PickemStat_strikeouts",
            "outs_recorded=PickemStat_outs_recorded",
            "hits_allowed=PickemStat_hits_allowed",
        ],
    )
    monkeypatch.setattr(cli, "parse_args", lambda: args)

    imported: list[str] = []

    def fake_import_ud_mlb_lines(algolia_object_id: str) -> pd.DataFrame:
        imported.append(algolia_object_id)
        if algolia_object_id.endswith("strikeouts"):
            return _live_rows("strikeouts", "Gerrit Cole", "team-home")
        if algolia_object_id.endswith("outs_recorded"):
            return _live_rows("outs_recorded", "Gerrit Cole", "team-home")
        if algolia_object_id.endswith("hits_allowed"):
            return _live_rows("hits_allowed", "Aaron Nola", "team-away")
        return pd.DataFrame()

    monkeypatch.setattr(cli, "import_ud_mlb_lines", fake_import_ud_mlb_lines)

    def fake_load_pipeline_config(
        *_: object,
        stat_override: str | None = None,
        **__: object,
    ) -> PipelineConfig:
        return _config(stat_override or "")

    monkeypatch.setattr(cli, "load_pipeline_config", fake_load_pipeline_config)
    monkeypatch.setattr(
        cli,
        "extract_mlb_live_underdog_config",
        lambda _config: type(
            "LiveUnderdogConfig",
            (),
            {
                "stat_ids": {
                    "strikeouts": "PickemStat_config_strikeouts",
                    "outs_recorded": "PickemStat_config_outs_recorded",
                    "earned_runs": "PickemStat_config_earned_runs",
                    "hits_allowed": "PickemStat_config_hits_allowed",
                    "bb_allowed": "PickemStat_config_bb_allowed",
                }
            },
        )(),
    )

    def fake_run_mlb_pitcher_prop_slate(
        stat_configs: dict[str, PipelineConfig],
        *,
        retrain: bool = False,
        stats: tuple[str, ...] | None = None,
        scorer: object | None = None,
    ) -> MlbPitcherPropSlateResult:
        del retrain, stats, scorer
        assert set(stat_configs) == {"strikeouts", "outs_recorded", "hits_allowed"}
        return MlbPitcherPropSlateResult(
            combined_frame=_slate_frame(),
            completed_stats=("strikeouts", "outs_recorded", "hits_allowed"),
            skipped_stats={},
            failed_stats={},
        )

    monkeypatch.setattr(
        cli,
        "run_mlb_pitcher_prop_slate",
        fake_run_mlb_pitcher_prop_slate,
    )

    returned_summary = cli.main()
    captured = capsys.readouterr()

    assert imported == [
        "PickemStat_strikeouts",
        "PickemStat_outs_recorded",
        "PickemStat_config_earned_runs",
        "PickemStat_hits_allowed",
        "PickemStat_config_bb_allowed",
    ]

    assert (tmp_path / "lines" / "strikeouts_2026-03-25.csv").exists()
    assert (tmp_path / "lines" / "outs_recorded_2026-03-25.csv").exists()
    assert (tmp_path / "lines" / "hits_allowed_2026-03-25.csv").exists()

    conservative_path = tmp_path / "betslips" / "mlb_live_2026-03-25_conservative.json"
    fullsend_path = tmp_path / "betslips" / "mlb_live_2026-03-25_fullsend.json"
    summary_path = tmp_path / "betslips" / "mlb_live_2026-03-25_summary.json"

    assert conservative_path.exists()
    assert fullsend_path.exists()
    assert summary_path.exists()

    conservative = json.loads(conservative_path.read_text())
    fullsend = json.loads(fullsend_path.read_text())
    assert conservative["slips"] == []
    assert len(fullsend["slips"]) == 1
    persisted_summary = json.loads(summary_path.read_text())
    assert returned_summary["completed_stats"] == [
        "strikeouts",
        "outs_recorded",
        "hits_allowed",
    ]
    assert returned_summary["mode"] == "proof"
    assert returned_summary["outcome"] == "passed"
    assert returned_summary["failure_reasons"] == []
    assert returned_summary["warnings"] == []
    assert returned_summary["combined_rows"] == 3
    assert returned_summary["slip_eligible_rows"] == 3
    assert returned_summary["stat_mix"] == {
        "hits_allowed": pytest.approx(1 / 3),
        "outs_recorded": pytest.approx(1 / 3),
        "strikeouts": pytest.approx(1 / 3),
    }
    assert returned_summary["probability_extremes"] == {"max": 0.62, "min": 0.57}
    assert returned_summary["ev_extreme"] == {"max": 0.16}
    assert returned_summary["slip_counts"] == {"conservative": 0, "fullsend": 1}
    assert returned_summary["summary_file"] == str(summary_path)
    assert persisted_summary["summary_file"] == str(summary_path)
    assert persisted_summary == returned_summary
    assert "MLB live shadow run" in captured.out
    assert "FULLSEND SLIPS" in captured.out
    assert "Slip 1" in captured.out
    assert "Gerrit Cole" in captured.out
    assert "strikeouts over 8.5" in captured.out.lower()
    assert "Aaron Nola" in captured.out
    assert "hits allowed under 6.5" in captured.out.lower()


def test_build_mlb_live_betslips_continues_after_stat_level_failures(
    monkeypatch: object, tmp_path: Path
) -> None:
    from scripts import build_mlb_live_betslips as cli

    args = Namespace(
        config="config/mlb.yaml",
        mode="debug",
        retrain=False,
        output_dir=tmp_path / "betslips",
        snapshot_dir=tmp_path / "lines",
        prefix="mlb_live",
        top_n=10,
        conservative_count=0,
        fullsend_count=1,
        stat_ids=[
            "strikeouts=PickemStat_strikeouts",
            "outs_recorded=PickemStat_outs_recorded",
            "hits_allowed=PickemStat_hits_allowed",
            "bb_allowed=PickemStat_bb_allowed",
        ],
    )
    monkeypatch.setattr(cli, "parse_args", lambda: args)

    def fake_import_ud_mlb_lines(algolia_object_id: str) -> pd.DataFrame:
        if algolia_object_id.endswith("strikeouts"):
            return pd.DataFrame()
        if algolia_object_id.endswith("outs_recorded"):
            raise json.JSONDecodeError("bad payload", "{}", 0)
        return _live_rows("hits_allowed", "Aaron Nola", "team-away")

    monkeypatch.setattr(cli, "import_ud_mlb_lines", fake_import_ud_mlb_lines)

    def fake_load_pipeline_config(
        *_: object,
        stat_override: str | None = None,
        **__: object,
    ) -> PipelineConfig:
        if stat_override == "bb_allowed":
            raise ConfigValidationError("Missing stat section 'mlb.bb_allowed'")
        return _config(stat_override or "")

    monkeypatch.setattr(cli, "load_pipeline_config", fake_load_pipeline_config)

    def fake_write_live_pitcher_prop_snapshot(
        live_lines: pd.DataFrame,
        stat: str,
        *,
        output_dir: Path,
        snapshot_date: object,
    ) -> Path:
        del output_dir, snapshot_date
        return tmp_path / f"lines/{stat}_2026-03-25.csv"

    monkeypatch.setattr(
        cli,
        "write_live_pitcher_prop_snapshot",
        fake_write_live_pitcher_prop_snapshot,
    )

    def fake_run_mlb_pitcher_prop_slate(
        stat_configs: dict[str, PipelineConfig],
        *,
        retrain: bool = False,
        stats: tuple[str, ...] | None = None,
        scorer: object | None = None,
    ) -> MlbPitcherPropSlateResult:
        del retrain, stats, scorer
        assert set(stat_configs) == {"hits_allowed"}
        return MlbPitcherPropSlateResult(
            combined_frame=_slate_frame(),
            completed_stats=("hits_allowed",),
            skipped_stats={},
            failed_stats={},
        )

    monkeypatch.setattr(
        cli,
        "run_mlb_pitcher_prop_slate",
        fake_run_mlb_pitcher_prop_slate,
    )

    returned_summary = cli.main()

    summary_path = tmp_path / "betslips" / "mlb_live_2026-03-25_summary.json"
    summary = json.loads(summary_path.read_text())

    assert summary["completed_stats"] == ["hits_allowed"]
    assert summary["mode"] == "debug"
    assert summary["outcome"] == "passed"
    assert summary["failure_reasons"] == []
    assert summary["warnings"] == []
    assert summary["skipped_stats"] == {"strikeouts": "no live lines returned"}
    assert summary["failed_stats"] == {
        "outs_recorded": "bad payload: line 1 column 1 (char 0)",
        "bb_allowed": "Missing stat section 'mlb.bb_allowed'",
    }
    assert summary["combined_rows"] == 3
    assert summary["slip_counts"] == {"conservative": 0, "fullsend": 1}
    assert summary["summary_file"] == str(summary_path)
    assert returned_summary["summary_file"] == str(summary_path)
    assert returned_summary == summary


def test_build_mlb_live_betslips_propagates_unexpected_fetch_errors(
    monkeypatch: object, tmp_path: Path
) -> None:
    from scripts import build_mlb_live_betslips as cli

    args = Namespace(
        config="config/mlb.yaml",
        mode="proof",
        retrain=False,
        output_dir=tmp_path / "betslips",
        snapshot_dir=tmp_path / "lines",
        prefix="mlb_live",
        top_n=10,
        conservative_count=0,
        fullsend_count=1,
        stat_ids=["strikeouts=PickemStat_strikeouts"],
    )
    monkeypatch.setattr(cli, "parse_args", lambda: args)

    def fake_import_ud_mlb_lines(_: str) -> pd.DataFrame:
        raise TypeError("boom")

    monkeypatch.setattr(cli, "import_ud_mlb_lines", fake_import_ud_mlb_lines)

    with pytest.raises(TypeError, match="boom"):
        cli.run_live_shadow_workflow(args)


def test_build_mlb_live_betslips_uses_config_stat_ids_by_default(
    monkeypatch: object, tmp_path: Path
) -> None:
    from scripts import build_mlb_live_betslips as cli

    args = Namespace(
        config="config/mlb.yaml",
        mode="proof",
        retrain=False,
        output_dir=tmp_path / "betslips",
        snapshot_dir=tmp_path / "lines",
        prefix="mlb_live",
        top_n=10,
        conservative_count=0,
        fullsend_count=1,
        stat_ids=[],
    )
    monkeypatch.setattr(cli, "parse_args", lambda: args)

    monkeypatch.setattr(
        cli,
        "load_pipeline_config",
        lambda *_args, stat_override=None, **_kwargs: _config(stat_override or ""),
    )
    monkeypatch.setattr(
        cli,
        "extract_mlb_live_underdog_config",
        lambda _config: type(
            "LiveUnderdogConfig",
            (),
            {
                "stat_ids": {
                    "strikeouts": "PickemStat_config_strikeouts",
                    "outs_recorded": "PickemStat_config_outs_recorded",
                    "earned_runs": "PickemStat_config_earned_runs",
                    "hits_allowed": "PickemStat_config_hits_allowed",
                    "bb_allowed": "PickemStat_config_bb_allowed",
                }
            },
        )(),
    )

    imported: list[str] = []

    def fake_import_ud_mlb_lines(algolia_object_id: str) -> pd.DataFrame:
        imported.append(algolia_object_id)
        stat = algolia_object_id.removeprefix("PickemStat_config_")
        return _live_rows(stat, "Pitcher", "team")

    monkeypatch.setattr(cli, "import_ud_mlb_lines", fake_import_ud_mlb_lines)
    monkeypatch.setattr(
        cli,
        "run_mlb_pitcher_prop_slate",
        lambda stat_configs, **_kwargs: MlbPitcherPropSlateResult(
            combined_frame=_slate_frame(),
            completed_stats=tuple(stat_configs),
            skipped_stats={},
            failed_stats={},
        ),
    )

    summary = cli.run_live_shadow_workflow(args)

    assert imported == [
        "PickemStat_config_strikeouts",
        "PickemStat_config_outs_recorded",
        "PickemStat_config_earned_runs",
        "PickemStat_config_hits_allowed",
        "PickemStat_config_bb_allowed",
    ]
    assert summary["mode"] == "proof"
    assert summary["outcome"] == "passed"
    assert summary["warnings"] == []


def test_build_mlb_live_betslips_cli_overrides_config_stat_ids(
    monkeypatch: object, tmp_path: Path
) -> None:
    from scripts import build_mlb_live_betslips as cli

    args = Namespace(
        config="config/mlb.yaml",
        mode="debug",
        retrain=False,
        output_dir=tmp_path / "betslips",
        snapshot_dir=tmp_path / "lines",
        prefix="mlb_live",
        top_n=10,
        conservative_count=0,
        fullsend_count=1,
        stat_ids=["strikeouts=PickemStat_cli_strikeouts"],
    )

    monkeypatch.setattr(
        cli,
        "load_pipeline_config",
        lambda *_args, stat_override=None, **_kwargs: _config(stat_override or ""),
    )
    monkeypatch.setattr(
        cli,
        "extract_mlb_live_underdog_config",
        lambda _config: type(
            "LiveUnderdogConfig",
            (),
            {
                "stat_ids": {
                    "strikeouts": "PickemStat_config_strikeouts",
                    "outs_recorded": "PickemStat_config_outs_recorded",
                }
            },
        )(),
    )

    imported: list[str] = []

    def fake_import_ud_mlb_lines(algolia_object_id: str) -> pd.DataFrame:
        imported.append(algolia_object_id)
        return _live_rows("strikeouts", "Pitcher", "team")

    monkeypatch.setattr(cli, "import_ud_mlb_lines", fake_import_ud_mlb_lines)
    monkeypatch.setattr(
        cli,
        "run_mlb_pitcher_prop_slate",
        lambda stat_configs, **_kwargs: MlbPitcherPropSlateResult(
            combined_frame=_slate_frame(),
            completed_stats=tuple(stat_configs),
            skipped_stats={},
            failed_stats={},
        ),
    )

    summary = cli.run_live_shadow_workflow(args)

    assert imported == ["PickemStat_cli_strikeouts", "PickemStat_config_outs_recorded"]
    assert summary["mode"] == "debug"
    assert summary["outcome"] == "passed"
    assert summary["warnings"] == []


def test_proof_mode_requires_full_supported_market_stat_id_coverage(
    monkeypatch: object, tmp_path: Path
) -> None:
    from scripts import build_mlb_live_betslips as cli

    args = Namespace(
        config="config/mlb.yaml",
        mode="proof",
        retrain=False,
        output_dir=tmp_path / "betslips",
        snapshot_dir=tmp_path / "lines",
        prefix="mlb_live",
        top_n=10,
        conservative_count=0,
        fullsend_count=1,
        stat_ids=[],
    )

    monkeypatch.setattr(
        cli,
        "load_pipeline_config",
        lambda *_args, stat_override=None, **_kwargs: _config(stat_override or ""),
    )
    monkeypatch.setattr(
        cli,
        "extract_mlb_live_underdog_config",
        lambda _config: type(
            "LiveUnderdogConfig",
            (),
            {"stat_ids": {"strikeouts": "PickemStat_only_strikeouts"}},
        )(),
    )

    with pytest.raises(ValueError, match="Missing required MLB stat-id mapping"):
        cli.run_live_shadow_workflow(args)


def test_debug_mode_allows_subset_stat_id_coverage(
    monkeypatch: object, tmp_path: Path
) -> None:
    from scripts import build_mlb_live_betslips as cli

    args = Namespace(
        config="config/mlb.yaml",
        mode="debug",
        retrain=False,
        output_dir=tmp_path / "betslips",
        snapshot_dir=tmp_path / "lines",
        prefix="mlb_live",
        top_n=10,
        conservative_count=0,
        fullsend_count=1,
        stat_ids=[],
    )

    monkeypatch.setattr(
        cli,
        "load_pipeline_config",
        lambda *_args, stat_override=None, **_kwargs: _config(stat_override or ""),
    )
    monkeypatch.setattr(
        cli,
        "extract_mlb_live_underdog_config",
        lambda _config: type(
            "LiveUnderdogConfig",
            (),
            {"stat_ids": {"strikeouts": "PickemStat_only_strikeouts"}},
        )(),
    )
    monkeypatch.setattr(
        cli,
        "import_ud_mlb_lines",
        lambda _algolia_object_id: _live_rows("strikeouts", "Pitcher", "team"),
    )
    monkeypatch.setattr(
        cli,
        "run_mlb_pitcher_prop_slate",
        lambda stat_configs, **_kwargs: MlbPitcherPropSlateResult(
            combined_frame=_slate_frame(),
            completed_stats=tuple(stat_configs),
            skipped_stats={},
            failed_stats={},
        ),
    )

    summary = cli.run_live_shadow_workflow(args)

    assert summary["mode"] == "debug"
    assert summary["stat_ids"] == {"strikeouts": "PickemStat_only_strikeouts"}


def test_build_mlb_live_betslips_reports_no_play_slate_explicitly(
    monkeypatch: object, tmp_path: Path
) -> None:
    from scripts import build_mlb_live_betslips as cli

    args = Namespace(
        config="config/mlb.yaml",
        mode="proof",
        retrain=False,
        output_dir=tmp_path / "betslips",
        snapshot_dir=tmp_path / "lines",
        prefix="mlb_live",
        top_n=10,
        conservative_count=0,
        fullsend_count=1,
        stat_ids=[],
    )

    monkeypatch.setattr(
        cli,
        "load_pipeline_config",
        lambda *_args, stat_override=None, **_kwargs: _config(stat_override or ""),
    )
    monkeypatch.setattr(
        cli,
        "extract_mlb_live_underdog_config",
        lambda _config: type(
            "LiveUnderdogConfig",
            (),
            {
                "stat_ids": {
                    "strikeouts": "PickemStat_config_strikeouts",
                    "outs_recorded": "PickemStat_config_outs_recorded",
                    "earned_runs": "PickemStat_config_earned_runs",
                    "hits_allowed": "PickemStat_config_hits_allowed",
                    "bb_allowed": "PickemStat_config_bb_allowed",
                }
            },
        )(),
    )
    monkeypatch.setattr(
        cli,
        "import_ud_mlb_lines",
        lambda algolia_object_id: _live_rows(
            algolia_object_id.removeprefix("PickemStat_config_"), "Pitcher", "team"
        ),
    )
    monkeypatch.setattr(
        cli,
        "run_mlb_pitcher_prop_slate",
        lambda stat_configs, **_kwargs: MlbPitcherPropSlateResult(
            combined_frame=pd.DataFrame(
                [
                    {
                        "player": "Pitcher One",
                        "player_id": "p1",
                        "team": "NYY",
                        "opponent": "BOS",
                        "game_date": pd.Timestamp("2026-03-25"),
                        "stat_id": "strikeouts",
                        "line": 7.5,
                        "play": "over",
                        "prob": 0.53,
                        "ev": -0.01,
                        "payout": 1.92,
                        "sport": "MLB",
                        "market": "strikeouts",
                        "run_mode": "prediction",
                        "lines_status": "present",
                    }
                ]
            ),
            completed_stats=tuple(stat_configs),
            skipped_stats={},
            failed_stats={},
        ),
    )

    summary = cli.run_live_shadow_workflow(args)

    assert summary["outcome"] == "no_play_slate"
    assert summary["failure_reasons"] == []
    assert summary["warnings"] == []
    assert summary["combined_rows"] == 1
    assert summary["slip_eligible_rows"] == 0


def test_proof_mode_fails_on_confidence_gate_extremes(
    monkeypatch: object, tmp_path: Path
) -> None:
    from scripts import build_mlb_live_betslips as cli

    args = Namespace(
        config="config/mlb.yaml",
        mode="proof",
        retrain=False,
        output_dir=tmp_path / "betslips",
        snapshot_dir=tmp_path / "lines",
        prefix="mlb_live",
        top_n=10,
        conservative_count=0,
        fullsend_count=1,
        stat_ids=[],
    )

    monkeypatch.setattr(
        cli,
        "load_pipeline_config",
        lambda *_args, stat_override=None, **_kwargs: _config(stat_override or ""),
    )
    monkeypatch.setattr(
        cli,
        "extract_mlb_live_underdog_config",
        lambda _config: type(
            "LiveUnderdogConfig",
            (),
            {
                "stat_ids": {
                    "strikeouts": "PickemStat_config_strikeouts",
                    "outs_recorded": "PickemStat_config_outs_recorded",
                    "earned_runs": "PickemStat_config_earned_runs",
                    "hits_allowed": "PickemStat_config_hits_allowed",
                    "bb_allowed": "PickemStat_config_bb_allowed",
                }
            },
        )(),
    )
    monkeypatch.setattr(
        cli,
        "import_ud_mlb_lines",
        lambda algolia_object_id: _live_rows(
            algolia_object_id.removeprefix("PickemStat_config_"), "Pitcher", "team"
        ),
    )
    monkeypatch.setattr(
        cli,
        "run_mlb_pitcher_prop_slate",
        lambda stat_configs, **_kwargs: MlbPitcherPropSlateResult(
            combined_frame=pd.DataFrame(
                [
                    {
                        "player": "Pitcher One",
                        "player_id": "p1",
                        "team": "NYY",
                        "opponent": "BOS",
                        "game_date": pd.Timestamp("2026-03-25"),
                        "stat_id": "strikeouts",
                        "line": 7.5,
                        "play": "over",
                        "prob": 0.84,
                        "ev": 0.36,
                        "payout": 1.92,
                        "sport": "MLB",
                        "market": "strikeouts",
                        "run_mode": "prediction",
                        "lines_status": "present",
                    },
                    {
                        "player": "Pitcher Two",
                        "player_id": "p2",
                        "team": "BOS",
                        "opponent": "NYY",
                        "game_date": pd.Timestamp("2026-03-25"),
                        "stat_id": "hits_allowed",
                        "line": 6.5,
                        "play": "under",
                        "prob": 0.61,
                        "ev": 0.10,
                        "payout": 1.95,
                        "sport": "MLB",
                        "market": "hits_allowed",
                        "run_mode": "prediction",
                        "lines_status": "present",
                    },
                ]
            ),
            completed_stats=tuple(stat_configs),
            skipped_stats={},
            failed_stats={},
        ),
    )

    summary = cli.run_live_shadow_workflow(args)

    assert summary["outcome"] == "failed"
    assert summary["failure_reasons"] == ["confidence_gate_failed"]
    assert summary["warnings"] == []


def test_proof_mode_allows_hits_allowed_extremes_with_stat_aware_thresholds(
    monkeypatch: object, tmp_path: Path
) -> None:
    from scripts import build_mlb_live_betslips as cli

    args = Namespace(
        config="config/mlb.yaml",
        mode="proof",
        retrain=False,
        output_dir=tmp_path / "betslips",
        snapshot_dir=tmp_path / "lines",
        prefix="mlb_live",
        top_n=10,
        conservative_count=0,
        fullsend_count=1,
        stat_ids=[],
    )

    monkeypatch.setattr(
        cli,
        "load_pipeline_config",
        lambda *_args, stat_override=None, **_kwargs: _config(stat_override or ""),
    )
    monkeypatch.setattr(
        cli,
        "extract_mlb_live_underdog_config",
        lambda _config: type(
            "LiveUnderdogConfig",
            (),
            {
                "stat_ids": {
                    "strikeouts": "PickemStat_config_strikeouts",
                    "outs_recorded": "PickemStat_config_outs_recorded",
                    "earned_runs": "PickemStat_config_earned_runs",
                    "hits_allowed": "PickemStat_config_hits_allowed",
                    "bb_allowed": "PickemStat_config_bb_allowed",
                }
            },
        )(),
    )
    monkeypatch.setattr(
        cli,
        "import_ud_mlb_lines",
        lambda algolia_object_id: _live_rows(
            algolia_object_id.removeprefix("PickemStat_config_"), "Pitcher", "team"
        ),
    )
    monkeypatch.setattr(
        cli,
        "run_mlb_pitcher_prop_slate",
        lambda stat_configs, **_kwargs: MlbPitcherPropSlateResult(
            combined_frame=pd.DataFrame(
                [
                    {
                        "player": "Michael McGreevy",
                        "player_id": "p1",
                        "team": "STL",
                        "opponent": "MIA",
                        "game_date": pd.Timestamp("2026-03-25"),
                        "stat_id": "hits_allowed",
                        "line": 7.5,
                        "play": "under",
                        "prob": 0.919,
                        "ev": 0.81,
                        "payout": 1.97,
                        "sport": "MLB",
                        "market": "hits_allowed",
                        "run_mode": "prediction",
                        "lines_status": "present",
                    },
                    {
                        "player": "Pitcher Two",
                        "player_id": "p2",
                        "team": "BOS",
                        "opponent": "NYY",
                        "game_date": pd.Timestamp("2026-03-25"),
                        "stat_id": "outs_recorded",
                        "line": 17.5,
                        "play": "under",
                        "prob": 0.76,
                        "ev": 0.21,
                        "payout": 1.95,
                        "sport": "MLB",
                        "market": "outs_recorded",
                        "run_mode": "prediction",
                        "lines_status": "present",
                    },
                ]
            ),
            completed_stats=tuple(stat_configs),
            skipped_stats={},
            failed_stats={},
        ),
    )

    summary = cli.run_live_shadow_workflow(args)

    assert summary["outcome"] == "no_play_slate"
    assert summary["failure_reasons"] == []
    assert summary["warnings"] == []


def test_debug_mode_downgrades_gate_failures_to_warnings(
    monkeypatch: object, tmp_path: Path
) -> None:
    from scripts import build_mlb_live_betslips as cli

    args = Namespace(
        config="config/mlb.yaml",
        mode="debug",
        retrain=False,
        output_dir=tmp_path / "betslips",
        snapshot_dir=tmp_path / "lines",
        prefix="mlb_live",
        top_n=10,
        conservative_count=0,
        fullsend_count=1,
        stat_ids=["strikeouts=PickemStat_only_strikeouts"],
    )

    monkeypatch.setattr(
        cli,
        "load_pipeline_config",
        lambda *_args, stat_override=None, **_kwargs: _config(stat_override or ""),
    )
    monkeypatch.setattr(
        cli,
        "extract_mlb_live_underdog_config",
        lambda _config: type("LiveUnderdogConfig", (), {"stat_ids": {}})(),
    )
    monkeypatch.setattr(
        cli,
        "import_ud_mlb_lines",
        lambda _algolia_object_id: _live_rows("strikeouts", "Pitcher", "team"),
    )
    monkeypatch.setattr(
        cli,
        "run_mlb_pitcher_prop_slate",
        lambda stat_configs, **_kwargs: MlbPitcherPropSlateResult(
            combined_frame=pd.DataFrame(
                [
                    {
                        "player": "Pitcher One",
                        "player_id": "p1",
                        "team": "NYY",
                        "opponent": "BOS",
                        "game_date": pd.Timestamp("2026-03-25"),
                        "stat_id": "strikeouts",
                        "line": 7.5,
                        "play": "over",
                        "prob": 0.84,
                        "ev": 0.36,
                        "payout": 1.92,
                        "sport": "MLB",
                        "market": "strikeouts",
                        "run_mode": "prediction",
                        "lines_status": "present",
                    }
                ]
            ),
            completed_stats=tuple(stat_configs),
            skipped_stats={},
            failed_stats={},
        ),
    )

    summary = cli.run_live_shadow_workflow(args)

    assert summary["outcome"] == "no_play_slate"
    assert summary["failure_reasons"] == []
    assert summary["warnings"] == ["stat_mix_gate_failed", "confidence_gate_failed"]
