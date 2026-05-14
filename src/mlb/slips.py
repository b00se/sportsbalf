"""Utilities for building betting slips from MLB pipeline output."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from itertools import combinations

import numpy as np
import pandas as pd

from src.mlb.pitcher_props.descriptors import STAT_DESCRIPTORS

_CANDIDATE_COLUMNS: tuple[str, ...] = (
    "player",
    "player_id",
    "team",
    "opponent",
    "game_date",
    "rest_days",
    "park_factor",
    "stat_id",
    "line",
    "play",
    "prob",
    "ev",
    "payout",
    "payout_multiplier",
    "american_price",
    "sport",
    "market",
)

_SCORER_METADATA_COLUMNS: tuple[str, ...] = (
    "predicted_value",
    "predicted_strikeouts",
    "predicted_outs_recorded",
    "predicted_earned_runs",
    "predicted_hits_allowed",
    "predicted_bb_allowed",
    "model_residual_std",
    "run_mode",
    "lines_status",
    "simulation_mean",
    "simulation_std",
    "simulation_median",
    "model_name",
    "model_strategy",
)

_LEGACY_REQUIRED_COLUMNS: tuple[str, ...] = (
    "player",
    "pitcher_id",
    "pitcher_team",
    "prob_over",
    "prob_under",
    "ev_over",
    "ev_under",
    "over_decimal_price",
    "under_decimal_price",
)


@dataclass
class SlipBuilderConfig:
    top_n: int = 12
    conservative_count: int = 3
    fullsend_count: int = 5
    fullsend_min_size: int = 3
    fullsend_max_size: int = 6
    max_legs_per_player: int = 3
    max_shared_legs: int = 3
    min_leg_ev: float = 0.0
    payout_table: dict[int, float] = None

    def __post_init__(self) -> None:
        if self.payout_table is None:
            self.payout_table = {2: 3, 3: 6, 4: 10, 5: 20, 6: 35, 7: 65, 8: 120}


def _empty_candidate_frame() -> pd.DataFrame:
    """Return an empty generic candidate-leg frame."""

    return pd.DataFrame(columns=_CANDIDATE_COLUMNS)


def _has_candidate_leg_schema(df: pd.DataFrame) -> bool:
    """Return whether a frame already contains candidate-leg columns."""

    return {"play", "prob", "ev", "payout"}.issubset(df.columns)


def _resolve_legacy_stat_descriptor(df: pd.DataFrame) -> tuple[str, str, str]:
    """Infer stat id, line column, and park-factor column for legacy rows."""

    stat_id = "strikeouts"
    if "stat_id" in df.columns:
        stat_values = (
            df["stat_id"].dropna().astype(str).str.strip().str.lower().unique().tolist()
        )
        if len(stat_values) == 1 and stat_values[0] in STAT_DESCRIPTORS:
            stat_id = stat_values[0]

    descriptor = STAT_DESCRIPTORS[stat_id]
    return (
        stat_id,
        descriptor.line_col,
        f"upcoming_{descriptor.park_factor_col}",
    )


def _legacy_stat_subframes(df: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    """Split legacy scorer rows into stat-specific frames."""

    if "stat_id" not in df.columns:
        return [("strikeouts", df.copy())]

    stat_values = (
        df["stat_id"].fillna("strikeouts").astype(str).str.strip().str.lower()
    )
    work = df.copy()
    work["stat_id"] = stat_values

    frames: list[tuple[str, pd.DataFrame]] = []
    for stat_id, group in work.groupby("stat_id", sort=False, dropna=False):
        resolved = stat_id if stat_id in STAT_DESCRIPTORS else "strikeouts"
        frames.append((resolved, group.copy()))
    return frames


def _standardize_candidate_frame(
    results: pd.DataFrame,
    *,
    top_n: int | None = None,
    min_ev: float = 0.0,
) -> pd.DataFrame:
    """Normalize either legacy or candidate-leg rows into a shared schema."""

    if results is None or results.empty:
        return _empty_candidate_frame()

    df = results.copy()
    if _has_candidate_leg_schema(df):
        if "player" not in df.columns and "pitcher_name" in df.columns:
            df["player"] = df["pitcher_name"]
        if "player_id" not in df.columns and "pitcher_id" in df.columns:
            df["player_id"] = df["pitcher_id"]
        if "team" not in df.columns and "pitcher_team" in df.columns:
            df["team"] = df["pitcher_team"]
        if "line" not in df.columns and "k_line" in df.columns:
            df["line"] = df["k_line"]
        if "stat_id" not in df.columns:
            if "market" in df.columns:
                df["stat_id"] = df["market"]
            else:
                df["stat_id"] = "strikeouts"
        if "sport" not in df.columns:
            df["sport"] = "MLB"
        if "market" not in df.columns:
            df["market"] = df["stat_id"]
    else:
        normalized_frames: list[pd.DataFrame] = []
        for stat_id, subset in _legacy_stat_subframes(df):
            _, line_col, park_factor_col = _resolve_legacy_stat_descriptor(subset)
            required_columns = set(_LEGACY_REQUIRED_COLUMNS) | {line_col}
            missing = required_columns - set(subset.columns)
            if missing:
                raise ValueError(
                    f"Results DataFrame missing required columns: {sorted(missing)}"
                )

            over = subset.copy()
            over = over.assign(
                player=subset["player"],
                player_id=subset["pitcher_id"],
                team=subset["pitcher_team"],
                opponent=subset.get("upcoming_opponent"),
                game_date=subset.get("upcoming_game_date"),
                rest_days=subset.get("upcoming_rest_days"),
                park_factor=subset.get(park_factor_col),
                play="over",
                prob=subset["prob_over"],
                ev=subset["ev_over"],
                payout=subset["over_decimal_price"],
                payout_multiplier=subset.get("over_payout_multiplier"),
                american_price=subset.get("over_american_price"),
                line=subset[line_col],
                stat_id=stat_id,
                sport="MLB",
                market=stat_id,
            )

            under = subset.copy()
            under = under.assign(
                player=subset["player"],
                player_id=subset["pitcher_id"],
                team=subset["pitcher_team"],
                opponent=subset.get("upcoming_opponent"),
                game_date=subset.get("upcoming_game_date"),
                rest_days=subset.get("upcoming_rest_days"),
                park_factor=subset.get(park_factor_col),
                play="under",
                prob=subset["prob_under"],
                ev=subset["ev_under"],
                payout=subset["under_decimal_price"],
                payout_multiplier=subset.get("under_payout_multiplier"),
                american_price=subset.get("under_american_price"),
                line=subset[line_col],
                stat_id=stat_id,
                sport="MLB",
                market=stat_id,
            )

            for frame in (over, under):
                if "pitcher_name" not in frame.columns:
                    frame["pitcher_name"] = frame["player"]
                if "pitcher_id" not in frame.columns:
                    frame["pitcher_id"] = frame["player_id"]
                if "pitcher_team" not in frame.columns:
                    frame["pitcher_team"] = frame["team"]
                if "k_line" not in frame.columns:
                    frame["k_line"] = frame["line"]

            normalized_frames.extend([over, under])

        df = pd.concat(normalized_frames, ignore_index=True)

    df = df.loc[df["ev"] > min_ev].copy()
    df = df.sort_values("ev", ascending=False).reset_index(drop=True)

    if top_n is not None:
        df = df.head(top_n).copy()

    for column in _CANDIDATE_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA

    if "game_date" in df.columns:
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")

    base_columns = list(_CANDIDATE_COLUMNS)
    passthrough_columns = [
        column
        for column in df.columns
        if column not in base_columns and column not in _SCORER_METADATA_COLUMNS
    ]
    selected_columns = (
        base_columns + list(_SCORER_METADATA_COLUMNS) + passthrough_columns
    )

    for column in selected_columns:
        if column not in df.columns:
            df[column] = pd.NA

    return df.loc[:, selected_columns].copy()


def prepare_long_df(
    results: pd.DataFrame, *, top_n: int | None = None, min_ev: float = 0.0
) -> pd.DataFrame:
    """Create an over/under long-form DataFrame sorted by leg EV."""

    return _standardize_candidate_frame(results, top_n=top_n, min_ev=min_ev)


def _clean_value(value):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.floating | np.integer):
        if pd.isna(value):
            return None
        return float(value)
    if isinstance(value, float) and pd.isna(value):
        return None
    if pd.isna(value):
        return None
    return value


def _coalesce(*values):
    """Return the first non-null, non-empty value from a sequence."""

    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if pd.isna(value):
            continue
        return value
    return None


def _candidate_identity(leg: dict) -> tuple[str, str, str]:
    """Return the identity tuple used to deduplicate slip combinations."""

    player = leg.get("player") or leg.get("pitcher") or leg.get("pitcher_name")
    stat_id = leg.get("stat_id") or leg.get("market") or leg.get("line")
    play = leg.get("play")
    return (str(player), str(stat_id), str(play))


def _player_count(combo: tuple[object, ...]) -> int:
    """Return the number of distinct players in a candidate combo."""

    players = set()
    for row in combo:
        player = _coalesce(
            getattr(row, "player", None),
            getattr(row, "pitcher_name", None),
        )
        if player is not None:
            players.add(str(player))
    return len(players)


def _team_count(combo: tuple[object, ...]) -> int:
    """Return the number of distinct teams in a candidate combo."""

    teams = set()
    for row in combo:
        team = _coalesce(getattr(row, "team", None), getattr(row, "pitcher_team", None))
        if team is not None:
            teams.add(str(team))
    return len(teams)


def _max_legs_for_any_player(combo: tuple[object, ...]) -> int:
    """Return the largest number of legs assigned to any one player."""

    counts: dict[str, int] = {}
    for row in combo:
        player = _coalesce(
            getattr(row, "player", None),
            getattr(row, "pitcher_name", None),
        )
        if player is None:
            continue
        player_key = str(player)
        counts[player_key] = counts.get(player_key, 0) + 1
    return max(counts.values(), default=0)


def generate_slips(
    df: pd.DataFrame,
    slip_size: int,
    payout_table: dict[int, float],
    *,
    max_legs_per_player: int = 3,
) -> list[dict]:
    """Generate slip combinations of a given size from candidate legs."""

    slips: list[dict] = []
    payout = payout_table.get(slip_size)
    if not payout:
        return slips

    normalized = _standardize_candidate_frame(df, min_ev=float("-inf"))

    for combo in combinations(normalized.itertuples(index=False), slip_size):
        if _player_count(combo) < 2 or _team_count(combo) < 2:
            continue
        if _max_legs_for_any_player(combo) > max_legs_per_player:
            continue

        probs = [float(x.prob) for x in combo]
        if any(p <= 0 for p in probs):
            continue

        p_win = float(np.prod(probs))
        total_ev = p_win * payout - 1

        legs = []
        for x in combo:
            player = _coalesce(
                getattr(x, "player", None),
                getattr(x, "pitcher_name", None),
            )
            team = _coalesce(
                getattr(x, "team", None),
                getattr(x, "pitcher_team", None),
            )
            stat_id = _coalesce(
                getattr(x, "stat_id", None),
                getattr(x, "market", None),
            )
            player_id = _coalesce(
                getattr(x, "player_id", None), getattr(x, "pitcher_id", None)
            )
            market = _coalesce(getattr(x, "market", None), stat_id)
            line_value = _coalesce(getattr(x, "line", None), getattr(x, "k_line", None))
            opponent = _coalesce(
                getattr(x, "opponent", None),
                getattr(x, "upcoming_opponent", None),
            )
            game_date = _coalesce(
                getattr(x, "game_date", None),
                getattr(x, "upcoming_game_date", None),
            )
            rest_days = _coalesce(
                getattr(x, "rest_days", None),
                getattr(x, "upcoming_rest_days", None),
            )
            park_factor = _coalesce(
                getattr(x, "park_factor", None),
                getattr(x, "upcoming_park_factor_K", None),
            )
            leg = {
                "player": _clean_value(player),
                "player_id": _clean_value(player_id),
                "team": _clean_value(team),
                "opponent": _clean_value(opponent),
                "game_date": _clean_value(game_date),
                "rest_days": _clean_value(rest_days),
                "park_factor": _clean_value(park_factor),
                "sport": _clean_value(getattr(x, "sport", None)),
                "market": _clean_value(market),
                "stat_id": _clean_value(stat_id),
                "play": x.play,
                "prob": float(x.prob),
                "ev": float(x.ev),
                "payout": float(x.payout),
                "payout_multiplier": _clean_value(
                    getattr(x, "payout_multiplier", None)
                ),
                "american_price": _clean_value(getattr(x, "american_price", None)),
                "line": _clean_value(line_value),
                "pitcher": _clean_value(player),
                "pitcher_name": _clean_value(player),
                "pitcher_team": _clean_value(team),
                "k_line": _clean_value(line_value),
            }
            extra_fields = {
                key: _clean_value(value)
                for key, value in x._asdict().items()
                if key not in leg
            }
            leg.update(extra_fields)
            legs.append(leg)

        slips.append(
            {
                "legs": legs,
                "avg_ev": total_ev / slip_size,
                "total_ev": total_ev,
                "slip_size": slip_size,
                "p_win": float(p_win),
                "payout": float(payout),
            }
        )

    return slips


def filter_diverse_slips(slips: Iterable[dict], max_shared_legs: int = 3) -> list[dict]:
    """Filter slips to limit shared legs across selections."""

    filtered: list[dict] = []
    seen: list[set] = []

    for slip in slips:
        leg_ids = {_candidate_identity(leg) for leg in slip["legs"]}
        if any(len(leg_ids & s) > max_shared_legs for s in seen):
            continue
        seen.append(leg_ids)
        filtered.append(slip)
    return filtered


def build_slip_sets(
    results: pd.DataFrame,
    *,
    config: SlipBuilderConfig | None = None,
) -> dict:
    """Create conservative and full-send slip collections from pipeline output."""

    cfg = config or SlipBuilderConfig()
    long_df = prepare_long_df(
        results,
        top_n=cfg.top_n,
        min_ev=cfg.min_leg_ev,
    )

    conservative_candidates = generate_slips(
        long_df,
        slip_size=2,
        payout_table=cfg.payout_table,
        max_legs_per_player=cfg.max_legs_per_player,
    )
    conservative_ranked = sorted(
        conservative_candidates, key=lambda x: x["total_ev"], reverse=True
    )
    conservative = filter_diverse_slips(
        conservative_ranked, max_shared_legs=cfg.max_shared_legs
    )[: cfg.conservative_count]
    for slip in conservative:
        slip.setdefault("units", 1)

    fullsend_candidates: list[dict] = []
    for size in range(cfg.fullsend_min_size, cfg.fullsend_max_size + 1):
        if len(long_df) >= size:
            fullsend_candidates.extend(
                generate_slips(
                    long_df,
                    slip_size=size,
                    payout_table=cfg.payout_table,
                    max_legs_per_player=cfg.max_legs_per_player,
                )
            )
    fullsend_ranked = sorted(
        fullsend_candidates, key=lambda x: x["total_ev"], reverse=True
    )
    fullsend = filter_diverse_slips(
        fullsend_ranked, max_shared_legs=cfg.max_shared_legs
    )[: cfg.fullsend_count]
    for slip in fullsend:
        slip.setdefault("units", 1)

    return {
        "conservative": conservative,
        "fullsend": fullsend,
    }
