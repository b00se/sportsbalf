"""Utilities for building betting slips from MLB pipeline output."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from typing import Dict, Iterable, List

import numpy as np
import pandas as pd


@dataclass
class SlipBuilderConfig:
    top_n: int = 12
    conservative_count: int = 3
    fullsend_count: int = 5
    fullsend_min_size: int = 3
    fullsend_max_size: int = 6
    max_shared_legs: int = 3
    min_leg_ev: float = 0.0
    base_multiplier_table: Dict[int, float] | None = None
    payout_table: Dict[int, float] | None = None  # Deprecated alias for base_multiplier_table

    def __post_init__(self) -> None:
        if self.base_multiplier_table is None:
            if self.payout_table is not None:
                self.base_multiplier_table = dict(self.payout_table)
            else:
                # Default to insurance-style 2-pick slips and power payouts for larger entries.
                self.base_multiplier_table = {
                    2: 2.0,
                    3: 6.0,
                    4: 10.0,
                    5: 20.0,
                    6: 35.0,
                    7: 65.0,
                    8: 120.0,
                }
        elif self.payout_table is None:
            # Keep legacy field in sync when callers still expect it.
            self.payout_table = dict(self.base_multiplier_table)


def prepare_long_df(results: pd.DataFrame, *, top_n: int | None = None, min_ev: float = 0.0) -> pd.DataFrame:
    """Create an over/under long-form DataFrame sorted by leg EV."""

    df = results.copy()
    required_cols = {
        "player",
        "pitcher_id",
        "pitcher_team",
        "k_line",
        "prob_over",
        "prob_under",
        "ev_over",
        "ev_under",
        "over_decimal_price",
        "under_decimal_price",
        "over_payout_multiplier",
        "under_payout_multiplier",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Results DataFrame missing required columns: {sorted(missing)}")

    base_cols = [
        "player",
        "pitcher_id",
        "pitcher_team",
        "k_line",
        "upcoming_opponent",
        "upcoming_game_date",
        "upcoming_rest_days",
        "upcoming_park_factor_K",
    ]

    over = df.copy()
    over = over.assign(
        play="over",
        prob=df["prob_over"],
        ev=df["ev_over"],
        payout=df["over_decimal_price"],
        payout_multiplier=df["over_payout_multiplier"],
    )

    under = df.copy()
    under = under.assign(
        play="under",
        prob=df["prob_under"],
        ev=df["ev_under"],
        payout=df["under_decimal_price"],
        payout_multiplier=df["under_payout_multiplier"],
    )

    combined = pd.concat([over, under], ignore_index=True)
    combined = combined.loc[combined["ev"] > min_ev].copy()
    combined = combined.sort_values("ev", ascending=False)
    combined = combined.reset_index(drop=True)

    columns = base_cols + ["play", "prob", "ev", "payout", "payout_multiplier"]
    combined = combined[columns]
    combined.rename(columns={"player": "pitcher_name"}, inplace=True)

    if top_n is not None:
        combined = combined.head(top_n)

    return combined


def _clean_value(value):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (np.floating, np.integer)):
        if pd.isna(value):
            return None
        return float(value)
    if isinstance(value, float) and pd.isna(value):
        return None
    if pd.isna(value):
        return None
    return value


def generate_slips(
    df: pd.DataFrame,
    slip_size: int,
    *,
    base_multipliers: Dict[int, float],
) -> List[dict]:
    """Generate slip combinations of a given size from candidate legs."""

    slips: List[dict] = []

    base_multiplier = base_multipliers.get(slip_size)
    if not base_multiplier:
        return slips

    for combo in combinations(df.itertuples(index=False), slip_size):
        pitcher_names = {x.pitcher_name for x in combo}
        if len(pitcher_names) < slip_size:
            continue

        probs = [float(x.prob) for x in combo]
        if any(p <= 0 for p in probs):
            continue

        p_win = float(np.prod(probs))
        leg_multipliers = []
        invalid = False
        for x in combo:
            try:
                multiplier = float(x.payout_multiplier)
            except (TypeError, ValueError):
                invalid = True
                break
            if not np.isfinite(multiplier) or multiplier <= 0:
                invalid = True
                break
            leg_multipliers.append(multiplier)
        if invalid or not leg_multipliers:
            continue

        total_multiplier = float(np.prod(leg_multipliers))
        payout = base_multiplier * total_multiplier
        total_ev = p_win * payout - 1

        legs = []
        for x in combo:
            leg = {
                "pitcher": x.pitcher_name,
                "pitcher_team": _clean_value(x.pitcher_team),
                "opponent": _clean_value(x.upcoming_opponent),
                "game_date": _clean_value(x.upcoming_game_date),
                "rest_days": _clean_value(x.upcoming_rest_days),
                "park_factor_K": _clean_value(x.upcoming_park_factor_K),
                "play": x.play,
                "prob": float(x.prob),
                "ev": float(x.ev),
                "payout": float(x.payout),
                "payout_multiplier": float(x.payout_multiplier),
                "k_line": float(x.k_line),
            }
            legs.append(leg)

        slips.append(
            {
                "legs": legs,
                "avg_ev": total_ev / slip_size,
                "total_ev": total_ev,
                "slip_size": slip_size,
                "p_win": float(p_win),
                "payout": payout,
            }
        )

    return slips


def filter_diverse_slips(slips: Iterable[dict], max_shared_legs: int = 3) -> List[dict]:
    """Filter slips to limit shared legs across selections."""

    filtered: List[dict] = []
    seen: List[set] = []

    for slip in slips:
        leg_ids = {
            (leg["pitcher"], leg["play"]) for leg in slip["legs"]
        }
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
        base_multipliers=cfg.base_multiplier_table,
    )
    conservative_ranked = sorted(
        conservative_candidates, key=lambda x: x["total_ev"], reverse=True
    )
    conservative = filter_diverse_slips(
        conservative_ranked, max_shared_legs=cfg.max_shared_legs
    )[: cfg.conservative_count]
    for slip in conservative:
        slip.setdefault("units", 1)

    fullsend_candidates: List[dict] = []
    for size in range(cfg.fullsend_min_size, cfg.fullsend_max_size + 1):
        if len(long_df) >= size:
            fullsend_candidates.extend(
                generate_slips(
                    long_df,
                    slip_size=size,
                    base_multipliers=cfg.base_multiplier_table,
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
