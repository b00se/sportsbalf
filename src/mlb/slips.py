"""Utilities for building betting slips from player prop predictions."""
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
            self.payout_table = dict(self.base_multiplier_table)


def prepare_long_df(
    results: pd.DataFrame,
    *,
    top_n: int | None = None,
    min_ev: float = 0.0,
) -> pd.DataFrame:
    """Create a long-form DataFrame for MLB strikeout props."""

    if results is None or results.empty:
        return pd.DataFrame()

    required_cols = {
        "player",
        "pitcher_id",
        "pitcher_team",
        "k_line",
        "upcoming_opponent",
        "upcoming_game_date",
        "upcoming_rest_days",
        "upcoming_park_factor_K",
        "prob_over",
        "prob_under",
        "ev_over",
        "ev_under",
        "over_decimal_price",
        "under_decimal_price",
        "over_payout_multiplier",
        "under_payout_multiplier",
    }
    missing = required_cols - set(results.columns)
    if missing:
        raise ValueError(f"Results DataFrame missing required columns: {sorted(missing)}")

    df = results.copy()

    over = df.assign(
        play="over",
        prob=df["prob_over"],
        ev=df["ev_over"],
        payout=df["over_decimal_price"],
        payout_multiplier=df["over_payout_multiplier"],
        american_price=df.get("over_american_price"),
    )

    under = df.assign(
        play="under",
        prob=df["prob_under"],
        ev=df["ev_under"],
        payout=df["under_decimal_price"],
        payout_multiplier=df["under_payout_multiplier"],
        american_price=df.get("under_american_price"),
    )

    combined = pd.concat([over, under], ignore_index=True)
    combined = combined.loc[combined["ev"] > min_ev].copy()
    combined = combined.sort_values("ev", ascending=False)

    if top_n is not None:
        combined = combined.head(top_n)

    combined.rename(
        columns={
            "player": "player",
            "pitcher_id": "player_id",
            "pitcher_team": "team",
            "upcoming_opponent": "opponent",
            "upcoming_game_date": "game_date",
            "upcoming_rest_days": "rest_days",
            "upcoming_park_factor_K": "park_factor",
            "k_line": "line",
        },
        inplace=True,
    )

    combined["sport"] = "MLB"
    combined["market"] = "strikeouts"

    columns = [
        "player",
        "player_id",
        "team",
        "opponent",
        "game_date",
        "rest_days",
        "park_factor",
        "line",
        "play",
        "prob",
        "ev",
        "payout",
        "payout_multiplier",
        "american_price",
        "sport",
        "market",
    ]

    return combined[columns]


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
    base_multipliers: Dict[int, float] | None = None,
) -> List[dict]:
    """Generate slip combinations of the specified size."""

    if base_multipliers is None:
        base_multipliers = {2: 2.0}

    slips: List[dict] = []
    base_multiplier = base_multipliers.get(slip_size)
    if base_multiplier is None:
        return slips

    for combo in combinations(df.itertuples(index=False), slip_size):
        player_names = {x.player for x in combo}
        if len(player_names) < slip_size:
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
                "player": x.player,
                "player_id": _clean_value(getattr(x, "player_id", None)),
                "team": _clean_value(getattr(x, "team", None)),
                "opponent": _clean_value(getattr(x, "opponent", None)),
                "game_date": _clean_value(getattr(x, "game_date", None)),
                "rest_days": _clean_value(getattr(x, "rest_days", None)),
                "park_factor": _clean_value(getattr(x, "park_factor", None)),
                "play": x.play,
                "prob": float(x.prob),
                "ev": float(x.ev),
                "payout": _clean_value(getattr(x, "payout", None)),
                "payout_multiplier": float(x.payout_multiplier),
                "line": _clean_value(getattr(x, "line", None)),
                "american_price": _clean_value(getattr(x, "american_price", None)),
                "sport": _clean_value(getattr(x, "sport", None)),
                "market": _clean_value(getattr(x, "market", None)),
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
                "sports": sorted({leg.get("sport") for leg in legs if leg.get("sport")}),
                "markets": sorted({leg.get("market") for leg in legs if leg.get("market")}),
            }
        )

    return slips


def filter_diverse_slips(slips: Iterable[dict], max_shared_legs: int = 3) -> List[dict]:
    """Filter slips to limit shared legs across selections."""

    filtered: List[dict] = []
    seen: List[set] = []

    for slip in slips:
        leg_ids = {
            (leg["player"], leg["play"]) for leg in slip["legs"]
        }
        if any(len(leg_ids & s) > max_shared_legs for s in seen):
            continue
        seen.append(leg_ids)
        filtered.append(slip)
    return filtered


def build_slip_sets(
    results: pd.DataFrame | None = None,
    *,
    config: SlipBuilderConfig | None = None,
    long_df: pd.DataFrame | None = None,
) -> dict:
    """Create conservative and full-send slip collections."""

    cfg = config or SlipBuilderConfig()

    if long_df is None:
        if results is None:
            raise ValueError("Either results or long_df must be provided")
        base_df = prepare_long_df(results, top_n=None, min_ev=cfg.min_leg_ev)
    else:
        base_df = long_df.copy()
        if cfg.min_leg_ev:
            base_df = base_df[base_df["ev"] > cfg.min_leg_ev]

    if base_df.empty:
        return {"conservative": [], "fullsend": []}

    base_df = base_df.sort_values("ev", ascending=False)
    if cfg.top_n:
        base_df = base_df.head(cfg.top_n)

    conservative_candidates = generate_slips(
        base_df,
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
        if len(base_df) >= size:
            fullsend_candidates.extend(
                generate_slips(
                    base_df,
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
