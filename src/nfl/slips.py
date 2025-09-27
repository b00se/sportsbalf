"""Slip preparation helpers for NFL pass attempt props."""
from __future__ import annotations

import pandas as pd


def prepare_long_df(
    results: pd.DataFrame,
    *,
    top_n: int | None = None,
    min_ev: float = 0.0,
) -> pd.DataFrame:
    """Return long-form higher/lower rows ready for slip generation."""

    if results is None or results.empty:
        return pd.DataFrame()

    required = {
        "qb_name",
        "qb_id",
        "team",
        "opponent",
        "ud_line",
        "prob_higher",
        "prob_lower",
        "ev_higher",
        "ev_lower",
        "over_decimal_price",
        "over_payout_multiplier",
        "under_decimal_price",
        "under_payout_multiplier",
    }
    missing = required - set(results.columns)
    if missing:
        raise ValueError(
            f"NFL predictions are missing required columns: {sorted(missing)}"
        )

    df = results.copy()

    higher = df.assign(
        play="higher",
        prob=df["prob_higher"],
        ev=df["ev_higher"],
        payout=df["over_decimal_price"],
        payout_multiplier=df["over_payout_multiplier"],
        american_price=df.get("over_american_price"),
    )

    lower = df.assign(
        play="lower",
        prob=df["prob_lower"],
        ev=df["ev_lower"],
        payout=df["under_decimal_price"],
        payout_multiplier=df["under_payout_multiplier"],
        american_price=df.get("under_american_price"),
    )

    combined = pd.concat([higher, lower], ignore_index=True)
    combined = combined.loc[combined["ev"] > min_ev].copy()
    combined = combined.sort_values("ev", ascending=False)

    if top_n is not None:
        combined = combined.head(top_n)

    combined["game_date"] = pd.to_datetime(combined.get("scheduled_at"))
    combined.rename(
        columns={
            "qb_name": "player",
            "qb_id": "player_id",
            "ud_line": "line",
            "rest_days": "rest_days",
        },
        inplace=True,
    )

    combined["sport"] = "NFL"
    combined["market"] = "pass_attempts"
    combined["park_factor"] = pd.NA

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

    for column in columns:
        if column not in combined.columns:
            combined[column] = pd.NA

    return combined[columns]
