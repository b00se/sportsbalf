import pandas as pd
from src.nfl.slips import prepare_long_df


def test_prepare_long_df_shapes():
    results = pd.DataFrame(
        [
            {
                "qb_name": "Aaron Rodgers",
                "qb_id": "rodgers",
                "team": "NYJ",
                "opponent": "BUF",
                "attempts_line": 30.5,
                "prob_over": 0.62,
                "prob_under": 0.38,
                "ev_over": 0.12,
                "ev_under": -0.05,
                "over_decimal_price": 1.88,
                "over_payout_multiplier": 1.0,
                "under_decimal_price": 1.84,
                "under_payout_multiplier": 1.0,
                "scheduled_at": "2025-09-25T20:15:00Z",
                "rest_days": 7,
            }
        ]
    )

    long_df = prepare_long_df(results, min_ev=-1.0)

    assert not long_df.empty
    assert set(long_df["play"]) == {"over", "under"}
    assert (long_df["sport"] == "NFL").all()
    assert (long_df["market"] == "pass_attempts").all()
    assert {"player", "player_id", "line", "prob", "ev", "payout_multiplier"}.issubset(
        long_df.columns
    )
    assert set(long_df["player"]) == {"Aaron Rodgers"}
    assert set(long_df["line"]) == {30.5}
    assert set(long_df["prob"]) == {0.62, 0.38}
    assert (long_df["payout_multiplier"] == 1.0).all()
