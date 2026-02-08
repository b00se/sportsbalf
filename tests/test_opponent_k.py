import pandas as pd
from src.mlb.features.opponent_k import add_opponent_k_rate


def test_add_opponent_k_rate_prevents_cross_team_leakage() -> None:
    games = pd.DataFrame(
        {
            "opponent_team": ["ATL", "NYM", "ATL", "NYM", "ATL"],
            "game_date": pd.to_datetime(
                [
                    "2024-04-01",
                    "2024-04-01",
                    "2024-04-08",
                    "2024-04-08",
                    "2024-04-15",
                ]
            ),
            "strikeouts": [2, 10, 4, 1, 8],
            "pitch_count": [10, 20, 10, 10, 20],
        }
    )

    result = add_opponent_k_rate(games)

    atl_rows = result[result["opponent_team"] == "ATL"].reset_index(drop=True)
    nym_rows = result[result["opponent_team"] == "NYM"].reset_index(drop=True)

    # ATL priors: 0.2 then 0.3.
    assert atl_rows.loc[1, "opponent_k_rate"] == 0.2
    assert atl_rows.loc[2, "opponent_k_rate"] == 0.3
    # NYM second game uses NYM-only prior, not ATL carryover.
    assert nym_rows.loc[1, "opponent_k_rate"] == 0.5

    # First game for each team gets the fallback mean over valid prior rates.
    expected_fallback = (0.2 + 0.3 + 0.5) / 3
    assert atl_rows.loc[0, "opponent_k_rate"] == expected_fallback
    assert nym_rows.loc[0, "opponent_k_rate"] == expected_fallback
