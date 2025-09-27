import pandas as pd

from src.mlb.features.mlb_features import aggregate_pitcher_games
from src.mlb.features.pitcher_enrichment import enrich_pitcher_games
from src.mlb.features.rolling import add_rolling_features


def make_player_df():
    return pd.DataFrame(
        {
            "pitcher": [1, 1, 1, 2, 2],
            "game_date": [
                "2023-04-01",
                "2023-04-01",
                "2023-04-08",
                "2023-04-02",
                "2023-04-10",
            ],
            "description": [
                "swinging_strike",
                "called_strike",
                "ball",
                "foul",
                "swinging_strike_blocked",
            ],
            "events": [
                "strikeout",
                "strikeout",
                None,
                None,
                "strikeout",
            ],
            "inning": [1, 1, 4, 2, 2],
            "pitch_type": ["FF", "CU", "FF", "FF", "CH"],
            "home_team": ["NYM", "NYM", "NYM", "BOS", "BOS"],
            "away_team": ["ATL", "ATL", "PHI", "NYY", "NYY"],
            "pitcher_days_since_prev_game": [None, None, 5, None, 3],
            "inning_topbot": ["Top", "Top", "Top", "Bot", "Bot"],
            "pitch_number": [1, 2, 1, 1, 1],
        }
    )


def test_aggregate_pitcher_games_produces_sorted_games():
    df = make_player_df()
    games = aggregate_pitcher_games(df)

    # Expect two games for pitcher 1 and two for pitcher 2
    assert games.shape[0] == 4
    # Rest days default to 5 when missing and preserve provided value
    rest_days = games.set_index(["pitcher", "game_date"])["rest_days"].to_dict()
    assert rest_days[(1, "2023-04-01")] == 5
    assert rest_days[(1, "2023-04-08")] == 5
    assert rest_days[(2, "2023-04-02")] == 5
    assert rest_days[(2, "2023-04-10")] == 3

    # Expanding stats reset per pitcher
    expanding = games.groupby("pitcher")["whiff_rate_expanding"].first()
    assert expanding.loc[1] > 0
    assert expanding.loc[2] == 0


def test_add_rolling_features_no_cross_pitcher_leakage():
    games = aggregate_pitcher_games(make_player_df())
    games = add_rolling_features(games, default_k=9, default_pitch_count=100)

    pitcher1 = games[games["pitcher"] == 1]
    pitcher2 = games[games["pitcher"] == 2]

    # First appearances fall back to defaults
    assert pitcher1.iloc[0]["rolling_K_avg_3"] == 9
    assert pitcher2.iloc[0]["rolling_pitch_count_5"] == 100

    # Later appearances incorporate prior game for same pitcher only
    assert pitcher1.iloc[1]["rolling_K_avg_3"] >= 0
    assert pitcher2.iloc[1]["rolling_K_avg_5"] >= 0


def test_enrich_pitcher_games_merges_context():
    player_df = make_player_df()
    opponent_k_df = pd.DataFrame(
        {
            "game_date": pd.to_datetime([
                "2023-04-01",
                "2023-04-02",
                "2023-04-08",
                "2023-04-10",
            ]),
            "Team": ["ATL", "NYY", "PHI", "NYY"],
            "K_pct_so_far": [0.26, 0.24, 0.27, 0.23],
        }
    )
    park_df = pd.DataFrame({"Team_abbr": ["NYM", "BOS"], "K_park_factor": [1.02, 0.98]})

    enriched = enrich_pitcher_games(player_df, "Test", 1, opponent_k_df, park_df)
    assert {"opponent_k_pct", "park_factor_K"} <= set(enriched.columns)
    assert (enriched["pitcher_name"] == "Test").all()


def test_enrich_pitcher_games_coerces_types(monkeypatch):
    player_df = make_player_df()
    games = aggregate_pitcher_games(player_df)

    stringified = games.copy()
    numeric_columns = [
        "pitch_count",
        "strikeouts",
        "max_inning",
        "num_pitch_types",
        "whiff_rate",
        "csw_pct",
        "whiff_rate_expanding",
        "csw_pct_expanding",
        "rest_days",
    ]
    for column in numeric_columns:
        stringified[column] = stringified[column].astype(str)
    stringified["game_date"] = stringified["game_date"].astype(str)

    monkeypatch.setattr(
        "src.mlb.features.pitcher_enrichment.aggregate_pitcher_games",
        lambda _: stringified.copy(),
    )

    opponent_k_df = pd.DataFrame(
        {
            "game_date": ["2023-04-01", "2023-04-02", "2023-04-08", "2023-04-10"],
            "Team": ["ATL", "NYY", "PHI", "NYY"],
            "K_pct_so_far": ["0.26", "0.24", "0.27", "0.23"],
        }
    )
    park_df = pd.DataFrame(
        {"Team_abbr": ["NYM", "BOS"], "K_park_factor": ["1.02", "0.98"]}
    )

    enriched = enrich_pitcher_games(player_df, "Test", 1, opponent_k_df, park_df)

    assert pd.api.types.is_datetime64_any_dtype(enriched["game_date"])
    for column in numeric_columns + ["opponent_k_pct", "park_factor_K"]:
        assert pd.api.types.is_numeric_dtype(enriched[column])
