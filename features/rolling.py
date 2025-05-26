
def add_rolling_features(games, default_k=5, default_pitch_count=85):
    games = games.sort_values("game_date").copy()

    games['rolling_K_avg_3'] = (
        games['strikeouts'].rolling(3).mean().shift(1).fillna(default_k)
    )
    games['rolling_K_avg_5'] = (
        games['strikeouts'].rolling(5).mean().shift(1).fillna(default_k)
    )
    games['rolling_pitch_count_5'] = (
        games['pitch_count'].rolling(5).mean().shift(1).fillna(default_pitch_count)
    )

    rolling_k_sum = games['strikeouts'].rolling(3).sum().shift(1)
    rolling_pitch_sum = games['pitch_count'].rolling(5).sum().shift(1)
    games['rolling_K_rate'] = (
        (rolling_k_sum / rolling_pitch_sum)
        .replace([float('inf'), -float('inf')], None)
        .fillna(0.055)
    )

    return games