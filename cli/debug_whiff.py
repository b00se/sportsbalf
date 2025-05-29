from pybaseball import statcast_pitcher
from features.mlb_features import aggregate_pitcher_games

def main():
    gerrit_id = 543037

    df = statcast_pitcher('2023-04-01', '2023-10-01', player_id=gerrit_id)
    games = aggregate_pitcher_games(df)
    print(games.columns)
    print(games.head())

if __name__ == '__main__':
    main()
