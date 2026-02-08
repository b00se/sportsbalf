import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from pybaseball import playerid_reverse_lookup

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

RAW_PATH = "data/raw/statcast"
PROCESSED_PATH = "data/processed"


def get_latest_game_data(processed_path):
    if not os.path.exists(processed_path):
        return None
    df = pd.read_parquet(processed_path)
    return pd.to_datetime(df["game_date"]).max()


def _unique_pitcher_id(
    starter_name: str, raw_name_map: dict[str, set[int]]
) -> int | None:
    """Return a unique pitcher id from raw names when lookup data is missing."""
    from src.utils.names import resolve_unique_name_match

    name_to_unique_id = {
        name: next(iter(ids)) for name, ids in raw_name_map.items() if len(ids) == 1
    }
    return resolve_unique_name_match(starter_name, name_to_unique_id)


def _build_raw_name_map(raw_df: pd.DataFrame) -> dict[str, set[int]]:
    """Create normalized name -> pitcher-id mapping from raw Statcast rows."""
    from src.utils.names import from_last_first, normalize_person_name

    if not {"player_name", "pitcher"}.issubset(raw_df.columns):
        return {}
    mapping: dict[str, set[int]] = {}
    raw_names = raw_df[["player_name", "pitcher"]].dropna().drop_duplicates()
    for row in raw_names.itertuples(index=False):
        display_name = from_last_first(row.player_name)
        norm = normalize_person_name(display_name)
        if not norm:
            continue
        mapping.setdefault(norm, set()).add(int(row.pitcher))
    return mapping


def _extract_mlbam_lookup(lookup_df: pd.DataFrame) -> dict[int, int]:
    """Convert pybaseball reverse lookup output into Fangraphs->MLBAM mapping."""
    if lookup_df.empty:
        return {}
    resolved: dict[int, int] = {}
    for row in lookup_df.itertuples(index=False):
        try:
            fg_id = int(float(row.key_fangraphs))
            mlbam_id = int(float(row.key_mlbam))
        except (TypeError, ValueError):
            continue
        resolved[fg_id] = mlbam_id
    return resolved


def load_pitcher_ids(starter_csv: str, raw_df: pd.DataFrame) -> list[tuple[str, int]]:
    """Load starter names and MLBAM ids with a Statcast-based fallback."""
    df = pd.read_csv(starter_csv)
    lookup = playerid_reverse_lookup(df["IDfg"].tolist(), key_type="fangraphs")
    lookup_map = _extract_mlbam_lookup(lookup)
    raw_name_map = _build_raw_name_map(raw_df)

    resolved: list[tuple[str, int]] = []
    unresolved: list[str] = []
    for row in df.itertuples(index=False):
        try:
            fg_id = int(float(row.IDfg))
        except (TypeError, ValueError):
            unresolved.append(str(row.Name))
            continue

        mlbam_id = lookup_map.get(fg_id)
        if mlbam_id is None:
            mlbam_id = _unique_pitcher_id(str(row.Name), raw_name_map)
        if mlbam_id is None:
            unresolved.append(str(row.Name))
            continue
        resolved.append((str(row.Name), int(mlbam_id)))

    if unresolved:
        sample = ", ".join(unresolved[:10])
        print(
            f"⚠️ Could not resolve MLBAM ids for {len(unresolved)} starters. "
            f"Examples: {sample}"
        )
    return resolved


def update_pitcher_dataset(season):
    from src.mlb.features.dynamic_opponent import compute_opponent_k_pct_dynamic
    from src.mlb.features.enrichments import add_park_factor
    from src.mlb.features.feature_store import build_historical_live_features
    from src.mlb.features.mlb_features import aggregate_pitcher_games
    from src.mlb.features.park_factors import compute_k_park_factors
    from src.mlb.features.rolling import add_rolling_features

    raw_file = os.path.join(RAW_PATH, f"statcast_raw_{season}.parquet")
    processed_file = os.path.join(PROCESSED_PATH, f"pitcher_game_data_{season}.parquet")
    starter_csv = f"data/raw/top_starters_{season}.csv"

    if not os.path.exists(raw_file):
        print(
            f"❌ No statcast_raw_{season}.parquet found. "
            "Run fetch_statcast_raw.py first."
        )
        return

    df = pd.read_parquet(raw_file)
    df["game_date"] = pd.to_datetime(df["game_date"])

    latest_date = get_latest_game_data(processed_file)
    if latest_date is None:
        print("⚠️ No existing dataset — run full generator first.")
        return

    start_date = latest_date.strftime("%Y-%m-%d")
    end_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"📆 Updating pitcher dataset from {start_date} to {end_date}")
    new_df = df[(df["game_date"] >= start_date) & (df["game_date"] <= end_date)]

    if new_df.empty:
        print("✅ No new games found — nothing to update.")
        return

    pitchers = load_pitcher_ids(starter_csv, df)
    mlbam_ids = [pid for _, pid in pitchers]
    new_df = new_df[new_df["pitcher"].isin(mlbam_ids)]

    season_start = df["game_date"].min().strftime("%Y-%m-%d")

    opponent_k_df = compute_opponent_k_pct_dynamic(season_start, end_date, source_df=df)
    park_df = compute_k_park_factors(season_start, end_date, source_df=df)

    all_games = []
    for name, mlbam_id in pitchers:
        player_df = new_df[new_df["pitcher"] == mlbam_id]
        if player_df.empty:
            continue

        if "description" not in player_df.columns:
            print(f"⚠️ No 'description' column found for {name}")
            continue

        if player_df["description"].notna().sum() == 0:
            print(f"⚠️ All descriptions missing for {name} — {mlbam_id}")
            continue

        games = aggregate_pitcher_games(player_df)
        games["game_date"] = pd.to_datetime(player_df["game_date"])

        games = (
            games.merge(
                opponent_k_df,
                left_on=["game_date", "opponent_team"],
                right_on=["game_date", "Team"],
                how="left",
            )
            .rename(columns={"K_pct_so_far": "opponent_k_pct"})
            .drop(columns=["Team"])
        )

        games = add_park_factor(games, park_df)
        games = add_rolling_features(games)
        games["pitcher_name"] = name
        games["pitcher_id"] = mlbam_id
        all_games.append(games)

    if not all_games:
        print("⚠️ No new pitcher games added.")
        return

    new_games = pd.concat(all_games, ignore_index=True)
    full_df = pd.read_parquet(processed_file)
    updated_df = pd.concat([full_df, new_games], ignore_index=True).drop_duplicates()
    updated_df = build_historical_live_features(updated_df)
    updated_df.to_parquet(processed_file, index=False)
    print(f"✅ Appended {len(new_games)} new rows. Total rows: {len(updated_df)}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    args = parser.parse_args()
    update_pitcher_dataset(args.season)
