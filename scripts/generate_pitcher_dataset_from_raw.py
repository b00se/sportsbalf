import os
import sys
from pathlib import Path

import pandas as pd
from pybaseball import playerid_reverse_lookup

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

RAW_PATH = "data/raw/statcast"
OUTPUT_PATH = "data/processed"


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


def load_pitcher_ids(csv_path: str, raw_df: pd.DataFrame) -> list[tuple[str, int]]:
    """Load starter names and MLBAM ids with a Statcast-based fallback."""
    df = pd.read_csv(csv_path)
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


def generate_dataset_from_raw(season):
    from src.mlb.features.dynamic_opponent import compute_opponent_k_pct_dynamic
    from src.mlb.features.feature_store import build_historical_live_features
    from src.mlb.features.park_factors import compute_k_park_factors
    from src.mlb.features.pitcher_enrichment import enrich_pitcher_games

    raw_file = os.path.join(RAW_PATH, f"statcast_raw_{season}.parquet")
    starter_csv = f"data/raw/top_starters_{season}.csv"
    output_file = os.path.join(OUTPUT_PATH, f"pitcher_game_data_{season}.parquet")

    print(f"📂 Loading raw statcast data from {raw_file}")
    df = pd.read_parquet(raw_file)
    df["game_date"] = pd.to_datetime(df["game_date"])

    print(f"📋 Loading starter list from {starter_csv}")
    pitchers = load_pitcher_ids(starter_csv, df)

    print("📆 Computing opponent K% from raw data...")
    opponent_k_df = compute_opponent_k_pct_dynamic(
        start_date=df["game_date"].min().strftime("%Y-%m-%d"),
        end_date=df["game_date"].max().strftime("%Y-%m-%d"),
        source_df=df,
    )
    opponent_k_df["game_date"] = pd.to_datetime(opponent_k_df["game_date"])

    print("🏟️ Computing park factors from raw data...")
    park_df = compute_k_park_factors(
        start_date=df["game_date"].min().strftime("%Y-%m-%d"),
        end_date=df["game_date"].max().strftime("%Y-%m-%d"),
        source_df=df,
    )

    print(f"🧠 Processing {len(pitchers)} pitchers...")
    all_games = []
    for name, mlbam_id in pitchers:
        player_df = df[df["pitcher"] == mlbam_id]
        if player_df.empty:
            print(f"⛔ No data for {name} ({mlbam_id})")
            continue

        enriched = enrich_pitcher_games(
            player_df, name, mlbam_id, opponent_k_df, park_df
        )
        if enriched is not None:
            all_games.append(enriched)

    if not all_games:
        print("❌ No pitcher games generated.")
        return

    full_df = pd.concat(all_games, ignore_index=True)
    full_df = build_historical_live_features(full_df)
    full_df.to_parquet(output_file, index=False)
    print(f"✅ Saved {len(full_df)} rows to {output_file}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    args = parser.parse_args()
    generate_dataset_from_raw(args.season)
