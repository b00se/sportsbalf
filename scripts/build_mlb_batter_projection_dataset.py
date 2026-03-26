"""Build Phase 1.5 MLB batter snapshot dataset for projection modeling."""

from __future__ import annotations

import argparse

from src.fantasy.adapters.mlb.datasets import build_player_season_snapshots
from src.fantasy.adapters.mlb.features import prepare_mlb_projection_frame


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", required=True, help="Input batter-game CSV/Parquet path."
    )
    parser.add_argument("--output", required=True, help="Output snapshot CSV path.")
    parser.add_argument("--metric", default="hits", help="Count metric label target.")
    parser.add_argument(
        "--snapshot-anchor-frequency",
        default="weekly",
        choices=("weekly", "daily"),
    )
    parser.add_argument("--snapshot-min-games", type=int, default=5)
    parser.add_argument("--entity-id-col", default="batter")
    parser.add_argument("--date-col", default="game_date")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    frame = prepare_mlb_projection_frame(
        args.input,
        entity_id_col=args.entity_id_col,
        date_col=args.date_col,
    )
    snapshot = build_player_season_snapshots(
        frame,
        entity_id_col=args.entity_id_col,
        date_col=args.date_col,
        target_col=args.metric,
        snapshot_min_games=args.snapshot_min_games,
        snapshot_anchor_frequency=args.snapshot_anchor_frequency,
    )
    snapshot.to_csv(args.output, index=False)


if __name__ == "__main__":
    main()
