"""MLB-specific feature engineering utilities."""

from .enrichments import add_opponent_k_pct, add_park_factor
from .feature_store import (
    LIVE_CONTEXT_FEATURE_COLUMNS,
    build_historical_live_features,
    coverage_metrics,
    ensure_live_feature_defaults,
    merge_live_feature_frame,
)
from .live_context import LiveContextService
from .mlb_features import aggregate_pitcher_games
from .opponent_k import add_opponent_k_rate
from .rolling import add_rolling_features

__all__ = [
    "aggregate_pitcher_games",
    "add_rolling_features",
    "add_park_factor",
    "add_opponent_k_pct",
    "add_opponent_k_rate",
    "build_historical_live_features",
    "ensure_live_feature_defaults",
    "merge_live_feature_frame",
    "coverage_metrics",
    "LIVE_CONTEXT_FEATURE_COLUMNS",
    "LiveContextService",
]
