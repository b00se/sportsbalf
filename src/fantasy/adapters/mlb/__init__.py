"""MLB fantasy projection adapters."""

from src.fantasy.adapters.mlb.backtest import (
    WalkForwardFold,
    aggregate_metric_scores,
    generate_walk_forward_folds,
)
from src.fantasy.adapters.mlb.datasets import build_player_season_snapshots
from src.fantasy.adapters.mlb.feature_engineering import add_phase15_rolling_features
from src.fantasy.adapters.mlb.priors import (
    attach_priors_to_snapshots,
    load_cached_priors,
)
from src.fantasy.adapters.mlb.projection_adapter import (
    MlbProjectionAdapterConfig,
    MlbSeasonProjectionAdapter,
)
from src.fantasy.adapters.mlb.registration import (
    PHASE1_MLB_METRICS,
    register_mlb_projection_adapters,
)

__all__ = [
    "MlbProjectionAdapterConfig",
    "MlbSeasonProjectionAdapter",
    "PHASE1_MLB_METRICS",
    "WalkForwardFold",
    "add_phase15_rolling_features",
    "aggregate_metric_scores",
    "attach_priors_to_snapshots",
    "build_player_season_snapshots",
    "generate_walk_forward_folds",
    "load_cached_priors",
    "register_mlb_projection_adapters",
]
