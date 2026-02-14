"""Registry wiring for MLB Phase 1 fantasy projection adapters."""

from __future__ import annotations

from src.fantasy.adapters.mlb.projection_adapter import (
    MlbProjectionAdapterConfig,
    MlbSeasonProjectionAdapter,
)
from src.fantasy.core.registry import register_projection_adapter

PHASE1_MLB_METRICS: tuple[str, ...] = (
    "plate_appearances",
    "hits",
    "total_bases",
    "walks",
    "strikeouts",
    "pa_vs_lhp",
    "pa_vs_rhp",
    "hard_hit_events",
    "hit_rate",
    "walk_rate",
    "strikeout_rate",
    "slugging_proxy",
)


def register_mlb_projection_adapters(**adapter_kwargs: object) -> None:
    """Register all Phase 1 MLB `(sport, metric, horizon)` projection adapters."""

    config = MlbProjectionAdapterConfig.from_mapping(dict(adapter_kwargs))
    for metric_id in PHASE1_MLB_METRICS:
        register_projection_adapter(
            "mlb",
            metric_id,
            "season",
            MlbSeasonProjectionAdapter(metric_id=metric_id, adapter_config=config),
        )
