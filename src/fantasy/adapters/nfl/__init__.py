"""NFL fantasy provider contracts."""

from src.fantasy.adapters.nfl.contest import (
    ContestProvenance,
    FieldConfig,
    NflContestConfig,
    Payout,
    PayoutConfig,
    ScoringConfig,
    UnknownScoringError,
    load_nfl_contest_config,
)
from src.fantasy.adapters.nfl.exposure import (
    EXPOSURE_COLUMNS,
    ExposureExport,
    ExposurePick,
    ExposureSchemaError,
    UserEntry,
    parse_exposure_csv,
    reconstruct_user_entries,
)
from src.fantasy.adapters.nfl.provenance import (
    SnapshotInput,
    SnapshotManifest,
    build_snapshot_manifest,
    write_snapshot_manifest,
)
from src.fantasy.adapters.nfl.projection_sources import (
    ProjectionSource,
    SourceAuditError,
    SourceAuditResult,
    TournamentEntry,
    audit_projection_source,
    run_projection_source_tournament,
)
from src.fantasy.adapters.nfl.rankings import (
    RANKINGS_COLUMNS,
    RankingsSchemaError,
    RankingsTable,
    export_rankings_csv,
    normalize_rankings,
    parse_rankings_csv,
    reorder_rankings,
)

__all__ = [
    "ContestProvenance",
    "FieldConfig",
    "NflContestConfig",
    "Payout",
    "PayoutConfig",
    "ScoringConfig",
    "UnknownScoringError",
    "load_nfl_contest_config",
    "EXPOSURE_COLUMNS",
    "ExposureExport",
    "ExposurePick",
    "ExposureSchemaError",
    "UserEntry",
    "parse_exposure_csv",
    "reconstruct_user_entries",
    "RANKINGS_COLUMNS",
    "RankingsSchemaError",
    "RankingsTable",
    "export_rankings_csv",
    "normalize_rankings",
    "parse_rankings_csv",
    "reorder_rankings",
    "SnapshotInput",
    "SnapshotManifest",
    "build_snapshot_manifest",
    "write_snapshot_manifest",
    "ProjectionSource",
    "SourceAuditError",
    "SourceAuditResult",
    "TournamentEntry",
    "audit_projection_source",
    "run_projection_source_tournament",
]
