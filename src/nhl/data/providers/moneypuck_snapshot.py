"""MoneyPuck curated snapshot provider for NHL PR#9."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from src.nhl.data.moneypuck_ingest import CANONICAL_SKATER_GAME_COLUMNS
from src.nhl.data.providers.base import LoadResult
from src.utils.io import read_csv


@dataclass(slots=True)
class MoneyPuckSnapshotProvider:
    """Provider that serves curated skater game rows from local parquet cache."""

    curated_cache_path: str

    @property
    def name(self) -> str:
        """Return provider identifier."""

        return "moneypuck_snapshot"

    def load_skater_games(self, seasons: Sequence[int]) -> LoadResult:
        """Load curated skater-game rows for requested seasons.

        Args:
            seasons: Target seasons for runtime inference context.

        Returns:
            Provider load result with canonical rows and metadata.

        Raises:
            RuntimeError: If cache is missing, invalid, or empty for seasons.
        """

        path = Path(self.curated_cache_path)
        if not path.exists():
            raise RuntimeError(
                "NHL provider curated cache path does not exist "
                f"for provider '{self.name}': {path}"
            )

        frame = read_csv(str(path))
        missing = [
            column
            for column in CANONICAL_SKATER_GAME_COLUMNS
            if column not in frame.columns
        ]
        if missing:
            raise RuntimeError(
                "NHL curated cache is missing canonical columns: "
                f"{', '.join(missing)}"
            )

        filtered = frame.loc[frame["season"].astype(int).isin(seasons)].copy()
        if filtered.empty:
            raise RuntimeError(
                "NHL provider returned no rows for requested seasons "
                f"{list(seasons)} from '{path}'."
            )

        metadata = {
            "provider": self.name,
            "source_path": str(path),
            "seasons": list(seasons),
            "rows": int(len(filtered)),
        }
        return LoadResult(
            data=filtered.loc[:, list(CANONICAL_SKATER_GAME_COLUMNS)].reset_index(
                drop=True
            ),
            metadata=metadata,
        )
