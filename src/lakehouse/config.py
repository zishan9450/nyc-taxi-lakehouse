from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    lake_root: Path
    raw_dir: Path
    # Silver is not published if more than this share of a month's rows is rejected.
    max_reject_rate: float = 0.05

    @classmethod
    def from_env(cls) -> Config:
        root = Path(os.environ.get("LAKE_ROOT", "./lake")).resolve()
        return cls(
            lake_root=root,
            raw_dir=root / "raw",
            max_reject_rate=float(os.environ.get("MAX_REJECT_RATE", "0.05")),
        )

    def table(self, name: str) -> str:
        """Path of a Delta table, e.g. cfg.table('silver/trips')."""
        return str(self.lake_root / name)
