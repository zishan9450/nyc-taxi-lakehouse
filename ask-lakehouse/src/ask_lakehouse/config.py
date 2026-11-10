from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_MODELS = {"anthropic": "claude-sonnet-5-5"}


@dataclass(frozen=True)
class Settings:
    db_path: Path = Path("data/gold.duckdb")
    semantic_path: Path = Path("semantic/nyc_taxi.yaml")
    max_rows: int = 1000
    query_timeout_s: float = 10.0
    max_repairs: int = 1
    log_path: Path | None = Path("logs/queries.jsonl")
    provider: str = "anthropic"
    model: str | None = None

    @classmethod
    def from_env(cls) -> Settings:
        e = os.environ.get
        log = e("ASK_LOG_PATH", "logs/queries.jsonl")
        return cls(
            db_path=Path(e("ASK_DB_PATH", "data/gold.duckdb")),
            semantic_path=Path(e("ASK_SEMANTIC_PATH", "semantic/nyc_taxi.yaml")),
            max_rows=int(e("ASK_MAX_ROWS", "1000")),
            query_timeout_s=float(e("ASK_TIMEOUT_S", "10")),
            max_repairs=int(e("ASK_MAX_REPAIRS", "1")),
            log_path=Path(log) if log else None,
            provider=e("LLM_PROVIDER", "anthropic"),
            model=e("LLM_MODEL") or None,
        )
