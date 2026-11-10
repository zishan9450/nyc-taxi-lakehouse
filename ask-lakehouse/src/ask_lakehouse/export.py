"""Export the lakehouse pipeline's gold Delta tables to parquet (no Spark or JVM needed)."""
from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

GOLD_TABLES = ["trips_by_zone_day", "revenue_by_hour", "tip_rate_by_borough", "dim_zone"]


def export_gold(lake_root: Path | str, out_dir: Path | str) -> list[Path]:
    from deltalake import DeltaTable  # pip install -e ".[export]"

    lake_root, out_dir = Path(lake_root), Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for name in GOLD_TABLES:
        table_path = lake_root / "gold" / name
        if not table_path.exists():
            raise FileNotFoundError(f"{table_path} not found; run the lakehouse pipeline first")
        arrow = DeltaTable(str(table_path)).to_pyarrow_table()
        target = out_dir / f"{name}.parquet"
        pq.write_table(arrow, target)
        written.append(target)
    return written
