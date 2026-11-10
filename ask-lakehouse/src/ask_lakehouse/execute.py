"""DuckDB execution over the gold tables, behind a read-only, no-external-access connection."""
from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb

from ask_lakehouse.semantic import Semantic


class QueryError(RuntimeError):
    pass


@dataclass(frozen=True)
class QueryResult:
    columns: list[str]
    rows: list[list]
    truncated: bool
    elapsed_ms: float


def to_jsonable(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def build_database(parquet_dir: Path | str, db_path: Path | str) -> list[str]:
    """Load every <table>.parquet in a directory into a fresh DuckDB file."""
    parquet_dir, db_path = Path(parquet_dir), Path(db_path)
    files = sorted(parquet_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"no .parquet files in {parquet_dir}")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    con = duckdb.connect(str(db_path))
    try:
        for path in files:
            con.execute(f'CREATE TABLE "{path.stem}" AS SELECT * FROM read_parquet(?)', [str(path)])
    finally:
        con.close()
    return [p.stem for p in files]


def schema_problems(db_path: Path | str, semantic: Semantic) -> list[str]:
    """Differences between the semantic layer and the actual database (empty list = in sync)."""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        actual: dict[str, list[str]] = {}
        for table, column in con.execute(
            "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'main'"
        ).fetchall():
            actual.setdefault(table, []).append(column)
    finally:
        con.close()
    problems = []
    for table, cols in semantic.tables.items():
        if table not in actual:
            problems.append(f"table {table} is in the semantic layer but not in the database")
            continue
        problems += [f"{table}.{c} is in the semantic layer but not in the database"
                     for c in cols if c not in actual[table]]
        problems += [f"{table}.{c} is in the database but not in the semantic layer"
                     for c in actual[table] if c not in cols]
    return problems


class Executor:
    def __init__(self, db_path: Path | str, max_rows: int = 1000, timeout_s: float = 10.0):
        self.max_rows = max_rows
        self.timeout_s = timeout_s
        # Hard boundary: nothing the model writes can modify data or read files.
        self._con = duckdb.connect(
            str(db_path),
            read_only=True,
            config={"enable_external_access": False, "memory_limit": "1GB", "threads": 2},
        )

    def run(self, sql: str) -> QueryResult:
        cur = self._con.cursor()
        timer = threading.Timer(self.timeout_s, cur.interrupt)
        timer.start()
        start = time.perf_counter()
        try:
            cur.execute(sql)
            columns = [d[0] for d in cur.description]
            rows = cur.fetchmany(self.max_rows + 1)
        except duckdb.InterruptException as exc:
            raise QueryError(f"query exceeded the {self.timeout_s:g}s time limit") from exc
        except duckdb.Error as exc:
            raise QueryError(str(exc)[:300]) from exc
        finally:
            timer.cancel()
            cur.close()
        truncated = len(rows) > self.max_rows
        rows = [[to_jsonable(v) for v in r] for r in rows[: self.max_rows]]
        return QueryResult(columns, rows, truncated, (time.perf_counter() - start) * 1000)

    def coverage(self) -> dict:
        """Date range and months present, so 'last month' has a meaning."""
        try:
            lo, hi = self._con.execute(
                "SELECT min(pickup_date), max(pickup_date) FROM trips_by_zone_day"
            ).fetchone()
            months = [m for (m,) in self._con.execute(
                "SELECT DISTINCT pickup_month FROM trips_by_zone_day ORDER BY 1").fetchall()]
        except duckdb.Error:
            return {}
        return {"first_date": to_jsonable(lo), "last_date": to_jsonable(hi), "months": months}

    def close(self) -> None:
        self._con.close()
