"""SQL guardrails. The model's SQL is untrusted input and is checked in code, not in the prompt.

Checks (any failure raises GuardrailError):
  1. exactly one statement, and it is a read-only query (SELECT / WITH / UNION)
  2. no DDL, DML, COPY, ATTACH, PRAGMA, SET or other commands anywhere in the tree
  3. every table is on the allowlist (or a CTE defined in the query); no table functions or
     file paths such as read_csv(...) or FROM '/etc/passwd'
  4. no file, system or settings functions
  5. every column exists in the semantic layer or is an alias defined in the query

What runs is the SQL re-generated from the parsed tree, not the original text, so the parser that
validated it and the parser that executes it cannot disagree.

This is defence in depth. The hard boundary is the database connection itself: read-only, with
external access disabled (see execute.py). A guardrail bug should not be able to hurt anything.
"""
from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from ask_lakehouse.semantic import Semantic

MAX_SQL_CHARS = 5000

_FORBIDDEN_NODES = tuple(
    getattr(exp, name)
    for name in (
        "Insert", "Update", "Delete", "Create", "Drop", "Alter", "Copy", "Command", "Attach",
        "Detach", "Set", "Pragma", "Merge", "TruncateTable", "Use", "Transaction", "Commit",
        "Rollback", "Export", "Install", "LoadData",
    )
    if hasattr(exp, name)
)
_BLOCKED_FUNCTION_PREFIXES = (
    "read_", "parquet_", "duckdb_", "pragma_", "sniff_", "glob", "getenv", "current_setting",
    "query", "load_", "install_", "scan_", "csv_", "iceberg_", "delta_scan", "http",
)


class GuardrailError(ValueError):
    """kind='unsafe': refuse outright. kind='invalid': the model may try once more."""

    def __init__(self, message: str, kind: str):
        super().__init__(message)
        self.kind = kind


@dataclass(frozen=True)
class Validated:
    sql: str
    tables: frozenset[str]


def _unsafe(msg: str) -> GuardrailError:
    return GuardrailError(msg, "unsafe")


def _invalid(msg: str) -> GuardrailError:
    return GuardrailError(msg, "invalid")


def validate(sql: str, semantic: Semantic) -> Validated:
    if not sql or not sql.strip():
        raise _invalid("empty SQL")
    if len(sql) > MAX_SQL_CHARS:
        raise _unsafe(f"SQL longer than {MAX_SQL_CHARS} characters")

    try:
        statements = [s for s in sqlglot.parse(sql, read="duckdb") if s is not None]
    except SqlglotError as exc:
        raise _invalid(f"SQL could not be parsed: {str(exc)[:200]}") from exc
    if len(statements) != 1:
        raise _unsafe("exactly one SQL statement is allowed")
    root = statements[0]

    for node in root.walk():
        if isinstance(node, _FORBIDDEN_NODES):
            raise _unsafe(f"{type(node).__name__.upper()} statements are not allowed")
    if not isinstance(root, exp.Query):
        raise _unsafe("only read-only SELECT queries are allowed")

    allowed_tables = {t.lower() for t in semantic.table_names}
    allowed_columns = {c.lower() for c in semantic.column_names}
    cte_names = {cte.alias.lower() for cte in root.find_all(exp.CTE) if cte.alias}

    used_tables: set[str] = set()
    for table in root.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier):
            raise _unsafe("table functions and file readers are not allowed")
        if table.catalog or (table.db and table.db.lower() != "main"):
            raise _unsafe(f"schema or catalog access is not allowed: {table.sql(dialect='duckdb')}")
        name = table.name.lower()
        if any(ch in name for ch in "/\\.:"):
            raise _unsafe("file paths and URLs are not allowed as tables")
        if name in cte_names and not table.db:
            continue
        if name not in allowed_tables:
            raise _invalid(f"unknown table '{table.name}'; allowed: {sorted(semantic.table_names)}")
        used_tables.add(name)

    for func in root.find_all(exp.Func):
        name = (func.name if isinstance(func, exp.Anonymous) else func.sql_name()).lower()
        if name.startswith(_BLOCKED_FUNCTION_PREFIXES) or type(func).__name__.startswith("Read"):
            raise _unsafe(f"function '{name}' is not allowed")

    aliases = {a.alias.lower() for a in root.find_all(exp.Alias) if a.alias}
    for cte in root.find_all(exp.CTE):
        table_alias = cte.args.get("alias")
        if table_alias is not None:
            aliases |= {c.name.lower() for c in table_alias.columns}
    for col in root.find_all(exp.Column):
        if isinstance(col.this, exp.Star):
            continue
        name = col.name.lower()
        if name not in allowed_columns and name not in aliases:
            raise _invalid(f"unknown column '{col.name}'")

    return Validated(sql=root.sql(dialect="duckdb", comments=False), tables=frozenset(used_tables))
