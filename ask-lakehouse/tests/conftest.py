from pathlib import Path

import pytest

from ask_lakehouse.config import Settings
from ask_lakehouse.demo_data import write_demo
from ask_lakehouse.execute import Executor, build_database
from ask_lakehouse.semantic import load_semantic

ROOT = Path(__file__).parent.parent


@pytest.fixture(scope="session")
def db_path(tmp_path_factory):
    d = tmp_path_factory.mktemp("gold")
    write_demo(d / "parquet")
    db = d / "gold.duckdb"
    build_database(d / "parquet", db)
    return db


@pytest.fixture(scope="session")
def semantic():
    return load_semantic(ROOT / "semantic" / "nyc_taxi.yaml")


@pytest.fixture()
def executor(db_path):
    ex = Executor(db_path, max_rows=1000, timeout_s=10)
    yield ex
    ex.close()


@pytest.fixture()
def settings(db_path, tmp_path):
    return Settings(
        db_path=db_path, semantic_path=ROOT / "semantic" / "nyc_taxi.yaml",
        log_path=tmp_path / "queries.jsonl", max_repairs=1,
    )
