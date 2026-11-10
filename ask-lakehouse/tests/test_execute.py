import duckdb
import pytest

from ask_lakehouse.execute import Executor, QueryError


def test_runs_a_query(executor):
    r = executor.run("SELECT borough, COUNT(*) AS zones FROM dim_zone GROUP BY borough ORDER BY 1")
    assert r.columns == ["borough", "zones"] and not r.truncated and r.rows[0][0] == "Bronx"


def test_dates_are_json_friendly(executor):
    r = executor.run("SELECT MIN(pickup_date) FROM trips_by_zone_day")
    assert r.rows == [["2024-01-01"]]


def test_row_cap_sets_truncated(db_path):
    ex = Executor(db_path, max_rows=10)
    r = ex.run("SELECT * FROM trips_by_zone_day")
    assert len(r.rows) == 10 and r.truncated
    ex.close()


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE dim_zone",
        "DELETE FROM dim_zone",
        "CREATE TABLE evil AS SELECT 1",
        "SELECT * FROM read_csv('/etc/passwd')",
        "COPY dim_zone TO '/tmp/leak.csv'",
        "SET enable_external_access = true",
    ],
)
def test_connection_itself_refuses_writes_and_file_access(executor, sql):
    """Even if the guardrails were bypassed, the database connection is the hard boundary."""
    with pytest.raises(QueryError):
        executor.run(sql)
    assert executor.run("SELECT COUNT(*) FROM dim_zone").rows[0][0] > 0


def test_timeout_interrupts_runaway_queries(db_path):
    ex = Executor(db_path, timeout_s=1.0)
    with pytest.raises(QueryError, match="time limit"):
        ex.run("SELECT COUNT(*) FROM range(3000000000) a, range(3000000000) b")
    ex.close()


def test_database_cannot_be_written_via_a_second_connection_path(db_path):
    with pytest.raises(duckdb.Error):
        duckdb.connect(str(db_path), read_only=True).execute("DROP TABLE dim_zone")
