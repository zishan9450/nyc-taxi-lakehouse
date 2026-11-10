import pytest

from ask_lakehouse.guardrails import GuardrailError, validate

OK_QUERIES = [
    "SELECT SUM(trips) FROM trips_by_zone_day",
    ("SELECT z.borough, SUM(t.trips) AS n FROM trips_by_zone_day t "
    "JOIN dim_zone z ON t.pu_location_id = z.location_id GROUP BY z.borough ORDER BY n DESC LIMIT 5"),
    ("WITH m AS (SELECT pickup_month, SUM(revenue) AS rev FROM trips_by_zone_day GROUP BY 1) "
    "SELECT pickup_month, rev FROM m ORDER BY rev DESC"),
    "SELECT borough FROM tip_rate_by_borough UNION ALL SELECT borough FROM dim_zone",
    "SELECT pickup_date, SUM(trips) OVER (ORDER BY pickup_date) AS running FROM revenue_by_hour",
    "SELECT * FROM main.dim_zone",
    "SELECT 1",
    "select SUM(Trips) from TRIPS_BY_ZONE_DAY;",
]


@pytest.mark.parametrize("sql", OK_QUERIES)
def test_valid_queries_pass(sql, semantic):
    assert validate(sql, semantic).sql


UNSAFE = [
    "DROP TABLE dim_zone",
    "DELETE FROM dim_zone",
    "UPDATE dim_zone SET zone = 'x'",
    "INSERT INTO dim_zone VALUES (1, 'a', 'b', 'c')",
    "CREATE TABLE x AS SELECT 1",
    "ALTER TABLE dim_zone ADD COLUMN x INT",
    "COPY dim_zone TO '/tmp/x.csv'",
    "ATTACH 'other.db'",
    "PRAGMA database_list",
    "SET enable_external_access = true",
    "SELECT 1; DROP TABLE dim_zone",
    "SELECT * FROM dim_zone; SELECT * FROM dim_zone",
    "SELECT * FROM read_csv('/etc/passwd')",
    "SELECT * FROM Read_Parquet('x.parquet')",
    "SELECT * FROM '/etc/passwd'",
    "SELECT * FROM 'https://example.com/x.csv'",
    "SELECT * FROM dim_zone, (SELECT * FROM read_json('x.json'))",
    "SELECT glob('/*')",
    "SELECT getenv('HOME')",
    "SELECT current_setting('access_mode')",
    "SELECT * FROM information_schema.tables",
    "SELECT * FROM other_db.main.dim_zone",
    "SELECT * FROM duckdb_tables()",
    "SELECT * FROM range(1000000000)",
]


@pytest.mark.parametrize("sql", UNSAFE)
def test_unsafe_sql_is_rejected(sql, semantic):
    with pytest.raises(GuardrailError) as exc:
        validate(sql, semantic)
    assert exc.value.kind == "unsafe", f"{sql!r} -> {exc.value}"


def test_comments_cannot_hide_a_second_statement(semantic):
    with pytest.raises(GuardrailError) as exc:
        validate("SELECT 1 -- harmless\n; DROP TABLE dim_zone", semantic)
    assert exc.value.kind == "unsafe"


def test_unknown_table_and_column_are_repairable(semantic):
    with pytest.raises(GuardrailError) as exc:
        validate("SELECT * FROM trips", semantic)
    assert exc.value.kind == "invalid" and "unknown table" in str(exc.value)
    with pytest.raises(GuardrailError) as exc:
        validate("SELECT passenger_age FROM dim_zone", semantic)
    assert exc.value.kind == "invalid" and "unknown column" in str(exc.value)


def test_syntax_error_is_repairable(semantic):
    with pytest.raises(GuardrailError) as exc:
        validate("SELEC trips FROM", semantic)
    assert exc.value.kind == "invalid"


def test_empty_and_oversized_sql(semantic):
    with pytest.raises(GuardrailError):
        validate("   ", semantic)
    with pytest.raises(GuardrailError) as exc:
        validate("SELECT 1 " + "+ 1 " * 3000, semantic)
    assert exc.value.kind == "unsafe"


def test_output_is_regenerated_from_the_parse_tree(semantic):
    v = validate("select  sum(trips)   from trips_by_zone_day /* note */", semantic)
    assert "note" not in v.sql and v.tables == {"trips_by_zone_day"}


def test_cte_names_are_not_mistaken_for_tables(semantic):
    v = validate("WITH x AS (SELECT * FROM dim_zone) SELECT zone FROM x", semantic)
    assert v.tables == {"dim_zone"}
