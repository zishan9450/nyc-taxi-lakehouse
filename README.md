# NYC Taxi Lakehouse

A medallion (bronze / silver / gold) pipeline for NYC TLC yellow taxi trips, plus a
natural-language assistant over the gold tables. PySpark and Delta Lake build the lake.
Airflow runs one month at a time. DuckDB serves the aggregates, and the assistant turns a
question into checked SQL.

Two parts live in this repo:

| Part | What it does | Start here |
|---|---|---|
| Pipeline | Download a month, land bronze, clean silver, publish gold | This page |
| [ask-lakehouse](ask-lakehouse/README.md) | Ask a question, get a table, the SQL, and the assumption | `ask-lakehouse/` |

```
 NYC TLC monthly parquet (public)
            |
            v
 fetch ---> BRONZE  raw + ingest metadata, one partition per month
            |       manifest table: month, file checksum, row count
            v
            SILVER  typed, validated, de-duplicated, MERGE upsert
            |   \--> QUARANTINE  rejected rows + reason code
            v
            GOLD    trips_by_zone_day, revenue_by_hour, tip_rate_by_borough, dim_zone

 Every stage writes to meta/run_log: rows in/out/rejected, duration, status, error.
 A quality gate sits between quarantine and the silver merge.
```

## Quickstart

```bash
make setup && source .venv/bin/activate     # Python 3.10+ and Java 17 required
make test                                   # unit tests (plain Spark)
make test-delta                             # pipeline tests on tiny files (needs Maven access)
python -m lakehouse run --month 2024-01     # download, load, publish one month
python -m lakehouse backfill --start 2024-01 --end 2024-06
```

Docker (Spark and Delta jars baked into the image):

```bash
docker compose run --rm pipeline run --month 2024-02
docker compose up --build airflow           # UI on http://localhost:8080
```

Inspect results with any Delta reader, e.g.:

```python
from lakehouse.spark import get_spark
spark = get_spark()
spark.read.format("delta").load("lake/meta/run_log").orderBy("logged_at").show(truncate=False)
spark.read.format("delta").load("lake/silver/trips_quarantine").groupBy("reject_reason").count().show()
```

## Layers

| Layer | Table | Contents |
|---|---|---|
| Bronze | `bronze/trips` | Raw columns (lower-cased), plus `_ingest_month`, `_source_file`, `_sha256`, `_ingested_at`. Partitioned by month. |
| Silver | `silver/trips` | Explicit schema, derived columns (`trip_id`, duration, date, hour), de-duplicated. Partitioned by `pickup_month`. |
| Quarantine | `silver/trips_quarantine` | Rows that failed a rule, with `reject_reason`. |
| Gold | `gold/*` | Small aggregates for BI and the analytics assistant. `dim_zone` maps zone ids to boroughs. |
| Meta | `meta/manifest`, `meta/run_log` | What was loaded (by checksum) and how every run went. |

## Design decisions

Short version here; reasoning in [docs/decisions.md](docs/decisions.md).

- **Idempotent by construction.** Bronze and gold replace one month's partition (`replaceWhere`).
  Silver uses `MERGE` on `trip_id`, with `WHEN NOT MATCHED BY SOURCE DELETE` scoped to the month.
  Re-running any month, or the whole backfill, never duplicates data.
- **Checksum manifest.** A month is skipped when the same file (by SHA-256) was already loaded.
  If upstream republishes a corrected file, the checksum changes and the month is replaced.
- **Quarantine, not delete.** Rows failing a rule keep their data and get a reason code. The first
  matching rule wins, so each rejected row has exactly one reason.
- **Gate before publish.** If a month's reject rate exceeds `MAX_REJECT_RATE` (default 5%), the
  silver merge is skipped and the task fails, but quarantine is still written so you can see why.
- **Explicit silver contract.** Older files lack columns that newer ones have, and column casing
  changed over time. Silver selects and casts a fixed list; missing columns become NULL. New
  upstream columns land in bronze automatically (`mergeSchema`) and are promoted to silver by
  editing one dict in `transforms.py`.
- **Pure transforms.** Business rules live in `transforms.py` with no I/O, so they are tested on a
  plain Spark session in seconds. Delta behaviour (idempotency, MERGE, schema evolution) is tested
  separately on tiny files.
- **Honest metric definitions.** TLC does not record cash tips, so `tip_rate` is computed on card
  trips only (`payment_type = 1`) and the gold table says so.

## Known limitations

- `trip_id` is a hash of the natural key columns. Genuinely different trips identical on all of
  them would collapse into one. TLC has no trip identifier, so this is the best available key.
- Runs on a single node (`local[2]`, 6g driver heap). The partitioning and structure carry over to a
  cluster, but cluster config is not included. Override with `SPARK_MASTER` and `SPARK_DRIVER_MEMORY`.
- Quality checks are written in plain code. Swapping them for Great Expectations or Soda is a
  reasonable next step but is not done.

## Ask a question

The assistant does not read Spark. It reads a DuckDB copy of the gold tables. Build that copy
after a month has loaded, from `ask-lakehouse/`:

```bash
cd ask-lakehouse
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,export,openai]"          # use ".[dev,export,anthropic]" for Claude
python -m ask_lakehouse export-gold --lake ../lake
```

`export-gold` checks that `semantic/nyc_taxi.yaml` still matches the database. Then serve it.
The variables below have to be exported. A bare `NAME=value` assignment stays in the shell and
the server never sees it.

```bash
export LLM_PROVIDER=openai
export OPENAI_API_KEY=...
export LLM_MODEL=gpt-4o-mini
python -m ask_lakehouse serve                 # http://127.0.0.1:8000
```

Claude is the other provider: `LLM_PROVIDER=anthropic` and `ANTHROPIC_API_KEY`. The key has to
be an API key from the Claude Console (`sk-ant-api03-...`). Details, guardrails, and the eval
harness are in [ask-lakehouse/README.md](ask-lakehouse/README.md).

Airflow can be stopped once the gold tables exist. The assistant uses the DuckDB file, not the
Spark session.

To load a chosen month from the Airflow UI, open
`http://localhost:8080/dags/nyc_lakehouse_monthly/trigger` and set **Logical date** to the first
day of that month, for example `2024-01-01 00:00:00`. The play button on the DAG list triggers
the current time and does not ask for a date. `catchup` is off, so unpausing the DAG does not
backfill earlier months.

## Results

One month, January 2024, loaded by the Airflow run `manual__2024-01-01` on a 16 GB Apple Silicon
laptop (`local[2]`, 6g driver). A wider backfill was not kept.

| Metric | Value |
|---|---|
| Months loaded | 2024-01 |
| Bronze rows | 2,964,624 |
| Silver rows kept | 2,927,078 |
| Rejected | 37,546 (1.27%) |
| Reject reasons | `negative_fare` 37,447; `dropoff_before_pickup` 56; `unreasonable_distance` 25; `pickup_outside_file_month` 18 |
| Stage duration | bronze 14.4s, silver 30.5s, gold 11.4s |
| Gold rows | `trips_by_zone_day` 6,948; `revenue_by_hour` 744; `tip_rate_by_borough` 8; `dim_zone` 265 |
| Gold revenue | $80,340,859.37 |
| Highest card tip rate | Manhattan, 0.2376 |

Date-filter benchmarks and a Spark versus single-node Python comparison were not measured.

## Project layout

```
src/lakehouse/   config, ingest (download + checksum), transforms (pure), quality, deltaio,
                 manifest, runlog, bronze, silver, gold, pipeline, cli
dags/            Airflow DAG: fetch >> bronze >> silver >> gold, monthly, 2 retries
tests/           unit tests (plain Spark) and delta-marked pipeline tests
docs/            design decisions
ask-lakehouse/   semantic layer, SQL guardrails, DuckDB serving, eval harness, web UI
```

## Roadmap

- `benchmarks/`: Spark vs plain Python across row counts, and partitioned vs unpartitioned reads.
- Great Expectations or Soda for declarative checks; OPTIMIZE / Z-ORDER on silver.
- Real-model eval scores in `ask-lakehouse/README.md` (the harness is tested; a scored run needs an API key).
