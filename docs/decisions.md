# Design decisions

Short records of what was chosen, what was rejected, and why.

## 1. Replace a month's partition instead of appending

**Chosen:** bronze, quarantine and gold use `replaceWhere` on the month column.
**Rejected:** plain append (re-runs duplicate rows) and full-table overwrite (slow, and a failed
run would wipe other months).
**Why:** a retry after a failure, a backfill and a manual re-run all become the same safe operation.

## 2. Checksum manifest rather than "file exists" checks

**Chosen:** store the SHA-256 of each loaded file; skip only if it matches.
**Why:** TLC sometimes republishes files. A name-based check would silently keep stale data.

## 3. MERGE with NOT MATCHED BY SOURCE DELETE for silver

**Chosen:** merge on `trip_id` within the month and delete month rows absent from the new source.
**Why:** after a corrected file arrives, silver should equal the latest clean data exactly, not the
union of every version ever loaded.

## 4. Quarantine plus a gate, instead of dropping or failing on bad rows

**Chosen:** every rejected row is kept with one reason; the load fails only if the reject rate is
abnormal (default above 5%).
**Why:** a few bad rows are normal in this data and should not stop the pipeline, but a sudden jump
usually means upstream changed and deserves a human. Quarantine is written before the gate so the
evidence is available when the gate fails.

## 5. Explicit silver schema

**Chosen:** a fixed column list with types; missing source columns become NULL.
**Rejected:** `select *` from bronze.
**Why:** consumers get a stable contract. Upstream changes show up in bronze first and are promoted
deliberately.

## 6. Transforms as pure functions

**Chosen:** rules and standardization in `transforms.py`, no I/O.
**Why:** the business logic is the part most likely to change and the part most worth testing; pure
functions run on a plain Spark session in seconds without Delta jars.

## 7. One Airflow task per stage

**Chosen:** `fetch >> bronze >> silver >> gold`, each its own task with retries.
**Why:** a flaky download retries without re-running Spark work, and the Airflow UI shows exactly
which stage failed.

## 8. Single node, two cores

**Chosen:** `local[2]` Spark and a 6g driver heap.
**Rejected:** `local[*]`. On a laptop that starts one task per core, and each task reads a whole
Parquet row group into the same driver heap. A six-month backfill died that way on a 16GB machine.
**Why:** monthly TLC files are a few million rows. Two concurrent tasks finish them without
exhausting the heap. `SPARK_MASTER` and `SPARK_DRIVER_MEMORY` override both.
