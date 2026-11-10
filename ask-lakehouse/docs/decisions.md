# Design decisions

## 1. DuckDB for serving, not Spark

The gold tables are small aggregates. DuckDB answers in milliseconds, needs no cluster or JVM, and
lets anyone run the demo with `pip install`. It also follows from the lakehouse benchmark: Spark is
the right tool for building the tables and the wrong one for serving them.

## 2. A semantic layer instead of schema-only prompting

Table and column names are not enough: with pre-aggregated data the model also needs to know that
averages must be trip-weighted, that tip data covers card trips only, and what "last month" means
for this dataset. Putting that in one YAML file means the prompt, the guardrails and the schema
test cannot drift apart.

## 3. Guardrails in code, with a read-only database as the real boundary

Prompts are requests, not controls. The model's SQL is parsed (sqlglot) and rejected unless it is
a single read-only query over allowlisted tables and columns. Because parsers can disagree, the SQL
that runs is re-generated from the validated tree. Because validators have bugs, the database
connection is also read-only with external access disabled, so a bypass still cannot write or read
files. Tests run each dangerous statement against both layers.

## 4. Repair once, but only for fixable errors

Unknown columns, syntax errors and database errors are often a model slip, so the error is fed back
for one retry. Unsafe SQL (DROP, file readers, multiple statements) is blocked immediately: a
second attempt would only teach the model to disguise the request.

## 5. Evaluate by result set, not SQL text

Many different queries are correct. Comparing the data they return avoids penalising valid SQL and
catches plausible-looking wrong SQL (for example averaging averages). Ambiguous questions list all
acceptable interpretations, and the harness also records whether the model stated its assumption.

## 6. Refusals are evaluated, not just answers

A useful assistant must decline questions the data cannot answer and requests it must not follow.
The eval set measures both false refusals (declining answerable questions) and correct refusals, so
tightening the prompt cannot silently trade one for the other.

## 7. A scripted "oracle" model for the harness

The oracle answers with the reference SQL. It proves the harness, matching and metrics work (100%),
and the tests prove a wrong model scores 0%. It says nothing about a real model, and its results are
never saved as real results.

## 8. Query results never go back to the model

Only the question and the schema reach the model. Rows are displayed, not re-prompted, so a poisoned
value in the data cannot become an instruction.
