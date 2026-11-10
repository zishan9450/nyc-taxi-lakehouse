# Ask your lakehouse

A natural-language analytics assistant over the gold tables of the
[NYC taxi lakehouse pipeline](../README.md). Ask a business question, get a table back, and
always see the SQL and the assumptions behind it.

The point of this project is not "an LLM writes SQL". It is everything around that:

- a **semantic layer** so the model uses your metric definitions instead of guessing,
- **guardrails in code** so model output is treated as untrusted input,
- an **eval harness** so you can say how often it is right, and prove a prompt change helped.

```
 question ("Which borough has the highest tip rate?")
     |
     v
 semantic layer (YAML)  tables, columns, metric definitions, caveats, data coverage
     |
     v
 LLM  -> JSON {sql, assumptions, refusal}        (Anthropic or OpenAI, swappable)
     |
     v
 guardrails (sqlglot)   one statement, SELECT only, allowlisted tables and columns,
     |                  no file readers or system functions; SQL is re-generated from the parse tree
     |   invalid (unknown column, syntax) -> one repair attempt with the error fed back
     |   unsafe (DROP, read_csv, ...)     -> blocked, no retry
     v
 DuckDB   read-only connection, external access disabled, row cap, timeout
     v
 answer: table + SQL + assumptions + latency + tokens     (every request logged to JSONL)
```

## Quickstart (no pipeline needed)

Run this from `ask-lakehouse/`, and activate this directory's `.venv`. The pipeline venv in
the parent directory does not contain this package.

```bash
make setup && source .venv/bin/activate
make test                  # no API key or network needed
make demo-data             # synthetic gold tables with the same schema as the pipeline's
```

The demo data is synthetic and its numbers are not real taxi statistics. To serve it, export
the variables below and run `make serve` or `python -m ask_lakehouse serve`
(`http://127.0.0.1:8000`). One question from the shell:

```bash
python -m ask_lakehouse ask "How many trips were there in each borough?"
```

### Use the pipeline's gold tables

```bash
pip install -e ".[export]"
python -m ask_lakehouse export-gold --lake ../lake
```

This reads the gold Delta tables directly (no Spark, no JVM), writes parquet, builds
`data/gold.duckdb` and checks that the semantic layer still matches the database. Airflow does
not need to stay running after that. Re-run `export-gold` after loading another month.

### Providers

The server reads the environment of the process that starts it. Use `export`.

```bash
# OpenAI. LLM_MODEL is required. gpt-4o-mini matches the client in this repo.
pip install -e ".[openai]"
export LLM_PROVIDER=openai
export OPENAI_API_KEY=...
export LLM_MODEL=gpt-4o-mini

# Anthropic. Default provider. The key comes from the Claude Console (sk-ant-api03-...).
pip install -e ".[anthropic]"
export LLM_PROVIDER=anthropic
export ANTHROPIC_API_KEY=...
```

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `LLM_PROVIDER` | `anthropic` | `anthropic` or `openai`. Must be exported |
| `LLM_MODEL` | `claude-sonnet-5-5` for Anthropic | required for `openai` |
| `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` | | read by the provider SDK |
| `ASK_DB_PATH` | `data/gold.duckdb` | |
| `ASK_MAX_ROWS` | `1000` | result cap; `truncated` is reported |
| `ASK_TIMEOUT_S` | `10` | query time limit |
| `ASK_MAX_REPAIRS` | `1` | retries after a fixable error |
| `ASK_LOG_PATH` | `logs/queries.jsonl` | one line per question: status, SQL, latency, tokens |

## The semantic layer

`semantic/nyc_taxi.yaml` is the single source of truth. The prompt, the SQL guardrails and a test
that compares it with the real database schema all read it.

Its most important job is metric definitions. The gold tables are pre-aggregated, so a naive
`AVG(avg_distance_miles)` is wrong (it ignores how many trips each row represents). The semantic
layer defines the trip-weighted version, and the eval set contains questions that catch a model
that ignores it.

## Guardrails

Model output is untrusted. Defence in depth, strongest layer last:

| Layer | What it stops |
|---|---|
| Prompt rules | Gentle steering only; never relied on |
| SQL validation | Multiple statements, anything but SELECT, DDL/DML/COPY/ATTACH/PRAGMA/SET, table functions and file paths, file/system functions, tables and columns outside the semantic layer |
| Re-generation | Validated SQL is rebuilt from the parse tree, so the validator and the database cannot disagree about what a string means |
| Database connection | Read-only, external access disabled, memory limit, timeout. Even a total bypass of the checks above cannot write or read files |

Question text is treated as untrusted too: it is delimited in the prompt, and query results are
never fed back to the model, so data cannot inject instructions.

## Evals

```bash
make eval LABEL=prompt-v1      # real model; writes evals/results/prompt-v1.json
make chart                     # accuracy across runs -> evals/results/accuracy.png
make eval-oracle               # harness self-check using the reference SQL as the "model"
```

`evals/questions.yaml` has 35 golden questions in seven categories: simple aggregates, joins, time
filters, tip rates, ambiguous questions, unanswerable questions and unsafe requests.

- **Answerable questions** carry reference SQL. A response passes when its **result set** matches
  the reference (row order ignored, numbers compared with tolerance, extra columns allowed). SQL
  text is never compared, because many different queries are correct. Ambiguous questions list
  every acceptable interpretation.
- **Unanswerable and unsafe questions** pass when nothing was executed: the model refused or the
  guardrails blocked the SQL.
- **Reported**: execution accuracy, valid-SQL rate, false-refusal rate, correct-refusal rate,
  whether ambiguous questions state their assumption, latency p50/p95, tokens, and a per-category
  breakdown. Each run records the prompt version and model, so runs are comparable.

Workflow: change the prompt, bump `PROMPT_VERSION` in `prompt.py`, run `make eval LABEL=...`,
read the failures in the JSON `details`, repeat. Keep the per-version chart for the README.

## Results

> No real-model results are included. The harness is tested (an oracle model scores 100%, a wrong
> model scores 0%, and a model that averages averages is caught), but a real run needs your API
> key. Fill this in from your own runs and delete this note.

| Prompt version | Model | Execution accuracy | Correct refusals | p50 latency | Tokens per question |
|---|---|---|---|---|---|
| v1 | TODO | TODO | TODO | TODO | TODO |

## Limits

- The demo has no authentication or rate limiting. Do not expose it publicly: every request costs
  model tokens.
- Column validation is name-based (a column must exist somewhere in the semantic layer or be an
  alias), not a full type check. The read-only database connection is the real boundary.
- Single-turn only: no conversation memory, so follow-up questions need full context.
- Results are a table; there is no chart rendering yet.
- 35 golden questions is enough to compare prompt versions, not to claim a general accuracy number.
  Extend to 50 or more before quoting percentages, and say which dataset they were measured on.

## Layout

```
semantic/        nyc_taxi.yaml: tables, columns, metric definitions, caveats
src/ask_lakehouse/
  semantic.py    load, validate, render for the prompt
  prompt.py      system prompt, PROMPT_VERSION
  llm.py         Anthropic / OpenAI clients, scripted model for tests
  generate.py    call the model, parse and validate its JSON reply
  guardrails.py  SQL validation (sqlglot)
  execute.py     DuckDB: read-only, no external access, row cap, timeout
  service.py     the pipeline: propose, validate, run, repair, log
  api.py         FastAPI app + static page
  evaluation.py  result-set matching, metrics, runner, chart
  demo_data.py   synthetic gold tables
  export.py      gold Delta tables -> parquet (no Spark)
evals/           questions.yaml and results/
tests/           guardrails, executor, pipeline, evals, API, export
docs/            decisions.md
```
