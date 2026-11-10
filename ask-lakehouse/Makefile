.PHONY: setup lint test demo-data serve eval eval-oracle chart

setup:
	python -m venv .venv && . .venv/bin/activate && pip install -e ".[dev,anthropic]"

lint:
	ruff check src tests

test:                ## no network, no API key: the model is scripted
	pytest

demo-data:           ## synthetic gold tables -> data/gold.duckdb
	python -m ask_lakehouse demo-data

serve:               ## http://127.0.0.1:8000 (needs ANTHROPIC_API_KEY, or LLM_PROVIDER=openai + OPENAI_API_KEY + LLM_MODEL)
	python -m ask_lakehouse serve

eval:                ## real model; LABEL names the run, e.g. make eval LABEL=prompt-v1
	python -m ask_lakehouse eval --label $(LABEL)

eval-oracle:         ## harness self-check, no model
	python -m ask_lakehouse eval --label oracle-check --llm oracle

chart:
	python -m ask_lakehouse eval-chart
