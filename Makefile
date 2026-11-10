.PHONY: setup lint test test-delta run backfill up

# /usr/bin/java on macOS is a stub until JAVA_HOME points at a real JDK.
# Homebrew's OpenJDK is not registered with that stub, so Spark's gateway exits
# unless make sets JAVA_HOME. CI already exports JAVA_HOME and is left alone.
BREW_JAVA := /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
ifeq ($(JAVA_HOME),)
  ifneq ($(wildcard $(BREW_JAVA)/bin/java),)
    export JAVA_HOME := $(BREW_JAVA)
    export PATH := $(JAVA_HOME)/bin:$(PATH)
  endif
endif

setup:
	python -m venv .venv && . .venv/bin/activate && pip install -e ".[dev]"
	# Python 3.11 skips a hidden .pth file. This venv is gitignored, and macOS
	# marks it hidden, so the editable hook never reaches sys.path.
	. .venv/bin/activate && python -c "import sysconfig; from pathlib import Path; link = Path(sysconfig.get_path('purelib')) / 'lakehouse'; link.unlink(missing_ok=True) if link.is_symlink() else None; link.symlink_to(Path('src/lakehouse').resolve(), target_is_directory=True)"

lint:
	ruff check src tests dags

test:            ## fast unit tests (plain Spark, no Delta jars)
	pytest

test-delta:      ## idempotency, schema evolution, MERGE, gates (needs Maven access)
	pytest -m delta

run:             ## one month end to end
	python -m lakehouse run --month 2024-01

backfill:
	python -m lakehouse backfill --start 2024-01 --end 2024-06

up:              ## Airflow UI on http://localhost:8080
	docker compose up --build airflow
