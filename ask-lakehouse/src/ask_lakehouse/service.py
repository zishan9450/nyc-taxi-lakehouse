"""The ask pipeline: question -> SQL proposal -> guardrails -> read-only execution -> answer."""
from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from ask_lakehouse.config import Settings
from ask_lakehouse.execute import Executor, QueryError
from ask_lakehouse.generate import Generator, ProposalError
from ask_lakehouse.guardrails import GuardrailError, validate
from ask_lakehouse.llm import LLM, make_llm
from ask_lakehouse.prompt import PROMPT_VERSION
from ask_lakehouse.semantic import Semantic, load_semantic

log = logging.getLogger(__name__)
MAX_QUESTION_CHARS = 1000

# status: ok | refused (the model declined) | blocked (guardrails stopped the SQL) | error
OK, REFUSED, BLOCKED, ERROR = "ok", "refused", "blocked", "error"


@dataclass
class Answer:
    question: str
    status: str
    message: str = ""
    sql: str | None = None
    assumptions: str = ""
    columns: list[str] = field(default_factory=list)
    rows: list[list] = field(default_factory=list)
    truncated: bool = False
    attempts: int = 0
    latency_ms: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    model: str = ""
    prompt_version: str = PROMPT_VERSION

    def to_dict(self) -> dict:
        return asdict(self)


class Assistant:
    def __init__(self, semantic: Semantic, executor: Executor, llm: LLM, settings: Settings):
        self.semantic = semantic
        self.executor = executor
        self.llm = llm
        self.settings = settings
        self._gen = Generator(llm, semantic, executor.coverage())
        self._log_lock = threading.Lock()

    @classmethod
    def from_settings(cls, settings: Settings, llm: LLM | None = None) -> Assistant:
        semantic = load_semantic(settings.semantic_path)
        executor = Executor(settings.db_path, settings.max_rows, settings.query_timeout_s)
        return cls(semantic, executor, llm or make_llm(settings.provider, settings.model), settings)

    def ask(self, question: str) -> Answer:
        start = time.perf_counter()
        question = (question or "").strip()
        answer = Answer(question=question, status=ERROR, model=self.llm.name)
        try:
            self._run(question, answer)
        except Exception:  # never leak internals to the caller; the log has the traceback
            log.exception("unexpected failure answering %r", question)
            answer.status, answer.message = ERROR, "Something went wrong answering that."
        answer.latency_ms = round((time.perf_counter() - start) * 1000, 1)
        self._log(answer)
        return answer

    def _run(self, question: str, answer: Answer) -> None:
        if not question or len(question) > MAX_QUESTION_CHARS:
            answer.status = REFUSED
            answer.message = f"Please ask a question of up to {MAX_QUESTION_CHARS} characters."
            return

        raw, error, final = "", "", (ERROR, "no usable reply from the model")
        for attempt in range(1, self.settings.max_repairs + 2):
            answer.attempts = attempt
            try:
                proposal = (
                    self._gen.propose(question) if attempt == 1
                    else self._gen.repair(question, raw, error)
                )
            except ProposalError as exc:
                answer.input_tokens += exc.tokens[0]  # type: ignore[attr-defined]
                answer.output_tokens += exc.tokens[1]  # type: ignore[attr-defined]
                raw, error, final = exc.raw, str(exc), (ERROR, str(exc))  # type: ignore[attr-defined]
                continue
            except Exception:
                log.exception("model call failed")
                answer.status, answer.message = ERROR, "The language model could not be reached."
                return

            answer.input_tokens += proposal.input_tokens
            answer.output_tokens += proposal.output_tokens
            answer.assumptions = proposal.assumptions
            if proposal.refusal:
                answer.status, answer.message = REFUSED, proposal.refusal
                return

            answer.sql, raw = proposal.sql, proposal.raw
            try:
                validated = validate(proposal.sql, self.semantic)
                result = self.executor.run(validated.sql)
            except GuardrailError as exc:
                if exc.kind == "unsafe":  # no second chances for unsafe SQL
                    answer.status, answer.message = BLOCKED, f"Blocked: {exc}"
                    return
                error, final = str(exc), (BLOCKED, f"Blocked: {exc}")
                continue
            except QueryError as exc:
                error, final = str(exc), (ERROR, f"The query failed: {exc}")
                continue

            answer.sql = validated.sql
            answer.status = OK
            answer.columns, answer.rows, answer.truncated = result.columns, result.rows, result.truncated
            if result.truncated:
                answer.message = f"Showing the first {self.settings.max_rows} rows."
            return

        answer.status, answer.message = final

    def _log(self, a: Answer) -> None:
        path = self.settings.log_path
        if not path:
            return
        record = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "question": a.question, "status": a.status, "sql": a.sql, "attempts": a.attempts,
            "latency_ms": a.latency_ms, "input_tokens": a.input_tokens,
            "output_tokens": a.output_tokens, "rows": len(a.rows), "model": a.model,
            "prompt_version": a.prompt_version, "message": a.message[:300],
        }
        with self._log_lock:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(record) + "\n")
