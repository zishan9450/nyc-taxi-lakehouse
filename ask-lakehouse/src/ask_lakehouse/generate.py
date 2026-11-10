from __future__ import annotations

import json
import re
from dataclasses import dataclass

from ask_lakehouse.llm import LLM
from ask_lakehouse.prompt import SYSTEM, build_repair_prompt, build_user_prompt
from ask_lakehouse.semantic import Semantic


class ProposalError(ValueError):
    """The model's reply could not be used (not JSON, or neither sql nor refusal)."""


@dataclass(frozen=True)
class Proposal:
    sql: str | None
    assumptions: str
    refusal: str | None
    raw: str
    input_tokens: int = 0
    output_tokens: int = 0


_FENCE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE)


def parse_response(text: str) -> tuple[str | None, str, str | None]:
    cleaned = _FENCE.sub("", text.strip())
    start, end = cleaned.find("{"), cleaned.rfind("}")
    if start == -1 or end <= start:
        raise ProposalError("reply was not a JSON object")
    try:
        data = json.loads(cleaned[start : end + 1])
    except json.JSONDecodeError as exc:
        raise ProposalError(f"reply was not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ProposalError("reply JSON must be an object")
    refusal = data.get("refusal")
    sql = data.get("sql")
    assumptions = str(data.get("assumptions") or "")
    if refusal:
        return None, assumptions, str(refusal)
    if not isinstance(sql, str) or not sql.strip():
        raise ProposalError("reply had neither 'sql' nor 'refusal'")
    return sql.strip(), assumptions, None


class Generator:
    def __init__(self, llm: LLM, semantic: Semantic, coverage: dict):
        self._llm = llm
        self._schema_text = semantic.render()
        self._coverage = coverage

    def propose(self, question: str) -> Proposal:
        user = build_user_prompt(question, self._schema_text, self._coverage)
        return self._call(user)

    def repair(self, question: str, previous_raw: str, error: str) -> Proposal:
        user = build_repair_prompt(question, self._schema_text, self._coverage, previous_raw, error)
        return self._call(user)

    def _call(self, user: str) -> Proposal:
        result = self._llm.complete(SYSTEM, user)
        tokens = (result.input_tokens or 0, result.output_tokens or 0)
        try:
            sql, assumptions, refusal = parse_response(result.text)
        except ProposalError as exc:
            exc.raw = result.text  # type: ignore[attr-defined]
            exc.tokens = tokens  # type: ignore[attr-defined]
            raise
        return Proposal(sql, assumptions, refusal, result.text, *tokens)
