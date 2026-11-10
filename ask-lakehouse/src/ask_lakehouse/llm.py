"""Thin, swappable LLM clients. The pipeline only needs `complete(system, user)`."""
from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from ask_lakehouse.config import DEFAULT_MODELS


@dataclass(frozen=True)
class LLMResult:
    text: str
    input_tokens: int | None = None
    output_tokens: int | None = None


class LLM(Protocol):
    name: str

    def complete(self, system: str, user: str, max_tokens: int = 1000) -> LLMResult: ...


class AnthropicLLM:
    def __init__(self, model: str):
        try:
            import anthropic
        except ImportError as exc:
            raise RuntimeError('Anthropic support is not installed: pip install -e ".[anthropic]"') from exc
        if not os.environ.get("ANTHROPIC_API_KEY"):
            raise RuntimeError("ANTHROPIC_API_KEY is not set")
        self._client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY
        self._model = model
        self.name = f"anthropic:{model}"

    def complete(self, system: str, user: str, max_tokens: int = 1000) -> LLMResult:
        # Sonnet 5.5 rejects temperature. between_tools skips the up-front thinking
        # pass so the token budget is spent on the JSON reply.
        msg = self._client.messages.create(
            model=self._model,
            max_tokens=max_tokens,
            system=system,
            thinking={"type": "between_tools"},
            messages=[{"role": "user", "content": user}],
        )
        text = "".join(b.text for b in msg.content if getattr(b, "type", "") == "text")
        return LLMResult(text, msg.usage.input_tokens, msg.usage.output_tokens)


class OpenAILLM:
    def __init__(self, model: str):
        try:
            import openai
        except ImportError as exc:
            raise RuntimeError('OpenAI support is not installed: pip install -e ".[openai]"') from exc
        if not os.environ.get("OPENAI_API_KEY"):
            raise RuntimeError("OPENAI_API_KEY is not set")
        self._client = openai.OpenAI()  # reads OPENAI_API_KEY
        self._model = model
        self.name = f"openai:{model}"

    def complete(self, system: str, user: str, max_tokens: int = 1000) -> LLMResult:
        resp = self._client.chat.completions.create(
            model=self._model, temperature=0, max_tokens=max_tokens,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        )
        usage = resp.usage
        return LLMResult(
            resp.choices[0].message.content or "",
            getattr(usage, "prompt_tokens", None), getattr(usage, "completion_tokens", None),
        )


class ScriptedLLM:
    """Deterministic stand-in for tests and offline harness checks."""

    def __init__(self, fn: Callable[[str, str], str], name: str = "scripted"):
        self._fn = fn
        self.name = name
        self.calls: list[tuple[str, str]] = []

    def complete(self, system: str, user: str, max_tokens: int = 1000) -> LLMResult:
        self.calls.append((system, user))
        return LLMResult(self._fn(system, user), 0, 0)


def make_llm(provider: str, model: str | None) -> LLM:
    if provider == "anthropic":
        return AnthropicLLM(model or DEFAULT_MODELS["anthropic"])
    if provider == "openai":
        if not model:
            raise ValueError("set LLM_MODEL when LLM_PROVIDER=openai")
        return OpenAILLM(model)
    raise ValueError(f"unknown LLM_PROVIDER {provider!r}; use 'anthropic' or 'openai'")
