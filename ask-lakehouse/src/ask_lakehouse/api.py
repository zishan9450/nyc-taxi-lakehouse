from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from ask_lakehouse.service import Assistant

_INDEX = Path(__file__).parent / "static" / "index.html"


class AskRequest(BaseModel):
    question: str = Field(min_length=1, max_length=1000)


def create_app(assistant: Assistant) -> FastAPI:
    app = FastAPI(title="Ask your lakehouse")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "model": assistant.llm.name}

    @app.get("/schema")
    def schema() -> dict:
        return {"schema": assistant.semantic.render()}

    @app.post("/ask")
    def ask(req: AskRequest) -> dict:
        return assistant.ask(req.question).to_dict()

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return _INDEX.read_text(encoding="utf-8")

    return app
