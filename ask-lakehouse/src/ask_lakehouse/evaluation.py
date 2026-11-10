"""Eval harness: golden questions scored by comparing RESULT SETS, not SQL text.

Two different queries can both be right, so an answer passes when the data it returns matches
the data returned by a reference query. Extra columns are tolerated, row order is ignored, and
numbers are compared with a small relative tolerance.

Refusal questions (unanswerable or unsafe) pass when nothing was executed: the model refused, or
the guardrails blocked the SQL.
"""
from __future__ import annotations

import dataclasses
import json
import math
import statistics
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import yaml

from ask_lakehouse.config import Settings
from ask_lakehouse.execute import Executor
from ask_lakehouse.llm import ScriptedLLM
from ask_lakehouse.prompt import PROMPT_VERSION
from ask_lakehouse.semantic import load_semantic
from ask_lakehouse.service import BLOCKED, OK, REFUSED, Assistant

REFUSE_STATUSES = {REFUSED, BLOCKED}


# --------------------------------------------------------------------------- questions
def load_questions(path: Path | str) -> list[dict]:
    questions = yaml.safe_load(Path(path).read_text())
    seen: set[str] = set()
    for q in questions:
        for key in ("id", "category", "question"):
            if key not in q:
                raise ValueError(f"question missing '{key}': {q}")
        if q["id"] in seen:
            raise ValueError(f"duplicate question id {q['id']}")
        seen.add(q["id"])
        if q.get("expect") != "refuse" and not q.get("accept_sql"):
            raise ValueError(f"{q['id']}: needs accept_sql or expect: refuse")
    return questions


# --------------------------------------------------------------------------- matching
def _norm(v):
    if v is None:
        return None
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, (int, float)):
        return float(v)
    return str(v)


def _key(v):
    if v is None:
        return (0, 0.0, "")
    if isinstance(v, float):
        return (1, v, "")
    return (2, 0.0, v)


def _cells_equal(a, b) -> bool:
    if a is None or b is None:
        return a is b
    if isinstance(a, float) and isinstance(b, float):
        return math.isclose(a, b, rel_tol=1e-6, abs_tol=1e-9)
    return a == b


def _seq_equal(x, y) -> bool:
    return len(x) == len(y) and all(_cells_equal(a, b) for a, b in zip(x, y, strict=True))


def _column(rows, i) -> list:
    return sorted((_norm(r[i]) for r in rows), key=_key)


def results_match(reference: list[list], candidate: list[list]) -> bool:
    """True if the candidate contains every reference column (any order, extras allowed) and the
    same rows (any order)."""
    if len(reference) != len(candidate):
        return False
    if not reference:
        return True
    n_ref, n_cand = len(reference[0]), len(candidate[0])
    if n_cand < n_ref:
        return False

    used: set[int] = set()
    mapping: list[int] = []
    for i in range(n_ref):
        ref_col = _column(reference, i)
        j = next(
            (j for j in range(n_cand) if j not in used and _seq_equal(ref_col, _column(candidate, j))),
            None,
        )
        if j is None:
            return False
        used.add(j)
        mapping.append(j)

    def row_key(t):
        return tuple(_key(c) for c in t)

    ref_rows = sorted((tuple(_norm(r[i]) for i in range(n_ref)) for r in reference), key=row_key)
    cand_rows = sorted((tuple(_norm(r[j]) for j in mapping) for r in candidate), key=row_key)
    return all(_seq_equal(a, b) for a, b in zip(ref_rows, cand_rows, strict=True))


# --------------------------------------------------------------------------- running
def evaluate_one(assistant: Assistant, q: dict) -> dict:
    ans = assistant.ask(q["question"])
    detail = {
        "id": q["id"], "category": q["category"], "question": q["question"],
        "status": ans.status, "sql": ans.sql, "message": ans.message,
        "assumptions": ans.assumptions, "attempts": ans.attempts, "latency_ms": ans.latency_ms,
        "input_tokens": ans.input_tokens, "output_tokens": ans.output_tokens,
    }
    if q.get("expect") == "refuse":
        detail.update(kind="refuse", passed=ans.status in REFUSE_STATUSES)
        return detail

    matched = False
    if ans.status == OK:
        for ref_sql in q["accept_sql"]:
            reference = assistant.executor.run(ref_sql)  # a broken reference should fail loudly
            if results_match(reference.rows, ans.rows):
                matched = True
                break
    detail.update(
        kind="answer", executed=ans.status == OK, passed=matched,
        assumption_stated=bool(ans.assumptions.strip()),
    )
    return detail


def _pct(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    return sorted_vals[min(len(sorted_vals) - 1, math.ceil(p * len(sorted_vals)) - 1)]


def summarize(details: list[dict]) -> dict:
    answer = [d for d in details if d["kind"] == "answer"]
    refuse = [d for d in details if d["kind"] == "refuse"]
    ambiguous = [d for d in answer if d["category"] == "ambiguous"]
    lat = sorted(d["latency_ms"] for d in details)

    def rate(items, pred) -> float | None:
        return round(sum(1 for d in items if pred(d)) / len(items), 4) if items else None

    by_category: dict[str, dict] = {}
    for cat in sorted({d["category"] for d in details}):
        items = [d for d in details if d["category"] == cat]
        by_category[cat] = {"n": len(items), "pass_rate": rate(items, lambda d: d["passed"])}

    return {
        "n_questions": len(details),
        "execution_accuracy": rate(answer, lambda d: d["passed"]),
        "valid_sql_rate": rate(answer, lambda d: d["executed"]),
        "false_refusal_rate": rate(answer, lambda d: d["status"] == REFUSED),
        "correct_refusal_rate": rate(refuse, lambda d: d["passed"]),
        "assumption_stated_rate_ambiguous": rate(ambiguous, lambda d: d["assumption_stated"]),
        "avg_attempts": round(statistics.mean(d["attempts"] for d in details), 2) if details else 0,
        "latency_ms_p50": _pct(lat, 0.5),
        "latency_ms_p95": _pct(lat, 0.95),
        "input_tokens_total": sum(d["input_tokens"] for d in details),
        "output_tokens_total": sum(d["output_tokens"] for d in details),
        "by_category": by_category,
    }


def oracle_llm(questions: list[dict]) -> ScriptedLLM:
    """Answers with the reference SQL. Proves the harness works; says nothing about a real model."""

    def reply(_system: str, user: str) -> str:
        for q in questions:
            if f"<<<\n{q['question']}\n>>>" in user:
                if q.get("expect") == "refuse":
                    return json.dumps({"sql": None, "assumptions": "", "refusal": "cannot help with that"})
                assumption = "oracle interpretation" if q["category"] == "ambiguous" else ""
                return json.dumps({"sql": q["accept_sql"][0], "assumptions": assumption, "refusal": None})
        raise ValueError("oracle: question not found in prompt")

    return ScriptedLLM(reply, name="oracle")


def format_summary(label: str, metrics: dict) -> str:
    def pct(v):
        return "n/a" if v is None else f"{v:.1%}"

    lines = [
        f"Run: {label}   questions: {metrics['n_questions']}",
        f"  execution accuracy        {pct(metrics['execution_accuracy'])}",
        f"  valid SQL rate            {pct(metrics['valid_sql_rate'])}",
        f"  false refusal rate        {pct(metrics['false_refusal_rate'])}",
        f"  correct refusal rate      {pct(metrics['correct_refusal_rate'])}",
        f"  assumptions (ambiguous)   {pct(metrics['assumption_stated_rate_ambiguous'])}",
        f"  latency p50 / p95         {metrics['latency_ms_p50']:.0f} / {metrics['latency_ms_p95']:.0f} ms",
        f"  tokens in / out           {metrics['input_tokens_total']} / {metrics['output_tokens_total']}",
        "  by category:",
    ]
    lines += [f"    {c:12s} n={v['n']:<3d} pass {pct(v['pass_rate'])}" for c, v in metrics["by_category"].items()]
    return "\n".join(lines)


def run_cli(settings: Settings, label: str, questions_path: str, out_dir: str, llm_kind: str) -> dict:
    questions = load_questions(questions_path)
    if llm_kind == "oracle":
        semantic = load_semantic(settings.semantic_path)
        executor = Executor(settings.db_path, settings.max_rows, settings.query_timeout_s)
        assistant = Assistant(
            semantic, executor, oracle_llm(questions), dataclasses.replace(settings, log_path=None)
        )
    else:
        assistant = Assistant.from_settings(
            dataclasses.replace(settings, log_path=Path("logs/evals.jsonl"))
        )

    details = []
    for i, q in enumerate(questions, 1):
        d = evaluate_one(assistant, q)
        details.append(d)
        print(f"[{i}/{len(questions)}] {'PASS' if d['passed'] else 'FAIL'} {d['id']} ({d['status']})")
    metrics = summarize(details)
    record = {
        "label": label,
        "prompt_version": PROMPT_VERSION,
        "model": assistant.llm.name,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "max_repairs": settings.max_repairs,
        "metrics": metrics,
        "details": details,
    }
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{label}.json").write_text(json.dumps(record, indent=2))
    print("\n" + format_summary(label, metrics))
    failed = [d for d in details if not d["passed"]]
    if failed:
        print("\nfailed:", ", ".join(d["id"] for d in failed))
    return record


def chart_results(results_dir: str, out_png: str) -> None:
    runs = sorted(
        (json.loads(p.read_text()) for p in Path(results_dir).glob("*.json")),
        key=lambda r: r["timestamp"],
    )
    if not runs:
        print(f"no result files in {results_dir}")
        return
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed; pip install matplotlib")
        return
    labels = [r["label"] for r in runs]
    fig, ax = plt.subplots(figsize=(max(6, 1.4 * len(runs) + 3), 4))
    for key, name in [
        ("execution_accuracy", "execution accuracy"),
        ("correct_refusal_rate", "correct refusals"),
        ("valid_sql_rate", "valid SQL"),
    ]:
        ax.plot(labels, [r["metrics"][key] for r in runs], marker="o", label=name)
    ax.set_ylim(0, 1.05)
    ax.set_ylabel("rate")
    ax.set_title("Eval results by run")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.autofmt_xdate()
    fig.tight_layout()
    Path(out_png).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=150)
    plt.close(fig)
    print(f"wrote {out_png}")
