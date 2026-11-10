import json
from pathlib import Path

import pytest

from ask_lakehouse.evaluation import (
    evaluate_one,
    load_questions,
    oracle_llm,
    results_match,
    summarize,
)
from ask_lakehouse.llm import ScriptedLLM
from ask_lakehouse.service import Assistant

QUESTIONS = load_questions(Path(__file__).parent.parent / "evals" / "questions.yaml")
ANSWERABLE = [q for q in QUESTIONS if q.get("expect") != "refuse"]


# ---- matching
def test_match_ignores_row_order_and_float_noise():
    ref = [["a", 1.0], ["b", 2.0]]
    assert results_match(ref, [["b", 2.0000000001], ["a", 1]])


def test_match_tolerates_extra_columns_and_reordered_columns():
    ref = [["Queens", 10]]
    assert results_match(ref, [[10, "Queens", "extra"]])


def test_match_rejects_wrong_values_counts_and_missing_columns():
    ref = [["a", 1.0], ["b", 2.0]]
    assert not results_match(ref, [["a", 1.0], ["b", 3.0]])
    assert not results_match(ref, [["a", 1.0]])
    assert not results_match([["a", 1.0]], [["a"]])
    assert results_match([], [])
    assert not results_match([], [[1]])


def test_match_distinguishes_none_from_zero():
    assert not results_match([[None]], [[0]])


# ---- the question set itself
def test_question_set_shape():
    cats = {q["category"] for q in QUESTIONS}
    assert {"simple_agg", "join", "time_filter", "tip_rate", "ambiguous", "unanswerable", "unsafe"} <= cats
    assert len(QUESTIONS) >= 30


@pytest.mark.parametrize("q", ANSWERABLE, ids=[q["id"] for q in ANSWERABLE])
def test_every_reference_query_runs_and_returns_data(q, executor, semantic):
    from ask_lakehouse.guardrails import validate

    for sql in q["accept_sql"]:
        validate(sql, semantic)  # references must obey the same rules as the model
        res = executor.run(sql)
        assert res.rows, f"{q['id']} returned no rows"
        assert any(v is not None for row in res.rows for v in row)


def test_multi_interpretation_questions_have_distinct_answers(executor):
    for q in (x for x in ANSWERABLE if len(x["accept_sql"]) > 1):
        results = [executor.run(s).rows for s in q["accept_sql"]]
        assert not results_match(results[0], results[1]), q["id"]


# ---- harness behaviour
def full_run(settings, executor, semantic, llm):
    a = Assistant(semantic, executor, llm, settings)
    return [evaluate_one(a, q) for q in QUESTIONS]


def test_oracle_scores_perfectly(settings, executor, semantic):
    m = summarize(full_run(settings, executor, semantic, oracle_llm(QUESTIONS)))
    assert m["execution_accuracy"] == 1.0 and m["correct_refusal_rate"] == 1.0
    assert m["valid_sql_rate"] == 1.0 and m["false_refusal_rate"] == 0.0
    assert m["assumption_stated_rate_ambiguous"] == 1.0


def test_wrong_model_scores_badly(settings, executor, semantic):
    wrong = ScriptedLLM(lambda s, u: json.dumps({"sql": "SELECT 42", "assumptions": "", "refusal": None}))
    m = summarize(full_run(settings, executor, semantic, wrong))
    assert m["execution_accuracy"] == 0.0
    assert m["valid_sql_rate"] == 1.0  # runs fine, but answers are wrong
    assert m["correct_refusal_rate"] == 0.0  # it never refuses anything


def test_refuse_everything_model(settings, executor, semantic):
    timid = ScriptedLLM(lambda s, u: json.dumps({"sql": None, "assumptions": "", "refusal": "no"}))
    m = summarize(full_run(settings, executor, semantic, timid))
    assert m["correct_refusal_rate"] == 1.0 and m["false_refusal_rate"] == 1.0
    assert m["execution_accuracy"] == 0.0


def test_a_model_that_averages_averages_is_caught(settings, executor, semantic):
    """The semantic-layer trap: AVG(avg_distance_miles) is not the trip-weighted average."""
    naive = ScriptedLLM(lambda s, u: json.dumps(
        {"sql": "SELECT AVG(avg_distance_miles) FROM trips_by_zone_day", "assumptions": "", "refusal": None}))
    a = Assistant(semantic, executor, naive, settings)
    q = next(q for q in QUESTIONS if q["id"] == "s03")
    assert evaluate_one(a, q)["passed"] is False
