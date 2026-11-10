import json

import pytest

from ask_lakehouse.generate import ProposalError, parse_response
from ask_lakehouse.llm import ScriptedLLM
from ask_lakehouse.service import BLOCKED, ERROR, OK, REFUSED, Assistant


def reply(sql=None, assumptions="", refusal=None):
    return json.dumps({"sql": sql, "assumptions": assumptions, "refusal": refusal})


# ---- parsing
def test_parse_plain_fenced_and_chatty_replies():
    body = reply("SELECT 1", "a")
    assert parse_response(body)[0] == "SELECT 1"
    assert parse_response(f"```json\n{body}\n```")[0] == "SELECT 1"
    assert parse_response(f"Sure! Here you go:\n{body}\nHope that helps")[0] == "SELECT 1"


def test_parse_refusal_and_errors():
    assert parse_response(reply(refusal="no data"))[2] == "no data"
    for bad in ["not json", "{}", '{"sql": ""}', "[1, 2]", '{"sql": 5}']:
        with pytest.raises(ProposalError):
            parse_response(bad)


# ---- pipeline
def make(settings, executor, semantic, fn):
    llm = ScriptedLLM(fn)
    return Assistant(semantic, executor, llm, settings), llm


def test_happy_path(settings, executor, semantic):
    a, llm = make(settings, executor, semantic,
                  lambda s, u: reply("SELECT SUM(trips) AS n FROM trips_by_zone_day", "all months"))
    ans = a.ask("How many trips?")
    assert ans.status == OK and ans.columns == ["n"] and ans.rows[0][0] > 0
    assert ans.assumptions == "all months" and ans.attempts == 1 and len(llm.calls) == 1
    assert "trips_by_zone_day" in ans.sql


def test_prompt_contains_schema_metrics_coverage_and_marks_question_untrusted(settings, executor, semantic):
    a, llm = make(settings, executor, semantic, lambda s, u: reply("SELECT 1"))
    a.ask("hello there")
    system, user = llm.calls[0]
    assert "trips_by_zone_day" in user and "SUM(avg_distance_miles * trips)" in user
    assert "2024-06" in user and "<<<\nhello there\n>>>" in user
    assert "untrusted" in system


def test_repair_after_unknown_column(settings, executor, semantic):
    replies = iter([reply("SELECT passenger_age FROM dim_zone"), reply("SELECT COUNT(*) FROM dim_zone")])
    a, llm = make(settings, executor, semantic, lambda s, u: next(replies))
    ans = a.ask("count zones")
    assert ans.status == OK and ans.attempts == 2
    assert "unknown column" in llm.calls[1][1]  # the error was fed back to the model


def test_repair_after_unparseable_reply(settings, executor, semantic):
    replies = iter(["I cannot format this", reply("SELECT COUNT(*) FROM dim_zone")])
    a, _ = make(settings, executor, semantic, lambda s, u: next(replies))
    assert a.ask("count zones").status == OK


def test_gives_up_after_max_repairs(settings, executor, semantic):
    a, llm = make(settings, executor, semantic, lambda s, u: reply("SELECT nope FROM dim_zone"))
    ans = a.ask("x")
    assert ans.status == BLOCKED and ans.attempts == 2 and len(llm.calls) == 2


def test_unsafe_sql_is_blocked_with_no_second_chance(settings, executor, semantic):
    a, llm = make(settings, executor, semantic, lambda s, u: reply("DROP TABLE dim_zone"))
    ans = a.ask("drop it")
    assert ans.status == BLOCKED and ans.attempts == 1 and len(llm.calls) == 1
    assert executor.run("SELECT COUNT(*) FROM dim_zone").rows[0][0] > 0


def test_model_refusal_is_passed_through(settings, executor, semantic):
    a, _ = make(settings, executor, semantic, lambda s, u: reply(refusal="no passenger data"))
    ans = a.ask("passenger age?")
    assert ans.status == REFUSED and ans.message == "no passenger data" and ans.sql is None


def test_database_error_triggers_repair_then_error(settings, executor, semantic):
    a, _ = make(settings, executor, semantic,
                lambda s, u: reply("SELECT CAST('abc' AS INTEGER) FROM dim_zone"))
    ans = a.ask("bad cast")
    assert ans.status == ERROR and ans.attempts == 2


def test_model_outage_is_reported_without_internals(settings, executor, semantic):
    def boom(s, u):
        raise RuntimeError("secret key sk-123 invalid")

    a, _ = make(settings, executor, semantic, boom)
    ans = a.ask("anything")
    assert ans.status == ERROR and "sk-123" not in ans.message


@pytest.mark.parametrize("q", ["", "   ", "x" * 1001])
def test_bad_questions_never_reach_the_model(settings, executor, semantic, q):
    a, llm = make(settings, executor, semantic, lambda s, u: reply("SELECT 1"))
    assert a.ask(q).status == REFUSED and llm.calls == []


def test_every_question_is_logged(settings, executor, semantic):
    a, _ = make(settings, executor, semantic, lambda s, u: reply("SELECT COUNT(*) FROM dim_zone"))
    a.ask("one")
    a.ask("two")
    lines = settings.log_path.read_text().strip().splitlines()
    assert len(lines) == 2 and json.loads(lines[0])["status"] == OK
    assert "rows" in json.loads(lines[0]) and "prompt_version" in json.loads(lines[0])
