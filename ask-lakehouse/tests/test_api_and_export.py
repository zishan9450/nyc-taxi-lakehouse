import json

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from ask_lakehouse.api import create_app
from ask_lakehouse.llm import ScriptedLLM
from ask_lakehouse.service import Assistant


def client(settings, executor, semantic, sql="SELECT COUNT(*) AS n FROM dim_zone"):
    llm = ScriptedLLM(lambda s, u: json.dumps({"sql": sql, "assumptions": "", "refusal": None}))
    return TestClient(create_app(Assistant(semantic, executor, llm, settings)))


def test_ask_endpoint(settings, executor, semantic):
    r = client(settings, executor, semantic).post("/ask", json={"question": "how many zones?"})
    body = r.json()
    assert r.status_code == 200 and body["status"] == "ok" and body["rows"][0][0] > 0


def test_blocked_sql_returns_200_with_status_blocked(settings, executor, semantic):
    r = client(settings, executor, semantic, sql="DROP TABLE dim_zone").post("/ask", json={"question": "x"})
    assert r.status_code == 200 and r.json()["status"] == "blocked"


def test_request_validation(settings, executor, semantic):
    c = client(settings, executor, semantic)
    assert c.post("/ask", json={"question": ""}).status_code == 422
    assert c.post("/ask", json={"question": "x" * 1001}).status_code == 422
    assert c.post("/ask", json={}).status_code == 422


def test_health_schema_and_index(settings, executor, semantic):
    c = client(settings, executor, semantic)
    assert c.get("/health").json()["status"] == "ok"
    assert "trips_by_zone_day" in c.get("/schema").json()["schema"]
    assert "Ask your lakehouse" in c.get("/").text


def test_export_gold_from_a_delta_lake(tmp_path):
    """Round trip: write Delta tables the way the pipeline does, export, build, validate."""
    deltalake = pytest.importorskip("deltalake")
    from ask_lakehouse.demo_data import build_gold
    from ask_lakehouse.execute import build_database, schema_problems
    from ask_lakehouse.export import export_gold
    from ask_lakehouse.semantic import load_semantic

    lake = tmp_path / "lake"
    for name, table in build_gold().items():
        deltalake.write_deltalake(str(lake / "gold" / name), table)
    written = export_gold(lake, tmp_path / "out")
    assert len(written) == 4 and pq.read_table(written[0]).num_rows > 0

    build_database(tmp_path / "out", tmp_path / "x.duckdb")
    sem = load_semantic(__import__("pathlib").Path(__file__).parent.parent / "semantic" / "nyc_taxi.yaml")
    assert schema_problems(tmp_path / "x.duckdb", sem) == []


def test_export_reports_missing_pipeline_output(tmp_path):
    pytest.importorskip("deltalake")
    from ask_lakehouse.export import export_gold

    with pytest.raises(FileNotFoundError, match="run the lakehouse pipeline"):
        export_gold(tmp_path / "nothing", tmp_path / "out")
