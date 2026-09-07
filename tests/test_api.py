from __future__ import annotations

from fastapi.testclient import TestClient

from data_inspect.api import app

client = TestClient(app)


def test_health():
    assert client.get("/health").json()["status"] == "ok"


def test_analyze_ask(messy_csv: str):
    res = client.post("/v1/analyze", json={"source": {"type": "csv", "path": messy_csv}})
    body = res.json()
    assert res.status_code == 200 and body["status"] == "succeeded"
    ask = client.post("/v1/ask", json={"job_id": body["job_id"], "question": "which columns are below 95% fill rate?"})
    assert "ssn" in ask.json()["answer"]


def test_records():
    res = client.post(
        "/v1/analyze",
        json={"source": {"type": "records", "records": [{"id": 1}, {"id": 2, "ssn": None}]}, "summarize": False},
    )
    assert res.json()["profile"]["row_count"] == 2