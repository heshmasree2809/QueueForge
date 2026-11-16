from fastapi.testclient import TestClient

from app.config import settings
from app.main import app


def test_create_read_and_idempotency():
    with TestClient(app) as client:
        body = {"type": "echo", "payload": {"x": 1}, "idempotency_key": "k1"}
        r1 = client.post("/api/v1/jobs", json=body)
        r2 = client.post("/api/v1/jobs", json=body)
        assert r1.status_code == 201 and r2.status_code == 200
        assert r1.json()["id"] == r2.json()["id"]
        got = client.get(f"/api/v1/jobs/{r1.json()['id']}")
        assert got.status_code == 200 and got.json()["status"] == "queued"
        assert client.get("/api/v1/jobs/999999").status_code == 44


def test_validation_rejects_bad_input():
    with TestClient(app) as client:
        assert client.post("/api/v1/jobs", json={"type": "nope"}).status_code == 422
        assert client.post("/api/v1/jobs", json={"type": "echo", "queue": "bad queue!"}).status_code == 422
        assert client.post("/api/v1/jobs", json={"type": "echo", "max_attempts": 0}).status_code == 422


def test_api_key_is_enforced_when_configured(monkeypatch):
    monkeypatch.setattr(settings, "API_KEY", "s3cret")
    with TestClient(app) as client:
        assert client.post("/api/v1/jobs", json={"type": "echo"}).status_code == 401
        ok = client.post("/api/v1/jobs", json={"type": "echo"}, headers={"X-API-Key": "s3cret"})
        assert ok.status_code == 201
        assert client.get("/api/v1/stats").status_code == 200  # reads stay open


def test_stats_and_health():
    with TestClient(app) as client:
        client.post("/api/v1/jobs", json={"type": "echo"})
        stats = client.get("/api/v1/stats").json()
        assert stats["counts"]["queued"] == 1
        assert client.get("/health").json() == {"status": "ok"}
