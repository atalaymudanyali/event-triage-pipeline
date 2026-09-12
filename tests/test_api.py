from fastapi.testclient import TestClient

from triage_pipeline.api import app

client = TestClient(app)


class TestHealthEndpoint:
    def test_health(self):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


class TestOpenAPIDocs:
    def test_docs_available(self):
        response = client.get("/openapi.json")
        assert response.status_code == 200
        schema = response.json()
        assert schema["info"]["title"] == "Triage Pipeline API"
        assert "/events" in schema["paths"]
        assert "/stats" in schema["paths"]


class TestMetricsEndpoint:
    def test_metrics_returns_200(self):
        response = client.get("/metrics")
        assert response.status_code == 200

    def test_metrics_contains_http_counters(self):
        client.get("/health")
        response = client.get("/metrics")
        assert "http_requests_total" in response.text
