from unittest.mock import patch

from fastapi.testclient import TestClient

from triage_pipeline.api import app

client = TestClient(app)

FAKE_TICKET = {
    "event_id": "test-123",
    "customer_name": "Alice",
    "customer_email": "alice@example.com",
    "subject": "Order issue",
    "message": "My order hasn't arrived.",
    "status": "pending",
    "category": None,
    "urgency": None,
    "suggested_action": None,
    "draft_response": None,
    "reasoning": None,
    "language": None,
    "created_at": "2025-01-01T00:00:00",
    "processed_at": None,
}


class TestCreateTicket:
    @patch("triage_pipeline.api.create_ticket", return_value=FAKE_TICKET)
    def test_create_returns_201(self, mock_create):
        response = client.post(
            "/api/tickets",
            json={
                "customer_name": "Alice",
                "customer_email": "alice@example.com",
                "subject": "Order issue",
                "message": "My order hasn't arrived.",
            },
        )
        assert response.status_code == 201
        assert response.json()["event_id"] == "test-123"
        assert response.json()["status"] == "pending"
        mock_create.assert_called_once()

    def test_create_rejects_missing_fields(self):
        response = client.post(
            "/api/tickets",
            json={"customer_name": "Alice"},
        )
        assert response.status_code == 422


class TestGenerateTicket:
    @patch("triage_pipeline.api.create_ticket", return_value=FAKE_TICKET)
    @patch("triage_pipeline.api.generate_ticket")
    def test_generate_returns_201(self, mock_gen, mock_create):
        response = client.post("/api/tickets/generate")
        assert response.status_code == 201
        mock_gen.assert_called_once()
        mock_create.assert_called_once()


class TestListTickets:
    @patch("triage_pipeline.api.get_tickets", return_value=[FAKE_TICKET])
    def test_list_returns_tickets(self, mock_list):
        response = client.get("/api/tickets")
        assert response.status_code == 200
        assert len(response.json()) == 1
        mock_list.assert_called_once_with(status=None, limit=50)

    @patch("triage_pipeline.api.get_tickets", return_value=[])
    def test_list_with_status_filter(self, mock_list):
        client.get("/api/tickets?status=pending")
        mock_list.assert_called_once_with(status="pending", limit=50)


class TestTicketStats:
    @patch(
        "triage_pipeline.api.get_ticket_stats",
        return_value={
            "total": 5,
            "pending": 2,
            "processed": 3,
            "failed": 0,
            "by_category": [],
            "by_urgency": [],
        },
    )
    def test_stats_returns_counts(self, mock_stats):
        response = client.get("/api/tickets/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 5
        assert data["pending"] == 2


class TestGetTicket:
    @patch("triage_pipeline.api.get_ticket", return_value=FAKE_TICKET)
    def test_get_existing_ticket(self, mock_get):
        response = client.get("/api/tickets/test-123")
        assert response.status_code == 200
        assert response.json()["event_id"] == "test-123"

    @patch("triage_pipeline.api.get_ticket", return_value=None)
    def test_get_missing_ticket_404(self, mock_get):
        response = client.get("/api/tickets/nonexistent")
        assert response.status_code == 404


class TestProcessTicket:
    @patch("triage_pipeline.api.get_ticket", return_value=None)
    def test_process_missing_ticket_404(self, mock_get):
        response = client.post("/api/tickets/nonexistent/process")
        assert response.status_code == 404

    @patch(
        "triage_pipeline.api.get_ticket",
        return_value={**FAKE_TICKET, "status": "processed"},
    )
    def test_process_already_processed_400(self, mock_get):
        response = client.post("/api/tickets/test-123/process")
        assert response.status_code == 400
        assert "already processed" in response.json()["detail"]
