import json

import pytest
from pydantic import ValidationError

from triage_pipeline.models import (
    SuggestedAction,
    SupportTicketEvent,
    TicketCategory,
    TriageResult,
    Urgency,
)


class TestSupportTicketEvent:
    def test_creates_with_auto_fields(self):
        event = SupportTicketEvent(
            customer_name="Test User",
            customer_email="test@example.com",
            subject="Test subject",
            message="Test message",
        )
        assert event.event_id
        assert event.timestamp
        assert event.customer_name == "Test User"

    def test_unique_event_ids(self):
        events = [
            SupportTicketEvent(
                customer_name="A",
                customer_email="a@test.com",
                subject="S",
                message="M",
            )
            for _ in range(100)
        ]
        ids = {e.event_id for e in events}
        assert len(ids) == 100

    def test_json_roundtrip(self):
        event = SupportTicketEvent(
            customer_name="Ayşe Yılmaz",
            customer_email="ayse@test.com",
            subject="Order issue",
            message="Where is my order?",
        )
        json_str = json.dumps(event.model_dump(), ensure_ascii=False)
        restored = SupportTicketEvent.model_validate(json.loads(json_str))
        assert restored.customer_name == "Ayşe Yılmaz"
        assert restored.event_id == event.event_id

    def test_rejects_missing_fields(self):
        with pytest.raises(ValidationError):
            SupportTicketEvent(customer_name="A", customer_email="a@test.com")


class TestTriageResult:
    def test_valid_result(self):
        result = TriageResult(
            event_id="abc-123",
            category=TicketCategory.ORDER_ISSUE,
            urgency=Urgency.HIGH,
            suggested_action=SuggestedAction.ROUTE_TO_AGENT,
            reasoning="Order not delivered after 10 days",
        )
        assert result.category == "order_issue"
        assert result.draft_response is None

    def test_with_draft_response(self):
        result = TriageResult(
            event_id="abc-123",
            category=TicketCategory.GENERAL_INQUIRY,
            urgency=Urgency.LOW,
            suggested_action=SuggestedAction.AUTO_RESPOND,
            draft_response="You can track your order at...",
            reasoning="Simple tracking question",
        )
        assert result.draft_response == "You can track your order at..."

    def test_rejects_invalid_category(self):
        with pytest.raises(ValidationError):
            TriageResult(
                event_id="abc-123",
                category="not_a_category",
                urgency=Urgency.LOW,
                suggested_action=SuggestedAction.AUTO_RESPOND,
                reasoning="test",
            )

    def test_serializes_enums_as_strings(self):
        result = TriageResult(
            event_id="abc-123",
            category=TicketCategory.PAYMENT_PROBLEM,
            urgency=Urgency.CRITICAL,
            suggested_action=SuggestedAction.ESCALATE,
            reasoning="Double charge",
        )
        data = result.model_dump()
        assert data["category"] == "payment_problem"
        assert data["urgency"] == "critical"
        assert data["suggested_action"] == "escalate"

    def test_json_roundtrip(self):
        result = TriageResult(
            event_id="abc-123",
            category=TicketCategory.REFUND_REQUEST,
            urgency=Urgency.MEDIUM,
            suggested_action=SuggestedAction.ROUTE_TO_AGENT,
            draft_response="We're looking into your refund.",
            reasoning="Refund not processed after 2 weeks",
        )
        json_str = json.dumps(result.model_dump())
        restored = TriageResult.model_validate(json.loads(json_str))
        assert restored.category == TicketCategory.REFUND_REQUEST
        assert restored.draft_response == result.draft_response
