from triage_pipeline.agent import (
    CLASSIFY_PROMPT,
    DRAFT_PROMPT,
    URGENCY_PROMPT,
    LLMRetryableError,
    _format_ticket,
)
from triage_pipeline.models import SupportTicketEvent


class TestFormatTicket:
    def test_includes_all_fields(self):
        event = SupportTicketEvent(
            customer_name="Ayşe Yılmaz",
            customer_email="ayse@test.com",
            subject="Order issue",
            message="Where is my order #123?",
        )
        result = _format_ticket(event)
        assert "Ayşe Yılmaz" in result
        assert "ayse@test.com" in result
        assert "Order issue" in result
        assert "Where is my order #123?" in result

    def test_format_structure(self):
        event = SupportTicketEvent(
            customer_name="Test",
            customer_email="t@t.com",
            subject="Sub",
            message="Msg",
        )
        result = _format_ticket(event)
        assert result.startswith("Customer:")
        assert "Subject:" in result
        assert "Message:" in result


class TestClassifyPrompt:
    def test_contains_all_categories(self):
        for cat in [
            "order_issue", "payment_problem", "product_defect",
            "delivery_delay", "account_access", "refund_request",
            "general_inquiry",
        ]:
            assert cat in CLASSIFY_PROMPT

    def test_mentions_language_detection(self):
        assert "language" in CLASSIFY_PROMPT.lower()


class TestUrgencyPrompt:
    def test_contains_all_urgency_levels(self):
        for level in ["low", "medium", "high", "critical"]:
            assert level in URGENCY_PROMPT

    def test_contains_all_actions(self):
        for action in ["auto_respond", "route_to_agent", "escalate", "request_info"]:
            assert action in URGENCY_PROMPT


class TestDraftPrompt:
    def test_mentions_turkish(self):
        assert "Turkish" in DRAFT_PROMPT

    def test_mentions_tone(self):
        assert "tone" in DRAFT_PROMPT.lower()


class TestLLMRetryableError:
    def test_is_exception(self):
        err = LLMRetryableError("429 rate limit")
        assert isinstance(err, Exception)
        assert "429" in str(err)
