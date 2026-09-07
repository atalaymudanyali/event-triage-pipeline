from triage_pipeline.llm import SYSTEM_PROMPT, build_user_prompt
from triage_pipeline.models import SupportTicketEvent


class TestBuildUserPrompt:
    def test_includes_all_fields(self):
        event = SupportTicketEvent(
            customer_name="Mehmet Kaya",
            customer_email="mehmet@test.com",
            subject="Payment charged twice",
            message="I was charged twice for order #123456.",
        )
        prompt = build_user_prompt(event)
        assert "Mehmet Kaya" in prompt
        assert "mehmet@test.com" in prompt
        assert "Payment charged twice" in prompt
        assert "charged twice for order #123456" in prompt

    def test_format_is_structured(self):
        event = SupportTicketEvent(
            customer_name="Test",
            customer_email="test@test.com",
            subject="Subject",
            message="Message body",
        )
        prompt = build_user_prompt(event)
        assert prompt.startswith("Customer:")
        assert "Subject:" in prompt
        assert "Message:" in prompt


class TestSystemPrompt:
    def test_contains_all_categories(self):
        for cat in ["order_issue", "payment_problem", "product_defect",
                     "delivery_delay", "account_access", "refund_request",
                     "general_inquiry"]:
            assert cat in SYSTEM_PROMPT

    def test_contains_all_urgency_levels(self):
        for level in ["low", "medium", "high", "critical"]:
            assert level in SYSTEM_PROMPT

    def test_contains_all_actions(self):
        for action in ["auto_respond", "route_to_agent", "escalate", "request_info"]:
            assert action in SYSTEM_PROMPT
