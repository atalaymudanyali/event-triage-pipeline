import pytest
from pydantic import ValidationError

from triage_pipeline.models import (
    ClassificationResult,
    DraftResponse,
    SuggestedAction,
    TicketCategory,
    TriageResult,
    Urgency,
    UrgencyAssessment,
)


class TestClassificationResult:
    def test_valid(self):
        result = ClassificationResult(
            category=TicketCategory.ORDER_ISSUE,
            language="tr",
            reasoning="Customer complains about missing order",
        )
        assert result.category == "order_issue"
        assert result.language == "tr"

    def test_rejects_invalid_category(self):
        with pytest.raises(ValidationError):
            ClassificationResult(
                category="not_valid",
                language="en",
                reasoning="test",
            )


class TestUrgencyAssessment:
    def test_valid(self):
        result = UrgencyAssessment(
            urgency=Urgency.CRITICAL,
            suggested_action=SuggestedAction.ESCALATE,
            reasoning="Financial loss — double charge",
        )
        assert result.urgency == "critical"
        assert result.suggested_action == "escalate"

    def test_rejects_invalid_urgency(self):
        with pytest.raises(ValidationError):
            UrgencyAssessment(
                urgency="super_urgent",
                suggested_action=SuggestedAction.ESCALATE,
                reasoning="test",
            )


class TestDraftResponse:
    def test_valid(self):
        draft = DraftResponse(
            response_text="We apologize for the inconvenience.",
            tone="empathetic",
        )
        assert draft.response_text == "We apologize for the inconvenience."
        assert draft.tone == "empathetic"


class TestTriageResultLanguage:
    def test_default_language(self):
        result = TriageResult(
            event_id="abc",
            category=TicketCategory.GENERAL_INQUIRY,
            urgency=Urgency.LOW,
            suggested_action=SuggestedAction.AUTO_RESPOND,
            reasoning="test",
        )
        assert result.language == "en"

    def test_custom_language(self):
        result = TriageResult(
            event_id="abc",
            category=TicketCategory.ORDER_ISSUE,
            urgency=Urgency.HIGH,
            suggested_action=SuggestedAction.ROUTE_TO_AGENT,
            reasoning="test",
            language="tr",
        )
        assert result.language == "tr"
