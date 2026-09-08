import uuid
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class TicketCategory(StrEnum):
    ORDER_ISSUE = "order_issue"
    PAYMENT_PROBLEM = "payment_problem"
    PRODUCT_DEFECT = "product_defect"
    DELIVERY_DELAY = "delivery_delay"
    ACCOUNT_ACCESS = "account_access"
    REFUND_REQUEST = "refund_request"
    GENERAL_INQUIRY = "general_inquiry"


class Urgency(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class SuggestedAction(StrEnum):
    AUTO_RESPOND = "auto_respond"
    ROUTE_TO_AGENT = "route_to_agent"
    ESCALATE = "escalate"
    REQUEST_INFO = "request_info"


class SupportTicketEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    customer_name: str
    customer_email: str
    subject: str
    message: str
    timestamp: str = Field(
        default_factory=lambda: datetime.now(UTC).isoformat()
    )


class ClassificationResult(BaseModel):
    category: TicketCategory
    language: str
    reasoning: str


class UrgencyAssessment(BaseModel):
    urgency: Urgency
    suggested_action: SuggestedAction
    reasoning: str


class DraftResponse(BaseModel):
    response_text: str
    tone: str


class TriageResult(BaseModel):
    event_id: str
    category: TicketCategory
    urgency: Urgency
    suggested_action: SuggestedAction
    draft_response: str | None = None
    reasoning: str
    language: str = "en"
