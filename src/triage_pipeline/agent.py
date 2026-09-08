import json

from google import genai
from google.genai import types

from triage_pipeline.config import settings
from triage_pipeline.models import (
    ClassificationResult,
    DraftResponse,
    SupportTicketEvent,
    TriageResult,
    UrgencyAssessment,
)

CLASSIFY_PROMPT = """\
You are a ticket classifier for a Turkish e-commerce company.

Analyze the support ticket and determine:
1. **category**: Must be one of: order_issue, payment_problem, product_defect, \
delivery_delay, account_access, refund_request, general_inquiry
2. **language**: The language the customer wrote in (e.g. "tr", "en")
3. **reasoning**: One sentence explaining your classification.\
"""

URGENCY_PROMPT = """\
You are an urgency assessor for a Turkish e-commerce support team.

Given a support ticket and its classification, determine:
1. **urgency**: Must be one of: low, medium, high, critical
2. **suggested_action**: Must be one of: auto_respond, route_to_agent, escalate, request_info

Urgency guidelines:
- critical: Financial loss (double charges, large refunds), damaged high-value items
- high: Order not delivered, wrong item, product defects
- medium: Refund delays, late deliveries, account access issues
- low: General inquiries, invoice requests, tracking questions

Action guidelines:
- auto_respond: Simple questions with clear answers (tracking, invoices, general info)
- route_to_agent: Issues needing human judgment but not urgent
- escalate: Critical financial issues or repeated failures
- request_info: Not enough information to act on

3. **reasoning**: One sentence explaining your urgency and action choice.\
"""

DRAFT_PROMPT = """\
You are a customer support agent drafting a response for a Turkish e-commerce company.

Write a brief, professional response (1-3 sentences) to the customer.
- If the customer wrote in Turkish, respond in Turkish.
- If the customer wrote in English, respond in English.
- Be empathetic and specific to their issue.
- Reference their order number if mentioned.

Also indicate the **tone** you used: "empathetic", "informational", or "urgent".\
"""


def _call_gemini(system_prompt: str, user_content: str, schema: type):
    client = genai.Client(api_key=settings.gemini_api_key)
    response = client.models.generate_content(
        model=settings.gemini_model,
        contents=user_content,
        config=types.GenerateContentConfig(
            system_instruction=system_prompt,
            response_mime_type="application/json",
            response_schema=schema,
            temperature=0.1,
        ),
    )
    return json.loads(response.text)


def _format_ticket(event: SupportTicketEvent) -> str:
    return (
        f"Customer: {event.customer_name} ({event.customer_email})\n"
        f"Subject: {event.subject}\n"
        f"Message: {event.message}"
    )


def step_classify(event: SupportTicketEvent) -> ClassificationResult:
    raw = _call_gemini(CLASSIFY_PROMPT, _format_ticket(event), ClassificationResult)
    return ClassificationResult.model_validate(raw)


def step_assess_urgency(
    event: SupportTicketEvent, classification: ClassificationResult
) -> UrgencyAssessment:
    context = (
        f"{_format_ticket(event)}\n\n"
        f"Classification: {classification.category.value}\n"
        f"Language: {classification.language}\n"
        f"Classification reasoning: {classification.reasoning}"
    )
    raw = _call_gemini(URGENCY_PROMPT, context, UrgencyAssessment)
    return UrgencyAssessment.model_validate(raw)


def step_draft_response(
    event: SupportTicketEvent,
    classification: ClassificationResult,
    urgency: UrgencyAssessment,
) -> DraftResponse:
    context = (
        f"{_format_ticket(event)}\n\n"
        f"Category: {classification.category.value}\n"
        f"Language: {classification.language}\n"
        f"Urgency: {urgency.urgency.value}\n"
        f"Action: {urgency.suggested_action.value}"
    )
    raw = _call_gemini(DRAFT_PROMPT, context, DraftResponse)
    return DraftResponse.model_validate(raw)


def triage_ticket(event: SupportTicketEvent) -> TriageResult:
    classification = step_classify(event)
    urgency = step_assess_urgency(event, classification)
    draft = step_draft_response(event, classification, urgency)

    return TriageResult(
        event_id=event.event_id,
        category=classification.category,
        urgency=urgency.urgency,
        suggested_action=urgency.suggested_action,
        draft_response=draft.response_text,
        reasoning=f"{classification.reasoning} {urgency.reasoning}",
        language=classification.language,
    )
