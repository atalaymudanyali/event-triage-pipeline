import json

from google import genai
from google.genai import types

from triage_pipeline.config import settings
from triage_pipeline.models import SupportTicketEvent, TriageResult

SYSTEM_PROMPT = """\
You are a customer support triage agent for a Turkish e-commerce company.

Your job is to analyze incoming support tickets and produce a structured triage decision.

For each ticket, determine:
1. **category**: The type of issue. Must be one of: order_issue, payment_problem, product_defect, delivery_delay, account_access, refund_request, general_inquiry
2. **urgency**: How urgent this is. Must be one of: low, medium, high, critical
3. **suggested_action**: What should happen next. Must be one of: auto_respond, route_to_agent, escalate, request_info
4. **draft_response**: A brief, professional response to send to the customer (1-3 sentences).
5. **reasoning**: A one-sentence explanation of why you chose this category, urgency, and action.

Urgency guidelines:
- critical: Financial loss (double charges, large refunds), damaged high-value items
- high: Order not delivered, wrong item, product defects
- medium: Refund delays, late deliveries, account access issues
- low: General inquiries, invoice requests, tracking questions

Action guidelines:
- auto_respond: Simple questions with clear answers (tracking, invoices, general info)
- route_to_agent: Issues needing human judgment but not urgent
- escalate: Critical financial issues or repeated failures
- request_info: Not enough information to act on\
"""


def build_user_prompt(event: SupportTicketEvent) -> str:
    return (
        f"Customer: {event.customer_name} ({event.customer_email})\n"
        f"Subject: {event.subject}\n"
        f"Message: {event.message}"
    )


def triage_ticket(event: SupportTicketEvent) -> TriageResult:
    client = genai.Client(api_key=settings.gemini_api_key)

    response = client.models.generate_content(
        model=settings.gemini_model,
        contents=build_user_prompt(event),
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
            response_mime_type="application/json",
            response_schema=TriageResult,
            temperature=0.1,
        ),
    )

    parsed = json.loads(response.text)
    parsed["event_id"] = event.event_id
    return TriageResult.model_validate(parsed)
