import psycopg2

from triage_pipeline.config import settings
from triage_pipeline.models import SupportTicketEvent, TriageResult


def get_connection():
    return psycopg2.connect(settings.postgres_dsn)


def save_triage_result(
    event: SupportTicketEvent, result: TriageResult
) -> bool:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO triage_results
                    (event_id, customer_name, customer_email, subject, message,
                     category, urgency, suggested_action, draft_response, language)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    result.event_id,
                    event.customer_name,
                    event.customer_email,
                    event.subject,
                    event.message,
                    result.category.value,
                    result.urgency.value,
                    result.suggested_action.value,
                    result.draft_response,
                    result.language,
                ),
            )
            inserted = cur.rowcount > 0
        conn.commit()
        return inserted
    finally:
        conn.close()
