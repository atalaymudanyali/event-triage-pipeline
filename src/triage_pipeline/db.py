import psycopg2
import psycopg2.extras

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


def get_recent_results(limit: int = 20) -> list[dict]:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT event_id, customer_name, customer_email, subject,
                       category, urgency, suggested_action, draft_response,
                       language, processed_at
                FROM triage_results
                ORDER BY processed_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_stats() -> dict:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT count(*) as total FROM triage_results")
            total = cur.fetchone()["total"]

            cur.execute(
                """
                SELECT category, count(*) as count
                FROM triage_results GROUP BY category ORDER BY count DESC
                """
            )
            by_category = [dict(row) for row in cur.fetchall()]

            cur.execute(
                """
                SELECT urgency, count(*) as count
                FROM triage_results GROUP BY urgency ORDER BY count DESC
                """
            )
            by_urgency = [dict(row) for row in cur.fetchall()]

            cur.execute(
                """
                SELECT suggested_action, count(*) as count
                FROM triage_results GROUP BY suggested_action ORDER BY count DESC
                """
            )
            by_action = [dict(row) for row in cur.fetchall()]

            return {
                "total_processed": total,
                "by_category": by_category,
                "by_urgency": by_urgency,
                "by_action": by_action,
            }
    finally:
        conn.close()
