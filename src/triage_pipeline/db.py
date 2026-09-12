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


def create_ticket(event: SupportTicketEvent) -> dict:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO tickets
                    (event_id, customer_name, customer_email, subject, message)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    event.event_id,
                    event.customer_name,
                    event.customer_email,
                    event.subject,
                    event.message,
                ),
            )
            row = dict(cur.fetchone())
        conn.commit()
        for col in ("created_at", "processed_at"):
            if row.get(col):
                row[col] = row[col].isoformat()
        return row
    finally:
        conn.close()


def get_tickets(status: str | None = None, limit: int = 50) -> list[dict]:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if status:
                cur.execute(
                    """
                    SELECT * FROM tickets
                    WHERE status = %s
                    ORDER BY created_at DESC LIMIT %s
                    """,
                    (status, limit),
                )
            else:
                cur.execute(
                    "SELECT * FROM tickets ORDER BY created_at DESC LIMIT %s",
                    (limit,),
                )
            rows = [dict(row) for row in cur.fetchall()]
        for r in rows:
            for col in ("created_at", "processed_at"):
                if r.get(col):
                    r[col] = r[col].isoformat()
        return rows
    finally:
        conn.close()


def get_ticket(event_id: str) -> dict | None:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM tickets WHERE event_id = %s", (event_id,))
            row = cur.fetchone()
        if row:
            row = dict(row)
            for col in ("created_at", "processed_at"):
                if row.get(col):
                    row[col] = row[col].isoformat()
        return row
    finally:
        conn.close()


def update_ticket_result(event_id: str, result: TriageResult):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tickets SET
                    status = 'processed',
                    category = %s,
                    urgency = %s,
                    suggested_action = %s,
                    draft_response = %s,
                    reasoning = %s,
                    language = %s,
                    processed_at = NOW()
                WHERE event_id = %s
                """,
                (
                    result.category.value,
                    result.urgency.value,
                    result.suggested_action.value,
                    result.draft_response,
                    result.reasoning,
                    result.language,
                    event_id,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def update_ticket_status(event_id: str, status: str):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE tickets SET status = %s WHERE event_id = %s",
                (status, event_id),
            )
        conn.commit()
    finally:
        conn.close()


def get_ticket_stats() -> dict:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT count(*) as total FROM tickets")
            total = cur.fetchone()["total"]

            cur.execute(
                "SELECT status, count(*) as count FROM tickets GROUP BY status"
            )
            by_status = {row["status"]: row["count"] for row in cur.fetchall()}

            cur.execute(
                """
                SELECT category, count(*) as count
                FROM tickets WHERE category IS NOT NULL
                GROUP BY category ORDER BY count DESC
                """
            )
            by_category = [dict(row) for row in cur.fetchall()]

            cur.execute(
                """
                SELECT urgency, count(*) as count
                FROM tickets WHERE urgency IS NOT NULL
                GROUP BY urgency ORDER BY count DESC
                """
            )
            by_urgency = [dict(row) for row in cur.fetchall()]

            return {
                "total": total,
                "pending": by_status.get("pending", 0),
                "processed": by_status.get("processed", 0),
                "failed": by_status.get("failed", 0),
                "by_category": by_category,
                "by_urgency": by_urgency,
            }
    finally:
        conn.close()
