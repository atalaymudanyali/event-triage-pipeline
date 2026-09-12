import json
import logging
import time
from pathlib import Path

import uvicorn
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import Counter, Histogram, make_asgi_app

from triage_pipeline.agent import triage_ticket
from triage_pipeline.config import settings
from triage_pipeline.db import (
    create_ticket,
    get_recent_results,
    get_stats,
    get_ticket,
    get_ticket_stats,
    get_tickets,
    update_ticket_result,
    update_ticket_status,
)
from triage_pipeline.models import CreateTicketRequest, SupportTicketEvent
from triage_pipeline.producer import generate_ticket

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Triage Pipeline API",
    description="Inspect pipeline state, view triage results, and submit test events.",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:8000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

HTTP_REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
)

HTTP_REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration",
    ["method", "endpoint"],
)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    if request.url.path.startswith("/metrics"):
        return await call_next(request)
    t0 = time.monotonic()
    response = await call_next(request)
    duration = time.monotonic() - t0
    HTTP_REQUEST_COUNT.labels(
        method=request.method,
        endpoint=request.url.path,
        status=response.status_code,
    ).inc()
    HTTP_REQUEST_DURATION.labels(
        method=request.method,
        endpoint=request.url.path,
    ).observe(duration)
    return response


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/events")
def list_events(limit: int = 20):
    results = get_recent_results(limit=min(limit, 100))
    for r in results:
        if r.get("processed_at"):
            r["processed_at"] = r["processed_at"].isoformat()
    return {"count": len(results), "events": results}


@app.get("/stats")
def stats():
    return get_stats()


@app.post("/events", status_code=201)
def submit_event(event: SupportTicketEvent):
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    value = json.dumps(event.model_dump(), ensure_ascii=False)

    try:
        producer.produce(
            topic=settings.kafka_topic,
            key=event.event_id,
            value=value.encode("utf-8"),
        )
        producer.flush(timeout=5)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to publish: {e}") from e

    logger.info("[%s] Event submitted via API: %s", event.event_id[:8], event.subject)
    return {"event_id": event.event_id, "status": "published"}


# --- Tickets API (frontend-driven, skips Kafka) ---


@app.post("/api/tickets", status_code=201)
def api_create_ticket(req: CreateTicketRequest):
    event = SupportTicketEvent(
        customer_name=req.customer_name,
        customer_email=req.customer_email,
        subject=req.subject,
        message=req.message,
    )
    ticket = create_ticket(event)
    return ticket


@app.post("/api/tickets/generate", status_code=201)
def api_generate_ticket():
    event = generate_ticket()
    ticket = create_ticket(event)
    return ticket


@app.get("/api/tickets")
def api_list_tickets(status: str | None = None, limit: int = 50):
    return get_tickets(status=status, limit=min(limit, 200))


@app.get("/api/tickets/stats")
def api_ticket_stats():
    return get_ticket_stats()


@app.get("/api/tickets/{event_id}")
def api_get_ticket(event_id: str):
    ticket = get_ticket(event_id)
    if not ticket:
        raise HTTPException(status_code=404, detail="Ticket not found")
    return ticket


@app.post("/api/tickets/{event_id}/process")
def api_process_ticket(event_id: str):
    ticket = get_ticket(event_id)
    if not ticket:
        raise HTTPException(status_code=404, detail="Ticket not found")
    if ticket["status"] == "processed":
        raise HTTPException(status_code=400, detail="Ticket already processed")

    update_ticket_status(event_id, "processing")

    event = SupportTicketEvent(
        event_id=ticket["event_id"],
        customer_name=ticket["customer_name"],
        customer_email=ticket["customer_email"],
        subject=ticket["subject"],
        message=ticket["message"],
    )

    try:
        result = triage_ticket(event)
        update_ticket_result(event_id, result)
        return get_ticket(event_id)
    except Exception as e:
        update_ticket_status(event_id, "failed")
        raise HTTPException(status_code=502, detail=f"Triage failed: {e}") from e


FRONTEND_DIST = Path(__file__).resolve().parent.parent.parent / "frontend" / "dist"

if FRONTEND_DIST.is_dir():
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIST / "assets"), name="static")

    _index_html = FRONTEND_DIST / "index.html"

    @app.middleware("http")
    async def spa_fallback(request: Request, call_next):
        response = await call_next(request)
        if (
            response.status_code == 404
            and request.method == "GET"
            and not request.url.path.startswith(("/api/", "/metrics"))
        ):
            return FileResponse(_index_html)
        return response


def start():
    uvicorn.run(
        "triage_pipeline.api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
