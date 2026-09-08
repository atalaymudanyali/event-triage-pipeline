import json
import logging

import uvicorn
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException

from triage_pipeline.config import settings
from triage_pipeline.db import get_recent_results, get_stats
from triage_pipeline.models import SupportTicketEvent

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Triage Pipeline API",
    description="Inspect pipeline state, view triage results, and submit test events.",
    version="1.0.0",
)


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


def start():
    uvicorn.run(
        "triage_pipeline.api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
