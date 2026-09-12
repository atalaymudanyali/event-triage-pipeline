# Event-Driven AI Triage Pipeline

An event-driven microservices pipeline where an AI agent consumes support ticket events from a message broker (Redpanda), classifies and triages them using an LLM (Gemini), and writes structured decisions to a database (Postgres). Includes Prometheus metrics and a Grafana dashboard for real-time observability.

Demonstrates the event-driven architecture pattern used by companies like Trendyol and Hepsiburada, extended with an AI agent as the consumer.

> **All event data is synthetic and clearly labeled as such.** The project demonstrates the architecture pattern, not real customer data processing.

## Architecture

```
                                ┌─────────────┐
                                │  Prometheus  │──── scrapes ──→ :8001 (consumer)
                                │   :9090      │──── scrapes ──→ :8000 (API /metrics)
                                └──────┬───────┘
                                       │
                                ┌──────▼───────┐
                                │   Grafana    │
                                │   :3000      │
                                └──────────────┘

[Producer] → [Redpanda Topic] → [AI Consumer] → [Postgres]
   │              │                    │              │
   │         support-tickets      Gemini API     triage results
   │                               ┌───┴───┐
   └── synthetic tickets       classify → urgency → draft response

                    [FastAPI :8000]
                    GET  /events  — recent triage results
                    GET  /stats   — classification distribution
                    POST /events  — submit test event
```

## Quick Start

```bash
# 1. Start infrastructure (Redpanda, Postgres, Prometheus, Grafana)
docker compose up -d

# 2. Install dependencies
uv sync

# 3. Set up your Gemini API key (free at https://aistudio.google.com/apikey)
cp .env.example .env
# Edit .env and add your GEMINI_API_KEY

# 4. Run the consumer (triages events with AI, starts metrics on :8001)
uv run consume

# 5. In another terminal — run the producer (publishes synthetic events)
uv run produce

# 6. (Optional) Start the API
uv run api
```

**View the dashboard:** Open [http://localhost:3000](http://localhost:3000) — no login required.

## Tech Stack

- **Python 3.12+** — producer, consumer, and API
- **Redpanda** — Kafka-compatible message broker (single binary, no ZooKeeper)
- **Postgres 16** — stores triage results with idempotent writes
- **Gemini 3.6 Flash** — LLM for classification and triage (free tier)
- **FastAPI** — REST API for pipeline inspection
- **Prometheus** — metrics collection via pull-based scraping
- **Grafana** — pre-built dashboard with 12 panels
- **Docker Compose** — local infrastructure orchestration

## AI Agent Pipeline

The consumer runs a 3-step agent for each support ticket:

1. **Classify** — determines ticket category (7 types) and detects language
2. **Assess Urgency** — assigns urgency level and suggests an action
3. **Draft Response** — writes a reply in the customer's language

Each step feeds its output as context to the next. Failed events retry with exponential backoff; events that exhaust retries go to a dead-letter topic.

## Observability

The Grafana dashboard at [http://localhost:3000](http://localhost:3000) shows:

| Row | Panels |
|-----|--------|
| Overview | Events processed, processing rate, error rate, DLT count |
| Performance | Triage duration by step (p50/p95), DB save latency |
| Distribution | Events by category (donut), events by urgency (donut) |
| Errors | Error rate by type, retries over time |
| API | Request rate by endpoint, API latency (p95) |

Metrics are exposed at:
- Consumer: `http://localhost:8001/metrics`
- API: `http://localhost:8000/metrics`
- Prometheus targets: `http://localhost:9090/targets`

## Project Structure

```
src/triage_pipeline/
├── config.py      # pydantic-settings configuration
├── models.py      # domain models (events, triage results, intermediate models)
├── producer.py    # synthetic event generator + Kafka producer
├── consumer.py    # Kafka consumer + retry/DLT + metrics instrumentation
├── agent.py       # 3-step LLM agent (classify → urgency → draft)
├── llm.py         # original V0 single-call implementation
├── db.py          # Postgres persistence and query helpers
├── api.py         # FastAPI REST API + HTTP metrics middleware
└── metrics.py     # Prometheus metric definitions

monitoring/
├── prometheus.yml                          # scrape configuration
└── grafana/
    ├── provisioning/
    │   ├── datasources/prometheus.yml      # auto-provision Prometheus datasource
    │   └── dashboards/dashboards.yml       # dashboard file provider
    └── dashboards/
        └── triage-pipeline.json            # pre-built 12-panel dashboard
```

## Ports

| Service | Port | URL |
|---------|------|-----|
| Redpanda (Kafka) | 19092 | — |
| Redpanda Console | 8090 | http://localhost:8090 |
| Postgres | 5433 | — |
| FastAPI | 8000 | http://localhost:8000 |
| Consumer Metrics | 8001 | http://localhost:8001/metrics |
| Prometheus | 9090 | http://localhost:9090 |
| Grafana | 3000 | http://localhost:3000 |

## Version Roadmap

- **V0** — Core pipeline: produce → consume → triage → store
- **V1** — Multi-step agent, retry/DLT, FastAPI API
- **V2** (current) — Prometheus + Grafana observability
- **V3** — Kubernetes deployment via kind
