# Event-Driven AI Triage Pipeline

An event-driven microservices pipeline where an AI agent consumes support ticket events from a message broker (Redpanda), classifies and triages them using an LLM (Gemini), and writes structured decisions to a database (Postgres). Includes a React dashboard, Prometheus metrics, and a Grafana dashboard for real-time observability.

Demonstrates the event-driven architecture pattern used by companies like Trendyol and Hepsiburada, extended with an AI agent as the consumer.

> **All event data is synthetic and clearly labeled as such.** The project demonstrates the architecture pattern, not real customer data processing.

## Architecture

```
                     ┌──────────────┐
                     │  React UI    │
                     │  :5173 (dev) │
                     └──────┬───────┘
                            │ /api/*
                            ▼
[Producer] → [Redpanda] → [AI Consumer] → [Postgres]
   │            │               │              ▲
   │       support-tickets  Gemini API         │
   │                        ┌───┴───┐          │
   └── synthetic        classify → urgency     │
       tickets               → draft response  │
                                               │
                     [FastAPI :8000] ───────────┘
                     GET  /events       — Kafka triage results
                     GET  /stats        — classification stats
                     POST /events       — submit to Kafka
                     POST /api/tickets  — create ticket (frontend)
                     POST /api/tickets/{id}/process — AI triage
                     GET  /api/tickets  — list tickets

                     ┌─────────────┐
                     │ Prometheus  │──── scrapes ──→ :8001 (consumer)
                     │  :9090      │──── scrapes ──→ :8000 (API /metrics)
                     └──────┬──────┘
                            │
                     ┌──────▼──────┐
                     │  Grafana    │
                     │  :3000      │
                     └─────────────┘
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

# 4. Initialize the database (both tables)
psql -h localhost -p 5433 -U postgres -d triage -f scripts/init_db.sql

# 5. Run the API (serves both backend and frontend)
uv run api

# 6. (Optional) Run the Kafka consumer for autonomous processing
uv run consume

# 7. (Optional) Run the producer to generate Kafka events
uv run produce
```

### Frontend Development

```bash
# For hot-reload development:
cd frontend
npm install
npm run dev        # Vite dev server on :5173, proxies /api/* to :8000
```

```bash
# For production (served by FastAPI):
cd frontend
npm run build      # outputs to frontend/dist/
uv run api         # FastAPI serves the built frontend at :8000
```

**Dashboard:** Open [http://localhost:5173](http://localhost:5173) (dev) or [http://localhost:8000](http://localhost:8000) (production).

**Grafana:** Open [http://localhost:3000](http://localhost:3000) — no login required.

## Tech Stack

- **Python 3.12+** — producer, consumer, and API
- **React + Vite** — frontend dashboard with customer and admin views
- **Redpanda** — Kafka-compatible message broker (single binary, no ZooKeeper)
- **Postgres 16** — stores triage results and tickets with idempotent writes
- **Gemini 3.6 Flash** — LLM for classification and triage (free tier)
- **FastAPI** — REST API for pipeline inspection + serves built frontend
- **Prometheus** — metrics collection via pull-based scraping
- **Grafana** — pre-built dashboard with 12 panels
- **Docker Compose** — local infrastructure orchestration

## Frontend

The React dashboard has two views:

**Customer View** (`/`) — submit a support ticket and look up its status by ticket ID.

**Admin View** (`/admin`) — monitor ticket stats, generate synthetic tickets, and trigger AI triage processing on individual tickets.

Two data paths feed the same AI pipeline:
- **Kafka path** (autonomous): producer → Redpanda → consumer → Postgres
- **API path** (interactive): frontend → `/api/tickets` → `triage_ticket()` → Postgres

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
├── api.py         # FastAPI REST API + HTTP metrics + SPA serving
└── metrics.py     # Prometheus metric definitions

frontend/src/
├── main.jsx           # React entry point
├── App.jsx            # Router layout (/ and /admin)
├── App.css            # Styles with CSS custom properties + dark mode
├── api.js             # Fetch wrapper for all API calls
├── pages/
│   ├── CustomerView.jsx   # Ticket form + status lookup
│   └── AdminView.jsx      # Stats cards + ticket table + controls
└── components/
    ├── TicketForm.jsx     # Controlled form (name, email, subject, message)
    ├── TicketTable.jsx    # Expandable rows with per-row Process button
    ├── TicketDetail.jsx   # Full triage result view
    └── StatusBadge.jsx    # Color-coded status/urgency pill

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
| Vite Dev Server | 5173 | http://localhost:5173 |
| Consumer Metrics | 8001 | http://localhost:8001/metrics |
| Prometheus | 9090 | http://localhost:9090 |
| Grafana | 3000 | http://localhost:3000 |

## Version Roadmap

- **V0** — Core pipeline: produce → consume → triage → store
- **V1** — Multi-step agent, retry/DLT, FastAPI API
- **V2** — Prometheus + Grafana observability
- **V3** (current) — React frontend dashboard
- **V4** — Kubernetes deployment via kind
