# Event-Driven AI Triage Pipeline

An event-driven microservices pipeline where an AI agent consumes support ticket events from a message broker (Redpanda), classifies and triages them using an LLM (Gemini), and writes structured decisions to a database (Postgres).

Demonstrates the event-driven architecture pattern used by companies like Trendyol and Hepsiburada, extended with an AI agent as the consumer.

> **All event data is synthetic and clearly labeled as such.** The project demonstrates the architecture pattern, not real customer data processing.

## Architecture

```
[Producer] → [Redpanda Topic] → [AI Consumer] → [Postgres]
   │              │                    │              │
   │         support-tickets      Gemini API     triage results
   │                                   │
   └── synthetic ticket generator      └── classify → decide action
```

## Quick Start

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Install dependencies
uv sync

# 3. Set up your Gemini API key (free at https://aistudio.google.com/apikey)
cp .env.example .env
# Edit .env and add your GEMINI_API_KEY

# 4. Run the producer (publishes synthetic events)
uv run produce

# 5. Run the consumer (triages events with AI)
uv run consume
```

## Tech Stack

- **Python** — producer and consumer services
- **Redpanda** — Kafka-compatible message broker (lighter, single binary)
- **Postgres** — stores triage results
- **Gemini 2.0 Flash** — LLM for classification and triage decisions
- **Docker Compose** — local infrastructure

## Project Structure

```
src/triage_pipeline/
├── config.py      # pydantic-settings configuration
├── models.py      # domain models (events, triage results)
├── producer.py    # synthetic event generator + Kafka producer
├── consumer.py    # Kafka consumer + triage logic
├── llm.py         # Gemini client
└── db.py          # Postgres connection and queries
```

## Version Roadmap

- **V0** (current) — Core pipeline: produce → consume → triage → store
- **V1** — Multi-step agent (classify → decide → execute), FastAPI layer
- **V2** — Prometheus + Grafana observability
- **V3** — Kubernetes deployment via kind
