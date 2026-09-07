# Event-Driven AI Triage Pipeline — Commit-by-Commit Explanations

A developer's walkthrough of every commit, explaining what was built, why, and how the pieces connect. Written as a study guide.

---

## V0 Commits

### Commit: Scaffold project with uv

**What:** Created the project skeleton — `pyproject.toml`, directory structure, `.env.example`, `.gitignore`, config module.

**Key concepts:**
- **uv** is a fast Python package manager. `uv sync` installs dependencies from `pyproject.toml` and creates a `uv.lock` lockfile (exact pinned versions for reproducible builds).
- **pyproject.toml** is the single config file for modern Python projects — dependencies, scripts, build system, tool config. Replaces the old `setup.py` + `requirements.txt` pattern.
- **`[project.scripts]`** registers CLI entry points: `produce = "triage_pipeline.producer:main"` means running `uv run produce` calls the `main()` function in `producer.py`.
- **pydantic-settings** reads config from environment variables and `.env` files with type validation. If a required config value is missing or wrong-typed, it fails at startup — not 3 hours later in production.

**How pydantic-settings works here:**
```python
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    gemini_api_key: str = ""
    kafka_bootstrap_servers: str = "localhost:19092"
    postgres_port: int = 5433
```
When `Settings()` is called:
1. Reads `.env` file (key=value pairs)
2. Checks environment variables (override `.env`)
3. Falls back to defaults defined in the class
4. Type-validates everything — `postgres_port` must be an `int`, not a random string

**Design decision: Why Redpanda instead of Kafka?**

Redpanda is wire-compatible with Kafka (same protocol, same client libraries) but runs as a single binary without ZooKeeper or the JVM. For a portfolio project that someone clones and runs with `docker compose up`, this matters — Kafka needs 3+ containers (ZooKeeper, broker, schema registry) and 2-4GB RAM. Redpanda needs one container and ~200MB. Same `confluent-kafka` Python client works with both.

**Design decision: Why Gemini instead of Ollama?**

The Career Copilot project already demonstrates local LLM integration with Ollama. Using Gemini here shows two things: (1) ability to work with cloud LLM APIs (which is how most companies deploy), and (2) handling real production concerns like API keys, rate limiting, and network errors that don't exist with local models.

**If an interviewer asks:** "Why not just use Kafka?" Answer: "Redpanda is Kafka-compatible — the same `confluent-kafka` client library, same consumer group protocol, same topic/partition model. I chose it because it's operationally simpler for local development (single binary, no JVM, no ZooKeeper), but the code would work against a real Kafka cluster with zero changes — just swap the bootstrap server address."

**If an interviewer asks:** "Why pydantic-settings instead of `os.environ`?" Answer: "Type safety at startup. With `os.environ.get()`, a misconfigured port like `PORT=abc` silently becomes a string and crashes later when you try to connect. With pydantic-settings, it fails immediately with a clear error. This is the 12-factor app approach — environment variables for config, but with validation."

### Commit: Add Docker Compose with Redpanda and Postgres

**What:** Created `docker-compose.yml` with three services (Redpanda, Redpanda Console, Postgres) and `scripts/init_db.sql` for the database schema.

**Key concepts:**
- **Redpanda** is a Kafka-compatible streaming platform. It speaks the exact same protocol as Apache Kafka, so the `confluent-kafka` Python client connects to it without any code changes. The difference is operational: Redpanda is a single C++ binary with no JVM and no ZooKeeper dependency.
- **Kafka listeners** — the `--kafka-addr` and `--advertise-kafka-addr` flags configure two network paths: `internal://redpanda:9092` for container-to-container communication (other Docker services), and `external://localhost:19092` for host-to-container communication (your Python code running locally). This dual-listener pattern is how Kafka brokers work in every production deployment — internal traffic stays inside the network, external clients connect through a different address.
- **Docker entrypoint init scripts** — Postgres automatically runs any `.sql` file mounted into `/docker-entrypoint-initdb.d/` on first startup. This is how the `triage_results` table gets created without a migration tool.
- **Health checks** — both Redpanda (`rpk cluster health`) and Postgres (`pg_isready`) have health checks so that `depends_on: condition: service_healthy` ensures services start in the right order.

**The `triage_results` schema:**
```sql
CREATE TABLE triage_results (
    id              SERIAL PRIMARY KEY,
    event_id        TEXT UNIQUE NOT NULL,   -- deduplication key
    customer_name   TEXT NOT NULL,
    customer_email  TEXT NOT NULL,
    subject         TEXT NOT NULL,
    message         TEXT NOT NULL,
    category        TEXT NOT NULL,          -- LLM classification
    urgency         TEXT NOT NULL,          -- LLM assessment
    suggested_action TEXT NOT NULL,         -- what the agent decided
    draft_response  TEXT,                   -- optional auto-drafted reply
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```
The `event_id` has a `UNIQUE` constraint — this is how we achieve **idempotent processing**. If the consumer crashes and re-reads the same event (Kafka's "at-least-once" delivery), the `INSERT` will fail on the unique constraint instead of creating a duplicate row.

**Design decision: Why port 19092 instead of 9092?**

Port 9092 is the Kafka default, but if you already have another Kafka or Redpanda running on your machine, it would conflict. Using 19092 for the external listener avoids port collisions. Same reasoning for Postgres on 5433 instead of the default 5432.

**Design decision: Why include Redpanda Console?**

Redpanda Console is a web UI (at `localhost:8090`) that lets you browse topics, see messages, check consumer group lag, and inspect partitions. During development, being able to visually inspect what's in the topic is invaluable for debugging. It's the equivalent of having pgAdmin for Postgres — you could work without it, but it saves significant time.

**If an interviewer asks:** "What's the difference between Kafka and Redpanda?" Answer: "At the protocol level, nothing — Redpanda implements the Kafka API, so any Kafka client library works unchanged. The differences are operational: Redpanda is a single binary (no JVM, no ZooKeeper, no KRaft controller), it's written in C++ with thread-per-core architecture, and it typically has lower tail latencies. For this project, I chose it because it simplifies local development — one container instead of three — while keeping the code production-portable to real Kafka."

**If an interviewer asks:** "How do you handle duplicate message processing?" Answer: "Two mechanisms. First, Kafka consumer groups track offsets — after a message is processed and committed, it won't be delivered again under normal operation. But if the consumer crashes between processing and committing, the message will be redelivered. That's where the second mechanism comes in: the `event_id UNIQUE` constraint in Postgres. An idempotent `INSERT` (or `ON CONFLICT DO NOTHING`) means reprocessing the same event is a no-op at the database level."
