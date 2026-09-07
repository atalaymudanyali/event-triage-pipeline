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

**`.gitignore` — what gets excluded and why:**
```
__pycache__/          # Python bytecode cache — regenerated on every run
*.py[cod]             # compiled Python files (.pyc, .pyo, .pyd)
*.egg-info/, dist/    # build artifacts from packaging — never commit these
.venv/                # virtual environment — each dev recreates with `uv sync`
.env                  # SECRETS live here (API keys, passwords) — .env.example is the template
.idea/, .vscode/      # IDE settings are personal preference, not project config
.pytest_cache/        # test runner cache
uv.lock               # lockfile — see note below
```

**Why `.env` is gitignored but `.env.example` is committed:**

`.env` contains real secrets (your `GEMINI_API_KEY`, database passwords). If you commit it, those secrets are in git history forever — even if you delete the file later, `git log` still has it. `.env.example` is the template with placeholder values (`your-api-key-here`) so new developers know which variables to set.

**Why `uv.lock` is gitignored here:**

In application projects (deployed services), you typically **do** commit the lockfile so every environment runs the exact same versions. In library projects, you don't. For this portfolio project, we gitignore it to keep the diff clean — anyone cloning runs `uv sync` which generates their own lockfile. In a production codebase, you'd commit it.

**If an interviewer asks:** "Why not commit the lockfile?" Answer: "It depends on whether you're building an application or a library. For a deployed service where reproducible builds matter, you commit it. For a library that others install as a dependency, you don't — you let their resolver pick compatible versions. This project is a portfolio demo, so I optimized for clone-and-run simplicity, but in production I'd commit it."

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

### Commit: Domain models for support ticket events and triage results

**What:** Created Pydantic models for the two core data shapes — `SupportTicketEvent` (what goes into the pipeline) and `TriageResult` (what comes out after LLM processing) — plus `StrEnum` types for constrained fields.

**Key concepts:**
- **Pydantic models** (`BaseModel`) define the shape of data with automatic type validation. If you pass wrong types, Pydantic raises an error immediately — no silent failures downstream.
- **StrEnum** (Python 3.11+) combines `str` and `Enum` — the values serialize as plain strings in JSON (`"order_issue"`) but are type-safe in Python code. This matters because both the Kafka messages and the Gemini response schema need plain string values, not `TicketCategory.ORDER_ISSUE`.
- **`Field(default_factory=...)`** generates a value at instance creation time. `event_id` gets a unique UUID, `timestamp` gets the current UTC time. Using a factory instead of a default value avoids the classic mutable-default-argument bug.

**The two models and how they flow through the system:**
```
SupportTicketEvent                    TriageResult
├── event_id (auto UUID)              ├── event_id (copied from event)
├── customer_name         ──LLM──►    ├── category (StrEnum)
├── customer_email                    ├── urgency (StrEnum)
├── subject                           ├── suggested_action (StrEnum)
├── message                           ├── draft_response (optional)
└── timestamp (auto UTC)              └── reasoning
```
The `event_id` is the thread that connects an event through the entire pipeline — from the producer, through Kafka, into the LLM call, and finally into the database. It's the deduplication key.

**Design decision: Why StrEnum instead of plain strings?**

Plain strings are flexible but error-prone — a typo like `"ordr_issue"` would silently pass through the system. StrEnum constrains the values at the Python level. When used as a Gemini `response_schema`, it also constrains the LLM output — Gemini will only return values from the enum, not invent new categories.

**Design decision: Why is `draft_response` optional?**

Not every action needs a drafted response. If the agent decides to `escalate` or `request_info`, a canned draft might not make sense. Making it `str | None` lets the LLM decide whether a response draft is appropriate for the situation.

**If an interviewer asks:** "Why Pydantic instead of dataclasses?" Answer: "Pydantic gives us three things dataclasses don't: (1) automatic JSON serialization/deserialization with `model_dump()` and `model_validate()`, which we need for Kafka messages. (2) Type coercion and validation — if the LLM returns `urgency: 'HIGH'` instead of `'high'`, Pydantic handles the case mismatch. (3) Schema generation — `TriageResult` becomes the `response_schema` for Gemini's structured output, so the LLM is constrained to return exactly these fields."

**If an interviewer asks:** "What's the difference between `str | None` and `Optional[str]`?" Answer: "They're identical at runtime — `Optional[str]` is just `Union[str, None]`. The `str | None` syntax (PEP 604, Python 3.10+) is the modern convention. We use it because it's cleaner and the project targets Python 3.12+."

### Commit: Producer — publish synthetic support tickets to Redpanda

**What:** Built the event producer — generates realistic-looking support ticket events from predefined customer/ticket templates and publishes them to a Redpanda topic as JSON.

**Key concepts:**
- **confluent-kafka Producer** is the Python client for Kafka-protocol brokers. `Producer({"bootstrap.servers": "..."})` connects to the broker. `.produce(topic, key, value, callback)` enqueues a message, and `.poll(0)` triggers delivery callbacks without blocking.
- **Message key** — we use `event_id` as the Kafka message key. In Kafka, messages with the same key always go to the same partition, which guarantees ordering per key. For support tickets, this means all events related to one ticket (if we later add follow-ups) would be processed in order.
- **Delivery callback** — `producer.produce()` is asynchronous; the message is buffered locally and sent in batches. The `callback` parameter receives a confirmation (or error) when the broker actually acknowledges the message. This is how you know a message was durably written vs silently dropped.

**The synthetic data approach:**
```python
CUSTOMERS = [("Ayşe Yılmaz", "ayse.yilmaz@email.com"), ...]
TICKETS = [("Order not delivered", "I placed an order...{order_id}..."), ...]
```
Templates use Python string formatting (`{order_id}`, `{amount}`) to inject random values, so each generated ticket is unique but realistic. The customer names are Turkish — intentional, since this demo targets Turkish e-commerce companies.

**Why `random.uniform(2, 6)` between events?**

A real support system receives tickets at irregular intervals, not at a fixed rate. The random delay simulates this and also prevents overwhelming the Gemini free tier (15 requests per minute). At 2-6 second intervals, we produce ~10-30 events per minute — comfortably under the rate limit.

**Design decision: Why `confluent-kafka` instead of `kafka-python`?**

`confluent-kafka` is the official Confluent client, built on top of `librdkafka` (a battle-tested C library). It's what companies actually use in production. `kafka-python` is pure Python — easier to install but slower, less reliable under load, and the original project is no longer maintained (there's a fork called `kafka-python-ng`). For a portfolio project that claims to demonstrate production patterns, the client library choice matters.

**Design decision: Why not use a data generation library like Faker?**

Faker is great for generating realistic data at scale, but it's a heavy dependency for what we need — 8 customers and 10 ticket templates. Handcrafted templates give us control over the exact scenarios the LLM will classify (we want coverage across all categories, not random noise). And each template is written to test a specific triage path — "payment charged twice" should trigger `critical` urgency, "how do I track my order" should trigger `low`.

**If an interviewer asks:** "What happens if the broker is down when you produce?" Answer: "The `confluent-kafka` producer has an internal buffer and retry mechanism. If the broker is temporarily unreachable, messages are buffered locally and retried automatically (configurable with `retries` and `retry.backoff.ms`). If the broker stays down beyond the retry window, the delivery callback fires with an error. In production, you'd log these failures and potentially write to a dead-letter queue or local file for replay."

**If an interviewer asks:** "How would you scale the producer?" Answer: "The producer isn't the bottleneck — Kafka producers can easily push millions of messages per second. If you needed to simulate high volume, you'd remove the sleep and produce in batches. The consumer side is where scaling matters — adding more consumer instances in the same consumer group distributes partitions across them."
