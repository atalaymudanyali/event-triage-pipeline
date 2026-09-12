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

### Commit: Consumer — triage events with Gemini and store results

**What:** Built the three remaining modules that complete the pipeline: `llm.py` (Gemini client), `db.py` (Postgres writer), and `consumer.py` (Kafka consumer that ties everything together).

**Key concepts:**
- **Gemini structured output** — the `google-genai` SDK lets you pass a Pydantic model as `response_schema` in the request config. Gemini then constrains its output to match that schema exactly — every field present, every enum value valid. Combined with `response_mime_type="application/json"`, the response is guaranteed to be parseable JSON matching our `TriageResult` model.
- **Manual offset commit** — `enable.auto.commit: False` means the consumer explicitly calls `consumer.commit(message=msg)` after successfully processing each event. This is the "at-least-once" delivery guarantee: if the consumer crashes before committing, Kafka redelivers the message. The alternative (`auto.commit: True`) commits offsets on a timer, which risks "at-most-once" — losing messages that were committed but not yet processed.
- **Idempotent writes** — `ON CONFLICT (event_id) DO NOTHING` makes the database insert safe to repeat. Combined with manual offset commit, this gives us effective exactly-once processing: even if a message is delivered twice, the second insert is silently ignored.

**How the consumer loop works:**
```
while True:
    msg = consumer.poll(1.0)          # block up to 1 second for a message
    if msg is None: continue          # no message available, poll again
    if msg.error(): handle_error()    # broker-level errors (partition EOF, etc.)

    event = deserialize(msg)          # JSON → SupportTicketEvent
    result = triage_ticket(event)     # call Gemini API
    save_triage_result(event, result) # write to Postgres
    consumer.commit(message=msg)      # tell Kafka we're done with this offset
```
The order is critical: process → save → commit. If we committed before saving, a crash would mean the event is "consumed" but the result is lost.

**The Gemini integration (`llm.py`):**
```python
response = client.models.generate_content(
    model=settings.gemini_model,
    contents=build_user_prompt(event),
    config=types.GenerateContentConfig(
        system_instruction=SYSTEM_PROMPT,
        response_mime_type="application/json",
        response_schema=TriageResult,        # ← constrains LLM output
        temperature=0.1,                     # ← low temperature for consistent classification
    ),
)
```
Key design choices:
- **`temperature=0.1`** — classification should be deterministic. A "payment charged twice" ticket should always be `critical`, not sometimes `medium`. Low temperature reduces randomness.
- **`response_schema=TriageResult`** — this is not just a hint; Gemini uses it for constrained decoding. The LLM physically cannot output a category that isn't in our `TicketCategory` enum.
- **`system_instruction`** with explicit guidelines — the prompt defines urgency levels and action mappings so the LLM's decisions are predictable and auditable.

**The database layer (`db.py`):**
```python
def save_triage_result(event, result) -> bool:
    cur.execute("""
        INSERT INTO triage_results (...) VALUES (...)
        ON CONFLICT (event_id) DO NOTHING
    """, (...))
    return cur.rowcount > 0  # True if inserted, False if duplicate
```
The function returns a boolean so the consumer can log whether this was a new event or a reprocessed duplicate. In production, you'd track this as a metric — a high duplicate rate might indicate consumer rebalancing issues.

**Design decision: Why one connection per insert instead of a connection pool?**

For V0, simplicity wins. The consumer processes one event every 2-6 seconds — opening and closing a connection each time is fine at this throughput. In V1/V2, when we add FastAPI and higher throughput, we'd switch to a connection pool (e.g., `psycopg2.pool.ThreadedConnectionPool` or `asyncpg` with a pool).

**Design decision: Why `continue` on LLM errors instead of crashing?**

If Gemini returns an error (rate limit, malformed response, network timeout), we log it and skip to the next message. The failed message's offset is NOT committed, so Kafka will redeliver it on the next poll. This gives us automatic retry without any retry logic — the consumer group protocol handles it. The downside is that a persistently failing message would block the partition. In V1, we'd add a dead-letter queue for messages that fail N times.

**If an interviewer asks:** "What's the difference between at-most-once, at-least-once, and exactly-once delivery?" Answer: "At-most-once: commit the offset before processing — fast but you might lose messages. At-least-once: commit after processing — no data loss but you might process duplicates. Exactly-once: use Kafka transactions (idempotent producer + transactional consumer) — guaranteed but complex and slower. We use at-least-once with idempotent database writes (`ON CONFLICT DO NOTHING`), which gives us effective exactly-once semantics without the complexity of Kafka transactions."

**If an interviewer asks:** "How would you handle the Gemini rate limit in production?" Answer: "Three layers: (1) Rate-aware consumption — track requests per minute and pause polling when approaching the limit. (2) Exponential backoff on 429 responses — the `tenacity` library (already a dependency via google-genai) handles this. (3) Multiple API keys or a higher-tier plan for production volume. The current 2-6 second random delay in the producer naturally keeps us under 15 RPM, but that's a development convenience, not a production strategy."

### Commit: Unit tests, pre-push hook, and GitHub Actions CI

**What:** Added 19 unit tests covering models, producer, and prompt building. Set up a `pre-push` git hook that runs lint + tests locally before every push, and a GitHub Actions workflow that runs the same checks in CI.

**Key concepts:**
- **Unit vs integration tests** — the tests in this commit need zero infrastructure (no Docker, no Gemini API key, no Postgres). They test pure Python logic: model validation, JSON serialization roundtrips, template population, prompt format. Integration tests (against the live stack) belong in CI only — they're slower, flakier, and require credentials.
- **Pre-push hook** — a script in `.git/hooks/pre-push` that Git runs before every `git push`. If it exits non-zero, the push is aborted. We run `ruff check` and `pytest` here so broken code never reaches GitHub. We use pre-push (not pre-commit) because running tests on every commit would slow down the feedback loop during development.
- **GitHub Actions** — the CI workflow triggers on pushes to `main` and on pull requests. It installs `uv`, sets up Python 3.13, and runs the same lint + test commands. This catches issues from contributors who didn't set up the hook.

**What the tests actually verify:**
```
test_models.py (9 tests):
├── Auto-generated fields (event_id, timestamp)
├── UUID uniqueness across 100 instances
├── JSON serialization roundtrip (model → JSON → model)
├── Rejection of missing required fields
├── StrEnum values serialize as plain strings
└── TriageResult validation (valid, with draft, invalid category)

test_producer.py (5 tests):
├── generate_ticket() returns valid SupportTicketEvent
├── Uses only known customer names/subjects
├── Templates are fully populated (no {order_id} leftovers)
└── Unique event IDs across batch

test_llm.py (5 tests):
├── User prompt includes all event fields
├── Prompt has structured format (Customer/Subject/Message)
└── System prompt contains all valid enum values
```

The system prompt tests (`test_contains_all_categories`, etc.) are particularly useful — if someone adds a new `TicketCategory` enum value but forgets to mention it in the system prompt, the test catches the mismatch.

**Design decision: Why not mock the Gemini API and test `triage_ticket()`?**

Mocking an LLM response tests that your parsing code works against a fake response you wrote yourself — it doesn't test that the real API returns what you expect. The actual value of testing the LLM call is verifying that the prompt + schema produce correct classifications, which requires the real API. We'll do that in integration tests. Mocking here would give false confidence.

**Design decision: Why keep the hook script in `scripts/` instead of `.git/hooks/`?**

The `.git/` directory is never committed — it's local to each clone. By keeping the script in `scripts/pre-push`, it's version-controlled and visible in the repo. New contributors copy it with `cp scripts/pre-push .git/hooks/pre-push`. Some projects automate this with a `make setup` or `post-checkout` hook, but for a portfolio project the manual step is fine.

**If an interviewer asks:** "Why pre-push instead of pre-commit?" Answer: "Pre-commit runs on every commit, which is great for instant feedback but slows down rapid iteration — especially when tests take more than a few seconds. Pre-push is the last checkpoint before code leaves your machine, so it catches issues without interrupting your commit flow. For a project with ~1 second test runtime either works, but the pattern scales better — in a larger project with a 30-second test suite, pre-commit would be painful."

**If an interviewer asks:** "How would you add integration tests?" Answer: "I'd add a `tests/integration/` directory with a `conftest.py` that checks for running Docker services and skips if unavailable. Tests would use the real Redpanda and Postgres from Docker Compose, and a real Gemini API key from the environment. In CI, the workflow would `docker compose up -d`, wait for health checks, run the integration tests, then `docker compose down`. Locally, developers run them optionally with `pytest tests/integration/`."

---

## V1 Commits

### Commit: Add intermediate models for multi-step agent pipeline

**What:** Added three new Pydantic models — `ClassificationResult`, `UrgencyAssessment`, `DraftResponse` — that represent the output of each agent step. Added a `language` field to `TriageResult`.

**Key concepts:**
- **Intermediate models** break a complex LLM task into smaller, verifiable steps. Instead of asking the LLM to do everything at once (classify, assess urgency, decide action, draft a response), each step has its own model with its own constrained output. This is the "chain of thought via code" pattern — the code controls the reasoning flow, not the LLM.
- **Each model is a contract.** `ClassificationResult` guarantees the LLM returns a valid `TicketCategory` and a language code. `UrgencyAssessment` guarantees a valid `Urgency` and `SuggestedAction`. If any step returns garbage, Pydantic catches it at that step — not at the end when it's harder to debug.

**Why split into steps?**

A single prompt doing everything has three problems: (1) It's hard to debug — if the urgency is wrong, was it because the classification was wrong, or the urgency logic was wrong? With separate steps, you can inspect each intermediate result. (2) Each step gets a focused prompt — a shorter, more specific prompt produces better results than a long one with many instructions. (3) Each step can be tested, retried, and monitored independently.

**Design decision: Why detect language as a step output?**

In V0, the LLM sometimes responded in Turkish and sometimes in English with no predictability. By making language detection explicit in step 1, step 3 (draft response) can be instructed to match the customer's language. This makes the agent's behavior deterministic and auditable.

**If an interviewer asks:** "Isn't 3 LLM calls per event expensive and slow?" Answer: "It's a tradeoff. A single call is faster and cheaper, but harder to debug and less reliable. With 3 calls, each step is simpler and more constrained, which means higher accuracy and better observability. In production, you'd measure whether the accuracy improvement justifies the cost. For many companies, a misclassified critical ticket costs far more than two extra API calls."

### Commit: Multi-step agent replacing single LLM call

**What:** Created `agent.py` with the 3-step pipeline: `step_classify` → `step_assess_urgency` → `step_draft_response`. Each step builds context from the previous step's output. Updated the consumer to import from `agent` instead of `llm`. Updated DB schema and insert to include `language`.

**Key concepts:**
- **Context chaining** — each step receives the original ticket plus the output of all previous steps. Step 2 (urgency) sees the classification result, so it can make urgency decisions based on the category. Step 3 (draft response) sees both classification and urgency, so it can match the tone to the situation.
- **Shared `_call_gemini` helper** — all three steps use the same function to call Gemini with structured output. This centralizes error handling, model configuration, and JSON parsing in one place.

**How context flows through the pipeline:**
```
Step 1: classify
  Input:  ticket (customer, subject, message)
  Output: ClassificationResult (category, language, reasoning)

Step 2: assess urgency
  Input:  ticket + classification result
  Output: UrgencyAssessment (urgency, suggested_action, reasoning)

Step 3: draft response
  Input:  ticket + classification + urgency assessment
  Output: DraftResponse (response_text, tone)

Final:   TriageResult assembled from all three outputs
```

**Design decision: Why a new `agent.py` instead of modifying `llm.py`?**

`llm.py` represents the V0 approach (single call). Keeping it around makes the evolution visible in the codebase — anyone reading the repo can see the before and after. The consumer's import change from `llm` to `agent` is a one-line diff that tells the whole story.

**If an interviewer asks:** "How would you add a new step to the agent?" Answer: "Define a new Pydantic model for the step's output, write a prompt, create a `step_*` function that calls `_call_gemini` with the appropriate context, and wire it into `triage_ticket()`. The pattern is designed to be additive — new steps don't modify existing ones."

### Commit: Retry with exponential backoff on rate limit errors

**What:** Wrapped `_call_gemini` with `tenacity` retry logic. Rate limit (429) and service unavailable (503) errors trigger exponential backoff (4s → 8s → 16s → 32s → 60s, up to 5 attempts). Other errors propagate immediately.

**Key concepts:**
- **tenacity** is a Python retry library (already a transitive dependency via `google-genai`). The `@retry` decorator wraps a function with configurable retry behavior — no retry loops or sleep calls in your code.
- **Exponential backoff** — each retry waits twice as long as the previous one. This is critical for rate limits: if 10 consumers all hit a 429 at the same time and retry after a fixed 1 second, they'll all hit the limit again simultaneously. Exponential backoff with jitter (which tenacity adds by default) spreads them out.
- **Retryable vs non-retryable errors** — a 429 (rate limit) is transient: wait and try again. A 401 (bad API key) is permanent: retrying won't fix it. The `LLMRetryableError` class separates these so only transient errors trigger retry.

**How the retry decorator works:**
```python
@retry(
    retry=retry_if_exception_type(LLMRetryableError),  # only retry these
    wait=wait_exponential(multiplier=2, min=4, max=60), # 4s, 8s, 16s, 32s, 60s
    stop=stop_after_attempt(5),                         # give up after 5 tries
    before_sleep=before_sleep_log(logger, logging.WARNING),  # log each retry
    reraise=True,                                       # raise the real error if exhausted
)
def _call_gemini(...):
```

**Design decision: Why 5 attempts with max 60s wait?**

The Gemini free tier allows 5 requests per minute. If we hit the limit, the first retry at 4 seconds might still fail, but by the third retry (~28s total elapsed) we're likely in a new rate limit window. 5 attempts with a 60s max gives us about 2 minutes of total retry time — enough to ride out a brief rate limit burst without blocking the consumer indefinitely.

**Design decision: Why classify errors by string matching instead of exception types?**

The `google-genai` SDK wraps API errors in generic exception types. A 429 and a 400 might both be a `google.genai.errors.ClientError`. Since we can't reliably distinguish by exception class, we check the error message for "429", "503", and "UNAVAILABLE". This is pragmatic — the alternative is catching every possible exception subclass, which is fragile against SDK version changes.

**If an interviewer asks:** "What's the difference between retry at the HTTP level vs the application level?" Answer: "HTTP-level retry (e.g., `httpx` transport retry) retries the raw request. Application-level retry (tenacity) retries the whole operation, which might include JSON parsing, validation, and context assembly. We retry at the application level because we want to re-execute the full function — if the response was partial or malformed, we want a fresh attempt, not just a re-send of the same bytes."

**If an interviewer asks:** "How does this interact with the consumer's own retry via Kafka redelivery?" Answer: "Two different layers. Tenacity retries transient LLM errors within a single event processing attempt — the consumer stays on the same message. Kafka redelivery happens when the consumer crashes or restarts without committing — it's a coarser retry for infrastructure failures. They complement each other: tenacity handles API flakiness, Kafka handles process failures."

### Commit: Dead-letter topic for failed events

**What:** Events that fail all retries are published to a dead-letter topic (`support-tickets-dlt`) instead of being silently skipped. The consumer now tracks per-event retry counts and uses structured logging.

**Key concepts:**
- **Dead-letter topic (DLT)** — a Kafka topic where messages that can't be processed are sent. Instead of losing the message or blocking the partition forever, the consumer publishes the failed event (with the error message) to a separate topic. A human or automated system can later inspect, fix, and replay these events.
- **Retry counting** — the consumer tracks `retry_counts[event_id]` in memory. Each time an event fails, the count increments. After `max_retries` (default 3) failures, the event goes to the DLT and the offset is committed (so Kafka doesn't redeliver it). If the event succeeds, the count is cleared.
- **Structured logging** — replaced `print()` with Python's `logging` module. Log levels (`INFO`, `WARNING`, `ERROR`) let you filter output. Timestamps make it possible to correlate events across the producer and consumer. In V2, these log lines become the basis for Prometheus metrics.

**The DLT flow:**
```
Event arrives → triage_ticket() fails
  ↓
retry_counts[event_id] += 1
  ↓
retries < max_retries?
  YES → don't commit, Kafka redelivers on next poll
  NO  → publish to DLT, commit offset, clear retry count
```

**What gets written to the DLT:**
```json
{
  "event": { "event_id": "...", "customer_name": "...", ... },
  "error": "429 RESOURCE_EXHAUSTED: rate limit exceeded"
}
```
The original event is preserved in full, so it can be replayed without data loss.

**Design decision: Why in-memory retry counts instead of persistent state?**

Simplicity. If the consumer restarts, retry counts reset to zero — the event gets fresh retries. This is fine because restarts are rare and the tenacity retry (within each attempt) handles most transient errors. Persistent retry counts (in Redis or Postgres) add complexity that's only justified at scale.

**Design decision: Why commit the offset after sending to DLT?**

If we didn't commit, Kafka would redeliver the same failing event on every poll, creating an infinite loop. By committing after the DLT publish, we acknowledge "we've handled this event (by routing it to DLT)" and move on. The DLT is the safety net — the event isn't lost, it's just in a different topic.

**If an interviewer asks:** "What happens to events in the dead-letter topic?" Answer: "In production, you'd have a monitoring alert on DLT message count. An engineer inspects the failed events, fixes the root cause (bad prompt, schema change, API outage), and replays them — either manually or with a replay tool that reads from the DLT and publishes back to the main topic. The DLT is a holding area, not a graveyard."

**If an interviewer asks:** "What if the DLT publish itself fails?" Answer: "The `producer.flush(timeout=5)` ensures the DLT publish is synchronous — we wait for broker acknowledgment. If it fails (broker down), the flush times out and the offset isn't committed (because we haven't reached that line yet). On the next poll, Kafka redelivers the original event, and we retry the whole flow including the DLT publish. The only way to lose data is if both the main topic consumer and the DLT producer fail simultaneously, which is a cluster-level outage."

### Commit: FastAPI app — events, stats, and manual submission

**What:** Added a FastAPI application with three endpoint groups: `GET /events` (recent triage results), `GET /stats` (classification distribution), and `POST /events` (submit a test event to Redpanda). Added query functions to `db.py`.

**Key concepts:**
- **FastAPI** is a modern Python web framework that auto-generates OpenAPI docs from type annotations. Pydantic models used as request/response types become the API schema — the same `SupportTicketEvent` model validates both Kafka messages and HTTP requests.
- **`RealDictCursor`** (psycopg2) returns query results as dictionaries instead of tuples. This makes the results directly JSON-serializable without manual column mapping.
- **Separation of concerns** — the API doesn't call the LLM directly. It either reads from the database (past results) or publishes to Redpanda (new events for the consumer to process). The consumer is the only component that calls Gemini. This keeps the architecture clean: API = read state + submit events, Consumer = process events.

**The three endpoints:**
```
GET  /health          → {"status": "ok"}
GET  /events?limit=20 → recent triage results from Postgres
GET  /stats           → {total_processed, by_category, by_urgency, by_action}
POST /events          → publish a SupportTicketEvent to Redpanda
```

**Design decision: Why does POST /events publish to Kafka instead of calling the agent directly?**

If the API called the agent directly, you'd have two code paths: one for Kafka events and one for HTTP events. Bugs could exist in one path but not the other. By publishing to Redpanda, every event goes through the same pipeline — the consumer processes it identically regardless of whether it came from the producer script or the API. This is the "single writer" pattern.

**Design decision: Why `uvicorn.run()` with `reload=True`?**

`reload=True` watches for file changes and restarts the server automatically during development. In production, you'd run uvicorn without reload and behind a reverse proxy (nginx, Traefik). The `start()` function wraps this so `uv run api` launches the server.

**If an interviewer asks:** "How would you secure this API in production?" Answer: "Multiple layers: (1) API key or JWT authentication middleware. (2) Rate limiting on the POST endpoint to prevent abuse. (3) Input validation — FastAPI + Pydantic already handle this, but you'd add size limits on the message field. (4) CORS configuration to restrict which frontends can call it. (5) Run behind a reverse proxy that handles TLS termination."

**If an interviewer asks:** "Why not use async/await with FastAPI?" Answer: "FastAPI supports both sync and async handlers. Our database calls use psycopg2, which is synchronous. Making the handlers async while using sync DB calls would actually be worse — FastAPI would run them in a thread pool anyway, adding overhead without benefit. If we switched to asyncpg (async Postgres client), then async handlers would make sense. For the current throughput, sync is simpler and correct."

---

### Commit: V1 tests — agent, intermediate models, and FastAPI endpoints

**What:** Added three test files covering all new V1 code: `test_agent.py` (multi-step agent prompts, `_format_ticket` helper, `LLMRetryableError`), `test_models_v1.py` (intermediate models: `ClassificationResult`, `UrgencyAssessment`, `DraftResponse`, plus `TriageResult.language` field), and `test_api.py` (FastAPI health endpoint and OpenAPI schema verification).

**Key concepts:**
- **Testing prompts, not LLM calls** — calling Gemini in CI would be slow, flaky, and cost money. Instead, we verify that every prompt contains the right enum values and keywords. If someone adds a new `TicketCategory` but forgets to update the classify prompt, these tests catch it.
- **FastAPI `TestClient`** — built on Starlette's test client, it lets you call endpoints without starting a real server. Requests run in-process against the ASGI app, so tests are fast and deterministic. No port binding, no HTTP overhead.
- **OpenAPI schema assertions** — checking that `/openapi.json` lists `/events` and `/stats` paths ensures the endpoints are actually registered. If someone accidentally removes a route decorator, the test catches it immediately.

**Test coverage strategy:**
```
test_agent.py     → prompt completeness, _format_ticket, LLMRetryableError
test_models_v1.py → intermediate model validation, enum enforcement, defaults
test_api.py       → health check, OpenAPI schema structure
```

**Design decision: Why test `_format_ticket` separately?**

`_format_ticket` is a pure function — given a `SupportTicketEvent`, it returns a formatted string. No LLM, no network, no side effects. These are the functions most worth testing because they're fast, deterministic, and compose into the rest of the pipeline. If the format changes and breaks the LLM's ability to parse tickets, the tests show exactly where the formatting expectation diverged.

**Design decision: Why not mock Gemini and test the full agent flow?**

Mocking `_call_gemini` and asserting the pipeline wires steps together would test *our mocking setup*, not our code. The interesting behavior — does each step feed the right context to the next — is already visible from reading the code and is validated by the prompt tests. Integration tests with a real LLM belong in a separate, optional test suite that runs manually, not in CI.

**If an interviewer asks:** "How would you test the retry/backoff logic without hitting rate limits?" Answer: "You'd inject the LLM client as a dependency and use a fake that raises `LLMRetryableError` N times before succeeding. Then assert that the function retried the expected number of times using tenacity's statistics — `_call_gemini.retry.statistics['attempt_number']`. This tests the retry configuration without needing any external service."

**If an interviewer asks:** "Your API tests don't test the database endpoints — why?" Answer: "The `/events` and `/stats` endpoints require a running Postgres with the schema applied. That's an integration test, not a unit test. We test the health endpoint and OpenAPI schema (pure application logic) here, and test database queries separately during live pipeline testing. In V2, a Docker-based test fixture could spin up Postgres for CI."

---

## V2 Commits

### Commit: Prometheus metrics module

**What:** Added `prometheus-client` dependency and created `metrics.py` — a dedicated module defining all six Prometheus metrics the pipeline will expose: event counters, triage duration histograms, error/retry/DLT counters, and DB save latency.

**Key concepts:**
- **Prometheus** is a pull-based monitoring system. Instead of your application pushing metrics to a server, Prometheus *scrapes* an HTTP endpoint (`/metrics`) on your app at regular intervals. This decouples your app from the monitoring infrastructure — if Prometheus goes down, your app keeps running.
- **Counter** is a monotonically increasing value — it only goes up. "Total events processed" is a counter. You never reset it; Prometheus calculates *rate* (events/second) by comparing values across scrapes. `rate(triage_events_processed_total[5m])` gives you the per-second processing rate over the last 5 minutes.
- **Histogram** records observations (like request durations) into configurable buckets. For `triage_duration_seconds` with buckets `(0.5, 1, 2, 5, 10, 20, 30, 60)`, Prometheus counts how many observations fell below each threshold. From this, you can compute percentiles: `histogram_quantile(0.95, ...)` gives p95 latency.
- **Labels** add dimensions to metrics. `EVENTS_PROCESSED` has labels `category` and `urgency`, so you can query `triage_events_processed_total{category="payment_problem"}` to see just payment issues. Labels are powerful but expensive — each unique label combination creates a new time series.
- **Why a dedicated module?** `prometheus-client` uses a global registry. Each metric must be registered exactly once per process. Python's module import cache guarantees `metrics.py` runs once — importing it from `consumer.py` and `agent.py` returns the same objects. If you defined metrics inline in each file, you'd risk duplicate registration errors.

**Design decision: Why custom histogram buckets instead of defaults?**

The default Prometheus buckets (`0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10`) are tuned for HTTP request latencies (milliseconds to low seconds). Our LLM calls take 1–10 seconds, so the default buckets would lump everything into the last two bins. Custom buckets `(0.5, 1, 2, 5, 10, 20, 30, 60)` spread the range where our data actually lives. For DB saves, which are sub-second, we use finer-grained buckets `(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1)`.

**If an interviewer asks:** "Why Prometheus over something like Datadog or CloudWatch?" Answer: "Prometheus is open-source, runs locally, and is the standard for Kubernetes monitoring (it's a CNCF graduated project). For a portfolio project, it shows you understand the industry-standard observability stack. In production, you'd often pair Prometheus with Thanos or Cortex for long-term storage, or use a managed service like Grafana Cloud."

**If an interviewer asks:** "What's the difference between push-based and pull-based monitoring?" Answer: "Push-based (like StatsD) means the app sends metrics to a server. Pull-based (Prometheus) means the server scrapes the app. Pull is simpler — your app just exposes an HTTP endpoint, and if the monitoring server goes down, the app doesn't need retry logic or a buffer. Pull also lets Prometheus discover targets dynamically (service discovery in Kubernetes)."

---

### Commit: Instrument consumer, agent, and API with metrics

**What:** Wired all six Prometheus metrics into the actual processing code. The consumer starts a metrics HTTP server on port 8001, tracks event counts, triage duration, errors, retries, DLT sends, and DB save latency. The agent records per-step LLM timing (classify, urgency, draft). The FastAPI app exposes `/metrics` and tracks HTTP request count and duration via custom middleware.

**Key concepts:**
- **`start_http_server(port)`** from `prometheus_client` starts a background thread serving the `/metrics` endpoint. The consumer uses this because it has no HTTP server of its own. The API already has one (FastAPI/uvicorn), so it mounts a Prometheus ASGI app at `/metrics` instead.
- **`.time()` context manager** — `TRIAGE_DURATION.labels(step="classify").time()` automatically records how long the block takes. If the block raises an exception, the duration is still recorded — important for tracking slow failures.
- **`time.monotonic()`** — used for the total triage duration in the consumer. Unlike `time.time()`, monotonic clocks never go backwards (no daylight saving jumps, no NTP corrections). Always use monotonic for measuring elapsed time.
- **`make_asgi_app()`** — creates an ASGI application from the default Prometheus registry. Mounting it on FastAPI at `/metrics` means both the pipeline metrics (from `metrics.py`) and the HTTP metrics (defined in `api.py`) are served from the same endpoint.
- **HTTP middleware** — FastAPI middleware wraps every request. Ours measures request duration and counts requests by method/endpoint/status. It skips `/metrics` requests to avoid self-referential counting (Prometheus scraping would inflate the count).

**Design decision: Why custom middleware instead of `prometheus-fastapi-instrumentator`?**

The custom middleware is ~15 lines using only `prometheus-client` (already installed). Adding another dependency for 4 endpoints would be over-engineering. More importantly, writing middleware from scratch demonstrates understanding of the request lifecycle — something interviewers value over knowing which library to `pip install`.

**Design decision: Why define HTTP metrics in `api.py` instead of `metrics.py`?**

The consumer and API run as separate processes. HTTP metrics only apply to the API process — putting them in `metrics.py` would register them in the consumer too, creating empty time series that Prometheus scrapes for no reason. Pipeline metrics (events, triage duration) go in `metrics.py` because they're used by both consumer and agent. HTTP metrics stay local to `api.py`.

**If an interviewer asks:** "How would you add consumer lag monitoring?" Answer: "Consumer lag (how far behind the consumer is from the latest message) is best tracked at the broker level. Redpanda exposes consumer group lag via its admin API. You'd either scrape Redpanda's built-in Prometheus metrics, or use a dedicated exporter like `kafka-lag-exporter`. Application-level lag tracking is unreliable because the consumer can't see messages it hasn't polled yet."

---

### Commit: Prometheus and Grafana in Docker Compose with pre-built dashboard

**What:** Added Prometheus (port 9090) and Grafana (port 3000) to Docker Compose. Prometheus scrapes both the consumer metrics server (port 8001) and the FastAPI `/metrics` endpoint (port 8000). Grafana starts with a pre-provisioned datasource and a 12-panel dashboard covering throughput, latency, errors, classification distribution, and API performance.

**Key concepts:**
- **Prometheus pull model** — Prometheus runs in a Docker container and reaches out to your Python processes every 5 seconds. The `scrape_configs` in `prometheus.yml` list the targets (host + port). Each scrape hits the `/metrics` HTTP endpoint, parses the text-based exposition format, and stores the time series in its local TSDB (time-series database).
- **`host.docker.internal`** — Docker containers can't reach `localhost` on the host machine (that's the container's own localhost). On Windows and Mac, Docker Desktop automatically resolves `host.docker.internal` to the host IP. On Linux, the `extra_hosts: ["host.docker.internal:host-gateway"]` directive in Docker Compose does the same thing.
- **Grafana provisioning** — instead of manually clicking through the Grafana UI to add a datasource and import a dashboard, provisioning config files in `/etc/grafana/provisioning/` automate this at startup. The datasource YAML points Grafana to `http://prometheus:9090` (Docker service name, resolved via Docker DNS). The dashboard provider YAML tells Grafana where to find JSON dashboard files.
- **Dashboard JSON** — Grafana dashboards are JSON documents describing panels, their PromQL queries, and layout. Exporting dashboards as JSON and storing them in version control is called "dashboards as code" — you can reproduce the exact same dashboard in any environment.
- **`GF_AUTH_ANONYMOUS_ENABLED`** — enables anonymous access to Grafana. Without it, you'd need to log in (default admin/admin). For a local dev environment and portfolio demos, anonymous Viewer access removes friction.

**The dashboard panels:**
```
Row 1 — Overview:     Events Processed | Processing Rate | Error Rate | DLT Events
Row 2 — Performance:  Triage Duration by Step (p50/p95) | DB Save Duration
Row 3 — Distribution: Events by Category (donut) | Events by Urgency (donut)
Row 4 — Errors:       Errors by Type | Retries Over Time
Row 5 — API:          Request Rate by Endpoint | API Latency (p95)
```

**Key PromQL queries explained:**
- `rate(triage_events_processed_total[5m])` — per-second rate of events processed, averaged over 5 minutes. `rate()` handles counter resets (process restarts) automatically.
- `histogram_quantile(0.95, sum by(le, step) (rate(triage_duration_seconds_bucket[5m])))` — the 95th percentile triage duration, broken down by step. `le` (less-than-or-equal) is the bucket boundary label that histograms use; `histogram_quantile` interpolates between buckets.
- Error rate formula divides errors by (events + errors) to get a 0–1 ratio. The `+ 1e-10` prevents division by zero when there's no traffic.

**Design decision: Why pin Prometheus and Grafana image versions?**

`prom/prometheus:v3.4.0` instead of `prom/prometheus:latest`. In production, `:latest` can break your setup when a new major version ships with breaking config changes. Pinned versions make `docker compose up` deterministic — the same config works the same way months later.

**If an interviewer asks:** "How would you alert on high error rates?" Answer: "Two options: (1) Prometheus alerting rules — define a rule like `rate(triage_errors_total[5m]) > 0.1` in a rules YAML file, and configure Alertmanager to send notifications (Slack, PagerDuty, email). (2) Grafana alerting — set alert conditions directly on dashboard panels. Prometheus alerting is more robust (survives Grafana downtime), but Grafana alerting is easier to set up for a small team."

**If an interviewer asks:** "What happens to metrics when the consumer restarts?" Answer: "Counters reset to zero. Prometheus handles this with `rate()` — it detects the reset (value decreases) and adjusts the calculation. Histograms behave the same way. This is why you always use `rate()` on counters instead of raw values — raw values show misleading drops on restart."

---

### Commit: V2 tests, README update, and final polish

**What:** Added `test_metrics.py` (verifying all six metric objects have correct types, labels, and custom buckets), extended `test_api.py` with `/metrics` endpoint tests, and overhauled the README to reflect V1 and V2 changes — updated architecture diagram, new Observability section, ports table, and current version roadmap.

**Key concepts:**
- **Testing Prometheus metrics** — you can inspect `_labelnames` and `_kwargs["buckets"]` on metric objects to verify they're configured correctly without needing a running Prometheus server. These are unit tests for your instrumentation *definitions*, not for whether Prometheus scrapes them.
- **Testing `/metrics` endpoint** — after calling `/health`, the `/metrics` response should contain `http_requests_total` because the middleware counted that request. This proves the middleware is active and the Prometheus ASGI app is mounted correctly.

**Design decision: Why overhaul the README after each version?**

The README is the first thing a recruiter or interviewer sees. If it says "V0 (current)" while the code is at V2, it signals neglect. Updating the README per version keeps the documentation honest and shows attention to communication — a skill that matters as much as the code itself in a portfolio project.

**If an interviewer asks:** "Walk me through the observability stack." Answer: "The consumer and API expose Prometheus metrics on ports 8001 and 8000. Prometheus scrapes both every 5 seconds and stores time series. Grafana connects to Prometheus and renders a pre-built dashboard with 12 panels — stat tiles for throughput, histograms for latency broken down by LLM step, pie charts for classification distribution, and time series for errors and API performance. Everything is provisioned as code — `docker compose up -d` gives you the full stack with no manual configuration."

---

## V3 Commits

### Commit: Backend — tickets table, new API endpoints, CORS

**What:** Added a `tickets` table for frontend-driven ticket management, six new DB functions, six new `/api/tickets/*` endpoints, a `CreateTicketRequest` model, and CORS middleware. The existing Kafka-based endpoints remain unchanged.

**Key concepts:**
- **Two data paths, one AI pipeline** — the Kafka path (producer → Redpanda → consumer) and the API path (frontend → `/api/tickets` → Postgres) both end up calling the same `triage_ticket()` function. The difference is how tickets arrive and where state is tracked. Kafka is for autonomous stream processing; the API is for interactive use.
- **CORS (Cross-Origin Resource Sharing)** — browsers block requests from one origin (e.g. `localhost:5173`) to another (`localhost:8000`) by default. `CORSMiddleware` adds the `Access-Control-Allow-Origin` header so the Vite dev server can call the FastAPI backend. Without it, every frontend fetch call would fail silently.
- **Status state machine** — tickets move through `pending → processing → processed` (or `pending → processing → failed`). The `processing` state prevents double-clicks: if a ticket is already being triaged, the UI knows not to send another request.
- **Route ordering** — `/api/tickets/stats` is registered before `/api/tickets/{event_id}` so FastAPI doesn't capture "stats" as an event_id path parameter. Order matters with path parameters.

**Design decision: Why skip Kafka for frontend tickets?**

Kafka is designed for asynchronous, decoupled event processing — not for request-response workflows. If the frontend published to Kafka and then polled the database waiting for the consumer to process the ticket, you'd have unnecessary complexity and unpredictable latency. Calling `triage_ticket()` directly from the API gives the frontend a synchronous response. The Kafka path still exists for the autonomous producer/consumer demo.

**If an interviewer asks:** "Isn't it bad to have two data paths?" Answer: "It's the right trade-off. The Kafka path is for production-style autonomous processing where you want decoupling, backpressure, and at-least-once delivery. The API path is for interactive use where you want immediate feedback. They share the same AI pipeline (`triage_ticket()`), so there's no business logic duplication. In a real system, you might have both: a Kafka consumer for bulk processing and an API for customer-facing ticket submission."

---

### Commit: Frontend — React app with customer and admin views

**What:** Created a React + Vite frontend with two views: a customer view for submitting support tickets and looking up their status, and an admin dashboard for monitoring tickets, generating synthetic data, and triggering AI triage processing.

**Key concepts:**
- **Vite** is a modern frontend build tool. Unlike Webpack, it serves source files as native ES modules during development (instant startup), and uses Rollup for optimized production builds. The `vite.config.js` proxy routes `/api/*` requests to the FastAPI backend so the frontend can call the API without CORS issues during development.
- **React Router** (`react-router-dom`) handles client-side routing — the browser URL changes (`/` and `/admin`) without full page reloads. `BrowserRouter` wraps the app, `Routes`/`Route` map paths to components, and `Link` renders navigation anchors that trigger client-side transitions.
- **Controlled components** — every form input's value is bound to React state (`value={form.subject}` + `onChange`). This means React owns the form data at all times, making validation, submission, and clearing straightforward.
- **CSS custom properties (variables)** — defining colors as `--accent`, `--bg-card`, etc. on `:root` means the entire theme can be changed by redefining a handful of values. The dark mode support uses `prefers-color-scheme` media query and `data-theme` attribute, with tokens redefined in both blocks.

**Design decision: Why two separate views instead of a single dashboard?**

In a real support system, customers and agents have different needs and permissions. Customers submit tickets and check status — they should never see the admin controls for processing or generating synthetic tickets. Splitting into `CustomerView` and `AdminView` mirrors this separation. For a portfolio project, it also demonstrates React Router and multi-page SPA architecture.

**Design decision: Why no "Process All" button?**

Gemini's free tier has a 5 requests-per-minute rate limit. Each ticket triggers 3 LLM calls (classify → urgency → draft response). Processing two tickets back-to-back would hit the rate limit. The per-row "Process" button forces manual, one-at-a-time triage — which is the right UX for a rate-limited API. In production with a paid tier, you'd add a batch processing feature.

**File structure and component hierarchy:**
```
frontend/src/
├── main.jsx              ← Entry point: mounts <App /> inside <BrowserRouter>
├── App.jsx               ← Layout: nav bar + <Routes> mapping / and /admin
├── App.css               ← All styles, CSS custom properties, dark mode
├── api.js                ← Fetch wrapper: createTicket, processTicket, etc.
├── pages/
│   ├── CustomerView.jsx  ← Ticket form + status lookup by event_id
│   └── AdminView.jsx     ← Stats cards + generate button + ticket table
└── components/
    ├── TicketForm.jsx    ← Controlled form with name/email/subject/message
    ├── TicketTable.jsx   ← Expandable rows with per-row Process button
    ├── TicketDetail.jsx  ← Expanded view: badges, draft response, reasoning
    └── StatusBadge.jsx   ← Color-coded pill for status/urgency/category
```

**If an interviewer asks:** "Why React instead of a server-rendered template?" Answer: "The admin dashboard needs interactive updates — clicking Process triggers a 5-30 second API call and should show a loading spinner, then update the row without refreshing the page. The ticket table has expandable rows. The stats cards should refresh on demand. These are all client-side state management patterns that React handles naturally. A server-rendered page with HTMX could work, but React is the industry standard for dashboard UIs and demonstrates a broader skill set for a portfolio project."

---

### Commit: Serve built frontend from FastAPI and update README

**What:** Modified FastAPI to serve the Vite production build (`frontend/dist/`) so the entire app runs on a single port (`:8000`) in production. Updated the README with the new architecture diagram, frontend section, dev workflow, and corrected version roadmap.

**Key concepts:**
- **SPA fallback middleware** — a single-page app uses client-side routing, so the server must return `index.html` for any path that isn't an API endpoint or static file. If the user navigates to `/admin` directly, the server can't 404 — it must serve `index.html` and let React Router handle the path. This is implemented as an HTTP middleware that intercepts 404 responses on GET requests and returns `index.html` instead, excluding `/api/*` and `/metrics` paths.
- **StaticFiles mount** — FastAPI's `StaticFiles` serves the Vite build's hashed asset files (`/assets/index-abc123.js`). Vite adds content hashes to filenames, enabling aggressive browser caching — the hash changes when the content changes, so you can set `Cache-Control: immutable` and never serve stale code.
- **Conditional mounting** — `if FRONTEND_DIST.is_dir()` means the API works standalone when no frontend build exists. During development, you run Vite's dev server separately (`:5173`) with its proxy. In production, `npm run build` creates the `dist/` directory and FastAPI serves everything.
- **Middleware vs. catch-all route** — a catch-all route (`/{path:path}`) would shadow mounted sub-applications like `/metrics` because FastAPI evaluates routes before mounts. Using a middleware instead means the request first tries all routes and mounts normally; only if nothing matches (404) does the middleware intercept and serve the SPA shell.

**Design decision: Why not a reverse proxy (Nginx)?**

For a portfolio project running locally, adding Nginx adds infrastructure complexity without proportional benefit. In production you'd absolutely put Nginx or a cloud load balancer in front, but for a demo that runs with `docker compose up && uv run api`, having FastAPI serve the frontend is the simplest deployment story. The code is guarded by the `is_dir()` check, so it's zero-cost when not building the frontend.

**If an interviewer asks:** "How would you deploy this differently in production?" Answer: "I'd put Nginx in front — it serves static assets directly with proper caching headers and proxies `/api/*` to FastAPI. The frontend build would run in a multi-stage Docker build, and the static files would go into the Nginx container. FastAPI would only handle API requests, which is better for performance since it doesn't need to serve JS/CSS. The SPA fallback would move to an Nginx `try_files` directive."

---

### Commit: V3 tests and commit explanations

**What:** Added `test_tickets_api.py` with 10 tests covering all six `/api/tickets/*` endpoints — create, generate, list, stats, detail, and process — including validation and error cases. Updated commit explanations for all V3 commits.

**Key concepts:**
- **Mocking DB functions at the API layer** — the tests patch `triage_pipeline.api.create_ticket` (the imported name in the API module), not `triage_pipeline.db.create_ticket` (the source). This is a common Python testing pattern: you mock where a function is *used*, not where it's *defined*, because the API module has already imported its own reference.
- **Testing error paths** — `test_process_already_processed_400` verifies that re-processing a completed ticket returns 400, and `test_process_missing_ticket_404` verifies the not-found case. These tests confirm the status state machine is enforced at the API level.
- **Testing validation** — `test_create_rejects_missing_fields` sends an incomplete JSON body and expects 422 (FastAPI's Pydantic validation). This proves the `CreateTicketRequest` model enforces required fields without writing custom validation code.

**Design decision: Why mock the DB instead of using a test database?**

The API tests verify routing, request validation, status codes, and response shapes — not database behavior. Mocking the DB functions keeps tests fast (no Postgres needed), isolated (no shared state between tests), and focused on what the API layer actually does. Integration tests that hit a real database would go in a separate test suite, gated behind a marker like `@pytest.mark.integration`.

**If an interviewer asks:** "How do you know the pipeline actually works end-to-end if everything is mocked?" Answer: "These are unit tests — they verify each layer in isolation. The consumer tests mock the LLM, the API tests mock the DB. End-to-end verification happens manually: `docker compose up`, submit a ticket through the frontend, watch it get triaged. In a production CI pipeline, I'd add integration tests that start real containers (Testcontainers or docker compose in CI) and run the full flow. But for a portfolio project, the manual demo is the integration test."

---

### Commit: Dockerize API with multi-stage build

**What:** Added a multi-stage Dockerfile that builds the React frontend with Node and packages the Python API with uv, so `docker compose up -d --build` starts the entire stack — infrastructure, API, frontend, and observability — with a single command. Updated docker-compose to include the API service and Prometheus to scrape it by container name.

**Key concepts:**
- **Multi-stage Docker build** — stage 1 (`node:22-alpine`) runs `npm ci` and `npm run build` to produce the Vite production bundle. Stage 2 (`uv:python3.12-bookworm-slim`) installs Python dependencies and copies the built frontend from stage 1 via `COPY --from=frontend`. The final image has no Node.js, no `node_modules`, no source JSX — only the compiled assets and the Python runtime.
- **Docker networking** — containers on the same Compose network resolve each other by service name. The API container uses `POSTGRES_HOST=postgres` and `KAFKA_BOOTSTRAP_SERVERS=redpanda:9092` (internal port, not the host-mapped one). Prometheus scrapes `triage-api:8000` instead of `host.docker.internal:8000`.
- **`env_file: .env`** — Docker Compose reads the `.env` file and injects all variables as environment variables into the container. pydantic-settings picks these up automatically, so the same config module works locally and in Docker.
- **`.dockerignore`** — excludes `.venv/`, `node_modules/`, `.git/`, and `frontend/dist/` from the build context. Without this, Docker would send hundreds of megabytes of unnecessary files to the daemon on every build.

**Design decision: Why override `POSTGRES_PORT` in the container?**

Locally, Postgres maps `5433:5432` so it doesn't conflict with any local Postgres. But inside Docker, the API talks to Postgres directly on the internal network — port 5432 (the default Postgres port). The `environment` block in docker-compose overrides `POSTGRES_HOST` and `POSTGRES_PORT` so the same config works in both contexts without changing the `.env` file.

**If an interviewer asks:** "Why not use Docker for consume/produce too?" Answer: "The consumer and producer are interactive demo tools — you run them in a terminal, watch the logs, and stop them manually. Putting them in Docker would hide the output and make the demo less visible. The API is different: it's a long-running server that should start automatically with the infrastructure. In production, you'd containerize everything, but for a portfolio demo, keeping consume/produce as local commands lets you show the event-driven flow in real time."
