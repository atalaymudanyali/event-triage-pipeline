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
