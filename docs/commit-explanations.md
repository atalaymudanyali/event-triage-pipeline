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
