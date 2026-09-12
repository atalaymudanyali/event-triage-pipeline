from prometheus_client import Counter, Histogram

EVENTS_PROCESSED = Counter(
    "triage_events_processed_total",
    "Total events successfully triaged",
    ["category", "urgency"],
)

TRIAGE_DURATION = Histogram(
    "triage_duration_seconds",
    "Time spent on triage steps",
    ["step"],
    buckets=(0.5, 1, 2, 5, 10, 20, 30, 60),
)

TRIAGE_ERRORS = Counter(
    "triage_errors_total",
    "Total triage processing errors",
    ["error_type"],
)

TRIAGE_RETRIES = Counter(
    "triage_retries_total",
    "Total retry attempts on failed events",
)

DLT_EVENTS = Counter(
    "triage_dlt_events_total",
    "Events sent to the dead-letter topic",
)

DB_SAVE_DURATION = Histogram(
    "triage_db_save_duration_seconds",
    "Time spent saving triage results to Postgres",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1),
)
