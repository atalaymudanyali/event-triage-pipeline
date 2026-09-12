CREATE TABLE IF NOT EXISTS triage_results (
    id              SERIAL PRIMARY KEY,
    event_id        TEXT UNIQUE NOT NULL,
    customer_name   TEXT NOT NULL,
    customer_email  TEXT NOT NULL,
    subject         TEXT NOT NULL,
    message         TEXT NOT NULL,
    category        TEXT NOT NULL,
    urgency         TEXT NOT NULL,
    suggested_action TEXT NOT NULL,
    draft_response  TEXT,
    language        TEXT NOT NULL DEFAULT 'en',
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_triage_results_category ON triage_results (category);
CREATE INDEX IF NOT EXISTS idx_triage_results_urgency ON triage_results (urgency);
CREATE INDEX IF NOT EXISTS idx_triage_results_processed_at ON triage_results (processed_at);

CREATE TABLE IF NOT EXISTS tickets (
    id                SERIAL PRIMARY KEY,
    event_id          TEXT UNIQUE NOT NULL,
    customer_name     TEXT NOT NULL,
    customer_email    TEXT NOT NULL,
    subject           TEXT NOT NULL,
    message           TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'pending',
    category          TEXT,
    urgency           TEXT,
    suggested_action  TEXT,
    draft_response    TEXT,
    reasoning         TEXT,
    language          TEXT DEFAULT 'en',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_tickets_status ON tickets (status);
CREATE INDEX IF NOT EXISTS idx_tickets_created_at ON tickets (created_at);
