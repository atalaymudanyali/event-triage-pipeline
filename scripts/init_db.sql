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
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_triage_results_category ON triage_results (category);
CREATE INDEX idx_triage_results_urgency ON triage_results (urgency);
CREATE INDEX idx_triage_results_processed_at ON triage_results (processed_at);
