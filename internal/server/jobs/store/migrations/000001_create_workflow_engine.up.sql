CREATE TABLE IF NOT EXISTS job_executions (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    definition_id TEXT NOT NULL,
    trigger TEXT NOT NULL,
    dedupe_key TEXT NOT NULL UNIQUE,
    payload TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    retry_initial_seconds INTEGER NOT NULL,
    retry_max_seconds INTEGER NOT NULL,
    run_at INTEGER NOT NULL,
    lease_owner TEXT,
    lease_until INTEGER,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    parent_execution_id TEXT,
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    finished_at INTEGER
);

CREATE INDEX IF NOT EXISTS job_executions_claim_idx ON job_executions(state, run_at, lease_until);

CREATE TABLE IF NOT EXISTS job_execution_resources (
    execution_id TEXT NOT NULL REFERENCES job_executions(id) ON DELETE CASCADE,
    resource_key TEXT NOT NULL,
    PRIMARY KEY (execution_id, resource_key)
);

CREATE TABLE IF NOT EXISTS job_resource_locks (
    resource_key TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES job_executions(id) ON DELETE CASCADE,
    lease_until INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS job_resource_locks_lease_idx ON job_resource_locks(lease_until);

CREATE TABLE IF NOT EXISTS job_execution_activities (
    execution_id TEXT NOT NULL REFERENCES job_executions(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    result TEXT,
    checkpoint TEXT,
    last_error TEXT,
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    completed_at INTEGER,
    PRIMARY KEY (execution_id, name)
);

CREATE TABLE IF NOT EXISTS job_execution_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL REFERENCES job_executions(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    data TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS job_execution_events_execution_idx ON job_execution_events(execution_id, sequence);
