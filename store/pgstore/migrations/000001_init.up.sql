CREATE TABLE workflows (
    id         TEXT PRIMARY KEY,
    name       TEXT        NOT NULL,
    spec       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE runs (
    id          TEXT PRIMARY KEY,
    workflow_id TEXT        NOT NULL REFERENCES workflows (id),
    status      TEXT        NOT NULL,
    input       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    error       TEXT        NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX runs_status_created_idx ON runs (status, created_at);

-- One row per (run, step). The primary key is the idempotency key: every
-- state change is a compare-and-set UPDATE on this row.
CREATE TABLE step_runs (
    run_id      TEXT        NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    step_id     TEXT        NOT NULL,
    status      TEXT        NOT NULL,
    attempt     INT         NOT NULL DEFAULT 0,
    output      JSONB,
    error       TEXT        NOT NULL DEFAULT '',
    started_at  TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, step_id)
);

CREATE TABLE run_events (
    seq        BIGSERIAL PRIMARY KEY,
    run_id     TEXT        NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    type       TEXT        NOT NULL,
    step_id    TEXT        NOT NULL DEFAULT '',
    data       JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX run_events_run_seq_idx ON run_events (run_id, seq);
