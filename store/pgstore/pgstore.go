// Package pgstore is the PostgreSQL implementation of the agentflow store.
// Every step transition is a single compare-and-set UPDATE keyed by
// (run_id, step_id), which is what makes step completion idempotent across
// any number of workers.
package pgstore

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/RatulSikder97/agentflow/store"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Store is a PostgreSQL-backed store.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to Postgres and verifies connectivity.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgstore: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgstore: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the connection pool.
func (s *Store) Close() { s.pool.Close() }

// Migrate applies all pending up migrations embedded in the binary.
func (s *Store) Migrate(_ context.Context) error {
	src, err := iofs.New(migrations, "migrations")
	if err != nil {
		return fmt.Errorf("pgstore: migration source: %w", err)
	}
	db := stdlib.OpenDBFromPool(s.pool)
	drv, err := pgxmigrate.WithInstance(db, &pgxmigrate.Config{})
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("pgstore: migration driver: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "pgx5", drv)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("pgstore: migrate: %w", err)
	}
	defer func() { _, _ = m.Close() }()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("pgstore: migrate up: %w", err)
	}
	return nil
}

// CreateWorkflow stores a workflow definition.
func (s *Store) CreateWorkflow(ctx context.Context, wf store.Workflow) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO workflows (id, name, spec) VALUES ($1, $2, $3)`, wf.ID, wf.Name, wf.Spec)
	if err != nil {
		return fmt.Errorf("pgstore: create workflow: %w", err)
	}
	return nil
}

// GetWorkflow loads a workflow by ID.
func (s *Store) GetWorkflow(ctx context.Context, id string) (store.Workflow, error) {
	var wf store.Workflow
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, spec, created_at FROM workflows WHERE id = $1`, id).
		Scan(&wf.ID, &wf.Name, &wf.Spec, &wf.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return wf, fmt.Errorf("workflow %s: %w", id, store.ErrNotFound)
	}
	if err != nil {
		return wf, fmt.Errorf("pgstore: get workflow: %w", err)
	}
	return wf, nil
}

// CreateRun inserts the run and its pending step rows in one transaction.
func (s *Store) CreateRun(ctx context.Context, run store.Run, stepIDs []string) error {
	input, err := json.Marshal(run.Input)
	if err != nil {
		return fmt.Errorf("pgstore: encode input: %w", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgstore: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`INSERT INTO runs (id, workflow_id, status, input) VALUES ($1, $2, $3, $4)`,
		run.ID, run.WorkflowID, string(run.Status), input); err != nil {
		return fmt.Errorf("pgstore: insert run: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO step_runs (run_id, step_id, status)
		 SELECT $1, unnest($2::text[]), $3`, run.ID, stepIDs, string(store.StepPending)); err != nil {
		return fmt.Errorf("pgstore: insert steps: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgstore: commit: %w", err)
	}
	return nil
}

const runCols = `id, workflow_id, status, input, error, created_at, updated_at`

func scanRun(row pgx.Row) (store.Run, error) {
	var r store.Run
	var input []byte
	if err := row.Scan(&r.ID, &r.WorkflowID, &r.Status, &input, &r.Error, &r.CreatedAt, &r.UpdatedAt); err != nil {
		return r, err
	}
	if err := json.Unmarshal(input, &r.Input); err != nil {
		return r, fmt.Errorf("decode input: %w", err)
	}
	return r, nil
}

// GetRun loads a run by ID.
func (s *Store) GetRun(ctx context.Context, id string) (store.Run, error) {
	r, err := scanRun(s.pool.QueryRow(ctx, `SELECT `+runCols+` FROM runs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return r, fmt.Errorf("run %s: %w", id, store.ErrNotFound)
	}
	if err != nil {
		return r, fmt.Errorf("pgstore: get run: %w", err)
	}
	return r, nil
}

// ListRuns returns runs in the given status, oldest first.
func (s *Store) ListRuns(ctx context.Context, status store.RunStatus) ([]store.Run, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+runCols+` FROM runs WHERE status = $1 ORDER BY created_at`, string(status))
	if err != nil {
		return nil, fmt.Errorf("pgstore: list runs: %w", err)
	}
	defer rows.Close()
	var out []store.Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, fmt.Errorf("pgstore: scan run: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgstore: list runs: %w", err)
	}
	return out, nil
}

// UpdateRun transitions a run to `to` if its current status is in `from`.
func (s *Store) UpdateRun(ctx context.Context, id string, from []store.RunStatus, to store.RunStatus, errMsg string) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE runs SET status = $3, error = $4, updated_at = now()
		 WHERE id = $1 AND status = ANY($2)`, id, strs(from), string(to), errMsg)
	if err != nil {
		return false, fmt.Errorf("pgstore: update run: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return true, nil
	}
	if _, err := s.GetRun(ctx, id); err != nil {
		return false, err
	}
	return false, nil
}

// ListSteps returns all step states of a run, ordered by step ID.
func (s *Store) ListSteps(ctx context.Context, runID string) ([]store.StepRun, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT run_id, step_id, status, attempt, output, error, started_at, finished_at, updated_at
		 FROM step_runs WHERE run_id = $1 ORDER BY step_id`, runID)
	if err != nil {
		return nil, fmt.Errorf("pgstore: list steps: %w", err)
	}
	defer rows.Close()
	var out []store.StepRun
	for rows.Next() {
		var st store.StepRun
		var output []byte
		if err := rows.Scan(&st.RunID, &st.StepID, &st.Status, &st.Attempt, &output,
			&st.Error, &st.StartedAt, &st.FinishedAt, &st.UpdatedAt); err != nil {
			return nil, fmt.Errorf("pgstore: scan step: %w", err)
		}
		if output != nil {
			st.Output = output
		}
		out = append(out, st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgstore: list steps: %w", err)
	}
	if len(out) == 0 {
		if _, err := s.GetRun(ctx, runID); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// UpdateStep applies u if the step's current status is in from. The WHERE
// clause makes this an atomic compare-and-set.
func (s *Store) UpdateStep(ctx context.Context, runID, stepID string, from []store.StepStatus, u store.StepUpdate) (bool, error) {
	var output any
	if u.Output != nil {
		output = []byte(u.Output)
	}
	inc := 0
	if u.IncAttempt {
		inc = 1
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE step_runs SET
			status      = $4,
			output      = COALESCE($5::jsonb, output),
			error       = $6,
			attempt     = attempt + $7,
			started_at  = CASE WHEN $4 = 'running' THEN COALESCE(started_at, now()) ELSE started_at END,
			finished_at = CASE WHEN $4 IN ('succeeded','failed','skipped') THEN now() ELSE finished_at END,
			updated_at  = now()
		WHERE run_id = $1 AND step_id = $2 AND status = ANY($3)`,
		runID, stepID, strs(from), string(u.Status), output, u.Error, inc)
	if err != nil {
		return false, fmt.Errorf("pgstore: update step: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// AppendEvent records an event; Seq comes from a BIGSERIAL.
func (s *Store) AppendEvent(ctx context.Context, ev store.Event) (store.Event, error) {
	var data any
	if ev.Data != nil {
		data = []byte(ev.Data)
	}
	err := s.pool.QueryRow(ctx,
		`INSERT INTO run_events (run_id, type, step_id, data) VALUES ($1, $2, $3, $4)
		 RETURNING seq, created_at`, ev.RunID, ev.Type, ev.StepID, data).Scan(&ev.Seq, &ev.Time)
	if err != nil {
		return ev, fmt.Errorf("pgstore: append event: %w", err)
	}
	return ev, nil
}

// ListEvents returns up to limit events of a run with Seq > afterSeq.
func (s *Store) ListEvents(ctx context.Context, runID string, afterSeq int64, limit int) ([]store.Event, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.pool.Query(ctx,
		`SELECT seq, run_id, type, step_id, data, created_at FROM run_events
		 WHERE run_id = $1 AND seq > $2 ORDER BY seq LIMIT $3`, runID, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("pgstore: list events: %w", err)
	}
	defer rows.Close()
	var out []store.Event
	for rows.Next() {
		var ev store.Event
		var data []byte
		if err := rows.Scan(&ev.Seq, &ev.RunID, &ev.Type, &ev.StepID, &data, &ev.Time); err != nil {
			return nil, fmt.Errorf("pgstore: scan event: %w", err)
		}
		if data != nil {
			ev.Data = data
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgstore: list events: %w", err)
	}
	return out, nil
}

func strs[T ~string](in []T) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = string(v)
	}
	return out
}
