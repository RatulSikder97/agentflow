// Package store holds the persistent data model shared by the engine and the
// API. Implementations live in subpackages: memstore (tests, single process)
// and pgstore (PostgreSQL).
package store

import (
	"encoding/json"
	"errors"
	"slices"
	"time"
)

// ErrNotFound is returned when a workflow or run does not exist.
var ErrNotFound = errors.New("not found")

// RunStatus is the lifecycle state of a run.
type RunStatus string

// Run states.
const (
	RunPending   RunStatus = "pending"
	RunRunning   RunStatus = "running"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
)

// Terminal reports whether no further transitions are possible.
func (s RunStatus) Terminal() bool { return s == RunSucceeded || s == RunFailed }

// StepStatus is the lifecycle state of one step within a run.
type StepStatus string

// Step states. Transitions:
//
//	pending -> queued -> running -> succeeded | failed
//	pending -> skipped
//	running -> running (retry attempt / redelivery)
const (
	StepPending   StepStatus = "pending"
	StepQueued    StepStatus = "queued"
	StepRunning   StepStatus = "running"
	StepSucceeded StepStatus = "succeeded"
	StepFailed    StepStatus = "failed"
	StepSkipped   StepStatus = "skipped"
)

// Terminal reports whether the step is finished.
func (s StepStatus) Terminal() bool {
	return s == StepSucceeded || s == StepFailed || s == StepSkipped
}

// Workflow is an immutable, stored workflow definition.
type Workflow struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Spec      string    `json:"spec"`
	CreatedAt time.Time `json:"created_at"`
}

// Run is one execution of a workflow.
type Run struct {
	ID         string         `json:"id"`
	WorkflowID string         `json:"workflow_id"`
	Status     RunStatus      `json:"status"`
	Input      map[string]any `json:"input"`
	Error      string         `json:"error,omitempty"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

// StepRun is the checkpointed state of one step in a run. A step's output is
// written exactly once, by the transition into StepSucceeded.
type StepRun struct {
	RunID      string          `json:"run_id"`
	StepID     string          `json:"step_id"`
	Status     StepStatus      `json:"status"`
	Attempt    int             `json:"attempt"`
	Output     json.RawMessage `json:"output,omitempty"`
	Error      string          `json:"error,omitempty"`
	StartedAt  *time.Time      `json:"started_at,omitempty"`
	FinishedAt *time.Time      `json:"finished_at,omitempty"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// StepUpdate describes a compare-and-set transition applied to a StepRun.
type StepUpdate struct {
	Status     StepStatus
	Output     json.RawMessage // set when non-nil
	Error      string
	IncAttempt bool
}

// Apply mutates s according to u. Shared by implementations so the
// transition semantics are identical everywhere.
func (u StepUpdate) Apply(s *StepRun, now time.Time) {
	s.Status = u.Status
	if u.Output != nil {
		s.Output = slices.Clone(u.Output)
	}
	s.Error = u.Error
	if u.IncAttempt {
		s.Attempt++
	}
	if u.Status == StepRunning && s.StartedAt == nil {
		t := now
		s.StartedAt = &t
	}
	if u.Status.Terminal() {
		t := now
		s.FinishedAt = &t
	}
	s.UpdatedAt = now
}

// Event types emitted during a run.
const (
	EventRunStarted    = "run.started"
	EventRunSucceeded  = "run.succeeded"
	EventRunFailed     = "run.failed"
	EventStepQueued    = "step.queued"
	EventStepStarted   = "step.started"
	EventStepRetrying  = "step.retrying"
	EventStepSucceeded = "step.succeeded"
	EventStepFailed    = "step.failed"
	EventStepSkipped   = "step.skipped"
)

// Event is an append-only record of something that happened in a run. Seq is
// assigned by the store and is monotonically increasing per store.
type Event struct {
	Seq    int64           `json:"seq"`
	RunID  string          `json:"run_id"`
	Type   string          `json:"type"`
	StepID string          `json:"step_id,omitempty"`
	Data   json.RawMessage `json:"data,omitempty"`
	Time   time.Time       `json:"time"`
}

// IsTerminalRunEvent reports whether no events follow this one.
func (e Event) IsTerminalRunEvent() bool {
	return e.Type == EventRunSucceeded || e.Type == EventRunFailed
}
