// Package memstore is an in-memory store used by tests and single-process
// deployments. It implements the same compare-and-set semantics as pgstore.
package memstore

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/RatulSikder97/agentflow/store"
)

// Store is a concurrency-safe in-memory store.
type Store struct {
	mu        sync.RWMutex
	workflows map[string]store.Workflow
	runs      map[string]store.Run
	steps     map[string]map[string]store.StepRun
	events    map[string][]store.Event
	seq       int64
	now       func() time.Time
}

// New returns an empty Store.
func New() *Store {
	return &Store{
		workflows: map[string]store.Workflow{},
		runs:      map[string]store.Run{},
		steps:     map[string]map[string]store.StepRun{},
		events:    map[string][]store.Event{},
		now:       func() time.Time { return time.Now().UTC() },
	}
}

// CreateWorkflow stores a workflow definition.
func (s *Store) CreateWorkflow(_ context.Context, wf store.Workflow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.workflows[wf.ID]; ok {
		return fmt.Errorf("workflow %s already exists", wf.ID)
	}
	if wf.CreatedAt.IsZero() {
		wf.CreatedAt = s.now()
	}
	s.workflows[wf.ID] = wf
	return nil
}

// GetWorkflow loads a workflow by ID.
func (s *Store) GetWorkflow(_ context.Context, id string) (store.Workflow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	wf, ok := s.workflows[id]
	if !ok {
		return store.Workflow{}, fmt.Errorf("workflow %s: %w", id, store.ErrNotFound)
	}
	return wf, nil
}

// CreateRun stores a run and one pending StepRun per step.
func (s *Store) CreateRun(_ context.Context, run store.Run, stepIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.workflows[run.WorkflowID]; !ok {
		return fmt.Errorf("workflow %s: %w", run.WorkflowID, store.ErrNotFound)
	}
	if _, ok := s.runs[run.ID]; ok {
		return fmt.Errorf("run %s already exists", run.ID)
	}
	now := s.now()
	run.CreatedAt, run.UpdatedAt = now, now
	run.Input = maps.Clone(run.Input)
	s.runs[run.ID] = run
	steps := make(map[string]store.StepRun, len(stepIDs))
	for _, id := range stepIDs {
		steps[id] = store.StepRun{RunID: run.ID, StepID: id, Status: store.StepPending, UpdatedAt: now}
	}
	s.steps[run.ID] = steps
	return nil
}

// GetRun loads a run by ID.
func (s *Store) GetRun(_ context.Context, id string) (store.Run, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.runs[id]
	if !ok {
		return store.Run{}, fmt.Errorf("run %s: %w", id, store.ErrNotFound)
	}
	return r, nil
}

// ListRuns returns runs in the given status, oldest first.
func (s *Store) ListRuns(_ context.Context, status store.RunStatus) ([]store.Run, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []store.Run
	for _, r := range s.runs {
		if r.Status == status {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// UpdateRun transitions a run to `to` if its current status is in `from`.
func (s *Store) UpdateRun(_ context.Context, id string, from []store.RunStatus, to store.RunStatus, errMsg string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	if !ok {
		return false, fmt.Errorf("run %s: %w", id, store.ErrNotFound)
	}
	if !slices.Contains(from, r.Status) {
		return false, nil
	}
	r.Status, r.Error, r.UpdatedAt = to, errMsg, s.now()
	s.runs[id] = r
	return true, nil
}

// ListSteps returns all step states of a run, ordered by step ID.
func (s *Store) ListSteps(_ context.Context, runID string) ([]store.StepRun, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	steps, ok := s.steps[runID]
	if !ok {
		return nil, fmt.Errorf("run %s: %w", runID, store.ErrNotFound)
	}
	out := make([]store.StepRun, 0, len(steps))
	for _, st := range steps {
		st.Output = slices.Clone(st.Output)
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StepID < out[j].StepID })
	return out, nil
}

// UpdateStep applies u if the step's current status is in from.
func (s *Store) UpdateStep(_ context.Context, runID, stepID string, from []store.StepStatus, u store.StepUpdate) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	steps, ok := s.steps[runID]
	if !ok {
		return false, fmt.Errorf("run %s: %w", runID, store.ErrNotFound)
	}
	st, ok := steps[stepID]
	if !ok {
		return false, fmt.Errorf("step %s/%s: %w", runID, stepID, store.ErrNotFound)
	}
	if !slices.Contains(from, st.Status) {
		return false, nil
	}
	u.Apply(&st, s.now())
	steps[stepID] = st
	return true, nil
}

// AppendEvent records an event and assigns its sequence number.
func (s *Store) AppendEvent(_ context.Context, ev store.Event) (store.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	ev.Seq = s.seq
	if ev.Time.IsZero() {
		ev.Time = s.now()
	}
	s.events[ev.RunID] = append(s.events[ev.RunID], ev)
	return ev, nil
}

// ListEvents returns up to limit events of a run with Seq > afterSeq.
func (s *Store) ListEvents(_ context.Context, runID string, afterSeq int64, limit int) ([]store.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	evs := s.events[runID]
	i := sort.Search(len(evs), func(i int) bool { return evs[i].Seq > afterSeq })
	end := len(evs)
	if limit > 0 && i+limit < end {
		end = i + limit
	}
	return slices.Clone(evs[i:end]), nil
}
