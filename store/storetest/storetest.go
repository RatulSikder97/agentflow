// Package storetest is a contract test suite every store implementation must
// pass. It pins down the compare-and-set semantics the engine relies on.
package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RatulSikder97/agentflow/store"
)

// Store is the full store surface under test.
type Store interface {
	CreateWorkflow(ctx context.Context, wf store.Workflow) error
	GetWorkflow(ctx context.Context, id string) (store.Workflow, error)
	CreateRun(ctx context.Context, run store.Run, stepIDs []string) error
	GetRun(ctx context.Context, id string) (store.Run, error)
	ListRuns(ctx context.Context, status store.RunStatus) ([]store.Run, error)
	UpdateRun(ctx context.Context, id string, from []store.RunStatus, to store.RunStatus, errMsg string) (bool, error)
	ListSteps(ctx context.Context, runID string) ([]store.StepRun, error)
	UpdateStep(ctx context.Context, runID, stepID string, from []store.StepStatus, u store.StepUpdate) (bool, error)
	AppendEvent(ctx context.Context, ev store.Event) (store.Event, error)
	ListEvents(ctx context.Context, runID string, afterSeq int64, limit int) ([]store.Event, error)
}

var (
	counter atomic.Int64
	epoch   = time.Now().UnixNano() // keeps IDs unique across runs against a persistent DB
)

func uniq(prefix string) string { return fmt.Sprintf("%s_%d_%d", prefix, epoch, counter.Add(1)) }

func seed(t *testing.T, s Store, steps ...string) store.Run {
	t.Helper()
	ctx := context.Background()
	wf := store.Workflow{ID: uniq("wf"), Name: "contract", Spec: "name: contract"}
	if err := s.CreateWorkflow(ctx, wf); err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}
	run := store.Run{ID: uniq("run"), WorkflowID: wf.ID, Status: store.RunRunning, Input: map[string]any{"k": "v"}}
	if err := s.CreateRun(ctx, run, steps); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	return run
}

// Run executes the contract suite. newStore must return an isolated or shared
// store; tests only touch rows they create.
func Run(t *testing.T, newStore func(t *testing.T) Store) {
	t.Run("NotFound", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()
		if _, err := s.GetWorkflow(ctx, "nope"); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("GetWorkflow err = %v, want ErrNotFound", err)
		}
		if _, err := s.GetRun(ctx, "nope"); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("GetRun err = %v, want ErrNotFound", err)
		}
		if _, err := s.UpdateRun(ctx, "nope", []store.RunStatus{store.RunRunning}, store.RunFailed, ""); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("UpdateRun err = %v, want ErrNotFound", err)
		}
	})

	t.Run("CreateRunAndSteps", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()
		run := seed(t, s, "b", "a")
		got, err := s.GetRun(ctx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != store.RunRunning || got.Input["k"] != "v" {
			t.Errorf("run = %+v", got)
		}
		steps, err := s.ListSteps(ctx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(steps) != 2 || steps[0].StepID != "a" || steps[1].StepID != "b" {
			t.Fatalf("steps = %+v, want [a b]", steps)
		}
		for _, st := range steps {
			if st.Status != store.StepPending || st.Attempt != 0 {
				t.Errorf("step %s = %+v, want pending/0", st.StepID, st)
			}
		}
	})

	t.Run("StepCompareAndSet", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()
		run := seed(t, s, "a")
		tests := []struct {
			name   string
			from   []store.StepStatus
			upd    store.StepUpdate
			wantOK bool
		}{
			{"pending->queued", []store.StepStatus{store.StepPending}, store.StepUpdate{Status: store.StepQueued}, true},
			{"stale pending->queued", []store.StepStatus{store.StepPending}, store.StepUpdate{Status: store.StepQueued}, false},
			{"queued->running", []store.StepStatus{store.StepQueued, store.StepRunning}, store.StepUpdate{Status: store.StepRunning, IncAttempt: true}, true},
			{"running->running retry", []store.StepStatus{store.StepQueued, store.StepRunning}, store.StepUpdate{Status: store.StepRunning, IncAttempt: true}, true},
			{"running->succeeded", []store.StepStatus{store.StepRunning}, store.StepUpdate{Status: store.StepSucceeded, Output: json.RawMessage(`{"x":1}`)}, true},
			{"double commit", []store.StepStatus{store.StepRunning}, store.StepUpdate{Status: store.StepSucceeded, Output: json.RawMessage(`{"x":2}`)}, false},
		}
		for _, tc := range tests {
			ok, err := s.UpdateStep(ctx, run.ID, "a", tc.from, tc.upd)
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if ok != tc.wantOK {
				t.Fatalf("%s: ok = %v, want %v", tc.name, ok, tc.wantOK)
			}
		}
		steps, _ := s.ListSteps(ctx, run.ID)
		st := steps[0]
		if st.Status != store.StepSucceeded || st.Attempt != 2 || st.StartedAt == nil || st.FinishedAt == nil {
			t.Errorf("final step = %+v", st)
		}
		var out map[string]int
		if err := json.Unmarshal(st.Output, &out); err != nil || out["x"] != 1 {
			t.Errorf("output = %s, want first commit {x:1}", st.Output)
		}
	})

	t.Run("ConcurrentCommitExactlyOnce", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()
		run := seed(t, s, "a")
		if _, err := s.UpdateStep(ctx, run.ID, "a", []store.StepStatus{store.StepPending}, store.StepUpdate{Status: store.StepRunning}); err != nil {
			t.Fatal(err)
		}
		var wins atomic.Int32
		var wg sync.WaitGroup
		for i := range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ok, err := s.UpdateStep(ctx, run.ID, "a", []store.StepStatus{store.StepRunning},
					store.StepUpdate{Status: store.StepSucceeded, Output: json.RawMessage(fmt.Sprintf(`%d`, i))})
				if err != nil {
					t.Error(err)
				}
				if ok {
					wins.Add(1)
				}
			}()
		}
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("%d commits won, want exactly 1", wins.Load())
		}
	})

	t.Run("RunCompareAndSet", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()
		run := seed(t, s, "a")
		ok, err := s.UpdateRun(ctx, run.ID, []store.RunStatus{store.RunRunning}, store.RunFailed, "boom")
		if err != nil || !ok {
			t.Fatalf("first UpdateRun = %v, %v", ok, err)
		}
		ok, err = s.UpdateRun(ctx, run.ID, []store.RunStatus{store.RunRunning}, store.RunSucceeded, "")
		if err != nil || ok {
			t.Fatalf("second UpdateRun = %v, %v; want false", ok, err)
		}
		got, _ := s.GetRun(ctx, run.ID)
		if got.Status != store.RunFailed || got.Error != "boom" {
			t.Errorf("run = %+v", got)
		}
		failed, err := s.ListRuns(ctx, store.RunFailed)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, r := range failed {
			found = found || r.ID == run.ID
		}
		if !found {
			t.Errorf("ListRuns(failed) missing %s", run.ID)
		}
	})

	t.Run("Events", func(t *testing.T) {
		s := newStore(t)
		ctx := context.Background()
		run := seed(t, s, "a")
		var last int64
		for i := range 5 {
			ev, err := s.AppendEvent(ctx, store.Event{RunID: run.ID, Type: store.EventStepStarted, StepID: "a",
				Data: json.RawMessage(fmt.Sprintf(`{"i":%d}`, i))})
			if err != nil {
				t.Fatal(err)
			}
			if ev.Seq <= last {
				t.Fatalf("seq %d not increasing after %d", ev.Seq, last)
			}
			last = ev.Seq
		}
		all, err := s.ListEvents(ctx, run.ID, 0, 0)
		if err != nil || len(all) != 5 {
			t.Fatalf("ListEvents = %d, %v", len(all), err)
		}
		page, err := s.ListEvents(ctx, run.ID, all[1].Seq, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) != 2 || page[0].Seq != all[2].Seq || page[1].Seq != all[3].Seq {
			t.Fatalf("page = %+v", page)
		}
	})
}
