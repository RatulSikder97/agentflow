package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/RatulSikder97/agentflow/store"
	"github.com/RatulSikder97/agentflow/workflow"
)

// advance is the scheduler. It is a pure function of the checkpointed step
// states: it queues pending steps whose dependencies are all terminal, skips
// steps on untaken branches, and finishes the run once nothing is left. It is
// safe to call any number of times from any number of processes because every
// transition it makes is a compare-and-set.
func (e *Engine) advance(ctx context.Context, runID string) error {
	run, err := e.store.GetRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("advance: %w", err)
	}
	if run.Status.Terminal() {
		return nil
	}
	wf, err := e.workflow(ctx, run.WorkflowID)
	if err != nil {
		return fmt.Errorf("advance: %w", err)
	}
	rows, err := e.store.ListSteps(ctx, runID)
	if err != nil {
		return fmt.Errorf("advance: %w", err)
	}
	steps := make(map[string]store.StepRun, len(rows))
	for _, r := range rows {
		steps[r.StepID] = r
	}

	for changed := true; changed; {
		changed = false
		for _, id := range wf.Order() {
			st := steps[id]
			if st.Status != store.StepPending {
				continue
			}
			spec, _ := wf.Step(id)
			ready, skip := readiness(wf, spec.DependsOn, id, steps)
			if !ready {
				continue
			}
			if skip {
				ok, err := e.store.UpdateStep(ctx, runID, id, []store.StepStatus{store.StepPending},
					store.StepUpdate{Status: store.StepSkipped})
				if err != nil {
					return fmt.Errorf("advance: skip %s: %w", id, err)
				}
				if ok {
					e.emit(ctx, runID, store.EventStepSkipped, id, nil)
				}
				st.Status = store.StepSkipped
				steps[id] = st
				changed = true
				continue
			}
			ok, err := e.store.UpdateStep(ctx, runID, id, []store.StepStatus{store.StepPending},
				store.StepUpdate{Status: store.StepQueued})
			if err != nil {
				return fmt.Errorf("advance: queue %s: %w", id, err)
			}
			if ok {
				// Emit first so "queued" always precedes the worker's "started".
				// If Enqueue fails the step stays "queued"; Reconcile re-publishes it.
				e.emit(ctx, runID, store.EventStepQueued, id, nil)
				if err := e.enqueue(ctx, runID, id); err != nil {
					return fmt.Errorf("advance: %w", err)
				}
			}
			st.Status = store.StepQueued
			steps[id] = st
		}
	}

	var failed []string
	done := true
	for _, st := range steps {
		switch {
		case st.Status == store.StepFailed:
			failed = append(failed, st.StepID)
		case !st.Status.Terminal():
			done = false
		}
	}
	switch {
	case len(failed) > 0:
		slices.Sort(failed)
		return e.finishRun(ctx, runID, store.RunFailed, "step(s) failed: "+strings.Join(failed, ", "), steps)
	case done:
		return e.finishRun(ctx, runID, store.RunSucceeded, "", steps)
	}
	return nil
}

// readiness decides whether a pending step can be scheduled. A step is ready
// when every dependency is terminal and none failed. It is skipped when all
// its dependencies were skipped, or when a branch dependency did not select it.
func readiness(wf *workflow.Workflow, deps []string, id string, steps map[string]store.StepRun) (ready, skip bool) {
	if len(deps) == 0 {
		return true, false
	}
	anySucceeded := false
	for _, d := range deps {
		ds := steps[d]
		if !ds.Status.Terminal() || ds.Status == store.StepFailed {
			return false, false
		}
		if ds.Status == store.StepSucceeded {
			anySucceeded = true
		}
	}
	if !anySucceeded {
		return true, true
	}
	for _, d := range deps {
		ds := steps[d]
		if ds.Status != store.StepSucceeded {
			continue
		}
		if dspec, _ := wf.Step(d); dspec.Kind != workflow.KindBranch {
			continue
		}
		var out branchOutput
		if err := json.Unmarshal(ds.Output, &out); err != nil || !slices.Contains(out.Taken, id) {
			return true, true
		}
	}
	return true, false
}

// branchOutput is the output of a branch step.
type branchOutput struct {
	Taken []string `json:"taken"`
	Case  int      `json:"case"` // index of the matched case, -1 for default
}

func (e *Engine) finishRun(ctx context.Context, runID string, status store.RunStatus, msg string, steps map[string]store.StepRun) error {
	ok, err := e.store.UpdateRun(ctx, runID, []store.RunStatus{store.RunRunning, store.RunPending}, status, msg)
	if err != nil {
		return fmt.Errorf("finish run: %w", err)
	}
	if !ok {
		return nil
	}
	if status == store.RunFailed {
		// Nothing downstream will ever run; make that visible.
		for id, st := range steps {
			if st.Status != store.StepPending {
				continue
			}
			if ok, err := e.store.UpdateStep(ctx, runID, id, []store.StepStatus{store.StepPending},
				store.StepUpdate{Status: store.StepSkipped}); err == nil && ok {
				e.emit(ctx, runID, store.EventStepSkipped, id, nil)
			}
		}
	}
	typ := store.EventRunSucceeded
	if status == store.RunFailed {
		typ = store.EventRunFailed
	}
	var data map[string]any
	if msg != "" {
		data = map[string]any{"error": msg}
	}
	e.emit(ctx, runID, typ, "", data)
	e.metrics.RunFinished(string(status))
	e.log.InfoContext(ctx, "run finished", "run_id", runID, "status", status)
	return nil
}

// Reconcile repairs runs after a crash of the scheduling process. For every
// running run it re-publishes steps stuck in "queued" for longer than
// staleAfter (the enqueue was lost) and re-runs the scheduler (a step
// completed but its successors were never queued). Steps in "running" are
// left to the queue's redelivery of unacknowledged jobs. It returns the
// number of jobs re-published.
func (e *Engine) Reconcile(ctx context.Context, staleAfter time.Duration) (int, error) {
	runs, err := e.store.ListRuns(ctx, store.RunRunning)
	if err != nil {
		return 0, fmt.Errorf("reconcile: %w", err)
	}
	n := 0
	cutoff := time.Now().Add(-staleAfter)
	for _, run := range runs {
		steps, err := e.store.ListSteps(ctx, run.ID)
		if err != nil {
			return n, fmt.Errorf("reconcile %s: %w", run.ID, err)
		}
		for _, st := range steps {
			if st.Status == store.StepQueued && st.UpdatedAt.Before(cutoff) {
				if err := e.enqueue(ctx, run.ID, st.StepID); err != nil {
					return n, fmt.Errorf("reconcile: %w", err)
				}
				n++
			}
		}
		if err := e.advance(ctx, run.ID); err != nil {
			return n, fmt.Errorf("reconcile: %w", err)
		}
	}
	return n, nil
}
