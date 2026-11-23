package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/RatulSikder97/agentflow/llm"
	"github.com/RatulSikder97/agentflow/queue"
	"github.com/RatulSikder97/agentflow/store"
	"github.com/RatulSikder97/agentflow/tool"
	"github.com/RatulSikder97/agentflow/workflow"
)

func attrRun(id string) attribute.KeyValue     { return attribute.String("agentflow.run_id", id) }
func attrWorkflow(n string) attribute.KeyValue { return attribute.String("agentflow.workflow", n) }
func attrStep(id string) attribute.KeyValue    { return attribute.String("agentflow.step_id", id) }
func attrKind(k workflow.Kind) attribute.KeyValue {
	return attribute.String("agentflow.step_kind", string(k))
}

// ExecuteStep runs one job to completion, including in-process retries.
//
// A nil return means the job is fully handled and the delivery may be
// acknowledged (this includes permanent step failures and stale jobs). A
// non-nil error means the job must stay unacknowledged so the queue
// redelivers it: the worker is shutting down or the store is unreachable.
func (e *Engine) ExecuteStep(ctx context.Context, job queue.Job) error {
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(job.Trace))

	run, err := e.store.GetRun(ctx, job.RunID)
	if errors.Is(err, store.ErrNotFound) {
		e.log.WarnContext(ctx, "dropping job for unknown run", "run_id", job.RunID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("execute: %w", err)
	}
	if run.Status.Terminal() {
		return nil
	}
	wf, err := e.workflow(ctx, run.WorkflowID)
	if err != nil {
		return fmt.Errorf("execute: %w", err)
	}
	spec, ok := wf.Step(job.StepID)
	if !ok {
		e.log.WarnContext(ctx, "dropping job for unknown step", "run_id", job.RunID, "step_id", job.StepID)
		return nil
	}
	rows, err := e.store.ListSteps(ctx, run.ID)
	if err != nil {
		return fmt.Errorf("execute: %w", err)
	}
	var st store.StepRun
	for _, r := range rows {
		if r.StepID == job.StepID {
			st = r
		}
	}
	switch st.Status {
	case store.StepSucceeded, store.StepFailed, store.StepSkipped:
		// Duplicate delivery of a finished step: its output is already
		// committed. Re-run the scheduler in case we crashed before it ran.
		return e.advance(ctx, run.ID)
	case store.StepPending:
		return nil // not scheduled yet; advance will publish it
	}

	ctx, span := e.tracer.Start(ctx, "agentflow.step "+spec.ID,
		trace.WithAttributes(attrRun(run.ID), attrWorkflow(wf.Name), attrStep(spec.ID), attrKind(spec.Kind)))
	defer span.End()

	data, err := buildData(run, rows)
	if err != nil {
		return fmt.Errorf("execute: %w", err)
	}
	attempt := st.Attempt
	for {
		attempt++
		if attempt > spec.Retry.MaxAttempts {
			// Only reachable after crashes mid-attempt: the attempt counter is
			// persisted, so a step that keeps killing workers still terminates.
			return e.failStep(ctx, run.ID, spec.ID, fmt.Errorf("gave up after %d attempts", spec.Retry.MaxAttempts))
		}
		ok, err := e.store.UpdateStep(ctx, run.ID, spec.ID,
			[]store.StepStatus{store.StepQueued, store.StepRunning},
			store.StepUpdate{Status: store.StepRunning, IncAttempt: true})
		if err != nil {
			return fmt.Errorf("execute: start %s: %w", spec.ID, err)
		}
		if !ok {
			return nil // another worker finished it
		}
		e.emit(ctx, run.ID, store.EventStepStarted, spec.ID, map[string]any{"attempt": attempt})

		start := time.Now()
		out, stepErr := e.attempt(ctx, run, spec, data, attempt)
		elapsed := time.Since(start)

		if stepErr == nil {
			raw, err := json.Marshal(out)
			if err != nil {
				stepErr = tool.Permanent(fmt.Errorf("encode output: %w", err))
			} else {
				ok, err := e.store.UpdateStep(ctx, run.ID, spec.ID, []store.StepStatus{store.StepRunning},
					store.StepUpdate{Status: store.StepSucceeded, Output: raw})
				if err != nil {
					return fmt.Errorf("execute: commit %s: %w", spec.ID, err)
				}
				e.metrics.StepFinished(string(spec.Kind), string(store.StepSucceeded), elapsed)
				if ok {
					e.emit(ctx, run.ID, store.EventStepSucceeded, spec.ID,
						map[string]any{"attempt": attempt, "duration_ms": elapsed.Milliseconds()})
				}
				return e.advance(ctx, run.ID)
			}
		}

		if ctx.Err() != nil {
			// Worker shutdown or crash: leave the step "running" and the job
			// unacknowledged so another worker picks it up.
			return ctx.Err()
		}
		e.metrics.StepFinished(string(spec.Kind), "error", elapsed)
		span.RecordError(stepErr)
		if retryable(stepErr) && attempt < spec.Retry.MaxAttempts {
			delay := Backoff(attempt, spec.Retry.Backoff.Std(), spec.Retry.MaxBackoff.Std(), rand.Float64)
			e.metrics.StepRetried(string(spec.Kind))
			e.emit(ctx, run.ID, store.EventStepRetrying, spec.ID, map[string]any{
				"attempt": attempt, "error": truncate(stepErr.Error(), 500), "backoff_ms": delay.Milliseconds(),
			})
			e.log.WarnContext(ctx, "step attempt failed, retrying", "run_id", run.ID, "step_id", spec.ID,
				"attempt", attempt, "backoff", delay, "err", stepErr)
			if err := e.sleep(ctx, delay); err != nil {
				return err
			}
			continue
		}
		span.SetStatus(codes.Error, stepErr.Error())
		return e.failStep(ctx, run.ID, spec.ID, stepErr)
	}
}

func (e *Engine) failStep(ctx context.Context, runID, stepID string, cause error) error {
	msg := truncate(cause.Error(), 2000)
	ok, err := e.store.UpdateStep(ctx, runID, stepID,
		[]store.StepStatus{store.StepQueued, store.StepRunning},
		store.StepUpdate{Status: store.StepFailed, Error: msg})
	if err != nil {
		return fmt.Errorf("execute: fail %s: %w", stepID, err)
	}
	if ok {
		e.emit(ctx, runID, store.EventStepFailed, stepID, map[string]any{"error": msg})
		e.log.ErrorContext(ctx, "step failed", "run_id", runID, "step_id", stepID, "err", cause)
	}
	return e.advance(ctx, runID)
}

// retryable classifies errors. Errors are retryable unless they (or an error
// they wrap) implement Retryable() and return false.
func retryable(err error) bool {
	var r interface{ Retryable() bool }
	if errors.As(err, &r) {
		return r.Retryable()
	}
	return true
}

// Backoff returns the delay before retry number `attempt` (1-based):
// exponential growth from base, capped at maxDelay, with "equal jitter" so
// the result lies in [d/2, d]. rnd must return values in [0, 1).
func Backoff(attempt int, base, maxDelay time.Duration, rnd func() float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := float64(base) * math.Pow(2, float64(attempt-1))
	if d > float64(maxDelay) || math.IsInf(d, 0) {
		d = float64(maxDelay)
	}
	half := d / 2
	return time.Duration(half + rnd()*half)
}

func buildData(run store.Run, rows []store.StepRun) (workflow.Data, error) {
	steps := make(map[string]any, len(rows))
	for _, r := range rows {
		if r.Status != store.StepSucceeded {
			continue
		}
		var out any
		if len(r.Output) > 0 {
			if err := json.Unmarshal(r.Output, &out); err != nil {
				return nil, fmt.Errorf("decode output of %s: %w", r.StepID, err)
			}
		}
		steps[r.StepID] = map[string]any{"output": out, "status": string(r.Status)}
	}
	input := run.Input
	if input == nil {
		input = map[string]any{}
	}
	return workflow.Data{"input": input, "steps": steps}, nil
}

// attempt executes a single attempt of a step under its timeout.
func (e *Engine) attempt(ctx context.Context, run store.Run, spec *workflow.Step, data workflow.Data, attempt int) (any, error) {
	ctx, cancel := context.WithTimeout(ctx, spec.Timeout.Std())
	defer cancel()
	key := run.ID + "/" + spec.ID
	switch spec.Kind {
	case workflow.KindBranch:
		return e.runBranch(spec, data)
	case workflow.KindMap:
		return e.runMap(ctx, run, spec, data, key, attempt)
	default:
		return e.runLeaf(ctx, run, spec, data, key, attempt)
	}
}

// runLeaf executes an llm or tool step (also used for each map item).
func (e *Engine) runLeaf(ctx context.Context, run store.Run, spec *workflow.Step, data workflow.Data, key string, attempt int) (any, error) {
	switch spec.Kind {
	case workflow.KindLLM:
		return e.runLLM(ctx, run, spec, data, attempt)
	case workflow.KindTool:
		return e.runTool(ctx, spec, data, key)
	default:
		return nil, tool.Permanent(fmt.Errorf("step %s: kind %q cannot run here", spec.ID, spec.Kind))
	}
}

func (e *Engine) runLLM(ctx context.Context, run store.Run, spec *workflow.Step, data workflow.Data, attempt int) (any, error) {
	system, err := workflow.Render(spec.System, data)
	if err != nil {
		return nil, tool.Permanent(fmt.Errorf("system: %w", err))
	}
	prompt, err := workflow.Render(spec.Prompt, data)
	if err != nil {
		return nil, tool.Permanent(fmt.Errorf("prompt: %w", err))
	}
	resp, err := e.llm.Complete(ctx, llm.Request{
		Model:     spec.Model,
		System:    system,
		Messages:  []llm.Message{{Role: llm.RoleUser, Content: prompt}},
		MaxTokens: spec.MaxTokens,
		Metadata:  map[string]string{"run_id": run.ID, "step_id": spec.ID, "attempt": strconv.Itoa(attempt)},
	})
	if err != nil {
		return nil, fmt.Errorf("llm: %w", err)
	}
	e.metrics.Tokens(spec.Model, resp.Usage.InputTokens, resp.Usage.OutputTokens)
	if spec.Output == workflow.OutputJSON {
		v, err := ExtractJSON(resp.Text)
		if err != nil {
			return nil, fmt.Errorf("llm: %w", err) // retryable: the model may comply next time
		}
		return v, nil
	}
	return resp.Text, nil
}

// ExtractJSON decodes the first JSON object or array in text, tolerating
// Markdown code fences and surrounding prose.
func ExtractJSON(text string) (any, error) {
	start := strings.IndexAny(text, "{[")
	if start < 0 {
		return nil, errors.New("no JSON value in model output")
	}
	dec := json.NewDecoder(strings.NewReader(text[start:]))
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("decode model JSON: %w", err)
	}
	return v, nil
}

func (e *Engine) runTool(ctx context.Context, spec *workflow.Step, data workflow.Data, key string) (any, error) {
	if spec.HTTP != nil {
		h := spec.HTTP
		url, err := workflow.Render(h.URL, data)
		if err != nil {
			return nil, tool.Permanent(fmt.Errorf("http.url: %w", err))
		}
		body, err := workflow.Render(h.Body, data)
		if err != nil {
			return nil, tool.Permanent(fmt.Errorf("http.body: %w", err))
		}
		headers := make(map[string]string, len(h.Headers))
		for k, v := range h.Headers {
			if headers[k], err = workflow.Render(v, data); err != nil {
				return nil, tool.Permanent(fmt.Errorf("http.headers.%s: %w", k, err))
			}
		}
		return tool.DoHTTP(ctx, e.http, tool.HTTPRequest{
			Method: h.Method, URL: url, Headers: headers, Body: body, IdempotencyKey: key,
		})
	}
	f, err := e.tools.Lookup(spec.Tool)
	if err != nil {
		return nil, tool.Permanent(err)
	}
	args, err := workflow.RenderValue(spec.Args, data)
	if err != nil {
		return nil, tool.Permanent(fmt.Errorf("args: %w", err))
	}
	argMap, _ := args.(map[string]any)
	if argMap == nil {
		argMap = map[string]any{}
	}
	out, err := f(ctx, tool.Call{Args: argMap, IdempotencyKey: key})
	if err != nil {
		return nil, fmt.Errorf("tool %s: %w", spec.Tool, err)
	}
	return out, nil
}

func (e *Engine) runBranch(spec *workflow.Step, data workflow.Data) (any, error) {
	for i, c := range spec.Cases {
		ok, err := workflow.Truthy(c.When, data)
		if err != nil {
			return nil, tool.Permanent(fmt.Errorf("case %d: %w", i, err))
		}
		if ok {
			return branchOutput{Taken: c.Then, Case: i}, nil
		}
	}
	return branchOutput{Taken: spec.Default, Case: -1}, nil
}

// runMap fans the nested step out over a list with bounded concurrency. The
// map step commits as one unit: if any item fails, the whole step is retried.
func (e *Engine) runMap(ctx context.Context, run store.Run, spec *workflow.Step, data workflow.Data, key string, attempt int) (any, error) {
	v, err := workflow.Lookup(spec.Over, data)
	if err != nil {
		return nil, tool.Permanent(fmt.Errorf("over: %w", err))
	}
	items, ok := v.([]any)
	if !ok {
		return nil, tool.Permanent(fmt.Errorf("over: %s is %T, want list", spec.Over, v))
	}
	results := make([]any, len(items))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(spec.Concurrency)
	for i, item := range items {
		g.Go(func() error {
			d := make(workflow.Data, len(data)+2)
			for k, v := range data {
				d[k] = v
			}
			d["item"], d["index"] = item, i
			out, err := e.runLeaf(gctx, run, spec.Step, d, key+"/"+strconv.Itoa(i), attempt)
			if err != nil {
				return fmt.Errorf("item %d: %w", i, err)
			}
			results[i] = out
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}
