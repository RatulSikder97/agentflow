// Package engine schedules and executes workflow runs.
//
// Execution model: the engine never holds a run in memory. A run is a set of
// checkpointed step rows in the Store; a step becomes a queue Job when all of
// its dependencies are terminal. Any worker can execute any job. Every state
// change is a compare-and-set on the step row, so duplicate deliveries,
// concurrent workers and crash-restarts converge on the same result: each
// step's output is committed exactly once, even though delivery is
// at-least-once.
package engine

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/RatulSikder97/agentflow/llm"
	"github.com/RatulSikder97/agentflow/queue"
	"github.com/RatulSikder97/agentflow/store"
	"github.com/RatulSikder97/agentflow/telemetry"
	"github.com/RatulSikder97/agentflow/tool"
	"github.com/RatulSikder97/agentflow/workflow"
)

// Store is the persistence the engine needs.
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

// Enqueuer publishes step jobs.
type Enqueuer interface {
	Enqueue(ctx context.Context, job queue.Job) error
}

// Completer is the LLM capability llm steps need.
type Completer interface {
	Complete(ctx context.Context, req llm.Request) (llm.Response, error)
}

// ToolLookup resolves tool names for tool steps.
type ToolLookup interface {
	Lookup(name string) (tool.Func, error)
}

// Config wires an Engine's dependencies. Store, Queue and LLM are required.
type Config struct {
	Store      Store
	Queue      Enqueuer
	LLM        Completer
	Tools      ToolLookup         // default: tool.NewRegistry()
	HTTPClient *http.Client       // for http tool steps; default 30s timeout
	Metrics    *telemetry.Metrics // optional
	Logger     *slog.Logger       // default: slog.Default()
	// Sleep waits between retry attempts; tests replace it to run instantly.
	Sleep func(ctx context.Context, d time.Duration) error
}

// Engine runs workflows. It is safe for concurrent use.
type Engine struct {
	store   Store
	queue   Enqueuer
	llm     Completer
	tools   ToolLookup
	http    *http.Client
	metrics *telemetry.Metrics
	log     *slog.Logger
	sleep   func(ctx context.Context, d time.Duration) error
	tracer  trace.Tracer

	workflows sync.Map // workflow ID -> *workflow.Workflow (workflows are immutable)
}

// New validates cfg and returns an Engine.
func New(cfg Config) (*Engine, error) {
	if cfg.Store == nil || cfg.Queue == nil || cfg.LLM == nil {
		return nil, errors.New("engine: Store, Queue and LLM are required")
	}
	e := &Engine{
		store:   cfg.Store,
		queue:   cfg.Queue,
		llm:     cfg.LLM,
		tools:   cfg.Tools,
		http:    cfg.HTTPClient,
		metrics: cfg.Metrics,
		log:     cfg.Logger,
		sleep:   cfg.Sleep,
		tracer:  otel.Tracer("github.com/RatulSikder97/agentflow/engine"),
	}
	if e.tools == nil {
		e.tools = tool.NewRegistry()
	}
	if e.http == nil {
		e.http = &http.Client{Timeout: 30 * time.Second}
	}
	if e.log == nil {
		e.log = slog.Default()
	}
	if e.sleep == nil {
		e.sleep = sleepCtx
	}
	return e, nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

var idEncoding = base32.NewEncoding("0123456789abcdefghijklmnopqrstuv").WithPadding(base32.NoPadding)

// NewID returns a random, URL-safe identifier with the given prefix.
func NewID(prefix string) string {
	var b [10]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + idEncoding.EncodeToString(b[:])
}

// SubmitWorkflow validates and stores a workflow definition. Stored
// workflows are immutable; submitting a changed file creates a new ID.
func (e *Engine) SubmitWorkflow(ctx context.Context, spec []byte) (store.Workflow, error) {
	wf, err := workflow.Parse(spec)
	if err != nil {
		return store.Workflow{}, err
	}
	rec := store.Workflow{ID: NewID("wf"), Name: wf.Name, Spec: string(spec), CreatedAt: time.Now().UTC()}
	if err := e.store.CreateWorkflow(ctx, rec); err != nil {
		return store.Workflow{}, fmt.Errorf("submit workflow: %w", err)
	}
	e.workflows.Store(rec.ID, wf)
	return rec, nil
}

// GetWorkflow returns a stored workflow.
func (e *Engine) GetWorkflow(ctx context.Context, id string) (store.Workflow, error) {
	return e.store.GetWorkflow(ctx, id)
}

func (e *Engine) workflow(ctx context.Context, id string) (*workflow.Workflow, error) {
	if wf, ok := e.workflows.Load(id); ok {
		return wf.(*workflow.Workflow), nil
	}
	rec, err := e.store.GetWorkflow(ctx, id)
	if err != nil {
		return nil, err
	}
	wf, err := workflow.Parse([]byte(rec.Spec))
	if err != nil {
		return nil, fmt.Errorf("stored workflow %s: %w", id, err)
	}
	e.workflows.Store(id, wf)
	return wf, nil
}

// ErrInvalidInput is returned by StartRun when required inputs are missing.
var ErrInvalidInput = errors.New("invalid input")

// StartRun creates a run and enqueues its root steps.
func (e *Engine) StartRun(ctx context.Context, workflowID string, input map[string]any) (store.Run, error) {
	wf, err := e.workflow(ctx, workflowID)
	if err != nil {
		return store.Run{}, fmt.Errorf("start run: %w", err)
	}
	if input == nil {
		input = map[string]any{}
	}
	if err := wf.CheckInputs(input); err != nil {
		return store.Run{}, fmt.Errorf("%w: %w", ErrInvalidInput, err)
	}
	run := store.Run{ID: NewID("run"), WorkflowID: workflowID, Status: store.RunRunning, Input: input}

	ctx, span := e.tracer.Start(ctx, "agentflow.run.start", trace.WithAttributes(attrRun(run.ID), attrWorkflow(wf.Name)))
	defer span.End()

	if err := e.store.CreateRun(ctx, run, wf.StepIDs()); err != nil {
		return store.Run{}, fmt.Errorf("start run: %w", err)
	}
	e.emit(ctx, run.ID, store.EventRunStarted, "", map[string]any{"workflow_id": workflowID, "workflow": wf.Name})
	if err := e.advance(ctx, run.ID); err != nil {
		return store.Run{}, fmt.Errorf("start run: %w", err)
	}
	return e.store.GetRun(ctx, run.ID)
}

// RunView is a run together with its checkpointed steps.
type RunView struct {
	store.Run
	Steps []store.StepRun `json:"steps"`
}

// GetRun returns a run and its steps.
func (e *Engine) GetRun(ctx context.Context, id string) (RunView, error) {
	run, err := e.store.GetRun(ctx, id)
	if err != nil {
		return RunView{}, err
	}
	steps, err := e.store.ListSteps(ctx, id)
	if err != nil {
		return RunView{}, err
	}
	return RunView{Run: run, Steps: steps}, nil
}

// ListEvents returns run events after a sequence number.
func (e *Engine) ListEvents(ctx context.Context, runID string, afterSeq int64, limit int) ([]store.Event, error) {
	return e.store.ListEvents(ctx, runID, afterSeq, limit)
}

// WaitRun polls until the run is terminal or ctx is done. A non-positive
// interval defaults to 50ms.
func (e *Engine) WaitRun(ctx context.Context, runID string, every time.Duration) (store.Run, error) {
	if every <= 0 {
		every = 50 * time.Millisecond
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		run, err := e.store.GetRun(ctx, runID)
		if err != nil {
			return run, err
		}
		if run.Status.Terminal() {
			return run, nil
		}
		select {
		case <-ctx.Done():
			return run, ctx.Err()
		case <-t.C:
		}
	}
}

// emit appends an event. Failures are logged, never fatal: events are an
// observation channel, step rows are the source of truth.
func (e *Engine) emit(ctx context.Context, runID, typ, stepID string, data map[string]any) {
	var raw json.RawMessage
	if data != nil {
		b, err := json.Marshal(data)
		if err == nil {
			raw = b
		}
	}
	if _, err := e.store.AppendEvent(ctx, store.Event{RunID: runID, Type: typ, StepID: stepID, Data: raw}); err != nil {
		e.log.WarnContext(ctx, "append event failed", "run_id", runID, "type", typ, "err", err)
	}
}

func (e *Engine) enqueue(ctx context.Context, runID, stepID string) error {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	job := queue.Job{RunID: runID, StepID: stepID}
	if len(carrier) > 0 {
		job.Trace = carrier
	}
	if err := e.queue.Enqueue(ctx, job); err != nil {
		return fmt.Errorf("enqueue %s: %w", job.Key(), err)
	}
	return nil
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
