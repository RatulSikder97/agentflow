package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RatulSikder97/agentflow/engine"
	"github.com/RatulSikder97/agentflow/llm/fake"
	"github.com/RatulSikder97/agentflow/queue"
	"github.com/RatulSikder97/agentflow/queue/memqueue"
	"github.com/RatulSikder97/agentflow/store"
	"github.com/RatulSikder97/agentflow/store/memstore"
	"github.com/RatulSikder97/agentflow/tool"
)

type harness struct {
	eng    *engine.Engine
	store  *memstore.Store
	queue  *memqueue.Queue
	llm    *fake.Provider
	tools  *tool.Registry
	delays []time.Duration
	mu     sync.Mutex
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newHarness(t testing.TB, qopts ...memqueue.Option) *harness {
	t.Helper()
	h := &harness{store: memstore.New(), queue: memqueue.New(qopts...), tools: tool.NewRegistry()}
	var err error
	h.llm, err = fake.Load("../examples/fake-llm.yaml")
	if err != nil {
		t.Fatal(err)
	}
	h.eng, err = engine.New(engine.Config{
		Store: h.store, Queue: h.queue, LLM: h.llm, Tools: h.tools, Logger: quietLogger(),
		Sleep: func(ctx context.Context, d time.Duration) error {
			h.mu.Lock()
			h.delays = append(h.delays, d)
			h.mu.Unlock()
			return ctx.Err()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.queue.Close() })
	return h
}

// startWorker runs a worker until the returned stop function is called.
func (h *harness) startWorker(t testing.TB, opts ...engine.WorkerOption) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = engine.NewWorker(h.eng, h.queue, opts...).Run(ctx)
	}()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done }) }
	t.Cleanup(stop)
	return stop
}

func (h *harness) start(t testing.TB, spec string, input map[string]any) store.Run {
	t.Helper()
	ctx := context.Background()
	wf, err := h.eng.SubmitWorkflow(ctx, []byte(spec))
	if err != nil {
		t.Fatalf("SubmitWorkflow: %v", err)
	}
	run, err := h.eng.StartRun(ctx, wf.ID, input)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	return run
}

func (h *harness) wait(t testing.TB, runID string) engine.RunView {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := h.eng.WaitRun(ctx, runID, time.Millisecond); err != nil {
		t.Fatalf("WaitRun: %v", err)
	}
	v, err := h.eng.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func (h *harness) runToEnd(t testing.TB, spec string, input map[string]any) engine.RunView {
	t.Helper()
	h.startWorker(t)
	return h.wait(t, h.start(t, spec, input).ID)
}

func stepOf(v engine.RunView, id string) store.StepRun {
	for _, s := range v.Steps {
		if s.StepID == id {
			return s
		}
	}
	return store.StepRun{}
}

func outputOf[T any](t *testing.T, v engine.RunView, id string) T {
	t.Helper()
	var out T
	if err := json.Unmarshal(stepOf(v, id).Output, &out); err != nil {
		t.Fatalf("decode %s output %s: %v", id, stepOf(v, id).Output, err)
	}
	return out
}

func readExample(t testing.TB, name string) string {
	t.Helper()
	b, err := os.ReadFile("../examples/" + name + "/workflow.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// counter is a tool that counts invocations per idempotency key.
type counter struct {
	mu    sync.Mutex
	calls map[string]int
}

func newCounter() *counter { return &counter{calls: map[string]int{}} }

func (c *counter) fn(_ context.Context, call tool.Call) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[call.IdempotencyKey]++
	return map[string]any{"n": c.calls[call.IdempotencyKey]}, nil
}

func (c *counter) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, v := range c.calls {
		n += v
	}
	return n
}

func TestResearchExampleFansOut(t *testing.T) {
	h := newHarness(t)
	v := h.runToEnd(t, readExample(t, "research-summarize"), map[string]any{"topic": "small language models"})
	if v.Status != store.RunSucceeded {
		t.Fatalf("run %s: %s", v.Status, v.Error)
	}
	notes := outputOf[[]string](t, v, "research")
	if len(notes) != 3 || !strings.Contains(notes[1], "training data and evaluation") {
		t.Errorf("research notes = %q", notes)
	}
	stats := outputOf[map[string]int](t, v, "stats")
	if stats["words"] == 0 {
		t.Errorf("stats = %v", stats)
	}
	if got := h.llm.Calls(); got != 5 { // plan + 3 notes + brief
		t.Errorf("llm calls = %d, want 5", got)
	}
}

func TestTriageExampleBranches(t *testing.T) {
	tests := []struct {
		body      string
		wantRoute string
		wantPrio  string
	}{
		{"I was charged twice, please refund me.", "billing_reply", "high"},
		{"The app shows error 500 on login.", "technical_reply", "normal"},
		{"Do you have a dark mode?", "general_reply", "low"},
	}
	replies := []string{"billing_reply", "technical_reply", "general_reply"}
	for _, tc := range tests {
		t.Run(tc.wantRoute, func(t *testing.T) {
			h := newHarness(t)
			v := h.runToEnd(t, readExample(t, "support-triage"),
				map[string]any{"ticket_id": "T-1", "subject": "Help", "body": tc.body})
			if v.Status != store.RunSucceeded {
				t.Fatalf("run %s: %s", v.Status, v.Error)
			}
			for _, r := range replies {
				want := store.StepSkipped
				if r == tc.wantRoute {
					want = store.StepSucceeded
				}
				if got := stepOf(v, r).Status; got != want {
					t.Errorf("%s status = %s, want %s", r, got, want)
				}
			}
			rec := outputOf[map[string]string](t, v, "record")
			if rec["route"] != tc.wantRoute || rec["priority"] != tc.wantPrio || rec["reply"] == "" {
				t.Errorf("record = %v", rec)
			}
		})
	}
}

func TestRetries(t *testing.T) {
	tests := []struct {
		name         string
		failures     int // calls that fail before success; -1 = always
		permanent    bool
		maxAttempts  int
		wantStatus   store.RunStatus
		wantAttempts int
		wantDelays   int
	}{
		{"succeeds first try", 0, false, 3, store.RunSucceeded, 1, 0},
		{"recovers after two failures", 2, false, 3, store.RunSucceeded, 3, 2},
		{"exhausts attempts", -1, false, 3, store.RunFailed, 3, 2},
		{"permanent error is not retried", -1, true, 5, store.RunFailed, 1, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			var calls atomic.Int32
			h.tools.Register("flaky", func(context.Context, tool.Call) (any, error) {
				n := int(calls.Add(1))
				if tc.failures < 0 || n <= tc.failures {
					err := fmt.Errorf("transient failure %d", n)
					if tc.permanent {
						return nil, tool.Permanent(err)
					}
					return nil, err
				}
				return "ok", nil
			})
			spec := fmt.Sprintf(`
name: retry
steps:
  - id: a
    kind: tool
    tool: flaky
    retry: {max_attempts: %d, backoff: 100ms, max_backoff: 1s}
  - id: b
    kind: tool
    tool: echo
    depends_on: [a]
`, tc.maxAttempts)
			v := h.runToEnd(t, spec, nil)
			if v.Status != tc.wantStatus {
				t.Fatalf("status = %s (%s), want %s", v.Status, v.Error, tc.wantStatus)
			}
			a := stepOf(v, "a")
			if a.Attempt != tc.wantAttempts {
				t.Errorf("attempts = %d, want %d", a.Attempt, tc.wantAttempts)
			}
			if len(h.delays) != tc.wantDelays {
				t.Errorf("backoff sleeps = %v, want %d", h.delays, tc.wantDelays)
			}
			for i, d := range h.delays {
				lo, hi := time.Duration(50<<i)*time.Millisecond, time.Duration(100<<i)*time.Millisecond
				if d < lo || d > hi {
					t.Errorf("delay[%d] = %v, want within [%v, %v]", i, d, lo, hi)
				}
			}
			if tc.wantStatus == store.RunFailed {
				if a.Status != store.StepFailed || a.Error == "" {
					t.Errorf("step a = %+v", a)
				}
				if b := stepOf(v, "b"); b.Status != store.StepSkipped {
					t.Errorf("downstream b = %s, want skipped", b.Status)
				}
			}
		})
	}
}

func TestStepTimeout(t *testing.T) {
	h := newHarness(t)
	v := h.runToEnd(t, `
name: timeout
steps:
  - id: slow
    kind: tool
    tool: sleep
    timeout: 20ms
    retry: {max_attempts: 2}
    args: {duration: 5s}
`, nil)
	s := stepOf(v, "slow")
	if v.Status != store.RunFailed || s.Attempt != 2 || !strings.Contains(s.Error, "deadline exceeded") {
		t.Fatalf("run=%s step=%+v", v.Status, s)
	}
}

func TestMissingInputs(t *testing.T) {
	h := newHarness(t)
	wf, err := h.eng.SubmitWorkflow(context.Background(), []byte(readExample(t, "research-summarize")))
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.eng.StartRun(context.Background(), wf.ID, map[string]any{})
	if !errors.Is(err, engine.ErrInvalidInput) {
		t.Fatalf("err = %v, want ErrInvalidInput", err)
	}
}

const chain = `
name: chain
steps:
  - {id: a, kind: tool, tool: count}
  - {id: b, kind: tool, tool: count, depends_on: [a]}
  - {id: c, kind: tool, tool: count, depends_on: [b]}
`

// Redelivering every job of a finished run must not execute anything again
// or change any committed output.
func TestDuplicateDeliveriesAreIdempotent(t *testing.T) {
	h := newHarness(t)
	c := newCounter()
	h.tools.Register("count", c.fn)
	v := h.runToEnd(t, chain, nil)
	if v.Status != store.RunSucceeded || c.total() != 3 {
		t.Fatalf("first pass: status=%s calls=%d", v.Status, c.total())
	}
	before, _ := json.Marshal(v.Steps)

	ctx := context.Background()
	for range 3 {
		for _, id := range []string{"a", "b", "c"} {
			if err := h.eng.ExecuteStep(ctx, queue.Job{RunID: v.ID, StepID: id}); err != nil {
				t.Fatalf("ExecuteStep(%s): %v", id, err)
			}
		}
	}
	after, _ := h.eng.GetRun(ctx, v.ID)
	afterJSON, _ := json.Marshal(after.Steps)
	if c.total() != 3 {
		t.Errorf("tool calls after redelivery = %d, want 3", c.total())
	}
	if string(before) != string(afterJSON) {
		t.Errorf("step rows changed:\nbefore %s\nafter  %s", before, afterJSON)
	}
}

// A worker dies in the middle of step b. A second worker must resume the run
// from the checkpoint: a is not re-executed, b is retried, c runs once.
func TestCrashResume(t *testing.T) {
	h := newHarness(t, memqueue.WithVisibility(100*time.Millisecond))
	c := newCounter()
	h.tools.Register("count", c.fn)

	var bCalls atomic.Int32
	bStarted := make(chan struct{})
	h.tools.Register("block_once", func(ctx context.Context, call tool.Call) (any, error) {
		if bCalls.Add(1) == 1 {
			close(bStarted)
			<-ctx.Done() // hang until the worker is killed
			return nil, ctx.Err()
		}
		return c.fn(ctx, call)
	})
	spec := strings.Replace(chain, "{id: b, kind: tool, tool: count", "{id: b, kind: tool, tool: block_once", 1)

	stopFirst := h.startWorker(t)
	run := h.start(t, spec, nil)
	select {
	case <-bStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("step b never started")
	}
	stopFirst() // crash

	mid, _ := h.eng.GetRun(context.Background(), run.ID)
	if a, b := stepOf(mid, "a"), stepOf(mid, "b"); a.Status != store.StepSucceeded || b.Status != store.StepRunning || b.Attempt != 1 {
		t.Fatalf("checkpoint after crash: a=%+v b=%+v", a, b)
	}

	h.startWorker(t) // replacement worker; the unacked job for b is redelivered
	v := h.wait(t, run.ID)
	if v.Status != store.RunSucceeded {
		t.Fatalf("run %s: %s", v.Status, v.Error)
	}
	calls := map[string]int{}
	c.mu.Lock()
	for k, n := range c.calls {
		calls[k[strings.LastIndex(k, "/")+1:]] = n
	}
	c.mu.Unlock()
	if calls["a"] != 1 || calls["b"] != 1 || calls["c"] != 1 {
		t.Errorf("committed executions = %v, want a=1 b=1 c=1", calls)
	}
	if b := stepOf(v, "b"); b.Attempt != 2 {
		t.Errorf("b attempts = %d, want 2 (one abandoned by the crash)", b.Attempt)
	}
}

// lossyQueue drops enqueues while dropping is set, simulating a crash between
// the scheduler's checkpoint and the publish.
type lossyQueue struct {
	*memqueue.Queue
	dropping atomic.Bool
}

func (q *lossyQueue) Enqueue(ctx context.Context, j queue.Job) error {
	if q.dropping.Load() {
		return nil
	}
	return q.Queue.Enqueue(ctx, j)
}

func TestReconcileRepublishesLostJobs(t *testing.T) {
	mq := memqueue.New()
	t.Cleanup(func() { _ = mq.Close() })
	lq := &lossyQueue{Queue: mq}
	lq.dropping.Store(true)
	st := memstore.New()
	llm, _ := fake.Load("../examples/fake-llm.yaml")
	tools := tool.NewRegistry()
	c := newCounter()
	tools.Register("count", c.fn)
	eng, err := engine.New(engine.Config{Store: st, Queue: lq, LLM: llm, Tools: tools, Logger: quietLogger()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	wf, _ := eng.SubmitWorkflow(ctx, []byte(chain))
	run, err := eng.StartRun(ctx, wf.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if mq.Len() != 0 {
		t.Fatalf("queue len = %d, want 0 (dropped)", mq.Len())
	}
	lq.dropping.Store(false)

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = engine.NewWorker(eng, mq).Run(wctx) }()

	n, err := eng.Reconcile(ctx, 0)
	if err != nil || n != 1 {
		t.Fatalf("Reconcile = %d, %v; want 1 job", n, err)
	}
	wctx2, cancel2 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel2()
	final, err := eng.WaitRun(wctx2, run.ID, time.Millisecond)
	if err != nil || final.Status != store.RunSucceeded || c.total() != 3 {
		t.Fatalf("after reconcile: %+v err=%v calls=%d", final, err, c.total())
	}
}

// gauge tracks the maximum number of concurrent tool executions.
type gauge struct {
	cur, max atomic.Int32
}

func (g *gauge) fn(ctx context.Context, _ tool.Call) (any, error) {
	n := g.cur.Add(1)
	defer g.cur.Add(-1)
	for {
		m := g.max.Load()
		if n <= m || g.max.CompareAndSwap(m, n) {
			break
		}
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(5 * time.Millisecond):
		return "ok", nil
	}
}

func TestConcurrencyBounds(t *testing.T) {
	t.Run("map concurrency", func(t *testing.T) {
		h := newHarness(t)
		var g gauge
		h.tools.Register("probe", g.fn)
		h.tools.Register("items", func(context.Context, tool.Call) (any, error) {
			out := make([]any, 24)
			for i := range out {
				out[i] = i
			}
			return out, nil
		})
		v := h.runToEnd(t, `
name: fan
steps:
  - {id: list, kind: tool, tool: items}
  - id: each
    kind: map
    depends_on: [list]
    over: steps.list.output
    concurrency: 3
    step: {kind: tool, tool: probe}
`, nil)
		if v.Status != store.RunSucceeded {
			t.Fatalf("run %s: %s", v.Status, v.Error)
		}
		if len(outputOf[[]string](t, v, "each")) != 24 {
			t.Error("map output length != 24")
		}
		if m := g.max.Load(); m > 3 || m < 2 {
			t.Errorf("max concurrent items = %d, want <= 3 (and actually parallel)", m)
		}
	})
	t.Run("worker concurrency", func(t *testing.T) {
		h := newHarness(t)
		var g gauge
		h.tools.Register("probe", g.fn)
		var b strings.Builder
		b.WriteString("name: wide\nsteps:\n")
		for i := range 12 {
			fmt.Fprintf(&b, "  - {id: s%d, kind: tool, tool: probe}\n", i)
		}
		h.startWorker(t, engine.WithConcurrency(2))
		v := h.wait(t, h.start(t, b.String(), nil).ID)
		if v.Status != store.RunSucceeded {
			t.Fatalf("run %s: %s", v.Status, v.Error)
		}
		if m := g.max.Load(); m != 2 {
			t.Errorf("max concurrent steps = %d, want 2", m)
		}
	})
}

func TestHTTPTool(t *testing.T) {
	var hits atomic.Int32
	var keys sync.Map
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keys.Store(r.Header.Get("Idempotency-Key"), true)
		if hits.Add(1) == 1 {
			http.Error(w, "warming up", http.StatusServiceUnavailable)
			return
		}
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"echo": %s, "auth": %q}`, body, r.Header.Get("Authorization"))
	}))
	defer srv.Close()

	h := newHarness(t)
	v := h.runToEnd(t, fmt.Sprintf(`
name: http
inputs: [msg]
steps:
  - id: call
    kind: tool
    retry: {max_attempts: 2}
    http:
      url: %s/hook
      headers: {Authorization: "Bearer {{ .input.msg }}"}
      body: '{"msg": "{{ .input.msg }}"}'
`, srv.URL), map[string]any{"msg": "hello"})
	if v.Status != store.RunSucceeded {
		t.Fatalf("run %s: %s", v.Status, v.Error)
	}
	out := outputOf[map[string]any](t, v, "call")
	body := out["body"].(map[string]any)
	if body["auth"] != "Bearer hello" || body["echo"].(map[string]any)["msg"] != "hello" {
		t.Errorf("output = %v", out)
	}
	n := 0
	keys.Range(func(k, _ any) bool { n++; return k.(string) == v.ID+"/call" })
	if n != 1 || hits.Load() != 2 {
		t.Errorf("idempotency keys = %d distinct over %d hits; want 1 key over 2 hits", n, hits.Load())
	}
}

func TestBackoff(t *testing.T) {
	base, maxD := 100*time.Millisecond, time.Second
	tests := []struct {
		attempt int
		rnd     float64
		want    time.Duration
	}{
		{1, 0, 50 * time.Millisecond},
		{1, 0.999999, 100 * time.Millisecond},
		{2, 0, 100 * time.Millisecond},
		{3, 0, 200 * time.Millisecond},
		{4, 0.5, 600 * time.Millisecond},
		{10, 0, 500 * time.Millisecond}, // capped at max
		{10, 0.999999, time.Second},
		{0, 0, 50 * time.Millisecond}, // clamped to attempt 1
		{5000, 0, 500 * time.Millisecond},
	}
	for _, tc := range tests {
		got := engine.Backoff(tc.attempt, base, maxD, func() float64 { return tc.rnd })
		if math.Abs(float64(got-tc.want)) > float64(time.Millisecond) {
			t.Errorf("Backoff(%d, rnd=%v) = %v, want %v", tc.attempt, tc.rnd, got, tc.want)
		}
	}
}

func TestExtractJSON(t *testing.T) {
	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{`{"a":1}`, `{"a":1}`, false},
		{"```json\n{\"a\": [1, 2]}\n```", `{"a":[1,2]}`, false},
		{`Sure! Here you go: ["x", "y"] hope that helps`, `["x","y"]`, false},
		{`no json here`, ``, true},
		{`{"a": `, ``, true},
	}
	for _, tc := range tests {
		v, err := engine.ExtractJSON(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("ExtractJSON(%q) err = %v", tc.in, err)
			continue
		}
		if !tc.wantErr {
			b, _ := json.Marshal(v)
			if string(b) != tc.want {
				t.Errorf("ExtractJSON(%q) = %s, want %s", tc.in, b, tc.want)
			}
		}
	}
}

func TestEventsTellTheStory(t *testing.T) {
	h := newHarness(t)
	c := newCounter()
	h.tools.Register("count", c.fn)
	v := h.runToEnd(t, chain, nil)
	evs, err := h.eng.ListEvents(context.Background(), v.ID, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var types []string
	for _, e := range evs {
		types = append(types, e.Type+":"+e.StepID)
	}
	want := []string{
		"run.started:", "step.queued:a", "step.started:a", "step.succeeded:a",
		"step.queued:b", "step.started:b", "step.succeeded:b",
		"step.queued:c", "step.started:c", "step.succeeded:c", "run.succeeded:",
	}
	if strings.Join(types, " ") != strings.Join(want, " ") {
		t.Errorf("events:\n got %v\nwant %v", types, want)
	}
}
