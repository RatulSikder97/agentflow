//go:build integration

// Package integration exercises agentflow against real PostgreSQL and Redis.
//
//	AGENTFLOW_TEST_DATABASE_URL=postgres://... AGENTFLOW_TEST_REDIS_URL=redis://... \
//	  go test -race -tags integration ./integration/
package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/RatulSikder97/agentflow/engine"
	"github.com/RatulSikder97/agentflow/llm/fake"
	"github.com/RatulSikder97/agentflow/queue/redisqueue"
	"github.com/RatulSikder97/agentflow/store"
	"github.com/RatulSikder97/agentflow/store/pgstore"
	"github.com/RatulSikder97/agentflow/tool"
)

const spec = `
name: crash-resume
steps:
  - {id: fetch, kind: tool, tool: record}
  - id: enrich
    kind: map
    depends_on: [fetch]
    over: steps.fetch.output.items
    concurrency: 2
    step: {kind: llm, prompt: "enrich {{ .item }}"}
  - {id: slow, kind: tool, tool: hang_once, depends_on: [enrich]}
  - {id: publish, kind: tool, tool: record, depends_on: [slow]}
`

// process is one simulated agentflow process: its own engine, pool, Redis
// client and consumer name, sharing only Postgres and Redis with others.
type process struct {
	eng   *engine.Engine
	queue *redisqueue.Queue
}

func newProcess(t testing.TB, name, stream string, tools *tool.Registry) *process {
	t.Helper()
	ctx := context.Background()
	pg, err := pgstore.Open(ctx, os.Getenv("AGENTFLOW_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Close)
	if err := pg.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	opt, err := redis.ParseURL(os.Getenv("AGENTFLOW_TEST_REDIS_URL"))
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })
	q, err := redisqueue.New(ctx, rdb, redisqueue.Options{
		Stream: stream, Consumer: name, MinIdle: 300 * time.Millisecond, Block: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	eng, err := engine.New(engine.Config{
		Store: pg, Queue: q, LLM: fake.New(), Tools: tools,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return &process{eng: eng, queue: q}
}

func (p *process) work(t testing.TB) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = engine.NewWorker(p.eng, p.queue, engine.WithConcurrency(4), engine.WithHeartbeat(100*time.Millisecond)).Run(ctx)
	}()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done }) }
	t.Cleanup(stop)
	return stop
}

func TestCrashResumeAcrossProcesses(t *testing.T) {
	if os.Getenv("AGENTFLOW_TEST_DATABASE_URL") == "" || os.Getenv("AGENTFLOW_TEST_REDIS_URL") == "" {
		t.Skip("AGENTFLOW_TEST_DATABASE_URL / AGENTFLOW_TEST_REDIS_URL not set")
	}
	stream := fmt.Sprintf("agentflow:test:%d", time.Now().UnixNano())

	var mu sync.Mutex
	committed := map[string]int{} // idempotency key -> successful executions
	tools := tool.NewRegistry()
	tools.Register("record", func(_ context.Context, c tool.Call) (any, error) {
		mu.Lock()
		defer mu.Unlock()
		committed[c.IdempotencyKey]++
		return map[string]any{"items": []any{"alpha", "beta", "gamma"}}, nil
	})
	var slowCalls atomic.Int32
	slowStarted := make(chan struct{})
	tools.Register("hang_once", func(ctx context.Context, c tool.Call) (any, error) {
		if slowCalls.Add(1) == 1 {
			close(slowStarted)
			<-ctx.Done() // process 1 dies while this step is in flight
			return nil, ctx.Err()
		}
		mu.Lock()
		defer mu.Unlock()
		committed[c.IdempotencyKey]++
		return "done", nil
	})

	p1 := newProcess(t, "process-1", stream, tools)
	ctx := context.Background()
	wf, err := p1.eng.SubmitWorkflow(ctx, []byte(spec))
	if err != nil {
		t.Fatal(err)
	}
	run, err := p1.eng.StartRun(ctx, wf.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	crash := p1.work(t)
	select {
	case <-slowStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("slow step never started")
	}
	crash()

	// Checkpoint: everything before "slow" is committed; "slow" is stuck running.
	mid, err := p1.eng.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	status := map[string]store.StepStatus{}
	for _, s := range mid.Steps {
		status[s.StepID] = s.Status
	}
	if status["fetch"] != store.StepSucceeded || status["enrich"] != store.StepSucceeded ||
		status["slow"] != store.StepRunning || status["publish"] != store.StepPending {
		t.Fatalf("checkpoint after crash = %v", status)
	}

	// A fresh process (new engine, new pool, new consumer) takes over. Its
	// XAUTOCLAIM picks up the job process-1 left in the PEL.
	p2 := newProcess(t, "process-2", stream, tools)
	p2.work(t)
	wctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	final, err := p2.eng.WaitRun(wctx, run.ID, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitRun: %v", err)
	}
	if final.Status != store.RunSucceeded {
		t.Fatalf("run %s: %s", final.Status, final.Error)
	}

	mu.Lock()
	defer mu.Unlock()
	for key, n := range committed {
		if n != 1 {
			t.Errorf("%s executed %d times, want exactly once", key, n)
		}
	}
	for _, step := range []string{"fetch", "slow", "publish"} {
		if committed[run.ID+"/"+step] != 1 {
			t.Errorf("step %s committed %d times", step, committed[run.ID+"/"+step])
		}
	}
	view, _ := p2.eng.GetRun(ctx, run.ID)
	for _, s := range view.Steps {
		if s.StepID == "slow" && s.Attempt != 2 {
			t.Errorf("slow attempts = %d, want 2", s.Attempt)
		}
		if s.StepID == "enrich" && !strings.Contains(string(s.Output), "enrich beta") {
			t.Errorf("enrich output = %s", s.Output)
		}
	}
	if n, _ := p2.queue.Pending(ctx); n != 0 {
		t.Errorf("pending jobs after completion = %d", n)
	}
}
