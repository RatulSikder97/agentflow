//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/RatulSikder97/agentflow/store"
	"github.com/RatulSikder97/agentflow/tool"
)

// BenchmarkPostgresRedisChain10 measures a 10-step sequential run through the
// production backends: every step is a Redis Streams round trip plus a
// handful of Postgres compare-and-set writes.
func BenchmarkPostgresRedisChain10(b *testing.B) {
	if os.Getenv("AGENTFLOW_TEST_DATABASE_URL") == "" || os.Getenv("AGENTFLOW_TEST_REDIS_URL") == "" {
		b.Skip("AGENTFLOW_TEST_DATABASE_URL / AGENTFLOW_TEST_REDIS_URL not set")
	}
	var spec strings.Builder
	spec.WriteString("name: chain\nsteps:\n  - {id: s0, kind: llm, prompt: \"step 0\"}\n")
	for i := 1; i < 10; i++ {
		fmt.Fprintf(&spec, "  - {id: s%d, kind: llm, depends_on: [s%d], prompt: \"step %d {{ .steps.s%d.output }}\"}\n", i, i-1, i, i-1)
	}
	p := newProcess(b, "bench", fmt.Sprintf("agentflow:bench:%d", time.Now().UnixNano()), tool.NewRegistry())
	p.work(b)
	ctx := context.Background()
	wf, err := p.eng.SubmitWorkflow(ctx, []byte(spec.String()))
	if err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		run, err := p.eng.StartRun(ctx, wf.ID, nil)
		if err != nil {
			b.Fatal(err)
		}
		v, err := p.eng.WaitRun(ctx, run.ID, time.Millisecond)
		if err != nil || v.Status != store.RunSucceeded {
			b.Fatalf("run %s: %v", v.Status, err)
		}
	}
}
