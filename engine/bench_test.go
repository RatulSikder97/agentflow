package engine_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/RatulSikder97/agentflow/engine"
	"github.com/RatulSikder97/agentflow/store"
)

// benchRun measures end-to-end run latency through store, queue, scheduler
// and worker with the fake LLM (no network), i.e. pure engine overhead.
func benchRun(b *testing.B, spec string, concurrency int) {
	h := newHarness(b)
	h.startWorker(b, engine.WithConcurrency(concurrency))
	ctx := context.Background()
	wf, err := h.eng.SubmitWorkflow(ctx, []byte(spec))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		run, err := h.eng.StartRun(ctx, wf.ID, map[string]any{"topic": "bench"})
		if err != nil {
			b.Fatal(err)
		}
		v, err := h.eng.WaitRun(ctx, run.ID, 200*time.Microsecond)
		if err != nil || v.Status != store.RunSucceeded {
			b.Fatalf("run %s: %v %s", v.Status, err, v.Error)
		}
	}
}

func chainSpec(n int) string {
	var s strings.Builder
	s.WriteString("name: chain\nsteps:\n  - {id: s0, kind: llm, prompt: \"step 0 {{ .input.topic }}\"}\n")
	for i := 1; i < n; i++ {
		fmt.Fprintf(&s, "  - {id: s%d, kind: llm, depends_on: [s%d], prompt: \"step %d {{ .steps.s%d.output }}\"}\n", i, i-1, i, i-1)
	}
	return s.String()
}

func wideSpec(n int) string {
	var s strings.Builder
	s.WriteString("name: wide\nsteps:\n")
	ids := make([]string, n)
	for i := range n {
		ids[i] = fmt.Sprintf("s%d", i)
		fmt.Fprintf(&s, "  - {id: s%d, kind: llm, prompt: \"part %d of {{ .input.topic }}\"}\n", i, i)
	}
	fmt.Fprintf(&s, "  - {id: join, kind: tool, tool: echo, depends_on: [%s]}\n", strings.Join(ids, ", "))
	return s.String()
}

const mapSpec = `
name: fanout
steps:
  - {id: items, kind: tool, tool: echo, args: {list: [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w, x, y, z, aa, bb, cc, dd, ee, ff, gg, hh, ii, jj, kk, ll, mm, nn, oo, pp, qq, rr, ss, tt, uu, vv, ww, xx, yy, zz, a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1, n1, o1, p1, q1, r1, s1, t1, u1, v1, w1, x1, y1, z1, a2, b2, c2, d2, e2, f2, g2, h2, i2, j2, k2, l2, m2, n2, o2, p2, q2, r2, s2, t2, u2, v2]}}
  - id: each
    kind: map
    depends_on: [items]
    over: steps.items.output.list
    concurrency: 16
    step: {kind: llm, prompt: "summarize {{ .item }}"}
`

func BenchmarkRunChain10(b *testing.B)    { benchRun(b, chainSpec(10), 8) }
func BenchmarkRunParallel50(b *testing.B) { benchRun(b, wideSpec(50), 16) }
func BenchmarkRunMap100(b *testing.B)     { benchRun(b, mapSpec, 8) }
