package api_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/RatulSikder97/agentflow/api"
	"github.com/RatulSikder97/agentflow/engine"
	"github.com/RatulSikder97/agentflow/llm/fake"
	"github.com/RatulSikder97/agentflow/queue/memqueue"
	"github.com/RatulSikder97/agentflow/store"
	"github.com/RatulSikder97/agentflow/store/memstore"
	"github.com/RatulSikder97/agentflow/telemetry"
)

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	q := memqueue.New()
	llm, err := fake.Load("../examples/fake-llm.yaml")
	if err != nil {
		t.Fatal(err)
	}
	m := telemetry.NewMetrics()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := engine.New(engine.Config{Store: memstore.New(), Queue: q, LLM: llm, Metrics: m, Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = engine.NewWorker(eng, q).Run(ctx) }()
	srv := httptest.NewServer(api.NewHandler(eng, api.Options{Logger: log, Metrics: m.Handler(), PollInterval: 5 * time.Millisecond}))
	t.Cleanup(func() { srv.Close(); cancel(); <-done; _ = q.Close() })
	return srv
}

func call(t *testing.T, method, url, ctype, body string, wantStatus int, out any) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s = %d %s, want %d", method, url, resp.StatusCode, b, wantStatus)
	}
	if out != nil {
		if err := json.Unmarshal(b, out); err != nil {
			t.Fatalf("decode %s: %v", b, err)
		}
	}
}

func TestErrorsMapToStatusCodes(t *testing.T) {
	srv := newServer(t)
	tests := []struct {
		name, method, path, body string
		want                     int
	}{
		{"yaml syntax", "POST", "/v1/workflows", "name: [", 400},
		{"invalid workflow", "POST", "/v1/workflows", "name: x\nsteps: []", 422},
		{"unknown workflow", "GET", "/v1/workflows/wf_nope", "", 404},
		{"start unknown workflow", "POST", "/v1/workflows/wf_nope/runs", "", 404},
		{"unknown run", "GET", "/v1/runs/run_nope", "", 404},
		{"unknown run events", "GET", "/v1/runs/run_nope/events", "", 404},
		{"healthz", "GET", "/healthz", "", 200},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			call(t, tc.method, srv.URL+tc.path, "", tc.body, tc.want, nil)
		})
	}
}

func TestSubmitStartWatch(t *testing.T) {
	srv := newServer(t)
	spec, err := os.ReadFile("../examples/support-triage/workflow.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var wf struct{ ID, Name string }
	call(t, "POST", srv.URL+"/v1/workflows", "application/yaml", string(spec), 201, &wf)
	if wf.Name != "support-triage" || !strings.HasPrefix(wf.ID, "wf_") {
		t.Fatalf("workflow = %+v", wf)
	}

	call(t, "POST", srv.URL+"/v1/workflows/"+wf.ID+"/runs", "application/json", `{"input":{}}`, 422, nil)
	call(t, "POST", srv.URL+"/v1/workflows/"+wf.ID+"/runs", "application/json", `{bad`, 400, nil)

	var run store.Run
	call(t, "POST", srv.URL+"/v1/workflows/"+wf.ID+"/runs", "application/json",
		`{"input":{"ticket_id":"T-9","subject":"Crash","body":"I get an error on save"}}`, 201, &run)

	// Stream events until the terminal event; the server closes the stream.
	resp, err := http.Get(srv.URL + "/v1/runs/" + run.ID + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q", ct)
	}
	var types []string
	var lastID string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if v, ok := strings.CutPrefix(line, "event: "); ok {
			types = append(types, v)
		}
		if v, ok := strings.CutPrefix(line, "id: "); ok {
			lastID = v
		}
	}
	if len(types) == 0 || types[0] != store.EventRunStarted || types[len(types)-1] != store.EventRunSucceeded {
		t.Fatalf("event types = %v", types)
	}

	var view engine.RunView
	call(t, "GET", srv.URL+"/v1/runs/"+run.ID, "", "", 200, &view)
	if view.Status != store.RunSucceeded || len(view.Steps) != 6 {
		t.Fatalf("run = %+v", view)
	}

	// Resuming after the terminal event replays nothing and closes the stream.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/v1/runs/"+run.ID+"/events", nil)
	req.Header.Set("Last-Event-ID", lastID)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp2.Body.Close() }()
	b, err := io.ReadAll(resp2.Body)
	if err != nil {
		t.Fatalf("resumed stream did not close: %v", err)
	}
	if strings.Contains(string(b), "event:") {
		t.Errorf("resumed stream replayed events: %s", b)
	}

	call(t, "GET", srv.URL+"/v1/workflows/"+wf.ID, "", "", 200, nil)
	mresp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mresp.Body.Close() }()
	mb, _ := io.ReadAll(mresp.Body)
	for _, want := range []string{`agentflow_runs_total{status="succeeded"} 1`, `agentflow_steps_total{kind="llm",status="succeeded"} 2`} {
		if !strings.Contains(string(mb), want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}
