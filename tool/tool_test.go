package tool_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/RatulSikder97/agentflow/tool"
)

func TestRegistry(t *testing.T) {
	r := tool.NewRegistry()
	if _, err := r.Lookup("nope"); !errors.Is(err, tool.ErrUnknownTool) {
		t.Errorf("Lookup(nope) = %v", err)
	}
	r.Register("x", tool.Echo)
	if _, err := r.Lookup("x"); err != nil {
		t.Error(err)
	}
	want := []string{"echo", "sleep", "word_count", "x"}
	got := r.Names()
	if len(got) != len(want) {
		t.Fatalf("Names = %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Names = %v, want %v", got, want)
		}
	}
}

func TestBuiltins(t *testing.T) {
	ctx := context.Background()
	out, _ := tool.WordCount(ctx, tool.Call{Args: map[string]any{"text": "héllo big world"}})
	if m := out.(map[string]any); m["words"] != 3 || m["chars"] != 15 {
		t.Errorf("WordCount = %v", m)
	}
	if _, err := tool.Sleep(ctx, tool.Call{Args: map[string]any{"duration": "bad"}}); err == nil {
		t.Error("Sleep(bad) succeeded")
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if _, err := tool.Sleep(cctx, tool.Call{Args: map[string]any{"duration": "1s"}}); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Sleep cancel = %v", err)
	}
}

func TestPermanent(t *testing.T) {
	base := errors.New("boom")
	err := tool.Permanent(base)
	var r interface{ Retryable() bool }
	if !errors.As(err, &r) || r.Retryable() || !errors.Is(err, base) {
		t.Errorf("Permanent(%v) = %v", base, err)
	}
	if tool.Permanent(nil) != nil {
		t.Error("Permanent(nil) != nil")
	}
}

func TestDoHTTP(t *testing.T) {
	tests := []struct {
		name          string
		status        int
		body          string
		wantErr       bool
		wantRetryable bool
	}{
		{"json", 200, `{"ok":true}`, false, false},
		{"text", 201, `created`, false, false},
		{"rate limited", 429, `slow`, true, true},
		{"server error", 502, `bad gateway`, true, true},
		{"client error", 404, `missing`, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Idempotency-Key") != "run/step" {
					t.Errorf("Idempotency-Key = %q", r.Header.Get("Idempotency-Key"))
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			out, err := tool.DoHTTP(context.Background(), srv.Client(), tool.HTTPRequest{
				Method: http.MethodPost, URL: srv.URL, Body: `{}`, IdempotencyKey: "run/step",
			})
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v", err)
			}
			if err != nil {
				var he *tool.HTTPError
				if !errors.As(err, &he) || he.Retryable() != tc.wantRetryable {
					t.Errorf("err = %v retryable mismatch", err)
				}
				return
			}
			if out.(map[string]any)["status"] != tc.status {
				t.Errorf("out = %v", out)
			}
		})
	}
}
