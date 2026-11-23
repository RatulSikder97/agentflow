package anthropic_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/RatulSikder97/agentflow/llm"
	"github.com/RatulSikder97/agentflow/llm/anthropic"
)

func TestNewRequiresKey(t *testing.T) {
	t.Setenv(anthropic.EnvAPIKey, "")
	if _, err := anthropic.New(); !errors.Is(err, anthropic.ErrNoAPIKey) {
		t.Fatalf("err = %v, want ErrNoAPIKey", err)
	}
}

func TestComplete(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" || r.Method != http.MethodPost {
			t.Errorf("request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("x-api-key") != "test-key" || r.Header.Get("anthropic-version") != anthropic.APIVersion {
			t.Errorf("headers = %v", r.Header)
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		_, _ = w.Write([]byte(`{
			"model": "claude-sonnet-5-5", "stop_reason": "end_turn",
			"content": [{"type": "thinking", "thinking": ""}, {"type": "text", "text": "Hello "}, {"type": "text", "text": "world"}],
			"usage": {"input_tokens": 12, "output_tokens": 3}}`))
	}))
	defer srv.Close()

	c, err := anthropic.New(anthropic.WithAPIKey("test-key"), anthropic.WithBaseURL(srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Complete(context.Background(), llm.Request{
		System:   "be brief",
		Messages: []llm.Message{{Role: llm.RoleUser, Content: "hi"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Text != "Hello world" || resp.Usage.InputTokens != 12 || resp.Usage.OutputTokens != 3 {
		t.Errorf("resp = %+v", resp)
	}
	if got["model"] != anthropic.DefaultModel || got["system"] != "be brief" || got["max_tokens"].(float64) != anthropic.DefaultMaxTokens {
		t.Errorf("request body = %v", got)
	}
	if _, ok := got["temperature"]; ok {
		t.Error("temperature must not be sent")
	}
}

func TestErrors(t *testing.T) {
	tests := []struct {
		name          string
		status        int
		body          string
		wantRetryable bool
		wantType      string
	}{
		{"rate limited", 429, `{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}`, true, "rate_limit_error"},
		{"overloaded", 529, `{"type":"error","error":{"type":"overloaded_error","message":"busy"}}`, true, "overloaded_error"},
		{"server error", 500, `oops`, true, ""},
		{"bad request", 400, `{"type":"error","error":{"type":"invalid_request_error","message":"bad"}}`, false, "invalid_request_error"},
		{"auth", 401, `{"type":"error","error":{"type":"authentication_error","message":"no"}}`, false, "authentication_error"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("retry-after", "7")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			c, _ := anthropic.New(anthropic.WithAPIKey("k"), anthropic.WithBaseURL(srv.URL))
			_, err := c.Complete(context.Background(), llm.Request{Messages: []llm.Message{{Role: llm.RoleUser, Content: "x"}}})
			var apiErr *anthropic.APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("err = %T %v, want *APIError", err, err)
			}
			if apiErr.Retryable() != tc.wantRetryable || apiErr.Type != tc.wantType || apiErr.StatusCode != tc.status {
				t.Errorf("apiErr = %+v retryable=%v", apiErr, apiErr.Retryable())
			}
			if apiErr.RetryAfter.Seconds() != 7 {
				t.Errorf("RetryAfter = %v", apiErr.RetryAfter)
			}
		})
	}
}

func TestRefusal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"model":"claude-sonnet-5-5","stop_reason":"refusal","content":[],"usage":{}}`))
	}))
	defer srv.Close()
	c, _ := anthropic.New(anthropic.WithAPIKey("k"), anthropic.WithBaseURL(srv.URL))
	_, err := c.Complete(context.Background(), llm.Request{Messages: []llm.Message{{Role: llm.RoleUser, Content: "x"}}})
	var ref *anthropic.RefusalError
	if !errors.As(err, &ref) || ref.Retryable() {
		t.Fatalf("err = %v, want non-retryable RefusalError", err)
	}
}
