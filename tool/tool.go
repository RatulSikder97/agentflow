// Package tool provides the registry of Go functions callable from tool
// steps, plus the HTTP tool used by steps that declare `http:`.
package tool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Call is the input to a tool invocation.
type Call struct {
	Args map[string]any
	// IdempotencyKey is stable across retries and redeliveries of the same
	// step (run_id/step_id[/index]). Tools with side effects should use it to
	// deduplicate.
	IdempotencyKey string
}

// Func is a tool implementation. Its result must be JSON-serializable.
type Func func(ctx context.Context, call Call) (any, error)

// Registry maps tool names to implementations. It is safe for concurrent use.
type Registry struct {
	mu    sync.RWMutex
	funcs map[string]Func
}

// NewRegistry returns a registry preloaded with the built-in tools.
func NewRegistry() *Registry {
	r := &Registry{funcs: map[string]Func{}}
	r.Register("echo", Echo)
	r.Register("word_count", WordCount)
	r.Register("sleep", Sleep)
	return r
}

// Register adds or replaces a tool.
func (r *Registry) Register(name string, f Func) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.funcs[name] = f
}

// ErrUnknownTool is returned for unregistered tool names.
var ErrUnknownTool = errors.New("unknown tool")

// Lookup returns a registered tool.
func (r *Registry) Lookup(name string) (Func, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.funcs[name]
	if !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownTool, name)
	}
	return f, nil
}

// Names lists registered tools, sorted.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.funcs))
	for n := range r.funcs {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Echo returns its arguments unchanged. Useful for assembling a record from
// earlier step outputs.
func Echo(_ context.Context, call Call) (any, error) { return call.Args, nil }

// WordCount counts words and characters in args.text.
func WordCount(_ context.Context, call Call) (any, error) {
	text, _ := call.Args["text"].(string)
	return map[string]any{
		"words": len(strings.Fields(text)),
		"chars": utf8.RuneCountInString(text),
	}, nil
}

// Sleep waits for args.duration (e.g. "250ms"), honoring cancellation.
func Sleep(ctx context.Context, call Call) (any, error) {
	s, _ := call.Args["duration"].(string)
	d, err := time.ParseDuration(s)
	if err != nil {
		return nil, Permanent(fmt.Errorf("sleep: %w", err))
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.C:
		return map[string]any{"slept": d.String()}, nil
	}
}

// permanentError marks an error as not worth retrying.
type permanentError struct{ err error }

func (e *permanentError) Error() string   { return e.err.Error() }
func (e *permanentError) Unwrap() error   { return e.err }
func (e *permanentError) Retryable() bool { return false }

// Permanent wraps err so the engine fails the step without retrying.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// HTTPRequest is a rendered HTTP tool invocation.
type HTTPRequest struct {
	Method         string
	URL            string
	Headers        map[string]string
	Body           string
	IdempotencyKey string
}

// HTTPError is a non-2xx response; 5xx and 429 are retryable.
type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("http tool: status %d: %s", e.StatusCode, e.Body)
}

// Retryable implements the engine's retry classification.
func (e *HTTPError) Retryable() bool {
	return e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500
}

// DoHTTP performs the request. A JSON response body is decoded; otherwise
// the body is returned as a string. The idempotency key is sent as the
// Idempotency-Key header.
func DoHTTP(ctx context.Context, client *http.Client, r HTTPRequest) (any, error) {
	var body io.Reader
	if r.Body != "" {
		body = bytes.NewBufferString(r.Body)
	}
	req, err := http.NewRequestWithContext(ctx, r.Method, r.URL, body)
	if err != nil {
		return nil, Permanent(fmt.Errorf("http tool: %w", err))
	}
	for k, v := range r.Headers {
		req.Header.Set(k, v)
	}
	if r.IdempotencyKey != "" {
		req.Header.Set("Idempotency-Key", r.IdempotencyKey)
	}
	if r.Body != "" && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http tool: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, fmt.Errorf("http tool: read body: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		return nil, &HTTPError{StatusCode: resp.StatusCode, Body: strings.TrimSpace(string(raw))}
	}
	var v any
	if json.Unmarshal(raw, &v) == nil {
		return map[string]any{"status": resp.StatusCode, "body": v}, nil
	}
	return map[string]any{"status": resp.StatusCode, "body": string(raw)}, nil
}
