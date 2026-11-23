// Package anthropic implements llm.Provider against the Anthropic Messages
// API using net/http.
package anthropic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/RatulSikder97/agentflow/llm"
)

// Defaults for the Messages API.
const (
	DefaultBaseURL     = "https://api.anthropic.com"
	DefaultModel       = "claude-sonnet-5-5"
	APIVersion         = "2026-10-21"
	DefaultMaxTokens   = 4096
	EnvAPIKey          = "ANTHROPIC_API_KEY"
	EnvBaseURL         = "ANTHROPIC_BASE_URL"
	defaultHTTPTimeout = 10 * time.Minute
)

// Client calls the Messages API.
type Client struct {
	apiKey  string
	baseURL string
	model   string
	http    *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithAPIKey overrides the API key read from ANTHROPIC_API_KEY.
func WithAPIKey(k string) Option { return func(c *Client) { c.apiKey = k } }

// WithBaseURL overrides the API base URL.
func WithBaseURL(u string) Option { return func(c *Client) { c.baseURL = strings.TrimRight(u, "/") } }

// WithDefaultModel sets the model used when a request does not name one.
func WithDefaultModel(m string) Option { return func(c *Client) { c.model = m } }

// WithHTTPClient sets the underlying HTTP client.
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// ErrNoAPIKey is returned by New when no API key is configured.
var ErrNoAPIKey = errors.New("anthropic: " + EnvAPIKey + " is not set")

// New returns a Client configured from the environment and options.
func New(opts ...Option) (*Client, error) {
	c := &Client{
		apiKey:  os.Getenv(EnvAPIKey),
		baseURL: DefaultBaseURL,
		model:   DefaultModel,
		http:    &http.Client{Timeout: defaultHTTPTimeout},
	}
	if u := os.Getenv(EnvBaseURL); u != "" {
		c.baseURL = strings.TrimRight(u, "/")
	}
	for _, o := range opts {
		o(c)
	}
	if c.apiKey == "" {
		return nil, ErrNoAPIKey
	}
	return c, nil
}

type wireRequest struct {
	Model     string        `json:"model"`
	MaxTokens int           `json:"max_tokens"`
	System    string        `json:"system,omitempty"`
	Messages  []llm.Message `json:"messages"`
}

type wireResponse struct {
	Model      string `json:"model"`
	StopReason string `json:"stop_reason"`
	Content    []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	Usage struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}

type wireError struct {
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

// APIError is a non-2xx response from the API.
type APIError struct {
	StatusCode int
	Type       string
	Message    string
	RetryAfter time.Duration
}

func (e *APIError) Error() string {
	return fmt.Sprintf("anthropic: %d %s: %s", e.StatusCode, e.Type, e.Message)
}

// Retryable reports whether retrying the same request may succeed:
// rate limits, overload and server errors are; client errors are not.
func (e *APIError) Retryable() bool {
	return e.StatusCode == http.StatusRequestTimeout ||
		e.StatusCode == http.StatusConflict ||
		e.StatusCode == http.StatusTooManyRequests ||
		e.StatusCode >= 500
}

// RefusalError is returned when the model declines the request
// (stop_reason "refusal"). It is not retryable.
type RefusalError struct{ Model string }

func (e *RefusalError) Error() string { return "anthropic: model " + e.Model + " refused the request" }

// Retryable implements the engine's retry classification.
func (e *RefusalError) Retryable() bool { return false }

// Complete sends a non-streaming Messages API request.
func (c *Client) Complete(ctx context.Context, req llm.Request) (llm.Response, error) {
	body := wireRequest{
		Model:     req.Model,
		MaxTokens: req.MaxTokens,
		System:    req.System,
		Messages:  req.Messages,
	}
	if body.Model == "" {
		body.Model = c.model
	}
	if body.MaxTokens == 0 {
		body.MaxTokens = DefaultMaxTokens
	}
	buf, err := json.Marshal(body)
	if err != nil {
		return llm.Response{}, fmt.Errorf("anthropic: encode request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/messages", bytes.NewReader(buf))
	if err != nil {
		return llm.Response{}, fmt.Errorf("anthropic: build request: %w", err)
	}
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("x-api-key", c.apiKey)
	httpReq.Header.Set("anthropic-version", APIVersion)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return llm.Response{}, fmt.Errorf("anthropic: send: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return llm.Response{}, fmt.Errorf("anthropic: read response: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		apiErr := &APIError{StatusCode: resp.StatusCode, Message: strings.TrimSpace(string(raw))}
		var we wireError
		if json.Unmarshal(raw, &we) == nil && we.Error.Type != "" {
			apiErr.Type, apiErr.Message = we.Error.Type, we.Error.Message
		}
		if s, err := strconv.Atoi(resp.Header.Get("retry-after")); err == nil {
			apiErr.RetryAfter = time.Duration(s) * time.Second
		}
		return llm.Response{}, apiErr
	}
	var wr wireResponse
	if err := json.Unmarshal(raw, &wr); err != nil {
		return llm.Response{}, fmt.Errorf("anthropic: decode response: %w", err)
	}
	if wr.StopReason == "refusal" {
		return llm.Response{}, &RefusalError{Model: wr.Model}
	}
	var text strings.Builder
	for _, b := range wr.Content {
		if b.Type == "text" { // thinking blocks are ignored
			text.WriteString(b.Text)
		}
	}
	return llm.Response{
		Text:       text.String(),
		Model:      wr.Model,
		StopReason: wr.StopReason,
		Usage:      llm.Usage{InputTokens: wr.Usage.InputTokens, OutputTokens: wr.Usage.OutputTokens},
	}, nil
}
