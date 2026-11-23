// Package llm defines the provider-neutral request/response types used by
// llm steps. Providers live in subpackages (anthropic, fake).
package llm

import "context"

// Role of a message author.
type Role string

// Message roles.
const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
)

// Message is one conversational turn.
type Message struct {
	Role    Role   `json:"role"`
	Content string `json:"content"`
}

// Request is a single completion request.
type Request struct {
	Model     string
	System    string
	Messages  []Message
	MaxTokens int
	// Metadata is opaque context (run and step IDs) that providers may use
	// for logging or deterministic behavior; it is never sent to the model.
	Metadata map[string]string
}

// Usage reports token consumption.
type Usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

// Response is a completed model response.
type Response struct {
	Text       string `json:"text"`
	Model      string `json:"model"`
	StopReason string `json:"stop_reason"`
	Usage      Usage  `json:"usage"`
}

// Provider produces completions.
type Provider interface {
	Complete(ctx context.Context, req Request) (Response, error)
}
