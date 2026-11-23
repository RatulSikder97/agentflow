// Package fake is a deterministic llm.Provider for tests, examples and
// benchmarks. Replies are chosen by substring rules; anything unmatched gets
// a stable echo of the prompt.
package fake

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/RatulSikder97/agentflow/llm"
)

// Rule replies with Reply when the prompt (system + messages) contains every
// string in Contains.
type Rule struct {
	Contains []string `yaml:"contains"`
	Reply    string   `yaml:"reply"`
}

// Provider is a deterministic fake LLM. It is safe for concurrent use.
type Provider struct {
	Rules   []Rule
	Latency time.Duration

	mu    sync.Mutex
	calls int
}

// New returns a Provider with the given rules.
func New(rules ...Rule) *Provider { return &Provider{Rules: rules} }

type fixtureFile struct {
	Rules []Rule `yaml:"rules"`
}

// Load reads rules from a YAML file of the form:
//
//	rules:
//	  - contains: ["Classify", "refund"]
//	    reply: '{"category": "billing"}'
func Load(path string) (*Provider, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("fake llm: %w", err)
	}
	var f fixtureFile
	if err := yaml.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("fake llm: parse %s: %w", path, err)
	}
	return New(f.Rules...), nil
}

// Calls returns how many completions were served.
func (p *Provider) Calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// Complete returns the first matching rule's reply or an echo.
func (p *Provider) Complete(ctx context.Context, req llm.Request) (llm.Response, error) {
	if p.Latency > 0 {
		t := time.NewTimer(p.Latency)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return llm.Response{}, ctx.Err()
		case <-t.C:
		}
	}
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()

	var b strings.Builder
	b.WriteString(req.System)
	for _, m := range req.Messages {
		b.WriteString("\n")
		b.WriteString(m.Content)
	}
	prompt := b.String()

	text := ""
	for _, r := range p.Rules {
		if matches(prompt, r.Contains) {
			text = r.Reply
			break
		}
	}
	if text == "" {
		text = echo(req)
	}
	return llm.Response{
		Text:       text,
		Model:      req.Model,
		StopReason: "end_turn",
		Usage:      llm.Usage{InputTokens: len(strings.Fields(prompt)), OutputTokens: len(strings.Fields(text))},
	}, nil
}

func matches(prompt string, subs []string) bool {
	for _, s := range subs {
		if !strings.Contains(prompt, s) {
			return false
		}
	}
	return true
}

// echo summarizes the last user message: its first line, truncated.
func echo(req llm.Request) string {
	last := ""
	if n := len(req.Messages); n > 0 {
		last = req.Messages[n-1].Content
	}
	line := strings.TrimSpace(last)
	if i := strings.IndexByte(line, '\n'); i >= 0 {
		line = strings.TrimSpace(line[:i])
	}
	const maxLen = 160
	if len(line) > maxLen {
		line = line[:maxLen] + "..."
	}
	return fmt.Sprintf("[%s] %s", req.Model, line)
}
