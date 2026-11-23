package fake_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/RatulSikder97/agentflow/llm"
	"github.com/RatulSikder97/agentflow/llm/fake"
)

func TestRules(t *testing.T) {
	p := fake.New(
		fake.Rule{Contains: []string{"classify", "refund"}, Reply: "billing"},
		fake.Rule{Contains: []string{"classify"}, Reply: "general"},
	)
	tests := []struct {
		system, prompt, want string
	}{
		{"", "classify: I want a refund", "billing"},
		{"classify", "please refund", "billing"}, // system prompt is matched too
		{"", "classify: hello", "general"},
		{"", "summarize this\nsecond line", "[m] summarize this"},
	}
	for _, tc := range tests {
		resp, err := p.Complete(context.Background(), llm.Request{
			Model: "m", System: tc.system, Messages: []llm.Message{{Role: llm.RoleUser, Content: tc.prompt}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if resp.Text != tc.want {
			t.Errorf("Complete(%q, %q) = %q, want %q", tc.system, tc.prompt, resp.Text, tc.want)
		}
	}
	if p.Calls() != len(tests) {
		t.Errorf("Calls = %d", p.Calls())
	}
}

func TestLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rules.yaml")
	if err := os.WriteFile(path, []byte("rules:\n  - contains: [ping]\n    reply: pong\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := fake.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	resp, _ := p.Complete(context.Background(), llm.Request{Messages: []llm.Message{{Role: llm.RoleUser, Content: "ping"}}})
	if resp.Text != "pong" {
		t.Errorf("reply = %q", resp.Text)
	}
	if _, err := fake.Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Error("Load(missing) succeeded")
	}
}
