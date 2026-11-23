package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// The README quickstart commands, executed verbatim.
func TestExamplesRun(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{"validate", []string{"validate", "-f", "../../examples/support-triage/workflow.yaml"},
			[]string{"ok: support-triage (6 steps)", "record"}},
		{"research", []string{"run", "-f", "../../examples/research-summarize/workflow.yaml",
			"-fixtures", "../../examples/fake-llm.yaml", "-input", `{"topic":"small language models"}`},
			[]string{"run.succeeded", "deployment economics", `"words":`}},
		{"triage", []string{"run", "-f", "../../examples/support-triage/workflow.yaml",
			"-fixtures", "../../examples/fake-llm.yaml",
			"-input", `{"ticket_id":"T-1042","subject":"Double charge","body":"I was charged twice, please refund one payment."}`},
			[]string{"step.skipped    technical_reply", `"route": "billing_reply"`, "run.succeeded"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := run(context.Background(), tc.args, &out); err != nil {
				t.Fatalf("run: %v\n%s", err, out.String())
			}
			for _, w := range tc.want {
				if !strings.Contains(out.String(), w) {
					t.Errorf("output missing %q:\n%s", w, out.String())
				}
			}
		})
	}
}

func TestBadInvocations(t *testing.T) {
	for _, args := range [][]string{
		{"bogus"},
		{"validate"},
		{"run", "-f", "../../examples/research-summarize/workflow.yaml", "-input", "not json"},
		{"run", "-f", "../../examples/research-summarize/workflow.yaml"}, // missing required input
		{"start"},
	} {
		if err := run(context.Background(), args, &bytes.Buffer{}); err == nil {
			t.Errorf("run(%q) succeeded, want error", args)
		}
	}
}
