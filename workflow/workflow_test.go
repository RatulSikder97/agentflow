package workflow_test

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/RatulSikder97/agentflow/workflow"
)

func TestParseValid(t *testing.T) {
	wf, err := workflow.Parse([]byte(`
name: demo
defaults:
  timeout: 5s
  retry: {max_attempts: 4}
steps:
  - id: a
    kind: llm
    prompt: hi
  - id: b
    kind: tool
    tool: echo
    depends_on: [a]
    retry: {backoff: 2s}
  - id: c
    kind: map
    depends_on: [a]
    over: steps.a.output
    step: {kind: llm, prompt: "{{ .item }}"}
  - id: d
    kind: tool
    depends_on: [b, c]
    http: {url: "http://example.invalid"}
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got, want := wf.Order(), []string{"a", "b", "c", "d"}; !slices.Equal(got, want) {
		t.Errorf("Order = %v, want %v", got, want)
	}
	a, _ := wf.Step("a")
	if a.Model != workflow.DefaultModel || a.MaxTokens != workflow.DefaultMaxTokens || a.Output != workflow.OutputText {
		t.Errorf("llm defaults not applied: %+v", a)
	}
	if a.Timeout.Std() != 5*time.Second || a.Retry.MaxAttempts != 4 {
		t.Errorf("workflow defaults not inherited: timeout=%v retry=%+v", a.Timeout.Std(), a.Retry)
	}
	b, _ := wf.Step("b")
	if b.Retry.MaxAttempts != 4 || b.Retry.Backoff.Std() != 2*time.Second {
		t.Errorf("partial retry override = %+v", b.Retry)
	}
	c, _ := wf.Step("c")
	if c.Concurrency != workflow.DefaultMapConcurrency || c.Step.ID != "c_item" || c.Step.Model == "" {
		t.Errorf("map defaults = %+v / %+v", c, c.Step)
	}
	d, _ := wf.Step("d")
	if d.HTTP.Method != "POST" {
		t.Errorf("http method default = %q", d.HTTP.Method)
	}
	if got := wf.Roots(); !slices.Equal(got, []string{"a"}) {
		t.Errorf("Roots = %v", got)
	}
}

func TestParseInvalid(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{"syntax", "name: [", "workflow syntax error"},
		{"unknown field", "name: x\nsteps:\n  - {id: a, kind: llm, prompt: p, promt: typo}", "field promt not found"},
		{"no name", "steps:\n  - {id: a, kind: llm, prompt: p}", "name is required"},
		{"no steps", "name: x", "at least one step"},
		{"bad id", "name: x\nsteps:\n  - {id: Bad-Id, kind: llm, prompt: p}", "must match"},
		{"duplicate", "name: x\nsteps:\n  - {id: a, kind: llm, prompt: p}\n  - {id: a, kind: llm, prompt: p}", "duplicate id"},
		{"unknown dep", "name: x\nsteps:\n  - {id: a, kind: llm, prompt: p, depends_on: [zz]}", "unknown step \"zz\""},
		{"self dep", "name: x\nsteps:\n  - {id: a, kind: llm, prompt: p, depends_on: [a]}", "depends on itself"},
		{"cycle", "name: x\nsteps:\n  - {id: a, kind: llm, prompt: p, depends_on: [b]}\n  - {id: b, kind: llm, prompt: p, depends_on: [a]}", "dependency cycle"},
		{"bad kind", "name: x\nsteps:\n  - {id: a, kind: shell}", "unknown kind"},
		{"llm no prompt", "name: x\nsteps:\n  - {id: a, kind: llm}", "requires prompt"},
		{"llm bad output", "name: x\nsteps:\n  - {id: a, kind: llm, prompt: p, output: xml}", "output must be"},
		{"tool both", "name: x\nsteps:\n  - {id: a, kind: tool, tool: echo, http: {url: u}}", "exactly one of tool or http"},
		{"tool neither", "name: x\nsteps:\n  - {id: a, kind: tool}", "exactly one of tool or http"},
		{"branch no cases", "name: x\nsteps:\n  - {id: a, kind: branch}", "at least one case"},
		{"branch target missing", "name: x\nsteps:\n  - {id: a, kind: branch, cases: [{when: 'true', then: [b]}]}", "does not exist"},
		{"branch target no dep", "name: x\nsteps:\n  - {id: a, kind: branch, cases: [{when: 'true', then: [b]}]}\n  - {id: b, kind: llm, prompt: p}", "must list \"a\" in depends_on"},
		{"map no over", "name: x\nsteps:\n  - {id: a, kind: map, step: {kind: llm, prompt: p}}", "requires over"},
		{"map nested map", "name: x\nsteps:\n  - {id: a, kind: map, over: x, step: {kind: map, over: y, step: {kind: llm, prompt: p}}}", "map is not allowed inside map"},
		{"bad duration", "name: x\ndefaults: {timeout: soon}\nsteps:\n  - {id: a, kind: llm, prompt: p}", "duration"},
		{"zero attempts", "name: x\nsteps:\n  - {id: a, kind: llm, prompt: p, retry: {max_attempts: -1}}", "max_attempts must be >= 1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := workflow.Parse([]byte(tc.yaml))
			if err == nil {
				t.Fatal("Parse succeeded, want error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

func TestCycleIsTyped(t *testing.T) {
	_, err := workflow.Parse([]byte("name: x\nsteps:\n  - {id: a, kind: llm, prompt: p, depends_on: [a2]}\n  - {id: a2, kind: llm, prompt: p, depends_on: [a]}"))
	var verr *workflow.ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("err = %T, want *ValidationError", err)
	}
}

func TestCheckInputs(t *testing.T) {
	wf, err := workflow.Parse([]byte("name: x\ninputs: [a, b]\nsteps:\n  - {id: s, kind: llm, prompt: p}"))
	if err != nil {
		t.Fatal(err)
	}
	if err := wf.CheckInputs(map[string]any{"a": 1}); err == nil || !strings.Contains(err.Error(), "b") {
		t.Errorf("CheckInputs = %v, want missing b", err)
	}
	if err := wf.CheckInputs(map[string]any{"a": 1, "b": 2}); err != nil {
		t.Errorf("CheckInputs = %v", err)
	}
}

func TestExamplesParse(t *testing.T) {
	files, err := filepath.Glob("../examples/*/workflow.yaml")
	if err != nil || len(files) < 2 {
		t.Fatalf("examples: %v %v", files, err)
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := workflow.Parse(b); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
}

func TestRender(t *testing.T) {
	data := workflow.Data{
		"input": map[string]any{"name": "Ada", "tags": []any{"x", "y"}},
		"steps": map[string]any{
			"cls": map[string]any{"output": map[string]any{"category": "billing"}},
		},
	}
	tests := []struct {
		name, tmpl, want string
		wantErr          bool
	}{
		{"plain", "no templates", "no templates", false},
		{"input", "hi {{ .input.name }}", "hi Ada", false},
		{"step output", "{{ .steps.cls.output.category }}", "billing", false},
		{"join", `{{ join "," .input.tags }}`, "x,y", false},
		{"json", `{{ json .input.tags }}`, `["x","y"]`, false},
		{"upper", `{{ upper .input.name }}`, "ADA", false},
		{"default", `{{ default "n/a" "" }}`, "n/a", false},
		{"first", `{{ first "" nil "z" }}`, "z", false},
		{"index missing step", `{{ with index .steps "nope" }}x{{ else }}none{{ end }}`, "none", false},
		{"missing key errors", "{{ .input.nmae }}", "", true},
		{"bad syntax", "{{ .input.name ", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := workflow.Render(tc.tmpl, data)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("Render = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTruthy(t *testing.T) {
	data := workflow.Data{"input": map[string]any{"n": 3, "s": "billing"}}
	tests := []struct {
		cond    string
		want    bool
		wantErr bool
	}{
		{`{{ eq .input.s "billing" }}`, true, false},
		{`{{ gt .input.n 5 }}`, false, false},
		{`true`, true, false},
		{``, false, false},
		{`maybe`, false, true},
	}
	for _, tc := range tests {
		got, err := workflow.Truthy(tc.cond, data)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("Truthy(%q) = %v, %v; want %v, err=%v", tc.cond, got, err, tc.want, tc.wantErr)
		}
	}
}

func TestRenderValue(t *testing.T) {
	data := workflow.Data{"input": map[string]any{"id": "T-1"}}
	got, err := workflow.RenderValue(map[string]any{
		"id":    "{{ .input.id }}",
		"n":     3,
		"items": []any{"a-{{ .input.id }}", true},
	}, data)
	if err != nil {
		t.Fatal(err)
	}
	m := got.(map[string]any)
	if m["id"] != "T-1" || m["n"] != 3 || m["items"].([]any)[0] != "a-T-1" || m["items"].([]any)[1] != true {
		t.Errorf("RenderValue = %#v", got)
	}
}

func TestLookup(t *testing.T) {
	data := workflow.Data{"steps": map[string]any{"p": map[string]any{"output": map[string]any{"list": []any{"a", "b"}}}}}
	tests := []struct {
		path    string
		want    any
		wantErr bool
	}{
		{"steps.p.output.list.1", "b", false},
		{".steps.p.output.list.0", "a", false},
		{"steps.p.output.missing", nil, true},
		{"steps.p.output.list.9", nil, true},
		{"steps.p.output.list.0.deeper", nil, true},
	}
	for _, tc := range tests {
		got, err := workflow.Lookup(tc.path, data)
		if (err != nil) != tc.wantErr {
			t.Errorf("Lookup(%q) err = %v", tc.path, err)
			continue
		}
		if !tc.wantErr && got != tc.want {
			t.Errorf("Lookup(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func BenchmarkParse(b *testing.B) {
	spec, err := os.ReadFile("../examples/support-triage/workflow.yaml")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := workflow.Parse(spec); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRender(b *testing.B) {
	data := workflow.Data{"input": map[string]any{"topic": "agents"}, "steps": map[string]any{}}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := workflow.Render("Research {{ .input.topic }} in depth.", data); err != nil {
			b.Fatal(err)
		}
	}
}
