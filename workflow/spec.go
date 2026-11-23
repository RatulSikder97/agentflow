// Package workflow defines the YAML workflow format, validates it, and
// computes the step dependency graph the engine schedules against.
package workflow

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Kind is the type of work a step performs.
type Kind string

// Supported step kinds.
const (
	KindLLM    Kind = "llm"
	KindTool   Kind = "tool"
	KindBranch Kind = "branch"
	KindMap    Kind = "map"
)

// OutputFormat controls how an LLM step's text response is decoded.
type OutputFormat string

// Supported LLM output formats.
const (
	OutputText OutputFormat = "text"
	OutputJSON OutputFormat = "json"
)

// Defaults applied when a workflow omits them.
const (
	DefaultModel          = "claude-sonnet-5-5"
	DefaultMaxTokens      = 4096
	DefaultTimeout        = 2 * time.Minute
	DefaultMaxAttempts    = 3
	DefaultBackoff        = 500 * time.Millisecond
	DefaultMaxBackoff     = 30 * time.Second
	DefaultMapConcurrency = 4
)

var idPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// Duration is a time.Duration that unmarshals from YAML strings like "30s".
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return fmt.Errorf("duration: %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("duration %q: %w", s, err)
	}
	if v < 0 {
		return fmt.Errorf("duration %q must not be negative", s)
	}
	*d = Duration(v)
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (d Duration) MarshalYAML() (any, error) { return time.Duration(d).String(), nil }

// Std returns the value as a time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// Retry configures per-step retries with exponential backoff and full jitter.
type Retry struct {
	MaxAttempts int      `yaml:"max_attempts,omitempty"`
	Backoff     Duration `yaml:"backoff,omitempty"`
	MaxBackoff  Duration `yaml:"max_backoff,omitempty"`
}

// Defaults are workflow-wide settings inherited by every step.
type Defaults struct {
	Model   string   `yaml:"model,omitempty"`
	Timeout Duration `yaml:"timeout,omitempty"`
	Retry   Retry    `yaml:"retry,omitempty"`
}

// HTTPCall describes a tool step that calls an HTTP endpoint instead of a
// registered Go function.
type HTTPCall struct {
	Method  string            `yaml:"method,omitempty"`
	URL     string            `yaml:"url"`
	Headers map[string]string `yaml:"headers,omitempty"`
	Body    string            `yaml:"body,omitempty"`
}

// Case is one arm of a branch step.
type Case struct {
	When string   `yaml:"when"`
	Then []string `yaml:"then"`
}

// Step is a single node in the workflow DAG.
type Step struct {
	ID        string   `yaml:"id"`
	Kind      Kind     `yaml:"kind"`
	DependsOn []string `yaml:"depends_on,omitempty"`
	Timeout   Duration `yaml:"timeout,omitempty"`
	Retry     *Retry   `yaml:"retry,omitempty"`

	// llm
	Model     string       `yaml:"model,omitempty"`
	System    string       `yaml:"system,omitempty"`
	Prompt    string       `yaml:"prompt,omitempty"`
	MaxTokens int          `yaml:"max_tokens,omitempty"`
	Output    OutputFormat `yaml:"output,omitempty"`

	// tool
	Tool string         `yaml:"tool,omitempty"`
	Args map[string]any `yaml:"args,omitempty"`
	HTTP *HTTPCall      `yaml:"http,omitempty"`

	// branch
	Cases   []Case   `yaml:"cases,omitempty"`
	Default []string `yaml:"default,omitempty"`

	// map
	Over        string `yaml:"over,omitempty"`
	Concurrency int    `yaml:"concurrency,omitempty"`
	Step        *Step  `yaml:"step,omitempty"`
}

// Workflow is a parsed, validated workflow definition.
type Workflow struct {
	Name        string   `yaml:"name"`
	Description string   `yaml:"description,omitempty"`
	Inputs      []string `yaml:"inputs,omitempty"`
	Defaults    Defaults `yaml:"defaults,omitempty"`
	Steps       []Step   `yaml:"steps"`

	index map[string]int
	order []string
}

// Parse decodes YAML into a Workflow, applies defaults and validates it.
// Unknown fields are rejected so typos fail loudly.
func Parse(data []byte) (*Workflow, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var wf Workflow
	if err := dec.Decode(&wf); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSyntax, err)
	}
	wf.applyDefaults()
	if err := wf.validate(); err != nil {
		return nil, err
	}
	return &wf, nil
}

func (w *Workflow) applyDefaults() {
	if w.Defaults.Model == "" {
		w.Defaults.Model = DefaultModel
	}
	if w.Defaults.Timeout == 0 {
		w.Defaults.Timeout = Duration(DefaultTimeout)
	}
	r := &w.Defaults.Retry
	if r.MaxAttempts == 0 {
		r.MaxAttempts = DefaultMaxAttempts
	}
	if r.Backoff == 0 {
		r.Backoff = Duration(DefaultBackoff)
	}
	if r.MaxBackoff == 0 {
		r.MaxBackoff = Duration(DefaultMaxBackoff)
	}
	for i := range w.Steps {
		w.applyStepDefaults(&w.Steps[i])
		if w.Steps[i].Step != nil {
			w.applyStepDefaults(w.Steps[i].Step)
		}
	}
}

func (w *Workflow) applyStepDefaults(s *Step) {
	if s.Timeout == 0 {
		s.Timeout = w.Defaults.Timeout
	}
	if s.Retry == nil {
		r := w.Defaults.Retry
		s.Retry = &r
	} else {
		if s.Retry.MaxAttempts == 0 {
			s.Retry.MaxAttempts = w.Defaults.Retry.MaxAttempts
		}
		if s.Retry.Backoff == 0 {
			s.Retry.Backoff = w.Defaults.Retry.Backoff
		}
		if s.Retry.MaxBackoff == 0 {
			s.Retry.MaxBackoff = w.Defaults.Retry.MaxBackoff
		}
	}
	switch s.Kind {
	case KindLLM:
		if s.Model == "" {
			s.Model = w.Defaults.Model
		}
		if s.MaxTokens == 0 {
			s.MaxTokens = DefaultMaxTokens
		}
		if s.Output == "" {
			s.Output = OutputText
		}
	case KindMap:
		if s.Concurrency == 0 {
			s.Concurrency = DefaultMapConcurrency
		}
	case KindTool:
		if s.HTTP != nil && s.HTTP.Method == "" {
			s.HTTP.Method = "POST"
		}
	}
}

// ValidationError aggregates every problem found in a workflow.
type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return "invalid workflow: " + strings.Join(e.Problems, "; ")
}

func (w *Workflow) validate() error {
	var p []string
	add := func(format string, args ...any) { p = append(p, fmt.Sprintf(format, args...)) }

	if w.Name == "" {
		add("name is required")
	}
	if len(w.Steps) == 0 {
		add("at least one step is required")
	}
	w.index = make(map[string]int, len(w.Steps))
	for i, s := range w.Steps {
		if !idPattern.MatchString(s.ID) {
			add("step %d: id %q must match %s", i, s.ID, idPattern)
			continue
		}
		if _, dup := w.index[s.ID]; dup {
			add("step %q: duplicate id", s.ID)
			continue
		}
		w.index[s.ID] = i
	}
	for _, s := range w.Steps {
		for _, d := range s.DependsOn {
			if _, ok := w.index[d]; !ok {
				add("step %q: depends_on unknown step %q", s.ID, d)
			}
			if d == s.ID {
				add("step %q: depends on itself", s.ID)
			}
		}
		p = append(p, validateStep(&s, false)...)
		if s.Kind == KindBranch {
			for _, target := range s.branchTargets() {
				ti, ok := w.index[target]
				if !ok {
					add("step %q: branch target %q does not exist", s.ID, target)
					continue
				}
				if !slices.Contains(w.Steps[ti].DependsOn, s.ID) {
					add("step %q: branch target %q must list %q in depends_on", s.ID, target, s.ID)
				}
			}
		}
	}
	if len(p) > 0 {
		return &ValidationError{Problems: p}
	}
	order, err := w.topoSort()
	if err != nil {
		return &ValidationError{Problems: []string{err.Error()}}
	}
	w.order = order
	return nil
}

func validateStep(s *Step, nested bool) []string {
	var p []string
	add := func(format string, args ...any) {
		p = append(p, fmt.Sprintf("step %q: ", s.ID)+fmt.Sprintf(format, args...))
	}
	if s.Retry != nil && s.Retry.MaxAttempts < 1 {
		add("retry.max_attempts must be >= 1")
	}
	switch s.Kind {
	case KindLLM:
		if strings.TrimSpace(s.Prompt) == "" {
			add("llm step requires prompt")
		}
		if s.Output != OutputText && s.Output != OutputJSON {
			add("output must be %q or %q", OutputText, OutputJSON)
		}
	case KindTool:
		if (s.Tool == "") == (s.HTTP == nil) {
			add("tool step requires exactly one of tool or http")
		}
		if s.HTTP != nil && s.HTTP.URL == "" {
			add("http.url is required")
		}
	case KindBranch:
		if nested {
			add("branch is not allowed inside map")
		}
		if len(s.Cases) == 0 {
			add("branch step requires at least one case")
		}
		for i, c := range s.Cases {
			if strings.TrimSpace(c.When) == "" || len(c.Then) == 0 {
				add("case %d requires when and then", i)
			}
		}
	case KindMap:
		if nested {
			add("map is not allowed inside map")
		}
		if s.Over == "" {
			add("map step requires over")
		}
		if s.Concurrency < 1 {
			add("concurrency must be >= 1")
		}
		if s.Step == nil {
			add("map step requires step")
		} else {
			if s.Step.ID == "" {
				s.Step.ID = s.ID + "_item"
			}
			p = append(p, validateStep(s.Step, true)...)
		}
	default:
		add("unknown kind %q (want llm, tool, branch or map)", s.Kind)
	}
	return p
}

func (s *Step) branchTargets() []string {
	var out []string
	for _, c := range s.Cases {
		out = append(out, c.Then...)
	}
	out = append(out, s.Default...)
	slices.Sort(out)
	return slices.Compact(out)
}

// BranchTargets returns every step a branch step may select.
func (s *Step) BranchTargets() []string { return s.branchTargets() }

// ErrSyntax is returned when the YAML cannot be decoded.
var ErrSyntax = errors.New("workflow syntax error")

// ErrCycle is returned when the dependency graph is not a DAG.
var ErrCycle = errors.New("dependency cycle")

// topoSort orders steps with Kahn's algorithm, breaking ties by declaration
// order so the result is deterministic.
func (w *Workflow) topoSort() ([]string, error) {
	indeg := make(map[string]int, len(w.Steps))
	children := make(map[string][]string, len(w.Steps))
	for _, s := range w.Steps {
		indeg[s.ID] += 0
		for _, d := range s.DependsOn {
			indeg[s.ID]++
			children[d] = append(children[d], s.ID)
		}
	}
	var ready, order []string
	for _, s := range w.Steps {
		if indeg[s.ID] == 0 {
			ready = append(ready, s.ID)
		}
	}
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		order = append(order, id)
		for _, c := range children[id] {
			indeg[c]--
			if indeg[c] == 0 {
				ready = append(ready, c)
			}
		}
	}
	if len(order) != len(w.Steps) {
		var stuck []string
		for _, s := range w.Steps {
			if indeg[s.ID] > 0 {
				stuck = append(stuck, s.ID)
			}
		}
		return nil, fmt.Errorf("%w among steps %s", ErrCycle, strings.Join(stuck, ", "))
	}
	return order, nil
}

// Order returns step IDs in a valid topological order.
func (w *Workflow) Order() []string { return slices.Clone(w.order) }

// Step returns the step with the given ID.
func (w *Workflow) Step(id string) (*Step, bool) {
	i, ok := w.index[id]
	if !ok {
		return nil, false
	}
	return &w.Steps[i], true
}

// StepIDs returns all step IDs in declaration order.
func (w *Workflow) StepIDs() []string {
	ids := make([]string, len(w.Steps))
	for i, s := range w.Steps {
		ids[i] = s.ID
	}
	return ids
}

// Roots returns steps with no dependencies.
func (w *Workflow) Roots() []string {
	var out []string
	for _, s := range w.Steps {
		if len(s.DependsOn) == 0 {
			out = append(out, s.ID)
		}
	}
	return out
}

// CheckInputs reports missing required inputs.
func (w *Workflow) CheckInputs(input map[string]any) error {
	var missing []string
	for _, k := range w.Inputs {
		if _, ok := input[k]; !ok {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required inputs: %s", strings.Join(missing, ", "))
	}
	return nil
}
