// Command afctl validates and runs workflows locally, and talks to an
// agentflow server.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/RatulSikder97/agentflow/engine"
	"github.com/RatulSikder97/agentflow/llm/anthropic"
	"github.com/RatulSikder97/agentflow/llm/fake"
	"github.com/RatulSikder97/agentflow/queue/memqueue"
	"github.com/RatulSikder97/agentflow/store"
	"github.com/RatulSikder97/agentflow/store/memstore"
	"github.com/RatulSikder97/agentflow/workflow"
)

const usage = `afctl - agentflow command line

Local (no server needed):
  afctl validate -f workflow.yaml
  afctl run      -f workflow.yaml [-input '{"k":"v"}'] [-llm fake|anthropic] [-fixtures rules.yaml]

Remote (AGENTFLOW_SERVER, default http://localhost:8080):
  afctl submit -f workflow.yaml
  afctl start  <workflow-id> [-input '{"k":"v"}']
  afctl status <run-id>
  afctl watch  <run-id>
  afctl version
`

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	err := run(ctx, os.Args[1:], os.Stdout)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "afctl:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(out, usage)
		return nil
	}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "validate":
		return cmdValidate(rest, out)
	case "run":
		return cmdRun(ctx, rest, out)
	case "submit":
		return cmdSubmit(ctx, rest, out)
	case "start":
		return cmdStart(ctx, rest, out)
	case "status":
		return cmdStatus(ctx, rest, out)
	case "watch":
		return cmdWatch(ctx, rest, out)
	case "version":
		fmt.Fprintln(out, version)
		return nil
	case "help", "-h", "--help":
		fmt.Fprint(out, usage)
		return nil
	default:
		return fmt.Errorf("unknown command %q\n\n%s", cmd, usage)
	}
}

// parseFlags lets flags appear after positional arguments.
func parseFlags(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) == 0 {
			return pos, nil
		}
		pos = append(pos, args[0])
		args = args[1:]
	}
}

func readSpec(path string) ([]byte, *workflow.Workflow, error) {
	if path == "" {
		return nil, nil, errors.New("-f is required")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	wf, err := workflow.Parse(b)
	if err != nil {
		return nil, nil, err
	}
	return b, wf, nil
}

func parseInput(s string) (map[string]any, error) {
	in := map[string]any{}
	if s == "" {
		return in, nil
	}
	if err := json.Unmarshal([]byte(s), &in); err != nil {
		return nil, fmt.Errorf("-input must be a JSON object: %w", err)
	}
	return in, nil
}

func cmdValidate(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	file := fs.String("f", "", "workflow YAML file")
	if _, err := parseFlags(fs, args); err != nil {
		return err
	}
	_, wf, err := readSpec(*file)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "ok: %s (%d steps)\n", wf.Name, len(wf.Steps))
	for i, id := range wf.Order() {
		s, _ := wf.Step(id)
		deps := ""
		if len(s.DependsOn) > 0 {
			deps = " <- " + strings.Join(s.DependsOn, ", ")
		}
		fmt.Fprintf(out, "  %2d. %-20s %-7s%s\n", i+1, id, s.Kind, deps)
	}
	return nil
}

func cmdRun(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	file := fs.String("f", "", "workflow YAML file")
	input := fs.String("input", "", "run input as a JSON object")
	provider := fs.String("llm", "fake", "LLM provider: fake or anthropic")
	fixtures := fs.String("fixtures", "", "fake LLM rules file")
	concurrency := fs.Int("concurrency", 8, "max steps in flight")
	timeout := fs.Duration("timeout", 10*time.Minute, "overall run timeout")
	if _, err := parseFlags(fs, args); err != nil {
		return err
	}
	spec, parsed, err := readSpec(*file)
	if err != nil {
		return err
	}
	in, err := parseInput(*input)
	if err != nil {
		return err
	}

	var completer engine.Completer
	switch *provider {
	case "fake":
		p := fake.New()
		if *fixtures != "" {
			if p, err = fake.Load(*fixtures); err != nil {
				return err
			}
		}
		completer = p
	case "anthropic":
		c, err := anthropic.New()
		if err != nil {
			return err
		}
		completer = c
	default:
		return fmt.Errorf("unknown -llm %q", *provider)
	}

	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	st, q := memstore.New(), memqueue.New()
	defer func() { _ = q.Close() }()
	eng, err := engine.New(engine.Config{
		Store: st, Queue: q, LLM: completer,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		return err
	}
	wctx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	go func() { _ = engine.NewWorker(eng, q, engine.WithConcurrency(*concurrency)).Run(wctx) }()

	wf, err := eng.SubmitWorkflow(ctx, spec)
	if err != nil {
		return err
	}
	run, err := eng.StartRun(ctx, wf.ID, in)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "run %s (%s)\n", run.ID, wf.Name)

	var first time.Time
	var after int64
	for done := false; !done; {
		evs, err := eng.ListEvents(ctx, run.ID, after, 0)
		if err != nil {
			return err
		}
		for _, ev := range evs {
			after = ev.Seq
			printEvent(out, &first, ev)
			done = done || ev.IsTerminalRunEvent()
		}
		if !done {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	view, err := eng.GetRun(ctx, run.ID)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "\noutputs:")
	byID := map[string]store.StepRun{}
	for _, s := range view.Steps {
		byID[s.StepID] = s
	}
	for _, id := range parsed.Order() {
		s := byID[id]
		if s.Status != store.StepSucceeded {
			fmt.Fprintf(out, "  %s: (%s)\n", id, s.Status)
			continue
		}
		var buf bytes.Buffer
		_ = json.Indent(&buf, s.Output, "    ", "  ")
		fmt.Fprintf(out, "  %s:\n    %s\n", id, buf.String())
	}
	if view.Status != store.RunSucceeded {
		return fmt.Errorf("run %s: %s", view.Status, view.Error)
	}
	return nil
}

// printEvent prints ev with its offset from the first event seen.
func printEvent(out io.Writer, first *time.Time, ev store.Event) {
	if first.IsZero() {
		*first = ev.Time
	}
	fmt.Fprintf(out, "%8.3fs  %-15s %-18s %s\n", ev.Time.Sub(*first).Seconds(), ev.Type, ev.StepID, string(ev.Data))
}

// --- remote commands ---

func serverURL() string {
	if s := os.Getenv("AGENTFLOW_SERVER"); s != "" {
		return strings.TrimRight(s, "/")
	}
	return "http://localhost:8080"
}

func do(ctx context.Context, method, path, ctype string, body io.Reader, out io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, method, serverURL()+path, body)
	if err != nil {
		return err
	}
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if json.Indent(&buf, b, "", "  ") != nil {
		buf.Reset()
		buf.Write(b)
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s %s: %s\n%s", method, path, resp.Status, buf.String())
	}
	_, err = fmt.Fprintln(out, buf.String())
	return err
}

func cmdSubmit(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("submit", flag.ContinueOnError)
	file := fs.String("f", "", "workflow YAML file")
	if _, err := parseFlags(fs, args); err != nil {
		return err
	}
	if *file == "" {
		return errors.New("-f is required")
	}
	b, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	return do(ctx, http.MethodPost, "/v1/workflows", "application/yaml", bytes.NewReader(b), out)
}

func cmdStart(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	input := fs.String("input", "", "run input as a JSON object")
	pos, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return errors.New("usage: afctl start <workflow-id> [-input JSON]")
	}
	in, err := parseInput(*input)
	if err != nil {
		return err
	}
	body, _ := json.Marshal(map[string]any{"input": in})
	return do(ctx, http.MethodPost, "/v1/workflows/"+pos[0]+"/runs", "application/json", bytes.NewReader(body), out)
}

func cmdStatus(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: afctl status <run-id>")
	}
	return do(ctx, http.MethodGet, "/v1/runs/"+args[0], "", nil, out)
}

func cmdWatch(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: afctl watch <run-id>")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, serverURL()+"/v1/runs/"+args[0]+"/events", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("watch: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var first time.Time
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), 4<<20)
	for sc.Scan() {
		line := sc.Text()
		data, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue
		}
		var ev store.Event
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			return fmt.Errorf("watch: decode event: %w", err)
		}
		printEvent(out, &first, ev)
	}
	return sc.Err()
}
