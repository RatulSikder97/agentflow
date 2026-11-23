// Package api exposes the engine over REST and Server-Sent Events.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/RatulSikder97/agentflow/engine"
	"github.com/RatulSikder97/agentflow/store"
	"github.com/RatulSikder97/agentflow/workflow"
)

// Service is the subset of the engine the API needs.
type Service interface {
	SubmitWorkflow(ctx context.Context, spec []byte) (store.Workflow, error)
	GetWorkflow(ctx context.Context, id string) (store.Workflow, error)
	StartRun(ctx context.Context, workflowID string, input map[string]any) (store.Run, error)
	GetRun(ctx context.Context, id string) (engine.RunView, error)
	ListEvents(ctx context.Context, runID string, afterSeq int64, limit int) ([]store.Event, error)
}

// Options configures the HTTP handler.
type Options struct {
	Logger       *slog.Logger
	Metrics      http.Handler  // mounted at /metrics when non-nil
	PollInterval time.Duration // SSE store poll interval (default 200ms)
	MaxBodyBytes int64         // default 1 MiB
}

type server struct {
	svc  Service
	opts Options
}

// NewHandler returns the HTTP handler for the API.
func NewHandler(svc Service, opts Options) http.Handler {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = 200 * time.Millisecond
	}
	if opts.MaxBodyBytes == 0 {
		opts.MaxBodyBytes = 1 << 20
	}
	s := &server{svc: svc, opts: opts}

	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.Recoverer)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	if opts.Metrics != nil {
		r.Handle("/metrics", opts.Metrics)
	}
	r.Route("/v1", func(r chi.Router) {
		r.Post("/workflows", s.submitWorkflow)
		r.Get("/workflows/{id}", s.getWorkflow)
		r.Post("/workflows/{id}/runs", s.startRun)
		r.Get("/runs/{id}", s.getRun)
		r.Get("/runs/{id}/events", s.streamEvents)
	})
	return r
}

type errorBody struct {
	Error   string   `json:"error"`
	Details []string `json:"details,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *server) fail(w http.ResponseWriter, r *http.Request, err error) {
	var verr *workflow.ValidationError
	switch {
	case errors.As(err, &verr):
		writeJSON(w, http.StatusUnprocessableEntity, errorBody{Error: "invalid workflow", Details: verr.Problems})
	case errors.Is(err, workflow.ErrSyntax):
		writeJSON(w, http.StatusBadRequest, errorBody{Error: err.Error()})
	case errors.Is(err, engine.ErrInvalidInput):
		writeJSON(w, http.StatusUnprocessableEntity, errorBody{Error: err.Error()})
	case errors.Is(err, store.ErrNotFound):
		writeJSON(w, http.StatusNotFound, errorBody{Error: err.Error()})
	default:
		s.opts.Logger.ErrorContext(r.Context(), "request failed", "path", r.URL.Path, "err", err)
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "internal error"})
	}
}

// submitWorkflow accepts a raw YAML body.
func (s *server) submitWorkflow(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.opts.MaxBodyBytes))
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, errorBody{Error: err.Error()})
		return
	}
	wf, err := s.svc.SubmitWorkflow(r.Context(), body)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"id": wf.ID, "name": wf.Name, "created_at": wf.CreatedAt})
}

func (s *server) getWorkflow(w http.ResponseWriter, r *http.Request) {
	wf, err := s.svc.GetWorkflow(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, wf)
}

type startRunRequest struct {
	Input map[string]any `json:"input"`
}

func (s *server) startRun(w http.ResponseWriter, r *http.Request) {
	var req startRunRequest
	if r.ContentLength != 0 {
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, s.opts.MaxBodyBytes))
		if err := dec.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid JSON body: " + err.Error()})
			return
		}
	}
	run, err := s.svc.StartRun(r.Context(), chi.URLParam(r, "id"), req.Input)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, run)
}

func (s *server) getRun(w http.ResponseWriter, r *http.Request) {
	run, err := s.svc.GetRun(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

// streamEvents streams run events as SSE. Events come from the store, so any
// API replica can serve any run and clients resume with Last-Event-ID. The
// stream ends after the run's terminal event.
func (s *server) streamEvents(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "id")
	if _, err := s.svc.GetRun(r.Context(), runID); err != nil {
		s.fail(w, r, err)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "streaming unsupported"})
		return
	}
	var after int64
	for _, v := range []string{r.Header.Get("Last-Event-ID"), r.URL.Query().Get("after")} {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > after {
			after = n
		}
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ctx := r.Context()
	poll := time.NewTicker(s.opts.PollInterval)
	defer poll.Stop()
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		evs, err := s.svc.ListEvents(ctx, runID, after, 500)
		if err != nil {
			if ctx.Err() == nil {
				s.opts.Logger.ErrorContext(ctx, "list events failed", "run_id", runID, "err", err)
			}
			return
		}
		for _, ev := range evs {
			if err := writeEvent(w, ev); err != nil {
				return
			}
			after = ev.Seq
			if ev.IsTerminalRunEvent() {
				flusher.Flush()
				return
			}
		}
		if len(evs) > 0 {
			flusher.Flush()
			continue // drain backlog before sleeping
		}
		// Client resumed after the terminal event: nothing more will come.
		if v, err := s.svc.GetRun(ctx, runID); err == nil && v.Status.Terminal() {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-keepalive.C:
			if _, err := io.WriteString(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-poll.C:
		}
	}
}

func writeEvent(w io.Writer, ev store.Event) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}
	_, err = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", ev.Seq, ev.Type, b)
	return err
}
