package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/RatulSikder97/agentflow/queue"
)

// Receiver is the consuming side of a queue.
type Receiver interface {
	Receive(ctx context.Context) (queue.Delivery, error)
}

// Worker pulls jobs from a queue and executes them with bounded concurrency.
type Worker struct {
	engine      *Engine
	queue       Receiver
	concurrency int64
	heartbeat   time.Duration
	log         *slog.Logger
}

// WorkerOption configures a Worker.
type WorkerOption func(*Worker)

// WithConcurrency bounds how many steps execute at once (default 8).
func WithConcurrency(n int) WorkerOption {
	return func(w *Worker) {
		if n > 0 {
			w.concurrency = int64(n)
		}
	}
}

// WithHeartbeat sets how often in-flight deliveries are extended; it must be
// well below the queue's visibility timeout (default 10s).
func WithHeartbeat(d time.Duration) WorkerOption {
	return func(w *Worker) {
		if d > 0 {
			w.heartbeat = d
		}
	}
}

// NewWorker returns a worker bound to an engine and a queue.
func NewWorker(e *Engine, q Receiver, opts ...WorkerOption) *Worker {
	w := &Worker{engine: e, queue: q, concurrency: 8, heartbeat: 10 * time.Second, log: e.log}
	for _, o := range opts {
		o(w)
	}
	return w
}

// Run consumes jobs until ctx is cancelled or the queue is closed. In-flight
// steps observe the same ctx: on shutdown they are abandoned unacknowledged
// and redelivered to another worker, which resumes from the last checkpoint.
func (w *Worker) Run(ctx context.Context) error {
	sem := semaphore.NewWeighted(w.concurrency)
	g, gctx := errgroup.WithContext(ctx)
	for {
		if err := sem.Acquire(gctx, 1); err != nil {
			break
		}
		d, err := w.queue.Receive(gctx)
		if err != nil {
			sem.Release(1)
			if gctx.Err() != nil {
				break
			}
			if errors.Is(err, queue.ErrClosed) {
				break
			}
			w.log.ErrorContext(gctx, "receive failed", "err", err)
			if err := sleepCtx(gctx, time.Second); err != nil {
				break
			}
			continue
		}
		g.Go(func() error {
			defer sem.Release(1)
			w.handle(gctx, d)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("worker: %w", err)
	}
	return nil
}

func (w *Worker) handle(ctx context.Context, d queue.Delivery) {
	job := d.Job()
	m := w.engine.metrics
	m.Inflight(1)
	defer m.Inflight(-1)

	hbCtx, stopHB := context.WithCancel(ctx)
	defer stopHB()
	go func() {
		t := time.NewTicker(w.heartbeat)
		defer t.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-t.C:
				if err := d.Extend(hbCtx); err != nil && hbCtx.Err() == nil {
					w.log.WarnContext(hbCtx, "extend delivery failed", "job", job.Key(), "err", err)
				}
			}
		}
	}()

	if err := w.engine.ExecuteStep(ctx, job); err != nil {
		if ctx.Err() == nil {
			w.log.ErrorContext(ctx, "step execution aborted; job will be redelivered", "job", job.Key(), "err", err)
		}
		return
	}
	if err := d.Ack(context.WithoutCancel(ctx)); err != nil {
		w.log.ErrorContext(ctx, "ack failed; job will be redelivered", "job", job.Key(), "err", err)
	}
}
