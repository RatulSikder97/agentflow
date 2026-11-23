// Command agentflow runs the API server, the reconciler and (optionally)
// embedded workers.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/RatulSikder97/agentflow/api"
	"github.com/RatulSikder97/agentflow/engine"
	"github.com/RatulSikder97/agentflow/internal/app"
	"github.com/RatulSikder97/agentflow/telemetry"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("agentflow exited", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	env, err := app.LoadEnv()
	if err != nil {
		return err
	}
	shutdownTracing, err := telemetry.SetupTracing(ctx, "agentflow")
	if err != nil {
		return err
	}
	defer func() { _ = shutdownTracing(context.WithoutCancel(ctx)) }()

	deps, err := app.Open(ctx, env, log)
	if err != nil {
		return err
	}
	defer deps.Close()

	srv := &http.Server{
		Addr:              env.Addr,
		Handler:           api.NewHandler(deps.Engine, api.Options{Logger: log, Metrics: deps.Metrics.Handler()}),
		ReadHeaderTimeout: 10 * time.Second,
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		log.Info("listening", "addr", env.Addr, "embedded_workers", env.Workers, "version", version)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(gctx), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	})
	g.Go(func() error { return reconcileLoop(gctx, deps.Engine, env.ReconcileInterval, log) })
	for i := 0; i < env.Workers; i++ {
		w := engine.NewWorker(deps.Engine, deps.Queue,
			engine.WithConcurrency(env.Concurrency), engine.WithHeartbeat(app.HeartbeatFor(env)))
		g.Go(func() error { return w.Run(gctx) })
	}
	return g.Wait()
}

// reconcileLoop repairs runs orphaned by a crash, once at startup and then
// periodically.
func reconcileLoop(ctx context.Context, eng *engine.Engine, every time.Duration, log *slog.Logger) error {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		n, err := eng.Reconcile(ctx, every)
		switch {
		case err != nil && ctx.Err() == nil:
			log.Error("reconcile failed", "err", err)
		case n > 0:
			log.Info("reconcile re-published jobs", "count", n)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}
