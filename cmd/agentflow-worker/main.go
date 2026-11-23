// Command agentflow-worker consumes step jobs from Redis and executes them.
// Run as many replicas as you like; they coordinate through the consumer
// group and the store.
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

	"github.com/RatulSikder97/agentflow/engine"
	"github.com/RatulSikder97/agentflow/internal/app"
	"github.com/RatulSikder97/agentflow/telemetry"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("agentflow-worker exited", "err", err)
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
	if env.DatabaseURL == "" || env.RedisURL == "" {
		return errors.New("agentflow-worker needs DATABASE_URL and REDIS_URL (use the server's embedded workers for in-memory mode)")
	}
	shutdownTracing, err := telemetry.SetupTracing(ctx, "agentflow-worker")
	if err != nil {
		return err
	}
	defer func() { _ = shutdownTracing(context.WithoutCancel(ctx)) }()

	deps, err := app.Open(ctx, env, log)
	if err != nil {
		return err
	}
	defer deps.Close()

	metricsAddr := os.Getenv("AGENTFLOW_METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":9090"
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", deps.Metrics.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	srv := &http.Server{Addr: metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	w := engine.NewWorker(deps.Engine, deps.Queue,
		engine.WithConcurrency(env.Concurrency), engine.WithHeartbeat(app.HeartbeatFor(env)))

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("metrics http: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		sctx, cancel := context.WithTimeout(context.WithoutCancel(gctx), 5*time.Second)
		defer cancel()
		return srv.Shutdown(sctx)
	})
	g.Go(func() error {
		log.Info("worker started", "concurrency", env.Concurrency, "metrics_addr", metricsAddr, "version", version)
		return w.Run(gctx)
	})
	return g.Wait()
}
