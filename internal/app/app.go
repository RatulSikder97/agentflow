// Package app wires concrete dependencies from environment variables. It is
// shared by the server and worker binaries.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/RatulSikder97/agentflow/engine"
	"github.com/RatulSikder97/agentflow/llm/anthropic"
	"github.com/RatulSikder97/agentflow/llm/fake"
	"github.com/RatulSikder97/agentflow/queue"
	"github.com/RatulSikder97/agentflow/queue/memqueue"
	"github.com/RatulSikder97/agentflow/queue/redisqueue"
	"github.com/RatulSikder97/agentflow/store/memstore"
	"github.com/RatulSikder97/agentflow/store/pgstore"
	"github.com/RatulSikder97/agentflow/telemetry"
	"github.com/RatulSikder97/agentflow/tool"
)

// Env is the process configuration.
type Env struct {
	Addr              string        // AGENTFLOW_ADDR, default ":8080"
	DatabaseURL       string        // DATABASE_URL; empty = in-memory store
	RedisURL          string        // REDIS_URL; empty = in-process queue
	LLM               string        // AGENTFLOW_LLM: "anthropic" (default) or "fake"
	FakeFixtures      string        // AGENTFLOW_FAKE_FIXTURES: YAML rules for the fake LLM
	Workers           int           // AGENTFLOW_WORKERS: embedded workers in the server
	Concurrency       int           // AGENTFLOW_CONCURRENCY: steps in flight per worker, default 8
	ClaimIdle         time.Duration // AGENTFLOW_CLAIM_IDLE: XAUTOCLAIM min idle, default 30s
	ReconcileInterval time.Duration // AGENTFLOW_RECONCILE_INTERVAL, default 30s
}

// LoadEnv reads Env from the process environment.
func LoadEnv() (Env, error) {
	e := Env{
		Addr:         getenv("AGENTFLOW_ADDR", ":8080"),
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		RedisURL:     os.Getenv("REDIS_URL"),
		LLM:          getenv("AGENTFLOW_LLM", "anthropic"),
		FakeFixtures: os.Getenv("AGENTFLOW_FAKE_FIXTURES"),
	}
	var err error
	defWorkers := 0
	if e.RedisURL == "" {
		defWorkers = 4
	}
	if e.Workers, err = intEnv("AGENTFLOW_WORKERS", defWorkers); err != nil {
		return e, err
	}
	if e.Concurrency, err = intEnv("AGENTFLOW_CONCURRENCY", 8); err != nil {
		return e, err
	}
	if e.ClaimIdle, err = durEnv("AGENTFLOW_CLAIM_IDLE", 30*time.Second); err != nil {
		return e, err
	}
	if e.ReconcileInterval, err = durEnv("AGENTFLOW_RECONCILE_INTERVAL", 30*time.Second); err != nil {
		return e, err
	}
	return e, nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func intEnv(k string, def int) (int, error) {
	v := os.Getenv(k)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", k, err)
	}
	return n, nil
}

func durEnv(k string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(k)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", k, err)
	}
	return d, nil
}

// Queue is both ends of a job queue.
type Queue interface {
	Enqueue(ctx context.Context, job queue.Job) error
	Receive(ctx context.Context) (queue.Delivery, error)
}

// Deps are the opened dependencies of a process.
type Deps struct {
	Store   engine.Store
	Queue   Queue
	LLM     engine.Completer
	Tools   *tool.Registry
	Metrics *telemetry.Metrics
	Engine  *engine.Engine
	closers []func()
}

// Close releases all resources in reverse order.
func (d *Deps) Close() {
	for i := len(d.closers) - 1; i >= 0; i-- {
		d.closers[i]()
	}
}

// Open connects to the configured store, queue and LLM and builds an Engine.
func Open(ctx context.Context, env Env, log *slog.Logger) (*Deps, error) {
	d := &Deps{Tools: tool.NewRegistry(), Metrics: telemetry.NewMetrics()}
	ok := false
	defer func() {
		if !ok {
			d.Close()
		}
	}()

	if env.DatabaseURL != "" {
		pg, err := pgstore.Open(ctx, env.DatabaseURL)
		if err != nil {
			return nil, err
		}
		d.closers = append(d.closers, pg.Close)
		if err := pg.Migrate(ctx); err != nil {
			return nil, err
		}
		d.Store = pg
		log.Info("store: postgres")
	} else {
		d.Store = memstore.New()
		log.Info("store: in-memory")
	}

	if env.RedisURL != "" {
		opt, err := redis.ParseURL(env.RedisURL)
		if err != nil {
			return nil, fmt.Errorf("REDIS_URL: %w", err)
		}
		rdb := redis.NewClient(opt)
		d.closers = append(d.closers, func() { _ = rdb.Close() })
		q, err := redisqueue.New(ctx, rdb, redisqueue.Options{MinIdle: env.ClaimIdle})
		if err != nil {
			return nil, err
		}
		d.Queue = q
		log.Info("queue: redis streams")
	} else {
		q := memqueue.New()
		d.closers = append(d.closers, func() { _ = q.Close() })
		d.Queue = q
		log.Info("queue: in-process")
	}

	switch env.LLM {
	case "fake":
		p := fake.New()
		if env.FakeFixtures != "" {
			var err error
			if p, err = fake.Load(env.FakeFixtures); err != nil {
				return nil, err
			}
		}
		d.LLM = p
		log.Info("llm: fake", "fixtures", env.FakeFixtures)
	case "anthropic":
		c, err := anthropic.New()
		if err != nil {
			return nil, fmt.Errorf("%w (or set AGENTFLOW_LLM=fake)", err)
		}
		d.LLM = c
		log.Info("llm: anthropic")
	default:
		return nil, fmt.Errorf("AGENTFLOW_LLM: unknown provider %q", env.LLM)
	}

	eng, err := engine.New(engine.Config{
		Store: d.Store, Queue: d.Queue, LLM: d.LLM, Tools: d.Tools, Metrics: d.Metrics, Logger: log,
	})
	if err != nil {
		return nil, err
	}
	d.Engine = eng
	ok = true
	return d, nil
}

// HeartbeatFor returns a delivery heartbeat comfortably below the claim idle.
func HeartbeatFor(env Env) time.Duration { return env.ClaimIdle / 3 }
