// Package telemetry wires Prometheus metrics and OpenTelemetry tracing.
package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// Metrics holds the engine's Prometheus collectors. A nil *Metrics is valid
// and records nothing.
type Metrics struct {
	Registry     *prometheus.Registry
	steps        *prometheus.CounterVec
	stepDuration *prometheus.HistogramVec
	retries      *prometheus.CounterVec
	runs         *prometheus.CounterVec
	tokens       *prometheus.CounterVec
	inflight     prometheus.Gauge
}

// NewMetrics registers all collectors on a fresh registry.
func NewMetrics() *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		steps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "agentflow_steps_total", Help: "Finished step executions by kind and status.",
		}, []string{"kind", "status"}),
		stepDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "agentflow_step_duration_seconds",
			Help:    "Wall time of a single step attempt.",
			Buckets: prometheus.ExponentialBuckets(0.001, 4, 10),
		}, []string{"kind"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "agentflow_step_retries_total", Help: "Step attempts that failed and were retried.",
		}, []string{"kind"}),
		runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "agentflow_runs_total", Help: "Runs that reached a terminal status.",
		}, []string{"status"}),
		tokens: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "agentflow_llm_tokens_total", Help: "LLM tokens consumed by model and direction.",
		}, []string{"model", "direction"}),
		inflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "agentflow_worker_inflight_steps", Help: "Steps currently executing in this process.",
		}),
	}
	m.Registry.MustRegister(m.steps, m.stepDuration, m.retries, m.runs, m.tokens, m.inflight,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// Handler serves the registry in the Prometheus exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Registry: m.Registry})
}

// StepFinished records one step attempt.
func (m *Metrics) StepFinished(kind, status string, d time.Duration) {
	if m == nil {
		return
	}
	m.steps.WithLabelValues(kind, status).Inc()
	m.stepDuration.WithLabelValues(kind).Observe(d.Seconds())
}

// StepRetried records a retry.
func (m *Metrics) StepRetried(kind string) {
	if m == nil {
		return
	}
	m.retries.WithLabelValues(kind).Inc()
}

// RunFinished records a terminal run.
func (m *Metrics) RunFinished(status string) {
	if m == nil {
		return
	}
	m.runs.WithLabelValues(status).Inc()
}

// Tokens records LLM token usage.
func (m *Metrics) Tokens(model string, in, out int) {
	if m == nil {
		return
	}
	m.tokens.WithLabelValues(model, "input").Add(float64(in))
	m.tokens.WithLabelValues(model, "output").Add(float64(out))
}

// Inflight adjusts the in-flight step gauge.
func (m *Metrics) Inflight(delta float64) {
	if m == nil {
		return
	}
	m.inflight.Add(delta)
}

// SetupTracing installs a global tracer provider and W3C propagator. When
// OTEL_EXPORTER_OTLP_ENDPOINT is unset, spans are created (so trace context
// still propagates through the queue) but not exported. The returned function
// flushes and shuts the provider down.
func SetupTracing(ctx context.Context, service string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	res, err := resource.Merge(resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(service)))
	if err != nil {
		return nil, fmt.Errorf("telemetry: resource: %w", err)
	}
	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" {
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("telemetry: otlp exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
