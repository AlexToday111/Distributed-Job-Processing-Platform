package observability

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type Metrics struct {
	Registry               *prometheus.Registry
	Events                 *prometheus.CounterVec
	HTTP                   *prometheus.CounterVec
	HTTPDuration           *prometheus.HistogramVec
	GRPC                   *prometheus.CounterVec
	Execution, Wait, Claim prometheus.Histogram
	Active                 prometheus.Gauge
	State                  *prometheus.GaugeVec
	Workers                *prometheus.GaugeVec
	Counters               map[string]prometheus.Counter
	Gauges                 map[string]prometheus.Gauge
}

func NewMetrics() *Metrics {
	m := &Metrics{Registry: prometheus.NewRegistry(), Events: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "job_events_total", Help: "Job lifecycle events."}, []string{"event"}), HTTP: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "http_requests_total", Help: "HTTP requests by matched route and status."}, []string{"route", "status"}), HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "http_request_duration_seconds", Help: "HTTP latency."}, []string{"route"}), GRPC: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "grpc_requests_total", Help: "RPC outcomes."}, []string{"method", "code"}), Execution: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "job_execution_duration_seconds", Help: "Handler execution latency."}), Wait: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "job_queue_wait_duration_seconds", Help: "Time from scheduled eligibility to dispatch."}), Claim: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "scheduler_claim_duration_seconds", Help: "Claim transaction latency."}), Active: prometheus.NewGauge(prometheus.GaugeOpts{Name: "worker_active_jobs", Help: "Accepted unfinished jobs including queued work."}), State: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "jobs", Help: "Durable job counts by state."}, []string{"status"}), Workers: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "workers", Help: "Durable registered and healthy workers."}, []string{"state"})}
	m.Registry.MustRegister(m.Events, m.HTTP, m.HTTPDuration, m.GRPC, m.Execution, m.Wait, m.Claim, m.Active, m.State, m.Workers, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m.Counters = map[string]prometheus.Counter{}
	for event, name := range map[string]string{"created": "jobs_created_total", "started": "jobs_started_total", "succeeded": "jobs_succeeded_total", "failed": "jobs_failed_total", "retried": "jobs_retried_total", "dead_lettered": "jobs_dead_lettered_total", "cancelled": "jobs_cancelled_total", "claimed": "scheduler_claimed_jobs_total", "heartbeat": "worker_heartbeat_total", "heartbeat_failure": "worker_heartbeat_failures_total"} {
		counter := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: "Observed " + event + " events in this process."})
		m.Counters[event] = counter
		m.Registry.MustRegister(counter)
	}
	m.Gauges = map[string]prometheus.Gauge{}
	for _, name := range []string{"jobs_ready", "jobs_running", "jobs_retry_wait", "workers_registered", "workers_healthy", "workers_active"} {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: "Durable snapshot, refreshed by scheduler."})
		m.Gauges[name] = g
		m.Registry.MustRegister(g)
	}
	return m
}
func (m *Metrics) Event(event string, n float64) {
	m.Events.WithLabelValues(event).Add(n)
	if counter := m.Counters[event]; counter != nil {
		counter.Add(n)
	}
}
func (m *Metrics) SetState(state string, n float64) {
	m.State.WithLabelValues(state).Set(n)
	if g := m.Gauges["jobs_"+strings.ToLower(state)]; g != nil {
		g.Set(n)
	}
}
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
func Logger(level string) *slog.Logger {
	var l slog.Level
	if e := l.UnmarshalText([]byte(level)); e != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}
func Tracing(ctx context.Context, name, endpoint string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	exporter, e := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint), otlptracehttp.WithURLPath("/v1/traces"), otlptracehttp.WithTimeout(3*time.Second))
	if e != nil {
		return nil, e
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", name))), sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())))
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}
