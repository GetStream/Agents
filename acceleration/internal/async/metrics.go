package async

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/GetStream/Vision-Agents/acceleration/internal/tracing"
)

// Metrics holds the Prometheus counters, histograms, and gauges emitted by async workers.
type Metrics struct {
	enqueued *prometheus.CounterVec
	handled  *prometheus.CounterVec
	duration *prometheus.HistogramVec
	inFlight *prometheus.GaugeVec
}

var (
	defaultMetricsOnce sync.Once
	defaultMetricsInst *Metrics
)

// DefaultMetrics returns the process-wide Metrics registered on prometheus.DefaultRegisterer.
func DefaultMetrics() *Metrics {
	defaultMetricsOnce.Do(func() {
		defaultMetricsInst = NewMetrics(prometheus.DefaultRegisterer)
	})
	return defaultMetricsInst
}

// NewMetrics builds and registers Metrics on reg (nil skips registration, useful in tests).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		enqueued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "router_async_tasks_enqueued_total",
			Help: "Total number of async tasks enqueued.",
		}, []string{"queue", "task_type", "scope"}),
		handled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "router_async_tasks_total",
			Help: "Total number of async task attempts completed, by outcome (ok, retry, failed, skipped_retry).",
		}, []string{"queue", "task_type", "scope", "outcome"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "router_async_task_duration_seconds",
			Help:    "Execution duration of async task attempts in seconds.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 1800},
		}, []string{"queue", "task_type", "scope"}),
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "router_async_tasks_in_flight",
			Help: "Number of async tasks currently executing.",
		}, []string{"queue", "scope"}),
	}
	if reg != nil {
		for _, c := range []prometheus.Collector{m.enqueued, m.handled, m.duration, m.inFlight} {
			if err := reg.Register(c); err != nil {
				var already prometheus.AlreadyRegisteredError
				if !isAlreadyRegistered(err, &already) {
					panic(err)
				}
			}
		}
	}
	return m
}

func isAlreadyRegistered(err error, target *prometheus.AlreadyRegisteredError) bool {
	if ae, ok := err.(prometheus.AlreadyRegisteredError); ok {
		*target = ae
		return true
	}
	return false
}

// ObserveEnqueue increments the enqueued counter.
func (m *Metrics) ObserveEnqueue(queue Queue, taskType string, scope Scope) {
	if m == nil {
		return
	}
	m.enqueued.WithLabelValues(string(queue), taskType, string(scope)).Inc()
}

// execute runs handler with logging, tracing, Prometheus metrics, panic recovery, and
// Sentry reporting on terminal failures.
func execute(
	ctx context.Context,
	handler RawHandler,
	payload []byte,
	meta Meta,
	logger *slog.Logger,
	metrics *Metrics,
) (err error) {
	if logger == nil {
		logger = slog.Default()
	}
	ctx = WithMeta(ctx, meta)
	ctx, span := tracing.Tracer("async").Start(ctx, "async.task "+meta.TaskType,
		trace.WithAttributes(
			attribute.String("async.task_type", meta.TaskType),
			attribute.String("async.queue", string(meta.Queue)),
			attribute.String("async.scope", string(meta.Scope)),
			attribute.String("async.task_id", meta.TaskID),
			attribute.Int("async.attempt", meta.Retried+1),
		),
	)
	defer span.End()

	if metrics != nil {
		metrics.inFlight.WithLabelValues(string(meta.Queue), string(meta.Scope)).Inc()
		defer metrics.inFlight.WithLabelValues(string(meta.Queue), string(meta.Scope)).Dec()
	}

	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("async: panic in %s: %v", meta.TaskType, r)
			logger.Error("async task panicked",
				"task_type", meta.TaskType,
				"queue", meta.Queue,
				"scope", meta.Scope,
				"task_id", meta.TaskID,
				"attempt", meta.Retried+1,
				"panic", r,
				"stack", string(debug.Stack()),
			)
		}
		elapsed := time.Since(start)
		if metrics != nil {
			metrics.duration.WithLabelValues(string(meta.Queue), meta.TaskType, string(meta.Scope)).Observe(elapsed.Seconds())
		}
		outcome := "ok"
		if err != nil {
			switch {
			case IsSkipRetry(err):
				outcome = "skipped_retry"
			case meta.Retried >= meta.MaxRetry:
				outcome = "failed"
			default:
				outcome = "retry"
			}
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			if outcome == "failed" || outcome == "skipped_retry" {
				logger.Error("async task failed permanently",
					"task_type", meta.TaskType,
					"queue", meta.Queue,
					"scope", meta.Scope,
					"task_id", meta.TaskID,
					"attempt", meta.Retried+1,
					"max_retry", meta.MaxRetry,
					"duration", elapsed,
					"error", err,
				)
				sentry.WithScope(func(s *sentry.Scope) {
					s.SetTag("async.task_type", meta.TaskType)
					s.SetTag("async.queue", string(meta.Queue))
					s.SetTag("async.scope", string(meta.Scope))
					s.SetContext("async", sentry.Context{
						"task_id": meta.TaskID,
						"attempt": meta.Retried + 1,
					})
					sentry.CaptureException(err)
				})
			} else {
				logger.Warn("async task failed, will retry",
					"task_type", meta.TaskType,
					"queue", meta.Queue,
					"scope", meta.Scope,
					"task_id", meta.TaskID,
					"attempt", meta.Retried+1,
					"max_retry", meta.MaxRetry,
					"duration", elapsed,
					"error", err,
				)
			}
		} else {
			logger.Debug("async task completed",
				"task_type", meta.TaskType,
				"queue", meta.Queue,
				"scope", meta.Scope,
				"task_id", meta.TaskID,
				"attempt", meta.Retried+1,
				"duration", elapsed,
			)
		}
		if metrics != nil {
			metrics.handled.WithLabelValues(string(meta.Queue), meta.TaskType, string(meta.Scope), outcome).Inc()
		}
	}()

	return handler(ctx, payload, meta)
}

// QueueDepth describes point-in-time counts for one queue in one scope.
type QueueDepth struct {
	Queue     Queue
	Scope     Scope
	Pending   int
	Active    int
	Retry     int
	Scheduled int
	Archived  int
}

// DepthProvider reports current queue depths for Prometheus scraping.
type DepthProvider interface {
	QueueDepths() []QueueDepth
}

// QueueDepthCollector exposes router_async_queue_depth{queue, scope, state} on /metrics.
type QueueDepthCollector struct {
	provider DepthProvider
	desc     *prometheus.Desc
}

// NewQueueDepthCollector creates a Prometheus collector backed by provider.
func NewQueueDepthCollector(provider DepthProvider) *QueueDepthCollector {
	return &QueueDepthCollector{
		provider: provider,
		desc: prometheus.NewDesc(
			"router_async_queue_depth",
			"Current number of async tasks in each queue and state.",
			[]string{"queue", "scope", "state"},
			nil,
		),
	}
}

func (c *QueueDepthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *QueueDepthCollector) Collect(ch chan<- prometheus.Metric) {
	if c.provider == nil {
		return
	}
	for _, d := range c.provider.QueueDepths() {
		q := string(d.Queue)
		s := string(d.Scope)
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(d.Pending), q, s, "pending")
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(d.Active), q, s, "active")
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(d.Retry), q, s, "retry")
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(d.Scheduled), q, s, "scheduled")
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(d.Archived), q, s, "archived")
	}
}
