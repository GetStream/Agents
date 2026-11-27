package async

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
)

// RuntimeOptions wires a Runtime from config.Async.
type RuntimeOptions struct {
	Config        config.Async
	Registry      *Registry
	GlobalRedis   asynq.RedisConnOpt
	ShardRedis    asynq.RedisConnOpt
	QueuePrefix   string
	CheckInterval time.Duration
	Metrics       *Metrics
	Registerer    prometheus.Registerer
	Logger        *slog.Logger
}

type backend interface {
	Dispatcher
	DepthProvider
	Start(ctx context.Context) error
	StopClaiming()
	Running() int
	Healthy(ctx context.Context) error
}

// Runtime owns the configured async backend (asynq or in_process), task registry,
// Prometheus queue-depth collector, and worker lifecycle.
type Runtime struct {
	cfg        config.Async
	registry   *Registry
	backend    backend
	collector  *QueueDepthCollector
	registerer prometheus.Registerer
}

// New builds a Runtime from opts.Config.Backend ("asynq" or "in_process").
func New(opts RuntimeOptions) (*Runtime, error) {
	if opts.Registry == nil {
		opts.Registry = NewRegistry()
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Metrics == nil {
		opts.Metrics = DefaultMetrics()
	}
	var b backend
	var err error
	switch opts.Config.Backend {
	case "", config.AsyncBackendAsynq:
		b, err = NewAsynqBackend(AsynqOptions{
			Config:        opts.Config,
			Registry:      opts.Registry,
			GlobalRedis:   opts.GlobalRedis,
			ShardRedis:    opts.ShardRedis,
			QueuePrefix:   opts.QueuePrefix,
			CheckInterval: opts.CheckInterval,
			Metrics:       opts.Metrics,
			Logger:        opts.Logger,
		})
	case config.AsyncBackendInProcess:
		b, err = NewInProcBackend(InProcOptions{
			Config:   opts.Config,
			Registry: opts.Registry,
			Metrics:  opts.Metrics,
			Logger:   opts.Logger,
		})
	default:
		return nil, fmt.Errorf("async: unsupported backend %q (want %s or %s)",
			opts.Config.Backend, config.AsyncBackendAsynq, config.AsyncBackendInProcess)
	}
	if err != nil {
		return nil, err
	}

	rt := &Runtime{
		cfg:        opts.Config,
		registry:   opts.Registry,
		backend:    b,
		collector:  NewQueueDepthCollector(b),
		registerer: opts.Registerer,
	}
	if rt.registerer != nil {
		_ = rt.registerer.Register(rt.collector)
	}
	return rt, nil
}

// Dispatcher returns the Dispatcher to inject into services that enqueue tasks.
func (r *Runtime) Dispatcher() Dispatcher { return r.backend }

// Registry returns the task registry for registering handlers before Start.
func (r *Runtime) Registry() *Registry { return r.registry }

// Backend returns the active backend name ("asynq" or "in_process").
func (r *Runtime) Backend() string { return r.cfg.Backend }

// Start begins worker pools and periodic schedulers.
func (r *Runtime) Start(ctx context.Context) error { return r.backend.Start(ctx) }

// StopClaiming stops pulling new tasks during graceful shutdown drain.
func (r *Runtime) StopClaiming() { r.backend.StopClaiming() }

// Running returns the number of tasks currently executing in this process.
func (r *Runtime) Running() int {
	if r == nil || r.backend == nil {
		return 0
	}
	return r.backend.Running()
}

// Healthy reports whether the async backend is ready to accept/process tasks.
func (r *Runtime) Healthy(ctx context.Context) error {
	if r == nil || r.backend == nil {
		return nil
	}
	return r.backend.Healthy(ctx)
}

// Close shuts down workers, unregisters the depth collector, and closes backend clients.
func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	if r.registerer != nil && r.collector != nil {
		r.registerer.Unregister(r.collector)
	}
	if r.backend == nil {
		return nil
	}
	return r.backend.Close()
}
