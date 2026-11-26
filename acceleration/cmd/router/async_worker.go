package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/GetStream/Vision-Agents/acceleration/internal/api"
	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/channelbridge"
	bridgetasks "github.com/GetStream/Vision-Agents/acceleration/internal/channelbridge/tasks"
	chatinboundtasks "github.com/GetStream/Vision-Agents/acceleration/internal/chatinbound/tasks"
	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/eventforward"
	eftasks "github.com/GetStream/Vision-Agents/acceleration/internal/eventforward/tasks"
	"github.com/GetStream/Vision-Agents/acceleration/internal/imagejobs"
	imgtasks "github.com/GetStream/Vision-Agents/acceleration/internal/imagejobs/tasks"
	"github.com/GetStream/Vision-Agents/acceleration/internal/knowledge/files"
	filestasks "github.com/GetStream/Vision-Agents/acceleration/internal/knowledge/files/tasks"
	"github.com/GetStream/Vision-Agents/acceleration/internal/knowledge/urls"
	urlstasks "github.com/GetStream/Vision-Agents/acceleration/internal/knowledge/urls/tasks"
	"github.com/GetStream/Vision-Agents/acceleration/internal/limitnotice"
	lntasks "github.com/GetStream/Vision-Agents/acceleration/internal/limitnotice/tasks"
	"github.com/GetStream/Vision-Agents/acceleration/internal/live"
	mainttasks "github.com/GetStream/Vision-Agents/acceleration/internal/maintenance/tasks"
	"github.com/GetStream/Vision-Agents/acceleration/internal/mcpevents"
	mcptasks "github.com/GetStream/Vision-Agents/acceleration/internal/mcpevents/tasks"
	"github.com/GetStream/Vision-Agents/acceleration/internal/omnichannel"
	omnitasks "github.com/GetStream/Vision-Agents/acceleration/internal/omnichannel/tasks"
	"github.com/GetStream/Vision-Agents/acceleration/internal/simulation"
	simtasks "github.com/GetStream/Vision-Agents/acceleration/internal/simulation/tasks"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// runAsyncWorker runs the router in dedicated background worker mode (`router async-worker`):
// it processes configured async queues (and periodic schedules when enabled) without serving
// the public API, peer gRPC, or live telephony/WebRTC surfaces.
func runAsyncWorker(args []string, settings config.Config, logger *slog.Logger) error {
	fs := flag.NewFlagSet("async-worker", flag.ContinueOnError)
	queuesFlag := fs.String("queues", "", "comma-separated queue names to process (overrides async.worker.queues)")
	concurrencyFlag := fs.Int("concurrency", 0, "worker concurrency (overrides async.worker.concurrency)")
	schedulerFlag := fs.String("scheduler", "", "enable periodic scheduler: true or false (overrides async.scheduler.enabled)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	settings.Async.Worker.Enabled = true
	if *queuesFlag != "" {
		var qs []string
		for _, part := range strings.Split(*queuesFlag, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				qs = append(qs, trimmed)
			}
		}
		settings.Async.Worker.Queues = qs
	}
	if *concurrencyFlag > 0 {
		settings.Async.Worker.Concurrency = *concurrencyFlag
	}
	if *schedulerFlag != "" {
		switch strings.ToLower(strings.TrimSpace(*schedulerFlag)) {
		case "true", "1", "yes":
			settings.Async.Scheduler.Enabled = true
		case "false", "0", "no":
			settings.Async.Scheduler.Enabled = false
		default:
			return fmt.Errorf("async-worker: invalid --scheduler value %q (want true or false)", *schedulerFlag)
		}
	}

	return runProcess(settings, logger, true)
}

func buildAsyncRuntime(settings config.Config, liveClient *live.Client, reg *async.Registry, logger *slog.Logger) (*async.Runtime, error) {
	var globalOpt, shardOpt asynq.RedisConnOpt
	if liveClient != nil {
		globalOpt = async.QueueRedis(settings.Redis, liveClient.Redis())
		shardOpt = async.QueueRedis(async.ShardRedis(settings.Redis), liveClient.ShardRedis())
	}
	return async.New(async.RuntimeOptions{
		Config:      settings.Async,
		Registry:    reg,
		GlobalRedis: globalOpt,
		ShardRedis:  shardOpt,
		Registerer:  prometheus.DefaultRegisterer,
		Logger:      logger,
	})
}

type asyncTaskDeps struct {
	pgStore    *store.Store
	retention  time.Duration
	server     *api.Server
	pages      *urls.Service
	uploads    *files.Service
	bridge     *channelbridge.Bridge
	mcpEvents  *mcpevents.Service
	forwarder  *eventforward.Forwarder
	drawings   *imagejobs.Service
	sender     *limitnotice.Sender
	anomalies  *limitnotice.Anomalies
	closer     *omnichannel.Closer
	simulations *simulation.Runner
	logger     *slog.Logger
}

func registerAsyncTasks(reg *async.Registry, deps asyncTaskDeps) {
	if deps.server != nil {
		chatinboundtasks.Register(reg, chatinboundtasks.Deps{Dispatcher: deps.server})
	}
	if deps.pages != nil {
		_ = urlstasks.Register(reg, urlstasks.Deps{Indexer: deps.pages, Refresher: deps.pages})
	}
	if deps.uploads != nil {
		filestasks.Register(reg, filestasks.Deps{Reader: deps.uploads})
	}
	if deps.bridge != nil {
		bridgetasks.Register(reg, bridgetasks.Deps{Sender: deps.bridge})
	}
	if deps.mcpEvents != nil {
		mcptasks.Register(reg, mcptasks.Deps{Runner: deps.mcpEvents})
	}
	if deps.forwarder != nil {
		eftasks.Register(reg, eftasks.Deps{Processor: deps.forwarder})
	}
	if deps.drawings != nil {
		imgtasks.Register(reg, imgtasks.Deps{Processor: deps.drawings})
	}
	if deps.sender != nil || deps.anomalies != nil {
		var sender lntasks.Sender
		if deps.sender != nil {
			sender = deps.sender
		}
		var checker lntasks.AnomalyChecker
		if deps.anomalies != nil {
			checker = deps.anomalies
		}
		lntasks.Register(reg, lntasks.Deps{Sender: sender, Checker: checker})
	}
	if deps.closer != nil {
		omnitasks.Register(reg, omnitasks.Deps{Closer: deps.closer})
	}
	if deps.simulations != nil {
		simtasks.Register(reg, simtasks.Deps{Executor: deps.simulations})
	}
	if deps.pgStore != nil {
		mainttasks.Register(reg, mainttasks.Deps{
			PruneDataChanges: func(ctx context.Context) error {
				return pruneDataChangesOnce(ctx, deps.pgStore, deps.retention, deps.logger)
			},
			PruneScheduledTasks: func(ctx context.Context) error {
				return pruneScheduledTasksOnce(ctx, deps.pgStore, deps.logger)
			},
			RedactToolOutput: func(ctx context.Context) error {
				return redactToolOutputOnce(ctx, deps.pgStore, deps.logger)
			},
		})
	}
}

func serveWorkerHealth(addr string, rt *async.Runtime, settings config.Config, logger *slog.Logger) (*http.Server, chan error, error) {
	mux := http.NewServeMux()
	healthHandler := func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		err := rt.Healthy(ctx)
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"status":      "ok",
			"role":        "async-worker",
			"backend":     settings.Async.Backend,
			"queues":      settings.Async.Worker.Queues,
			"concurrency": settings.Async.Worker.Concurrency,
			"scheduler":   settings.Async.Scheduler.Enabled,
			"running":     rt.Running(),
		}
		if err != nil {
			resp["status"] = "unhealthy"
			resp["error"] = err.Error()
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		_ = json.NewEncoder(w).Encode(resp)
	}
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/healthz", healthHandler)
	mux.HandleFunc("/readyz", healthHandler)

	listener, err := listen(addr, settings.ListenFD, "listen_fd")
	if err != nil {
		return nil, nil, err
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: readHeaderTimeout, IdleTimeout: idleTimeout}
	done := make(chan error, 1)
	go func() {
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			done <- err
			return
		}
		done <- nil
	}()
	logger.Info("async-worker health listener ready",
		"address", listener.Addr().String(),
		"backend", settings.Async.Backend,
		"queues", settings.Async.Worker.Queues,
		"concurrency", settings.Async.Worker.Concurrency,
		"scheduler", settings.Async.Scheduler.Enabled,
	)
	return srv, done, nil
}

func pruneDataChangesOnce(ctx context.Context, pgStore *store.Store, retention time.Duration, logger *slog.Logger) error {
	var firstErr error
	removed, err := pgStore.PruneDataChanges(ctx, retention)
	if err != nil {
		logger.Error("could not prune recorded changes", "error", err)
		firstErr = err
	} else if removed > 0 {
		logger.Debug("pruned recorded changes", "rows", removed)
	}
	if removed, err := pgStore.LimitGrantUses.Prune(ctx); err != nil {
		logger.Error("could not prune the uses of ended grants", "error", err)
		if firstErr == nil {
			firstErr = err
		}
	} else if removed > 0 {
		logger.Debug("pruned the uses of ended grants", "rows", removed)
	}
	return firstErr
}

func pruneScheduledTasksOnce(ctx context.Context, pgStore *store.Store, logger *slog.Logger) error {
	runs, tasks, err := pgStore.ScheduledTasks.Prune(ctx)
	if err != nil {
		logger.Error("could not prune scheduled tasks", "error", err)
		return err
	}
	if runs > 0 || tasks > 0 {
		logger.Info("pruned scheduled tasks", "runs", runs, "tasks", tasks)
	}
	return nil
}

func redactToolOutputOnce(ctx context.Context, pgStore *store.Store, logger *slog.Logger) error {
	var redacted int64
	for range redactBatches {
		rows, err := pgStore.ResponseItems.Redact(ctx, time.Now().UTC(), redactBatch)
		if err != nil {
			logger.Error("could not redact tool output", "error", err)
			return err
		}
		redacted += rows
		if rows < redactBatch {
			break
		}
	}
	if redacted > 0 {
		logger.Info("redacted tool output", "rows", redacted, "ceiling", redactBatch*redactBatches)
	}
	return nil
}
