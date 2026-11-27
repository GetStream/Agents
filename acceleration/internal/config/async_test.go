package config

import (
	"os"
	"path/filepath"
	"time"
)

func (s *ConfigSuite) TestAsyncDefaultsByEnvironment() {
	s.Run("local defaults to in_process", func() {
		s.SetupTest()
		s.T().Setenv(EnvVar, Local)
		local, _, err := Load("")
		s.Require().NoError(err)
		s.Equal(AsyncBackendInProcess, local.Async.Backend)
		s.True(local.Async.Worker.Enabled)
		s.Equal([]string{"all"}, local.Async.Worker.Queues)
		s.Equal(8, local.Async.Worker.Concurrency)
		s.Equal(AsyncScopeBoth, local.Async.Scope)
	})

	s.Run("staging defaults to asynq", func() {
		s.SetupTest()
		s.T().Setenv(EnvVar, Staging)
		staging, _, err := Load("")
		s.Require().NoError(err)
		s.Equal(AsyncBackendAsynq, staging.Async.Backend)
		s.True(staging.Async.Worker.Enabled)
		s.True(staging.Async.Scheduler.Enabled)
		s.Equal(32, staging.Async.Worker.Concurrency)
	})

	s.Run("testing defaults to in_process with scheduler off", func() {
		s.SetupTest()
		s.T().Setenv(EnvVar, Testing)
		testingCfg, _, err := Load("")
		s.Require().NoError(err)
		s.Equal(AsyncBackendInProcess, testingCfg.Async.Backend)
		s.False(testingCfg.Async.Scheduler.Enabled)
	})
}

func (s *ConfigSuite) TestAsyncEnvOverrides() {
	s.T().Setenv("ROUTER_ASYNC_BACKEND", "asynq")
	s.T().Setenv("ROUTER_ASYNC_SCOPE", "shard")
	s.T().Setenv("ROUTER_ASYNC_WORKER_ENABLED", "true")
	s.T().Setenv("ROUTER_ASYNC_WORKER_QUEUES", "events:10,webhooks:4")
	s.T().Setenv("ROUTER_ASYNC_WORKER_CONCURRENCY", "16")
	s.T().Setenv("ROUTER_ASYNC_WORKER_QUEUE_CONCURRENCY", "knowledge-files=2,image-draw=3")
	s.T().Setenv("ROUTER_ASYNC_WORKER_STRICT_PRIORITY", "true")
	s.T().Setenv("ROUTER_ASYNC_WORKER_SHUTDOWN_TIMEOUT", "45s")
	s.T().Setenv("ROUTER_ASYNC_SCHEDULER_ENABLED", "false")

	cfg, _, err := Load("")
	s.Require().NoError(err)
	s.Equal(AsyncBackendAsynq, cfg.Async.Backend)
	s.Equal(AsyncScopeShard, cfg.Async.Scope)
	s.True(cfg.Async.Worker.Enabled)
	s.Equal([]string{"events:10", "webhooks:4"}, cfg.Async.Worker.Queues)
	s.Equal(16, cfg.Async.Worker.Concurrency)
	s.Equal(2, cfg.Async.Worker.QueueConcurrency["knowledge-files"])
	s.Equal(3, cfg.Async.Worker.QueueConcurrency["image-draw"])
	s.Equal(4, cfg.Async.Worker.QueueConcurrency["simulations"], "unspecified caps keep their default")
	s.True(cfg.Async.Worker.StrictPriority)
	s.Equal(45*time.Second, cfg.Async.Worker.ShutdownTimeout)
	s.False(cfg.Async.Scheduler.Enabled)
}

func (s *ConfigSuite) TestAsyncEmptyEnvVarsKeepDefaults() {
	s.T().Setenv("ROUTER_ENV", "local")
	s.T().Setenv("ROUTER_ASYNC_BACKEND", "")
	s.T().Setenv("ROUTER_ASYNC_WORKER_QUEUES", "")
	s.T().Setenv("ROUTER_ASYNC_SCHEDULER_ENABLED", "")

	cfg, _, err := Load("")
	s.Require().NoError(err)
	s.Equal(AsyncBackendInProcess, cfg.Async.Backend)
	s.Equal([]string{"all"}, cfg.Async.Worker.Queues)
	s.True(cfg.Async.Scheduler.Enabled)
}

func (s *ConfigSuite) TestAsyncValidationRefusesInvalidValues() {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "disabled backend is refused",
			yaml: "async:\n  backend: disabled\n",
			want: "async.backend",
		},
		{
			name: "unknown backend is refused",
			yaml: "async:\n  backend: celery\n",
			want: "async.backend",
		},
		{
			name: "unknown scope is refused",
			yaml: "async:\n  scope: regional\n",
			want: "async.scope",
		},
		{
			name: "unknown queue is refused",
			yaml: "async:\n  worker:\n    queues:\n      - bogus\n",
			want: "unknown queue",
		},
		{
			name: "non-positive weight is refused",
			yaml: "async:\n  worker:\n    queues:\n      - events:0\n",
			want: "positive integer",
		},
		{
			name: "non-positive concurrency is refused",
			yaml: "async:\n  worker:\n    concurrency: 0\n",
			want: "async.worker.concurrency",
		},
		{
			name: "unknown queue_concurrency key is refused",
			yaml: "async:\n  worker:\n    queue_concurrency:\n      bogus: 2\n",
			want: "queue_concurrency",
		},
		{
			name: "non-positive queue_concurrency value is refused",
			yaml: "async:\n  worker:\n    queue_concurrency:\n      events: 0\n",
			want: "queue_concurrency.events",
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.SetupTest()
			path := filepath.Join(s.T().TempDir(), "router.yaml")
			s.Require().NoError(os.WriteFile(path, []byte(tc.yaml), 0o600))
			_, _, err := Load(path)
			s.Require().Error(err)
			s.ErrorContains(err, tc.want)
		})
	}
}
