package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
)

func TestArgumentsAsyncWorkerSubcommand(t *testing.T) {
	path, cmd, rest := arguments([]string{"--config", "local.yaml", "async-worker", "--queues", "webhooks,maintenance", "--concurrency", "12"})
	assert.Equal(t, "local.yaml", path)
	assert.Equal(t, "async-worker", cmd)
	assert.Equal(t, []string{"--queues", "webhooks,maintenance", "--concurrency", "12"}, rest)
}

func TestWorkerHealthEndpoint(t *testing.T) {
	var cfg config.Config
	cfg.Async.Backend = config.AsyncBackendInProcess
	cfg.Async.Worker.Enabled = true
	cfg.Async.Worker.Queues = []string{"webhooks", "maintenance"}
	cfg.Async.Worker.Concurrency = 4
	cfg.Async.Scheduler.Enabled = false

	reg := async.NewRegistry()
	rt, err := buildAsyncRuntime(cfg, nil, reg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.NoError(t, rt.Start(context.Background()))
	defer rt.Close()

	srv, done, err := serveWorkerHealth("127.0.0.1:0", rt, cfg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-done
	}()

	// Probe the handler directly through the server's mux as well as checking it listens.
	req, err := http.NewRequest(http.MethodGet, "http://example.invalid/health", nil)
	require.NoError(t, err)
	rw := newRecorder()
	srv.Handler.ServeHTTP(rw, req)
	require.Equal(t, http.StatusOK, rw.code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rw.body, &body))
	assert.Equal(t, "ok", body["status"])
	assert.Equal(t, "async-worker", body["role"])
	assert.Equal(t, "in_process", body["backend"])
}

type simpleRecorder struct {
	header http.Header
	code   int
	body   []byte
}

func newRecorder() *simpleRecorder {
	return &simpleRecorder{header: make(http.Header), code: http.StatusOK}
}

func (r *simpleRecorder) Header() http.Header         { return r.header }
func (r *simpleRecorder) WriteHeader(statusCode int)  { r.code = statusCode }
func (r *simpleRecorder) Write(p []byte) (int, error) { r.body = append(r.body, p...); return len(p), nil }
