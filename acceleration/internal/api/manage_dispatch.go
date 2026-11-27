package api

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/node"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

const (
	// internalTaskEnqueuePath is the internal-only management RPC for enqueueing a
	// shard-scoped or global async task on the receiving shard's asynq broker.
	internalTaskEnqueuePath = "/v1/manage/internal/tasks/enqueue"
	// internalEventDispatchPath is the internal-only management RPC alias specifically
	// for event dispatching (e.g. Stream Chat message.new or cross-shard events).
	internalEventDispatchPath = "/v1/manage/internal/events/dispatch"

	internalEnqueueBodyLimit = 2 << 20
	internalRPCTimeout       = 10 * time.Second
)

type internalEnqueueRequest struct {
	TaskType    string          `json:"task_type"`
	Queue       async.Queue     `json:"queue"`
	Scope       async.Scope     `json:"scope"`
	TaskID      string          `json:"task_id,omitempty"`
	MaxRetry    *int            `json:"max_retry,omitempty"`
	TimeoutMs   int64           `json:"timeout_ms,omitempty"`
	ProcessInMs int64           `json:"process_in_ms,omitempty"`
	UniqueMs    int64           `json:"unique_ms,omitempty"`
	Payload     json.RawMessage `json:"payload"`
}

type internalEnqueueResponse struct {
	ID        string          `json:"id,omitempty"`
	Queue     async.Queue     `json:"queue"`
	State     async.TaskState `json:"state,omitempty"`
	Duplicate bool            `json:"duplicate,omitempty"`
}

// authorizedInternalRPC reports whether r carries internal management credentials:
// either a keyring-authenticated peer gRPC forward (node.Forwarded) or X-Manage-Key
// matching s.manageKey (same isolation as /v1/manage/price-lists). Customer auth modes
// (api_key, proxy) are never accepted.
func (s *Server) authorizedInternalRPC(r *http.Request) bool {
	if node.Forwarded(r.Context()) {
		return true
	}
	if s.manageKey != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get(manageKeyHeader)), []byte(s.manageKey)) == 1 {
		return true
	}
	return false
}

// handleInternalEnqueue receives an internal-only management RPC and enqueues the task
// into THIS shard's async dispatcher (asynq Redis or in_process). Never exposed in public
// OpenAPI specs or customer-facing proxy allow-lists.
func (s *Server) handleInternalEnqueue(w http.ResponseWriter, r *http.Request) {
	if !s.authorizedInternalRPC(r) {
		if s.manageKey == "" {
			writeError(w, notFound("no such route"))
			return
		}
		writeError(w, unauthenticated("an internal management key is required"))
		return
	}
	if s.async == nil {
		writeError(w, unavailable("async dispatcher is not configured"))
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, internalEnqueueBodyLimit))
	if err != nil {
		writeError(w, invalidRequest("could not read enqueue request"))
		return
	}
	var req internalEnqueueRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, invalidRequest("invalid enqueue payload"))
		return
	}
	if req.TaskType == "" {
		writeError(w, invalidRequest("task_type is required"))
		return
	}
	if req.Queue == "" {
		req.Queue = async.QueueEvents
	}
	if !async.ValidQueue(string(req.Queue)) {
		writeError(w, invalidRequest(fmt.Sprintf("unknown queue %q", req.Queue)))
		return
	}
	if req.Scope == "" {
		req.Scope = async.ScopeShard
	}
	if req.Scope != async.ScopeShard && req.Scope != async.ScopeGlobal {
		writeError(w, invalidRequest(fmt.Sprintf("invalid scope %q", req.Scope)))
		return
	}

	opts := async.EnqueueOptions{
		Queue:    req.Queue,
		TaskID:   req.TaskID,
		MaxRetry: 3,
		Timeout:  2 * time.Minute,
	}
	if req.MaxRetry != nil {
		opts.MaxRetry = *req.MaxRetry
		opts.HasRetry = true
	}
	if req.TimeoutMs > 0 {
		opts.Timeout = time.Duration(req.TimeoutMs) * time.Millisecond
	}
	if req.ProcessInMs > 0 {
		opts.ProcessIn = time.Duration(req.ProcessInMs) * time.Millisecond
	}
	if req.UniqueMs > 0 {
		opts.UniqueTTL = time.Duration(req.UniqueMs) * time.Millisecond
	}

	info, err := s.async.EnqueueRaw(r.Context(), req.TaskType, req.Payload, req.Scope, opts)
	if errors.Is(err, async.ErrTaskIDConflict) || errors.Is(err, async.ErrDuplicateTask) {
		writeJSON(w, internalEnqueueResponse{
			ID:        req.TaskID,
			Queue:     req.Queue,
			Duplicate: true,
		})
		return
	}
	if err != nil {
		s.logger.ErrorContext(r.Context(), "internal async enqueue failed",
			"task_type", req.TaskType, "queue", req.Queue, "error", err)
		writeError(w, unavailable("could not enqueue task"))
		return
	}
	writeJSON(w, internalEnqueueResponse{
		ID:    info.ID,
		Queue: info.Queue,
		State: info.State,
	})
}

// DispatchToShard enqueues task onto targetShard's asynq broker: directly via s.async
// when targetShard is empty or matches this shard, or via the internal management RPC on
// a router in targetShard when it belongs to another shard.
func (s *Server) DispatchToShard(ctx context.Context, place placement, targetNode string, work string, task async.Task, opts ...async.Option) error {
	if s.async == nil {
		return stack.Wrap(errors.New("api: async dispatcher is not configured"))
	}
	here := ""
	if s.directory != nil {
		here = s.directory.Shard()
	}

	// Fast path: local shard (or single-shard deployment).
	if len(place.shards) == 0 || (len(place.shards) == 1 && (place.shards[0] == "" || place.shards[0] == here)) {
		return enqueueIdempotent(ctx, s.async, task, opts...)
	}

	// Remote shard: serialize once and send via internal management RPC.
	resolved := async.ResolveOptions(task, opts...)
	payload, err := json.Marshal(task)
	if err != nil {
		return stack.Wrap(fmt.Errorf("api: marshal task %s: %w", task.TaskType(), err))
	}
	maxRetry := resolved.MaxRetry
	wire := internalEnqueueRequest{
		TaskType:    task.TaskType(),
		Queue:       resolved.Queue,
		Scope:       task.Scope(),
		TaskID:      resolved.TaskID,
		MaxRetry:    &maxRetry,
		TimeoutMs:   resolved.Timeout.Milliseconds(),
		ProcessInMs: resolved.ProcessIn.Milliseconds(),
		UniqueMs:    resolved.UniqueTTL.Milliseconds(),
		Payload:     payload,
	}
	body, err := json.Marshal(wire)
	if err != nil {
		return stack.Wrap(err)
	}

	path := internalTaskEnqueuePath
	if resolved.Queue == async.QueueEvents {
		path = internalEventDispatchPath
	}

	for _, shard := range place.shards {
		if shard == "" || shard == here {
			return enqueueIdempotent(ctx, s.async, task, opts...)
		}
		if s.directory == nil || s.forwarder == nil {
			if place.strict {
				return errResidencyDown
			}
			return enqueueIdempotent(ctx, s.async, task, opts...)
		}
		address := targetNode
		if address == "" {
			var lookupErr error
			address, lookupErr = s.directory.NodeInShard(ctx, shard)
			if lookupErr != nil || address == "" {
				if !place.strict {
					s.doneHere(shard, work, address, lookupErr)
					return enqueueIdempotent(ctx, s.async, task, opts...)
				}
				s.logger.Warn("no node of target shard available for async dispatch",
					"shard", shard, "task_type", task.TaskType(), "error", lookupErr)
				continue
			}
		}

		rpcCtx, cancel := context.WithTimeout(node.ToShard(ctx, shard), internalRPCTimeout)
		req, reqErr := http.NewRequestWithContext(rpcCtx, http.MethodPost, path, bytes.NewReader(body))
		if reqErr == nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, rpcErr := s.forwarder.Ask(rpcCtx, address, req, body)
		cancel()

		if rpcErr == nil && resp != nil && resp.Status >= 200 && resp.Status < 300 {
			return nil
		}
		if rpcErr == nil && resp != nil {
			rpcErr = fmt.Errorf("shard %s returned HTTP %d", shard, resp.Status)
		}
		if !place.strict {
			s.doneHere(shard, work, address, rpcErr)
			return enqueueIdempotent(ctx, s.async, task, opts...)
		}
		if status.Code(rpcErr) != codes.Unavailable {
			s.logger.Warn("target shard rejected async dispatch",
				"shard", shard, "node", address, "task_type", task.TaskType(), "error", rpcErr)
		}
		targetNode = "" // fall back to any healthy node on subsequent shards
	}

	return errResidencyDown
}

func enqueueIdempotent(ctx context.Context, d async.Dispatcher, task async.Task, opts ...async.Option) error {
	_, err := async.Enqueue(ctx, d, task, opts...)
	if errors.Is(err, async.ErrTaskIDConflict) || errors.Is(err, async.ErrDuplicateTask) {
		return nil
	}
	return err
}
