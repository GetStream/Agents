// Package tasks defines typed async tasks for limit notices and cost anomaly sweeps.
package tasks

import (
	"context"
	"encoding/json"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

const (
	// TypePostNotice posts a single recorded notice (money limit, balance, or cost anomaly).
	TypePostNotice = "limitnotice:post_notice"
	// TypeSweepNotices retries any unposted notices in the notices table.
	TypeSweepNotices = "limitnotice:sweep_notices"
	// TypeCheckAnomalies scans for daily cost anomalies and records notices.
	TypeCheckAnomalies = "limitnotice:check_anomalies"

	postNoticeTimeout = 30 * time.Second
	postNoticeRetries = 5

	sweepTimeout = 2 * time.Minute
	sweepRetries = 1
)

// PostNotice posts a single notice to Slack / configured poster.
type PostNotice struct {
	OrganizationID string          `json:"organization_id"`
	Kind           string          `json:"kind"`
	DedupeKey      string          `json:"dedupe_key"`
	Payload        json.RawMessage `json:"payload"`
}

func (PostNotice) TaskType() string   { return TypePostNotice }
func (PostNotice) Queue() async.Queue { return async.QueueMaintenance }
func (PostNotice) Scope() async.Scope { return async.ScopeGlobal }
func (PostNotice) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(postNoticeTimeout),
		async.WithMaxRetry(postNoticeRetries),
	}
}

// Notice converts the task payload into store.Notice.
func (p PostNotice) Notice() store.Notice {
	return store.Notice{
		OrganizationID: p.OrganizationID,
		Kind:           p.Kind,
		DedupeKey:      p.DedupeKey,
		Payload:        p.Payload,
	}
}

// SweepNotices claims and posts any unposted notices.
type SweepNotices struct{}

func (SweepNotices) TaskType() string   { return TypeSweepNotices }
func (SweepNotices) Queue() async.Queue { return async.QueueMaintenance }
func (SweepNotices) Scope() async.Scope { return async.ScopeGlobal }
func (SweepNotices) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(sweepTimeout),
		async.WithMaxRetry(sweepRetries),
	}
}

// CheckAnomalies runs a daily cost-anomaly check pass.
type CheckAnomalies struct{}

func (CheckAnomalies) TaskType() string   { return TypeCheckAnomalies }
func (CheckAnomalies) Queue() async.Queue { return async.QueueMaintenance }
func (CheckAnomalies) Scope() async.Scope { return async.ScopeGlobal }
func (CheckAnomalies) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(sweepTimeout),
		async.WithMaxRetry(sweepRetries),
	}
}

// Sender posts individual notices and sweeps unposted notices.
type Sender interface {
	SendNotice(ctx context.Context, notice store.Notice) error
	SweepOnce(ctx context.Context) error
}

// AnomalyChecker runs cost anomaly detection.
type AnomalyChecker interface {
	CheckOnce(ctx context.Context) error
}

// PostNoticeHandler adapts Sender to async.Handler[PostNotice].
type PostNoticeHandler struct {
	Sender Sender
}

func (h *PostNoticeHandler) Handle(ctx context.Context, task PostNotice) error {
	return h.Sender.SendNotice(ctx, task.Notice())
}

// SweepNoticesHandler adapts Sender to async.Handler[SweepNotices].
type SweepNoticesHandler struct {
	Sender Sender
}

func (h *SweepNoticesHandler) Handle(ctx context.Context, _ SweepNotices) error {
	return h.Sender.SweepOnce(ctx)
}

// CheckAnomaliesHandler adapts AnomalyChecker to async.Handler[CheckAnomalies].
type CheckAnomaliesHandler struct {
	Checker AnomalyChecker
}

func (h *CheckAnomaliesHandler) Handle(ctx context.Context, _ CheckAnomalies) error {
	return h.Checker.CheckOnce(ctx)
}
