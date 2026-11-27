package limitnotice

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	lntasks "github.com/GetStream/Vision-Agents/acceleration/internal/limitnotice/tasks"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// Store is where the notices are kept: *store.Notices.
type Store interface {
	Insert(ctx context.Context, notice *store.Notice) (bool, error)
	Claim(ctx context.Context, stale time.Duration, limit int) ([]store.Notice, error)
	MarkPosted(ctx context.Context, notice store.Notice) error
}

// Sender posts the notices of every kind the channel is told of, money limits and cost
// anomalies, and is the one loop that posts those whose post failed or whose router stopped
// before it posted.
type Sender struct {
	store       Store
	poster      Poster
	environment string
	links       Links
	dispatcher  async.Dispatcher
	logger      *slog.Logger
	every       time.Duration

	stop context.CancelFunc
	done chan struct{}
}

// SenderOptions configure a Sender. Store and Poster are required.
type SenderOptions struct {
	Store  Store
	Poster Poster
	// Environment is named in every message (EnvironmentTag).
	Environment string
	// Links are the pages a message links to.
	Links Links
	// Dispatcher enqueues notice posting onto the shared async runtime (QueueMaintenance).
	// When nil, Note posts synchronously and Start sweeps periodically.
	Dispatcher async.Dispatcher
	Logger     *slog.Logger
}

// NewSender returns a Sender that keeps notices in the options' store and posts them to its
// poster. Start starts its loop.
func NewSender(options SenderOptions) *Sender {
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Sender{
		store: options.Store, poster: options.Poster, environment: options.Environment, links: options.Links,
		dispatcher: options.Dispatcher, logger: logger, every: sweepEvery,
	}
}

// Start looks for the notices never posted every sweepEvery until Close.
func (s *Sender) Start() {
	ctx, stop := context.WithCancel(context.Background())
	s.stop, s.done = stop, make(chan struct{})
	go s.run(ctx)
}

// Close stops the loop, leaving what it had not posted to the next router's.
func (s *Sender) Close() {
	if s.stop == nil {
		return
	}
	s.stop()
	<-s.done
}

// Note writes the notice and, when this call wrote it, posts it (or enqueues a PostNotice
// task when Dispatcher is configured). written says it did.
func (s *Sender) Note(ctx context.Context, notice store.Notice) (written bool, err error) {
	written, err = s.store.Insert(ctx, &notice)
	if err != nil || !written {
		return written, err
	}
	if s.dispatcher != nil {
		if _, err := async.Enqueue(ctx, s.dispatcher, lntasks.PostNotice{
			OrganizationID: notice.OrganizationID,
			Kind:           notice.Kind,
			DedupeKey:      notice.DedupeKey,
			Payload:        notice.Payload,
		}, async.WithTaskID("limitnotice:"+notice.OrganizationID+":"+notice.Kind+":"+notice.DedupeKey)); err != nil &&
			!errors.Is(err, async.ErrTaskIDConflict) && !errors.Is(err, async.ErrDuplicateTask) {
			s.logger.Warn("could not enqueue notice task; periodic sweep will post it",
				"organization", notice.OrganizationID, "kind", notice.Kind, "error", err)
		}
		return true, nil
	}
	s.send(ctx, notice)
	return true, nil
}

// SendNotice posts one notice and marks it posted. Returns an error on transient post failure
// so the async worker retries it.
func (s *Sender) SendNotice(ctx context.Context, notice store.Notice) error {
	text, err := s.message(notice)
	if err != nil {
		return async.SkipRetry(err)
	}
	if err := s.poster.Post(ctx, text); err != nil {
		return err
	}
	if err := s.store.MarkPosted(ctx, notice); err != nil {
		s.logger.Error("posted a notice, and could not record it; it may be posted again",
			"organization", notice.OrganizationID, "kind", notice.Kind, "error", err)
		return nil
	}
	s.logger.Info("posted a notice", "organization", notice.OrganizationID, "kind", notice.Kind, "key", notice.DedupeKey)
	return nil
}

// SweepOnce claims and posts any unposted notices once.
func (s *Sender) SweepOnce(ctx context.Context) error {
	s.sweep(ctx)
	return nil
}

func (s *Sender) run(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(s.every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sweep(ctx)
		}
	}
}

func (s *Sender) sweep(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	notices, err := s.store.Claim(ctx, stale, sweepBatch)
	if err != nil {
		s.logger.Error("could not read the notices never posted", "error", err)
		return
	}
	for _, notice := range notices {
		s.send(ctx, notice)
	}
}

func (s *Sender) send(ctx context.Context, notice store.Notice) {
	text, err := s.message(notice)
	if err != nil {
		s.logger.Error("could not read a notice to post", "organization", notice.OrganizationID, "kind", notice.Kind, "error", err)
		return
	}
	if err := s.poster.Post(ctx, text); err != nil {
		s.logger.Error("could not post a notice; it is retried",
			"organization", notice.OrganizationID, "kind", notice.Kind, "error", err)
		return
	}
	if err := s.store.MarkPosted(ctx, notice); err != nil {
		s.logger.Error("posted a notice, and could not record it; it may be posted again",
			"organization", notice.OrganizationID, "kind", notice.Kind, "error", err)
		return
	}
	s.logger.Info("posted a notice", "organization", notice.OrganizationID, "kind", notice.Kind, "key", notice.DedupeKey)
}

// message is the notice as it is posted, by its kind.
func (s *Sender) message(notice store.Notice) (string, error) {
	switch notice.Kind {
	case store.LimitNoCredit, store.LimitSpendHardLimit, store.LimitSpendLimit:
		limit, err := notice.LimitNotice()
		return Message(s.environment, s.links, limit), err
	case store.NoticeCostAnomaly:
		anomaly, err := notice.CostAnomaly()
		return AnomalyMessage(s.environment, s.links, anomaly), err
	}
	return "", fmt.Errorf("limitnotice: no message for a %s notice", notice.Kind)
}
