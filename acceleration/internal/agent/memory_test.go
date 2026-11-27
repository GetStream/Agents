package agent

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/memory"
	"github.com/GetStream/Vision-Agents/acceleration/internal/memory/mem0"
	"github.com/GetStream/Vision-Agents/acceleration/internal/metered"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
)

// MemoryWriterSuite runs the writer against the real mem0 store and a mem0 that is out of
// quota, so what reaches the log is what a deployment would see.
type MemoryWriterSuite struct {
	suite.Suite
	logs   *bytes.Buffer
	writer *memoryWriter
}

func TestMemoryWriterSuite(t *testing.T) { suite.Run(t, new(MemoryWriterSuite)) }

func (s *MemoryWriterSuite) SetupTest() {
	quota := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"Usage quota exceeded for this billing period."}`))
	}))
	s.T().Cleanup(quota.Close)

	s.logs = &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(s.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	store, err := mem0.New(mem0.Options{APIKey: "test-key", BaseURL: quota.URL, Logger: logger})
	s.Require().NoError(err)
	scope := memory.Scope{AppID: "router", UserID: "acme", AgentID: "support", RunID: "session-1"}
	s.writer = newMemoryWriter(metered.NewMemory(store, nil).For(routing.Owner{}), scope, false, logger)
	s.T().Cleanup(s.writer.Close)
}

// failures is every ERROR line logged.
func (s *MemoryWriterSuite) failures() []string {
	var logged []string
	for _, line := range strings.Split(s.logs.String(), "\n") {
		if strings.Contains(line, "level=ERROR") {
			logged = append(logged, line)
		}
	}
	return logged
}

func (s *MemoryWriterSuite) TestARecallMem0RefusesIsOneErrorNamingTheQuota() {
	s.Nil(s.writer.Recall(context.Background(), 5))

	logged := s.failures()
	s.Require().Len(logged, 1)
	s.Contains(logged[0], "429 Too Many Requests (mem0 quota or rate limit)")
	s.Contains(logged[0], "Usage quota exceeded")
}

func (s *MemoryWriterSuite) TestAnExchangeMem0RefusesIsOneErrorNamingTheQuota() {
	s.writer.Remember("turn-1", []llm.Message{{Role: llm.User, Content: "I moved to Austin"}})
	s.writer.Close()

	logged := s.failures()
	s.Require().Len(logged, 1)
	s.Contains(logged[0], "429 Too Many Requests (mem0 quota or rate limit)")
}

// TestCloseGivesUpOnAMem0ThatNeverAnswers is the teardown of a call whose memory store is
// stuck: every queued exchange used to get its own memoryTimeout, so closing waited for
// all of them. Now it waits for one deadline and drops what is left, counted once.
func (s *MemoryWriterSuite) TestCloseGivesUpOnAMem0ThatNeverAnswers() {
	const queued = 20
	var attempts atomic.Int64
	release := make(chan struct{})
	stuck := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		<-release
	}))
	s.T().Cleanup(stuck.Close)
	s.T().Cleanup(func() { close(release) })

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	store, err := mem0.New(mem0.Options{APIKey: "test-key", BaseURL: stuck.URL, Logger: logger})
	s.Require().NoError(err)
	writer := newMemoryWriter(metered.NewMemory(store, nil).For(routing.Owner{}), memory.Scope{AppID: "router", UserID: "acme", AgentID: "support", RunID: "session-1"}, false, logger)
	writer.drainTimeout = 200 * time.Millisecond
	for i := 0; i < queued; i++ {
		writer.Remember("turn-1", []llm.Message{{Role: llm.User, Content: "I moved to Austin"}})
	}

	closed := make(chan struct{})
	started := time.Now()
	go func() {
		writer.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		s.FailNow("Close was still waiting on a mem0 that never answers")
	}

	s.GreaterOrEqual(time.Since(started), 200*time.Millisecond, "closing does not drop exchanges before the deadline")
	s.EqualValues(queued, writer.dropped.Load(), "the exchange in flight and every queued one are counted")
	s.EqualValues(1, attempts.Load(), "no exchange is tried once the deadline has passed")
	s.Equal(1, strings.Count(logs.String(), "dropped exchanges"), "the drops are logged once")
	s.Contains(logs.String(), "count=20")
}

// countingBackend counts the Remember calls that reach the provider. A stuck one blocks
// until the call's context ends; a healthy one returns at once. It keeps the scope of every
// exchange and every turn it was asked to forget.
type countingBackend struct {
	stuck bool
	calls atomic.Int64
	// hold, while open, holds every Remember back.
	hold chan struct{}

	mu         sync.Mutex
	remembered []memory.Scope
	forgot     [][]string
}

func (b *countingBackend) Recall(context.Context, memory.Query) (memory.Recalled, error) {
	return memory.Recalled{}, nil
}

func (b *countingBackend) Remember(ctx context.Context, scope memory.Scope, _ []llm.Message) (memory.Billed, error) {
	if b.hold != nil {
		<-b.hold
	}
	b.calls.Add(1)
	b.mu.Lock()
	b.remembered = append(b.remembered, scope)
	b.mu.Unlock()
	if b.stuck {
		<-ctx.Done()
		return memory.Billed{}, ctx.Err()
	}
	return memory.Billed{}, nil
}

func (b *countingBackend) Truncate(context.Context, string, string) (memory.Billed, error) {
	return memory.Billed{}, nil
}

func (b *countingBackend) ForgetRun(context.Context, string, string) (memory.Billed, error) {
	return memory.Billed{}, nil
}

func (b *countingBackend) ForgetTurns(ctx context.Context, appID, runID string, turnIDs []string) (memory.Billed, error) {
	b.mu.Lock()
	b.forgot = append(b.forgot, append([]string{appID, runID}, turnIDs...))
	b.mu.Unlock()
	if b.stuck {
		<-ctx.Done()
		return memory.Billed{}, ctx.Err()
	}
	return memory.Billed{}, nil
}

func (b *countingBackend) rememberedTurns() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	turns := make([]string, 0, len(b.remembered))
	for _, scope := range b.remembered {
		turns = append(turns, scope.TurnID)
	}
	return turns
}

func (b *countingBackend) forgotten() [][]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.forgot)
}
func (b *countingBackend) Provider() string { return "counting" }
func (b *countingBackend) Close() error     { return nil }

func (s *MemoryWriterSuite) writerOver(backend *countingBackend, queued int) *memoryWriter {
	writer := newMemoryWriter(metered.NewMemory(backend, nil).For(routing.Owner{}), memory.Scope{AppID: "router", UserID: "acme", AgentID: "support", RunID: "session-1"}, false, slog.New(slog.DiscardHandler))
	writer.drainTimeout = 100 * time.Millisecond
	for i := 0; i < queued; i++ {
		writer.Remember("turn-1", []llm.Message{{Role: llm.User, Content: "I moved to Austin"}})
	}
	return writer
}

// TestAStuckProviderIsNotCalledOnceTheDeadlineHasPassed counts at the provider, where the
// metered store would otherwise still reserve, call and record a failure for every queued
// exchange with a cancelled context.
func (s *MemoryWriterSuite) TestAStuckProviderIsNotCalledOnceTheDeadlineHasPassed() {
	backend := &countingBackend{stuck: true}
	writer := s.writerOver(backend, 10)
	writer.Close()

	s.EqualValues(1, backend.calls.Load())
	s.EqualValues(10, writer.dropped.Load())
}

func (s *MemoryWriterSuite) TestAHealthyProviderGetsEveryQueuedExchangeOnClose() {
	backend := &countingBackend{}
	writer := s.writerOver(backend, 10)
	writer.Close()

	s.EqualValues(10, backend.calls.Load())
	s.EqualValues(0, writer.dropped.Load())
}

func (s *MemoryWriterSuite) TestTheDrainDeadlineIsTwoWrites() {
	s.Equal(2*memoryTimeout, s.writer.drainTimeout)
}

func (s *MemoryWriterSuite) TestAnExchangeIsRememberedUnderTheTurnItWasSaidIn() {
	backend := &countingBackend{}
	writer := s.writerOver(backend, 0)
	writer.Remember("turn-1", []llm.Message{{Role: llm.User, Content: "I am vegetarian"}})
	writer.Remember("turn-2", []llm.Message{{Role: llm.User, Content: "I live in Austin"}})
	writer.Close()

	s.Equal([]string{"turn-1", "turn-2"}, backend.rememberedTurns())
	backend.mu.Lock()
	defer backend.mu.Unlock()
	s.Equal("session-1", backend.remembered[1].RunID, "the turn is written beside the session, not instead of it")
}

// A turn taken back while its exchange is still queued, and the call ending before the second
// pass, would otherwise be written after the only deletion.
func (s *MemoryWriterSuite) TestAnExchangeQueuedForAForgottenTurnIsNeverWritten() {
	backend := &countingBackend{hold: make(chan struct{})}
	writer := s.writerOver(backend, 0)
	writer.drainTimeout = 3 * time.Second
	writer.Remember("turn-1", []llm.Message{{Role: llm.User, Content: "I live in Austin"}})
	writer.Remember("turn-2", []llm.Message{{Role: llm.User, Content: "I am vegetarian"}})

	writer.Forget([]string{"turn-2"})
	close(backend.hold)
	writer.Close()

	s.Equal([]string{"turn-1"}, backend.rememberedTurns())
}

func (s *MemoryWriterSuite) TestForgottenTurnsAreForgottenAgainForWhatTheStoreExtractsLate() {
	backend := &countingBackend{}
	writer := s.writerOver(backend, 0)
	writer.lateAfter = 50 * time.Millisecond

	writer.Forget([]string{"turn-1", "turn-2"})

	s.Eventually(func() bool { return len(backend.forgotten()) == 2 }, 3*time.Second, 10*time.Millisecond)
	s.Equal([][]string{
		{"router", "session-1", "turn-1", "turn-2"},
		{"router", "session-1", "turn-1", "turn-2"},
	}, backend.forgotten(), "both passes forget the same turns of the same session")
	writer.Close()
}

func (s *MemoryWriterSuite) TestClosingDoesNotWaitToForgetAgain() {
	backend := &countingBackend{}
	writer := s.writerOver(backend, 0)
	writer.lateAfter = time.Hour

	writer.Forget([]string{"turn-1"})
	s.Eventually(func() bool { return len(backend.forgotten()) == 1 }, 3*time.Second, 10*time.Millisecond)
	closed := make(chan struct{})
	go func() {
		writer.Close()
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		s.FailNow("Close waited for the second pass")
	}
	s.Len(backend.forgotten(), 1)
}

func (s *MemoryWriterSuite) TestTurnsForgottenAfterCloseAskTheStoreNothing() {
	backend := &countingBackend{}
	writer := s.writerOver(backend, 0)
	writer.Close()

	writer.Forget([]string{"turn-1"})

	s.Never(func() bool { return len(backend.forgotten()) > 0 }, 200*time.Millisecond, 10*time.Millisecond,
		"nothing waits for a deletion started once the writer closed")
}

func (s *MemoryWriterSuite) TestAnIncognitoSessionHasNothingToForget() {
	backend := &countingBackend{}
	writer := newMemoryWriter(metered.NewMemory(backend, nil).For(routing.Owner{}), memory.Scope{AppID: "router", UserID: "acme", AgentID: "support", RunID: "session-1"}, true, slog.New(slog.DiscardHandler))
	writer.Forget([]string{"turn-1"})
	writer.Close()

	s.Empty(backend.forgotten())
}

// TestCloseGivesUpOnADeletionMem0NeverAnswers is an edit just before the call ends, against a
// stuck store: the deletion has a timeout per turn, and Close must not wait all of it.
func (s *MemoryWriterSuite) TestCloseGivesUpOnADeletionMem0NeverAnswers() {
	backend := &countingBackend{stuck: true}
	writer := s.writerOver(backend, 0)
	writer.Forget([]string{"turn-1", "turn-2", "turn-3"})
	s.Require().Eventually(func() bool { return len(backend.forgotten()) == 1 }, 3*time.Second, 5*time.Millisecond)

	closed := make(chan struct{})
	go func() {
		writer.Close()
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		s.FailNow("Close waited for a deletion mem0 never answered")
	}
}
