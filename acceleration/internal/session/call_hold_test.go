//go:build integration

package session

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidishook"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llmrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/node"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sttrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv/redisready"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts"
	"github.com/GetStream/Vision-Agents/acceleration/internal/ttsrouter"
)

// CallHoldSuite covers a session's call hold in the directory across renewals.
type CallHoldSuite struct {
	suite.Suite
	ctx    context.Context
	client rueidis.Client
	prefix string
}

func TestCallHoldSuite(t *testing.T) {
	suite.Run(t, new(CallHoldSuite))
}

func (s *CallHoldSuite) SetupTest() {
	address := os.Getenv("ROUTER_REDIS_ADDR")
	if address == "" {
		s.T().Skip("ROUTER_REDIS_ADDR is not set")
	}
	s.Require().NoError(redisready.Ready(address))
	client, err := redisready.NewClient(rueidis.ClientOption{InitAddress: []string{address}, DisableCache: true})
	s.Require().NoError(err)
	s.T().Cleanup(client.Close)
	s.ctx = s.T().Context()
	s.prefix = "call-hold-test:" + uuid.NewString()
	s.client = client
}

func (s *CallHoldSuite) manager(client rueidis.Client, address string, edge agent.Edge, logger *slog.Logger) (*Manager, *node.Directory) {
	directory, err := node.NewDirectory(node.DirectoryOptions{
		Redis: client, Address: address, Prefix: s.prefix, Logger: logger,
	})
	s.Require().NoError(err)
	s.T().Cleanup(directory.Close)

	reasoning := llmrouter.NewRegistry()
	reasoning.Register("stub", func(routing.Spec) (llmrouter.Provider, error) { return &stubLLM{reply: "Hello."}, nil })
	reasoner, err := llmrouter.New(llmrouter.Options{Config: stubConfig(), Registry: reasoning, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(reasoner.Close)

	transcription := sttrouter.NewRegistry()
	transcription.Register("stub", func(routing.Spec) (stt.STT, error) { return &stubSTT{emitter: stt.NewEmitter(64)}, nil })
	transcriber, err := sttrouter.New(sttrouter.Options{Config: stubConfig(), Registry: transcription, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(transcriber.Close)

	speech := ttsrouter.NewRegistry()
	speech.Register("stub", func(routing.Spec) (tts.TTS, error) { return &stubTTS{emitter: tts.NewEmitter(64)}, nil })
	speaker, err := ttsrouter.New(ttsrouter.Options{Config: stubConfig(), Registry: speech, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(speaker.Close)

	m, err := NewManager(ManagerOptions{
		LLM:       reasoner,
		STT:       transcriber,
		TTS:       speaker,
		Directory: directory,
		Logger:    logger,
		Edge:      func(context.Context, Spec, streamapp.Bound, *slog.Logger) (agent.Edge, error) { return edge, nil },
	})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.Require().NoError(m.Shutdown()) })
	return m, directory
}

func (s *CallHoldSuite) spec(customerID, id string) Spec {
	return Spec{
		ID:         id,
		CustomerID: customerID,
		Text:       true,
		Incognito:  true,
		LLMTarget:  "en-low-latency",
	}
}

// After Redis is cleared and another customer takes the call hold, the next renewal
// gets 0 and ends the original session cleanly while leaving the new customer's hold intact.
func (s *CallHoldSuite) TestASessionEndsWhenAnotherCustomerTakesItsCallAfterRedisIsCleared() {
	logged := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logged, nil))
	first, firstDir := s.manager(s.client, "node-a:1", newQuietEdge(), logger)
	second, _ := s.manager(s.client, "node-b:1", newQuietEdge(), slog.New(slog.DiscardHandler))

	callID := "shared-" + uuid.NewString()
	created, err := first.Create(s.ctx, s.spec("acme", callID))
	s.Require().NoError(err)
	holder, err := firstDir.CallHolder(s.ctx, 0, callID)
	s.Require().NoError(err)
	s.Require().Equal("acme", holder)

	// Clear the call key as a Redis flush would, then let another customer take the call.
	s.Require().NoError(s.client.Do(s.ctx, s.client.B().Del().Key(s.prefix+":call:0:"+callID).Build()).Error())
	other, err := second.Create(s.ctx, s.spec("globex", callID))
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = other.Close() })
	holder, err = firstDir.CallHolder(s.ctx, 0, callID)
	s.Require().NoError(err)
	s.Require().Equal("globex", holder)

	// Wait for the directory's renewal tick (10s) to see 0 and end acme's session.
	s.Require().Eventually(func() bool {
		return created.State() == Ended && !first.Running("acme", callID)
	}, 15*time.Second, 50*time.Millisecond, "the session whose call another customer took is still running")

	_, held := first.Get(callID, Owner{CustomerID: "acme", Kind: auth.KindServer})
	s.False(held)
	s.Equal(EndReason("closed"), created.EndReason())
	s.Contains(logged.String(), "ending a session whose call another customer holds")
	s.Require().Eventually(func() bool {
		nodeAddr, err := firstDir.Node(s.ctx, "acme", callID)
		return err == nil && nodeAddr != "node-a:1"
	}, settleFor, 20*time.Millisecond, "the ended session's directory claim must be released")
	exists, err := s.client.Do(s.ctx, s.client.B().Exists().Key(s.prefix+":session:acme:"+callID).Build()).AsInt64()
	s.Require().NoError(err)
	s.Zero(exists, "the ended session's directory key must be deleted")

	// Closing acme's session must not remove globex's hold on the call.
	holder, err = firstDir.CallHolder(s.ctx, 0, callID)
	s.Require().NoError(err)
	s.Equal("globex", holder)
	s.Equal(Live, other.State())
}

// A transient Redis error during renewal does not end the running session.
func (s *CallHoldSuite) TestASessionKeepsRunningWhenRedisFailsDuringRenewal() {
	hook := &failingMultiHook{failed: make(chan struct{}, 8)}
	logged := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logged, nil))
	first, firstDir := s.manager(rueidishook.WithHook(s.client, hook), "node-a:1", newQuietEdge(), logger)

	callID := "blip-" + uuid.NewString()
	created, err := first.Create(s.ctx, s.spec("acme", callID))
	s.Require().NoError(err)

	hook.fail.Store(true)
	select {
	case <-hook.failed:
	case <-time.After(15 * time.Second):
		s.FailNow("renewal never ran while Redis was failing")
	}

	s.Equal(Live, created.State())
	s.True(first.Running("acme", callID))
	s.NotContains(logged.String(), "ending a session whose call another customer holds")

	hook.fail.Store(false)
	holder, err := firstDir.CallHolder(s.ctx, 0, callID)
	s.Require().NoError(err)
	s.Equal("acme", holder)
}

// If another customer takes the call while Create is still joining, Create turns the session away.
func (s *CallHoldSuite) TestASessionWhoseCallAnotherCustomerTookBeforeItWasHeldIsTurnedAway() {
	logged := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logged, nil))
	joining, joined := make(chan struct{}), make(chan struct{})
	first, firstDir := s.manager(s.client, "node-a:1", gatedEdge{newQuietEdge(), joining, joined}, logger)
	second, _ := s.manager(s.client, "node-b:1", newQuietEdge(), slog.New(slog.DiscardHandler))

	callID := "raced-" + uuid.NewString()
	spec := s.spec("acme", callID)
	spec.Text = false
	spec.STTTarget = "en-low-latency"
	spec.TTSTarget = "en-low-latency"
	refused := make(chan error, 1)
	go func() {
		_, err := first.Create(s.ctx, spec)
		refused <- err
	}()
	select {
	case <-joining:
	case <-time.After(settleFor):
		s.FailNow("the session never reached Join")
	}

	// Clear Redis and let globex take the call while acme is still in Join.
	s.Require().NoError(s.client.Do(s.ctx, s.client.B().Del().Key(s.prefix+":call:0:"+callID).Build()).Error())
	other, err := second.Create(s.ctx, s.spec("globex", callID))
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = other.Close() })

	// Wait for the renewal tick to observe 0 while Create is still blocked in Join.
	s.Require().Eventually(func() bool {
		return strings.Contains(logged.String(), "its hold is not renewed")
	}, 15*time.Second, 50*time.Millisecond, "the renewal never observed the lost hold")

	close(joined)
	select {
	case err := <-refused:
		s.Require().ErrorIs(err, ErrSessionExists)
	case <-time.After(settleFor):
		s.FailNow("Create did not return")
	}

	s.False(first.Running("acme", callID))
	holder, err := firstDir.CallHolder(s.ctx, 0, callID)
	s.Require().NoError(err)
	s.Equal("globex", holder)
}

// failingMultiHook fails DoMulti when fail is set, simulating a transient Redis outage on renewal.
type failingMultiHook struct {
	fail   atomic.Bool
	failed chan struct{}
}

func (h *failingMultiHook) Do(client rueidis.Client, ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	return client.Do(ctx, cmd)
}

func (h *failingMultiHook) DoMulti(client rueidis.Client, ctx context.Context, multi ...rueidis.Completed) []rueidis.RedisResult {
	if h.fail.Load() {
		select {
		case h.failed <- struct{}{}:
		default:
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		return client.DoMulti(cancelled, multi...)
	}
	return client.DoMulti(ctx, multi...)
}

func (h *failingMultiHook) DoCache(client rueidis.Client, ctx context.Context, cmd rueidis.Cacheable, ttl time.Duration) rueidis.RedisResult {
	return client.DoCache(ctx, cmd, ttl)
}

func (h *failingMultiHook) DoMultiCache(client rueidis.Client, ctx context.Context, multi ...rueidis.CacheableTTL) []rueidis.RedisResult {
	return client.DoMultiCache(ctx, multi...)
}

func (h *failingMultiHook) Receive(client rueidis.Client, ctx context.Context, subscribe rueidis.Completed, fn func(msg rueidis.PubSubMessage)) error {
	return client.Receive(ctx, subscribe, fn)
}

func (h *failingMultiHook) DoStream(client rueidis.Client, ctx context.Context, cmd rueidis.Completed) rueidis.RedisResultStream {
	return client.DoStream(ctx, cmd)
}

func (h *failingMultiHook) DoMultiStream(client rueidis.Client, ctx context.Context, multi ...rueidis.Completed) rueidis.MultiRedisResultStream {
	return client.DoMultiStream(ctx, multi...)
}
