//go:build integration

package session

import (
	"context"
	"io"
	"log/slog"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/audio"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation/chattest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llmrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sttrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts"
	"github.com/GetStream/Vision-Agents/acceleration/internal/ttsrouter"
)

// runtimeSlice is how long a slice of runtime runs in these tests, so a session lives
// through several in a fraction of a second.
const runtimeSlice = 50 * time.Millisecond

// shutdownWithin is how long a shutdown may take once nothing holds it: it writes the rows
// the sessions queued, which the recorders give up on after 15 seconds, so waiting less
// than that fails a shutdown that is only writing them.
const shutdownWithin = 30 * time.Second

// RuntimeSuite opens sessions on a manager over Postgres and reads back what their time cost
// and what they wrote down.
type RuntimeSuite struct {
	suite.Suite
	ctx    context.Context
	store  *store.Store
	prices *pricing.List

	manager  *Manager
	customer string
	// refuses makes every call the manager opens refuse the agent.
	refuses bool
	// edges are the calls the manager opened, in order.
	edgesMu sync.Mutex
	edges   []*quietEdge
	// model answers the session's conversation, unless gated does instead, for a test that
	// holds a reply open. voice speaks and ears hears the session's calls.
	model *stubLLM
	gated *gatedLLM
	voice *stubTTS
	ears  *stubSTT
	// apps is the Stream apps sessions act in, when a test has the agent end its call.
	apps *streamapp.Clients
}

func TestRuntimeSuite(t *testing.T) {
	suite.Run(t, new(RuntimeSuite))
}

func (s *RuntimeSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN not set")
	}
	s.ctx = context.Background()
	opened, err := store.Open(testenv.Database(dsn, "session_runtime"))
	s.Require().NoError(err)
	s.Require().NoError(opened.Migrate(s.ctx))
	s.store = opened
	s.T().Cleanup(func() { s.Require().NoError(opened.Close()) })
	perMinute := map[pricing.Meter]pricing.Rate{pricing.Seconds: {USD: 0.06, Per: 60}}
	s.prices, err = pricing.NewList("runtime", pricing.AllAtCost([]pricing.Entry{
		{Modality: string(routing.Runtime), Provider: runtimeProvider, Model: store.ModalityText, Region: pricing.DefaultRegion, Free: true},
		{Modality: string(routing.Runtime), Provider: runtimeProvider, Model: store.ModalityVoice, Region: pricing.DefaultRegion, Rates: perMinute},
		{Modality: string(routing.Runtime), Provider: runtimeProvider, Model: store.ModalityVideo, Region: pricing.DefaultRegion, Rates: perMinute},
		{Modality: string(routing.Runtime), Provider: runtimeProvider, Model: store.ModalityText, Region: pricing.RegionEU, Rates: perMinute},
	}), slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.Require().NoError(err)
}

func (s *RuntimeSuite) SetupTest() {
	s.customer = "runtime-" + uuid.NewString()
	s.refuses = false
	s.manager = nil
	s.edges = nil
	s.gated = nil
	s.apps = nil
}

// manages builds a manager over stub providers that keeps its sessions in Postgres.
func (s *RuntimeSuite) manages() {
	logger := slog.New(slog.DiscardHandler)

	transcription := sttrouter.NewRegistry()
	s.ears = &stubSTT{emitter: stt.NewEmitter(64)}
	transcription.Register("stub", func(routing.Spec) (stt.STT, error) { return s.ears, nil })
	transcriber, err := sttrouter.New(sttrouter.Options{Config: stubConfig(), Registry: transcription, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(transcriber.Close)

	// The agent opens its conversation model first, then its flow controller, which has the
	// caller's words over a reply stop it.
	s.model = &stubLLM{reply: "Hello."}
	var opened atomic.Int32
	reasoning := llmrouter.NewRegistry()
	reasoning.Register("stub", func(routing.Spec) (llmrouter.Provider, error) {
		switch opened.Add(1) {
		case 1:
		case 2:
			return &stubLLM{reply: `{"disposition":"respond","floor":"stop"}`}, nil
		default:
			return &stubLLM{reply: "Hello."}, nil
		}
		if s.gated != nil {
			return s.gated, nil
		}
		return s.model, nil
	}, routing.OverrideRegion)
	// One instance in each region, so an eu session opens.
	inRegions := stubConfig()
	inEU := inRegions.Providers[0]
	inEU.Region = pricing.RegionEU
	inRegions.Providers = append(inRegions.Providers, inEU)
	reasoner, err := llmrouter.New(llmrouter.Options{Config: inRegions, Registry: reasoning, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(reasoner.Close)

	speech := ttsrouter.NewRegistry()
	s.voice = &stubTTS{emitter: tts.NewEmitter(64)}
	speech.Register("stub", func(routing.Spec) (tts.TTS, error) { return s.voice, nil })
	speaker, err := ttsrouter.New(ttsrouter.Options{Config: stubConfig(), Registry: speech, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(speaker.Close)

	manager, err := NewManager(ManagerOptions{
		LLM:    reasoner,
		STT:    transcriber,
		TTS:    speaker,
		Store:  s.store,
		Pricer: s.prices,
		Stream: s.apps,
		Logger: logger,
		Edge: func(context.Context, Spec, streamapp.Bound, *slog.Logger) (agent.Edge, error) {
			edge := newQuietEdge()
			s.edgesMu.Lock()
			s.edges = append(s.edges, edge)
			s.edgesMu.Unlock()
			if s.refuses {
				return refusingEdge{edge}, nil
			}
			return attendedEdge{edge}, nil
		},
	})
	s.Require().NoError(err)
	manager.sliceEvery = runtimeSlice
	s.manager = manager
	s.T().Cleanup(func() { manager.Shutdown() })
}

// opens creates a session of the test's customer, defaulting what the test did not name.
func (s *RuntimeSuite) opens(spec Spec) (*Session, error) {
	spec.CustomerID = s.customer
	spec.Attribution = routing.Attribution{APIKey: "key-runtime"}
	spec.LLMTarget = "en-low-latency"
	spec.NoReview = true
	if !spec.Text {
		spec.STTTarget, spec.TTSTarget = "en-low-latency", "en-low-latency"
		if spec.CallID == "" {
			spec.CallID = "call-" + uuid.NewString()
		}
	}
	return s.manager.Create(s.ctx, spec)
}

// runtime shuts the manager down, which writes what its sessions' time is still queued as,
// and returns the session's runtime rows, oldest first.
func (s *RuntimeSuite) runtime(sessionID string) []store.Request {
	s.Require().NoError(s.manager.Shutdown())
	return s.written(sessionID)
}

// written is the session's runtime rows written so far, oldest first.
func (s *RuntimeSuite) written(sessionID string) []store.Request {
	var rows []store.Request
	s.Require().NoError(s.store.GlobalDB().NewSelect().Model(&rows).
		Where("customer_id = ?", s.customer).
		Where("session_id = ?", sessionID).
		Where("modality = ?", string(routing.Runtime)).
		Order("started_at").Scan(s.ctx))
	return rows
}

// modes is the models the rows were billed as, each run of the same one once.
func modes(rows []store.Request) []string {
	var billed []string
	for _, row := range rows {
		if len(billed) == 0 || billed[len(billed)-1] != row.Model {
			billed = append(billed, row.Model)
		}
	}
	return billed
}

// rowEnd is when the slice the row records ended.
func rowEnd(row store.Request) time.Time {
	if row.DurationMs == nil {
		return row.StartedAt
	}
	return row.StartedAt.Add(time.Duration(*row.DurationMs * float64(time.Millisecond)))
}

func totalSeconds(rows []store.Request) int64 {
	var total int64
	for _, row := range rows {
		total += row.Meters[pricing.Seconds]
	}
	return total
}

func (s *RuntimeSuite) TestASessionRecordsRuntimeUntilItCloses() {
	s.manages()
	created, err := s.opens(Spec{Text: true})
	s.Require().NoError(err)

	time.Sleep(runtimeSlice*2 + runtimeSlice/2)
	s.Require().NoError(created.Close())

	rows := s.runtime(created.ID())
	s.Require().GreaterOrEqual(len(rows), 3, "a slice is written as each one ends, and the last when the session closes")
	s.Equal([]string{store.ModalityText}, modes(rows))
	s.EqualValues(1, totalSeconds(rows), "the slices add up to the session's life, rounded up to a second")
	for _, row := range rows {
		s.Equal(runtimeProvider, row.Provider)
		s.Equal(created.ID(), row.OperationID)
		s.Equal("key-runtime", row.APIKey, "billed to whoever opened the session")
		s.Zero(row.CostMicros, "time in writing is priced free here")
		s.False(row.Billable, "and so is recorded without being billed")
	}
}

func (s *RuntimeSuite) TestASessionsRuntimeIsPricedInItsRegion() {
	s.manages()
	created, err := s.opens(Spec{Text: true, Region: pricing.RegionEU})
	s.Require().NoError(err)

	time.Sleep(runtimeSlice)
	s.Require().NoError(created.Close())

	rows := s.runtime(created.ID())
	s.Require().NotEmpty(rows)
	for _, row := range rows {
		s.Equal("runtime-stream-text-eu", row.SKU, "priced by the eu entry, not the free us one")
		s.True(row.Billable)
	}
}

func (s *RuntimeSuite) TestStartingVoiceSwitchesTheRuntimeMode() {
	s.manages()
	created, err := s.opens(Spec{Text: true})
	s.Require().NoError(err)

	time.Sleep(runtimeSlice)
	s.Require().NoError(created.StartVoice(s.ctx, Call{}))
	time.Sleep(runtimeSlice)
	created.SawVideo()
	time.Sleep(runtimeSlice)
	s.Require().NoError(created.StopVoice(s.ctx))
	created.SawVideo()
	time.Sleep(runtimeSlice)
	s.Require().NoError(created.Close())

	rows := s.runtime(created.ID())
	s.Equal([]string{store.ModalityText, store.ModalityVoice, store.ModalityVideo, store.ModalityText}, modes(rows),
		"billed as what held the session: writing, the call, the call with video, then writing again")
	s.EqualValues(1, totalSeconds(rows))
	for _, row := range rows {
		s.Equal(row.Model != store.ModalityText, row.Billable, "only the call's time is billed at these prices")
	}
}

func (s *RuntimeSuite) TestASessionThatFailsToOpenIsNotBilled() {
	s.manages()
	s.refuses = true
	id := "runtime-" + uuid.NewString()

	_, err := s.opens(Spec{ID: id})
	s.Require().Error(err)

	s.Empty(s.runtime(id), "the time spent failing to join is not the customer's")
	kept, err := s.store.Sessions.Exists(s.ctx, id)
	s.Require().NoError(err)
	s.False(kept, "a session that never opened is not one to list or count")
}

func (s *RuntimeSuite) TestShuttingDownStopsEverySessionsRuntimeBeforeClosingAny() {
	s.manages()
	first, err := s.opens(Spec{})
	s.Require().NoError(err)
	second, err := s.opens(Spec{})
	s.Require().NoError(err)
	// Each call is still speaking, so closing it waits for the speech to go out.
	s.edgesMu.Lock()
	edges := slices.Clone(s.edges)
	s.edgesMu.Unlock()
	s.Require().Len(edges, 2)
	for _, edge := range edges {
		edge.holdSpeech(true)
	}

	down := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		down <- s.manager.Shutdown()
		close(finished)
	}()
	// A session is halted when its own close begins, and every runtime is stopped before the
	// first close, so a row that ends after this instant is a session billed while another
	// is closing.
	var closing time.Time
	s.Require().Eventually(func() bool {
		closing = time.Now()
		return first.Halted() || second.Halted()
	}, settleFor, time.Millisecond, "shutdown never began closing the sessions")
	time.Sleep(10 * runtimeSlice)
	// A shutdown that has already returned gave up on the speech, which it does only once
	// finishWithin has run out, so a slow host cannot fail this.
	select {
	case <-finished:
		s.Require().GreaterOrEqual(time.Since(closing), finishWithin/2, "shutdown should still be waiting on a call that is speaking")
	default:
	}

	// Rows are queued and written to Postgres later, in the order they were queued. The marker
	// is queued after every row the sessions made while the closing went on, so once it is
	// written so are they, however far behind the writer is.
	marker := "marker-" + uuid.NewString()
	s.manager.runtime.Record(routing.ProviderConfig{Provider: runtimeProvider, Model: store.ModalityText}, routing.Stat{
		Owner:     routing.Owner{CustomerID: s.customer, Attribution: routing.Attribution{SessionID: marker}},
		StartedAt: time.Now().UTC(),
		Success:   true,
	})
	s.Require().Eventually(func() bool { return len(s.written(marker)) > 0 }, shutdownWithin, runtimeSlice, "the writer never caught up")
	for _, id := range []string{first.ID(), second.ID()} {
		for _, row := range s.written(id) {
			s.False(rowEnd(row).After(closing.Add(time.Millisecond)),
				"no session goes on being billed while another one is being closed: %s ran until %s, closing began at %s",
				id, rowEnd(row).Format(time.RFC3339Nano), closing.Format(time.RFC3339Nano))
		}
	}

	for _, edge := range edges {
		edge.holdSpeech(false)
	}
	select {
	case err := <-down:
		s.Require().NoError(err)
	case <-time.After(shutdownWithin):
		s.Fail("shutdown never finished")
	}
}

func (s *RuntimeSuite) TestTheGreetingIsTheSessionsFirstRecordedTurn() {
	s.manages()
	created, err := s.opens(Spec{Greeting: "Hi, I'm listening."})
	s.Require().NoError(err)
	s.edgesMu.Lock()
	call := s.edges[0]
	s.edgesMu.Unlock()
	call.arrives(visitor)

	var turns []store.AgentResponse
	// Eventually runs its condition off the test goroutine, where a failed Require cannot stop the test.
	s.Require().EventuallyWithT(func(c *assert.CollectT) {
		require.NoError(c, created.FlushRecords(s.ctx))
		turns, err = s.store.Responses.Select(s.ctx, s.customer, created.ID(), store.ResponseFilter{Limit: 10})
		require.NoError(c, err)
		require.NotEmpty(c, turns)
		require.Equal(c, store.ResponseCompleted, turns[0].Status)
	}, settleFor, 10*time.Millisecond, "the greeting was never written down")
	items, err := s.store.ResponseItems.Select(s.ctx, s.customer, created.ID(), store.ItemFilter{ResponseID: turns[0].ID, Limit: 10})
	s.Require().NoError(err)
	said := slices.IndexFunc(items, func(item store.AgentResponseItem) bool {
		return item.Kind == store.ItemAnswer && item.Text == "Hi, I'm listening."
	})
	s.GreaterOrEqual(said, 0, "the first turn is the greeting, said as written")
}

// speaks has the visitor say text into the session's call.
func (s *RuntimeSuite) speaks(text string) {
	s.edgesMu.Lock()
	call := s.edges[len(s.edges)-1]
	s.edgesMu.Unlock()
	call.inbound <- agent.InboundAudio{Participant: visitor, Audio: audio.PcmData{Samples: make([]int16, 320), SampleRate: 16_000, Channels: 1}}
	s.ears.emitter.Send(stt.Transcript{Participant: visitor, Mode: stt.ModeFinal, Text: text, Language: "en", Confidence: 1})
}

// asked opens a voice session the visitor is in and has them ask something.
func (s *RuntimeSuite) asked(spec Spec, question string) *Session {
	created, err := s.opens(spec)
	s.Require().NoError(err)
	s.edgesMu.Lock()
	call := s.edges[0]
	s.edgesMu.Unlock()
	call.arrives(visitor)
	s.speaks(question)
	return created
}

// firstTurn is the session's first recorded turn once it is written, with its items.
func (s *RuntimeSuite) firstTurn(created *Session) (store.AgentResponse, []store.AgentResponseItem, error) {
	if err := created.FlushRecords(s.ctx); err != nil {
		return store.AgentResponse{}, nil, err
	}
	turns, err := s.store.Responses.Select(s.ctx, s.customer, created.ID(), store.ResponseFilter{Limit: 10})
	if err != nil || len(turns) == 0 {
		return store.AgentResponse{}, nil, err
	}
	items, err := s.store.ResponseItems.Select(s.ctx, s.customer, created.ID(), store.ItemFilter{ResponseID: turns[0].ID, Limit: 10})
	return turns[0], items, err
}

// answered waits until the first turn's answer is written down, and returns the turn.
func (s *RuntimeSuite) answered(created *Session, answer string) store.AgentResponse {
	var turn store.AgentResponse
	s.Require().EventuallyWithT(func(c *assert.CollectT) {
		var items []store.AgentResponseItem
		var err error
		turn, items, err = s.firstTurn(created)
		require.NoError(c, err)
		require.True(c, slices.ContainsFunc(items, func(item store.AgentResponseItem) bool {
			return item.Kind == store.ItemAnswer && item.Text == answer
		}))
	}, settleFor, 10*time.Millisecond, "the answer was never written down")
	return turn
}

// settled waits until the first turn has ended as status, and returns it.
func (s *RuntimeSuite) settled(created *Session, status string) store.AgentResponse {
	var turn store.AgentResponse
	s.Require().EventuallyWithT(func(c *assert.CollectT) {
		var err error
		turn, _, err = s.firstTurn(created)
		require.NoError(c, err)
		require.Equal(c, status, turn.Status)
	}, settleFor, 10*time.Millisecond, "the turn never ended as %s", status)
	s.Require().NotNil(turn.FinishedAt)
	return turn
}

func (s *RuntimeSuite) TestAnAnsweredVoiceTurnIsCompletedOnceItsReplyHasBeenSaid() {
	s.manages()
	release := s.voice.holds("Hello.")
	s.T().Cleanup(release)
	created := s.asked(Spec{}, "when do you open")

	turn := s.answered(created, "Hello.")
	s.Equal(store.ResponseRunning, turn.Status, "the reply is still being said")
	s.Nil(turn.FinishedAt)

	said := time.Now().Truncate(time.Microsecond)
	release()

	turn = s.settled(created, store.ResponseCompleted)
	s.False(turn.FinishedAt.Before(said), "the turn ended before its reply had been said")
	s.Require().NoError(created.Close())
}

func (s *RuntimeSuite) TestAVoiceTurnTalkedOverWhileItsReplyIsSaidIsCancelled() {
	s.manages()
	release := s.voice.holds("Hello.")
	s.T().Cleanup(release)
	created := s.asked(Spec{}, "when do you open")
	s.answered(created, "Hello.")

	s.speaks("actually, never mind")

	s.settled(created, store.ResponseCancelled)
	release()
	s.Require().NoError(created.Close())
}

func (s *RuntimeSuite) TestAVoiceTurnWhoseCallerLeavesWhileItsReplyIsSaidIsCancelled() {
	s.manages()
	release := s.voice.holds("Hello.")
	s.T().Cleanup(release)
	created := s.asked(Spec{}, "when do you open")
	s.answered(created, "Hello.")

	s.edgesMu.Lock()
	call := s.edges[0]
	s.edgesMu.Unlock()
	call.leaves(visitor)

	s.settled(created, store.ResponseCancelled)
	// The rest of the reply going out to an empty call changes nothing.
	release()
	s.Require().NoError(created.Close())
	s.Equal(store.ResponseCancelled, s.settled(created, store.ResponseCancelled).Status)
}

func (s *RuntimeSuite) TestAVoiceTurnTalkedOverWhileItsReplyIsWrittenIsCancelled() {
	s.gated = newGatedLLM()
	s.manages()
	s.T().Cleanup(func() {
		for range 4 {
			s.gated.allowed <- struct{}{}
		}
	})
	created := s.asked(Spec{}, "when do you open")
	select {
	case <-s.gated.entered:
	case <-time.After(settleFor):
		s.FailNow("the model was never asked")
	}

	s.speaks("actually, never mind")

	s.settled(created, store.ResponseCancelled)
	s.Require().NoError(created.Close())
}

func (s *RuntimeSuite) TestTheGoodbyeOfACallTheAgentEndsIsCompleted() {
	server := chattest.NewServer(s.T())
	s.apps = streamapp.NewClients(streamapp.NewStatic(map[string]streamapp.Identity{
		s.customer: {CustomerID: s.customer, StreamApp: 1, APIKey: "runtime-app", Secret: streamapp.NewSecret("runtime-app-secret"), BaseURL: server.URL},
	}, streamapp.NewDeployment(streamapp.DeploymentOptions{APIKey: "deploy-key", Secret: "deploy-secret"})), streamapp.ClientsOptions{})
	s.manages()
	s.model.reply = "Goodbye."
	s.model.calls = []llm.ToolCall{{ID: "call-1", Name: "end_call", Arguments: "{}"}}
	created := s.asked(Spec{ConfigID: "cfg-1", Calls: endsCalls()}, "that is all, thanks")

	s.Require().Eventually(func() bool { return created.State() == Ended && !s.manager.Running(created.ID()) },
		settleFor, 10*time.Millisecond, "the agent never hung up")

	turn := s.settled(created, store.ResponseCompleted)
	s.Equal(store.ResponseCompleted, turn.Status, "the goodbye was said in full before the call ended")
}
