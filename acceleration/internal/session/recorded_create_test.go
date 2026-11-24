//go:build integration

package session

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	getstream "github.com/GetStream/getstream-go/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	persistent "github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation/chattest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llmrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/search"
	"github.com/GetStream/Vision-Agents/acceleration/internal/searchrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sttrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts"
	"github.com/GetStream/Vision-Agents/acceleration/internal/ttsrouter"
)

// RecordedCreateSuite reads back, from Postgres, what Create recorded about a session.
type RecordedCreateSuite struct {
	suite.Suite
	ctx      context.Context
	store    *store.Store
	customer string
	// dsn is the suite's database, which store keeps every table in.
	dsn string
	// logged is what the test's managers logged.
	logged *syncBuffer
}

func TestRecordedCreateSuite(t *testing.T) {
	suite.Run(t, new(RecordedCreateSuite))
}

func (s *RecordedCreateSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN is not set")
	}
	s.ctx = context.Background()
	s.dsn = testenv.Database(dsn, "recorded_create")
	db, err := store.Open(s.dsn)
	s.Require().NoError(err)
	s.Require().NoError(db.Migrate(s.ctx))
	s.store = db
	s.T().Cleanup(func() { s.Require().NoError(db.Close()) })
}

func (s *RecordedCreateSuite) SetupTest() {
	s.customer = "recorded-" + uuid.NewString()
	s.logged = &syncBuffer{}
}

func (s *RecordedCreateSuite) TestAGreetingSlowerThanTheRowIsStored() {
	// The greeting's Said comes out before it is spoken, so with a voice that takes a while
	// it reaches the fan-out well before Create has queued the session's row.
	m := s.manager(newQuietEdge(), slowTTS{&stubTTS{emitter: tts.NewEmitter(64)}}, nil)

	created, err := m.Create(s.ctx, s.spec(Spec{Greeting: "Hi, I'm listening."}))
	s.Require().NoError(err)

	// Eventually runs its condition off the test goroutine, where a failed Require cannot stop the test.
	s.Require().EventuallyWithT(func(c *assert.CollectT) {
		require.NoError(c, m.records.Flush(s.ctx))
		got, err := s.store.Responses.Select(s.ctx, s.customer, created.ID(), store.ResponseFilter{Limit: 10})
		require.NoError(c, err)
		require.Len(c, got, 1)
	}, settleFor, 20*time.Millisecond, "the greeting's turn is not stored")
}

func (s *RecordedCreateSuite) TestTheStoredToolsKeepSearch() {
	m := s.manager(newQuietEdge(), &stubTTS{emitter: tts.NewEmitter(64)}, stubSearch{})

	created, err := m.Create(s.ctx, s.spec(Spec{SearchTarget: "en-low-latency"}))
	s.Require().NoError(err)
	s.Require().NoError(created.Close())
	s.Require().NoError(m.records.Flush(s.ctx))

	stored, err := s.store.SessionTools.Get(s.ctx, s.customer, created.ID())
	s.Require().NoError(err)
	names := make([]string, 0, len(stored))
	for _, tool := range stored {
		names = append(names, tool.Name)
	}
	s.Contains(names, "search", "search starts on joining, and its tool is offered from then on")
}

func (s *RecordedCreateSuite) TestASessionWhoseCallRefusedItLeavesNoRow() {
	m := s.manager(refusingEdge{newQuietEdge()}, &stubTTS{emitter: tts.NewEmitter(64)}, nil)
	from := time.Now().UTC().Add(-time.Hour)

	_, err := m.Create(s.ctx, s.spec(Spec{}))
	s.Require().Error(err)
	s.Require().NoError(m.records.Flush(s.ctx))

	// Base 291f2e34c: no row, no activity and a peak of none.
	rows, err := s.store.Sessions.Select(s.ctx, s.customer, store.SessionFilter{Limit: 10})
	s.Require().NoError(err)
	s.Empty(rows, "a session that never joined is listed")
	to := time.Now().UTC().Add(time.Hour)
	buckets, err := s.store.CustomerActivity(s.ctx, s.customer, store.ActivityDaily, from, to)
	s.Require().NoError(err)
	for _, bucket := range buckets {
		s.Zero(bucket.Sessions, "a session that never joined is counted as activity")
	}
	peak, err := s.store.CustomerConcurrencyPeak(s.ctx, s.customer, from, to)
	s.Require().NoError(err)
	s.Zero(peak.Sessions, "a session that never joined is counted in the peak")
}

// A turn taken while a slow greeting is spoken runs before Create queues the session's row.
// The title it names the conversation is not in the row, which takes the spec's.
func (s *RecordedCreateSuite) TestATitleNamedDuringTheGreetingIsKept() {
	m := s.manager(newQuietEdge(), &stubTTS{emitter: tts.NewEmitter(64)}, nil)
	joining := s.joining(m)
	joining.naming = &naming{titles: s.titler(m, "Refund a late order")}

	joining.name(agent.Responded{TurnID: "turn-1", Text: "It ships today."})
	joining.naming.titles.running.Wait()

	s.Equal("Refund a late order", s.queued(m, joining, sessionRow(joining)).Title)
}

// The rows below change while Create builds the row and before it queues it.
func (s *RecordedCreateSuite) TestAConnectionChosenAsTheRowIsQueuedIsKept() {
	m := s.manager(newQuietEdge(), &stubTTS{emitter: tts.NewEmitter(64)}, nil)
	joining := s.joining(m)
	row := sessionRow(joining)

	joining.chose("crm", "connection-1")

	s.Equal([]store.SessionConnectorSelection{{Name: "crm", ConnectionID: "connection-1"}},
		s.queued(m, joining, row).ConnectorSelections)
}

func (s *RecordedCreateSuite) TestFramesSeenAsTheRowIsQueuedMakeItAVideoOne() {
	m := s.manager(newQuietEdge(), &stubTTS{emitter: tts.NewEmitter(64)}, nil)
	joining := s.joining(m)
	row := sessionRow(joining)
	frames := framesRunner{parts: []llm.ContentPart{{Image: &llm.ImagePart{URL: "data:image/jpeg;base64,AA=="}}}}

	_, err := (&videoRunner{next: frames, session: joining}).Run(s.ctx, llm.ToolCall{Name: agent.VideoFramesTool})
	s.Require().NoError(err)

	s.Equal(store.ModalityVideo, s.queued(m, joining, row).Modality)
}

// A title named before the row is built is in the row.
func (s *RecordedCreateSuite) TestTheRowCarriesTheTitleNamedBeforeItWasBuilt() {
	m := s.manager(newQuietEdge(), &stubTTS{emitter: tts.NewEmitter(64)}, nil)
	joining := s.joining(m)
	joining.title, joining.description = "Refund a late order", "Where an order went."

	row := sessionRow(joining)

	s.Equal("Refund a late order", row.Title)
	s.Equal("Where an order went.", row.Description)
}

// The tests below read the spec while a login's chose writes it, on the goroutines Create and
// the manager read it from. They show their failure only under -race.
func (s *RecordedCreateSuite) TestTheSpecReadsRaceNoChose() {
	m := s.manager(newQuietEdge(), &stubTTS{emitter: tts.NewEmitter(64)}, nil)
	joining := s.joining(m)
	joining.naming = &naming{titles: s.titler(m, "Refund a late order")}
	m.sessions[joining.Key()] = joining
	// Shutdown closes what is registered, and this session has no agent to close.
	s.T().Cleanup(func() { delete(m.sessions, joining.Key()) })
	owner := OwnerOf(joining.spec)
	reads := map[string]func(){
		"the session row":   func() { sessionRow(joining) },
		"the call row":      func() { row(joining) },
		"the episode card":  func() { (*callCards)(nil).started(joining, streamapp.Bound{}) },
		"the title asked":   func() { _ = joining.retitle(s.ctx, nil) },
		"a lookup":          func() { m.Get(joining.id, owner) },
		"a listing":         func() { m.List(owner) },
		"the newest":        func() { m.newest(func(Spec) bool { return true }, nil) },
		"a seat superseded": func() { m.supersede(Spec{Text: true}) },
	}
	for name, read := range reads {
		s.Run(name, func() {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range 50 {
					joining.chose("crm", fmt.Sprint("connection-", i))
				}
			}()
			for range 50 {
				read()
			}
			wg.Wait()
		})
	}
}

// Another router opened the id for another customer while this one joined its call: the
// check that the id is free ran on both before either row was written. This one's row is
// refused once its session is held, which ends it and keeps its turns and call off the id.
func (s *RecordedCreateSuite) TestASessionWhoseIDAnotherCustomerTookOnceItWasHeldIsEnded() {
	late := s.manager(newQuietEdge(), &stubTTS{emitter: tts.NewEmitter(64)}, nil)
	release := s.holdWriter(late)
	other := "other-" + s.customer
	spec := s.spec(Spec{ID: "shared-" + uuid.NewString(), Greeting: "Hi, I'm listening."})
	spec.CustomerID = other

	created, err := late.Create(s.ctx, spec)
	s.Require().NoError(err)
	s.opened(spec.ID)
	release()

	s.Require().Eventually(func() bool { return created.State() == Ended }, settleFor, 10*time.Millisecond,
		"the session on another customer's id is still running")
	_, held := late.Get(spec.ID, Owner{CustomerID: other, Kind: auth.KindServer})
	s.False(held)
	s.Require().NoError(late.records.Flush(s.ctx))
	s.theOwnersOnly(spec.ID, other)
	s.Require().NotEmpty(created.conversation(), "the greeting is what a review would read")
	late.reviews.running.Wait()
	s.NotContains(s.logged.String(), "review the call", "a session with no row of its own is reviewed")
}

// A carry-on skips the check that the id is free, so its row can be refused as a new
// session's is, by another customer's written on another shard meanwhile.
func (s *RecordedCreateSuite) TestACarriedOnSessionWhoseIDAnotherCustomerTookIsEnded() {
	other := "other-" + s.customer
	late := s.chatting(s.store, chattest.Client(s.T()))
	caller := routing.Caller{UserID: "employee-1"}
	first, err := late.Create(s.ctx, Spec{CustomerID: other, Text: true, PersistConversation: true,
		LLMTarget: "en-low-latency", Caller: caller})
	s.Require().NoError(err)
	s.Require().NoError(first.Close())
	s.Require().NoError(late.records.Flush(s.ctx))
	// Meanwhile the id became the suite customer's, as another shard's row would make it.
	s.Require().NoError(s.store.Sessions.Delete(s.ctx, other, first.ID()))
	s.opened(first.ID())

	created, err := late.Create(s.ctx, Spec{ID: first.ID(), CustomerID: other, Text: true, PersistConversation: true,
		ConversationID: first.Spec().ConversationID, Reopened: first.CreatedAt(), LLMTarget: "en-low-latency", Caller: caller})
	if !errors.Is(err, ErrSessionExists) {
		s.Require().NoError(err)
		s.Require().Eventually(func() bool { return created.State() == Ended }, settleFor, 10*time.Millisecond,
			"the carried-on session on another customer's id is still running")
	}
	_, held := late.Get(first.ID(), Owner{CustomerID: other, Kind: auth.KindServer})
	s.False(held)
	s.Require().NoError(late.records.Flush(s.ctx))
	s.theOwnersOnly(first.ID(), other)
}

// The same, with the row refused before Create held the session.
func (s *RecordedCreateSuite) TestASessionWhoseIDAnotherCustomerTookBeforeItWasHeldIsTurnedAway() {
	joining, joined := make(chan struct{}), make(chan struct{})
	late := s.manager(gatedEdge{newQuietEdge(), joining, joined}, &stubTTS{emitter: tts.NewEmitter(64)}, nil)
	other := "other-" + s.customer
	spec := s.spec(Spec{ID: "shared-" + uuid.NewString(), Greeting: "Hi, I'm listening."})
	spec.CustomerID = other
	refused := make(chan error, 1)
	go func() {
		_, err := late.Create(s.ctx, spec)
		refused <- err
	}()
	select {
	case <-joining:
	case <-time.After(settleFor):
		s.FailNow("the session never joined its call")
	}
	s.opened(spec.ID)

	// Create waits to hold the session until the writer has refused its row.
	late.mu.Lock()
	unlock := sync.OnceFunc(late.mu.Unlock)
	defer unlock()
	close(joined)
	s.Require().Eventually(func() bool {
		return strings.Contains(s.logged.String(), "ending a session whose id another customer's session has")
	}, settleFor, 10*time.Millisecond, "the row was not refused")
	unlock()

	select {
	case err := <-refused:
		s.Require().ErrorIs(err, ErrSessionExists)
	case <-time.After(settleFor):
		s.FailNow("Create did not return")
	}
	_, held := late.Get(spec.ID, Owner{CustomerID: other, Kind: auth.KindServer})
	s.False(held, "the session on another customer's id is held")
	s.Require().NoError(late.records.Flush(s.ctx))
	s.theOwnersOnly(spec.ID, other)
}

// opened writes the suite customer's running session id, as the router that opened it first
// would.
func (s *RecordedCreateSuite) opened(id string) {
	s.Require().NoError(s.store.Sessions.Upsert(s.ctx, &store.AgentSession{ID: id, CustomerID: s.customer,
		Title: "the first one", State: store.SessionRunning, CreatedAt: time.Now().UTC()}))
}

// theOwnersOnly checks that session id is still the suite customer's, running as it was, and
// that nothing of other's was written under it.
func (s *RecordedCreateSuite) theOwnersOnly(id, other string) {
	row, err := s.store.Sessions.Get(s.ctx, s.customer, id)
	s.Require().NoError(err)
	s.Equal(store.SessionRunning, row.State)
	s.Equal("the first one", row.Title)
	turns, err := s.store.Responses.Select(s.ctx, other, id, store.ResponseFilter{Limit: 10})
	s.Require().NoError(err)
	s.Empty(turns, "the refused session's turns are under the id")
	calls, err := s.store.Calls.Select(s.ctx, other, store.CallFilter{SessionID: id})
	s.Require().NoError(err)
	s.Empty(calls, "the refused session's call is under the id")
}

// holdWriter stalls m's writer on a session row another transaction is inserting, until the
// returned release rolls that transaction back.
func (s *RecordedCreateSuite) holdWriter(m *Manager) (release func()) {
	tx, err := s.store.GlobalDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	release = sync.OnceFunc(func() { s.NoError(tx.Rollback()) })
	s.T().Cleanup(release)
	held := store.AgentSession{ID: uuid.NewString(), CustomerID: s.customer, State: store.SessionRunning, CreatedAt: time.Now().UTC()}
	_, err = tx.ExecContext(s.ctx, "INSERT INTO agent_sessions (id, customer_id, state) VALUES (?, ?, ?)",
		held.ID, held.CustomerID, store.SessionRunning)
	s.Require().NoError(err)
	m.records.Opened(held, nil, nil)
	s.Require().Eventually(func() bool { return len(m.records.(*sessionRecorder).queue) == 0 }, settleFor, 5*time.Millisecond,
		"the writer never took the held row")
	return release
}

// joining is a session as Create holds it while the greeting is spoken: handed the recorder,
// its row not queued yet.
func (s *RecordedCreateSuite) joining(m *Manager) *Session {
	return &Session{
		id: uuid.NewString(), spec: s.spec(Spec{}), modality: store.ModalityVoice,
		records: m.records, created: time.Now(),
	}
}

// queued queues row as Create does once the greeting is over, and reads it back.
func (s *RecordedCreateSuite) queued(m *Manager, joining *Session, row store.AgentSession) store.AgentSession {
	m.records.Opened(row, nil, nil)
	joining.startRecording()
	s.Require().NoError(m.records.Flush(s.ctx))
	rows, err := s.store.Sessions.Select(s.ctx, s.customer, store.SessionFilter{Limit: 10})
	s.Require().NoError(err)
	s.Require().Len(rows, 1)
	return rows[0]
}

// titler names every conversation title, on m's recorder.
func (s *RecordedCreateSuite) titler(m *Manager, title string) *titler {
	logger := slog.New(slog.DiscardHandler)
	config := stubConfig()
	config.Aliases[titleTarget] = routing.Alias{Languages: []string{"en"}, RequireRealtime: true}
	naming := llmrouter.NewRegistry()
	naming.Register("stub", func(routing.Spec) (llmrouter.Provider, error) {
		return &stubLLM{reply: `{"title": "` + title + `", "description": "Where an order went."}`}, nil
	})
	router, err := llmrouter.New(llmrouter.Options{Config: config, Registry: naming, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(router.Close)
	named := newTitler(router, m.records.(*sessionRecorder), logger)
	s.T().Cleanup(named.Close)
	return named
}

// spec is a session of the test's customer on the stub models.
func (s *RecordedCreateSuite) spec(spec Spec) Spec {
	spec.CustomerID, spec.CallID = s.customer, "call-1"
	spec.LLMTarget, spec.STTTarget, spec.TTSTarget = "en-low-latency", "en-low-latency", "en-low-latency"
	return spec
}

// manager is a session manager over the suite's Postgres, joining every call on edge and
// speaking with voice. A nil finder leaves the deployment without search.
func (s *RecordedCreateSuite) manager(edge agent.Edge, voice tts.TTS, finder search.Provider) *Manager {
	return s.managerOn(s.store, nil, edge, voice, finder)
}

// managerOn is manager over on, keeping conversations in conversations unless it is nil.
func (s *RecordedCreateSuite) managerOn(on *store.Store, conversations *persistent.Service, edge agent.Edge, voice tts.TTS,
	finder search.Provider) *Manager {
	logger := slog.New(slog.NewTextHandler(s.logged, nil))
	transcription := sttrouter.NewRegistry()
	transcription.Register("stub", func(routing.Spec) (stt.STT, error) { return &stubSTT{emitter: stt.NewEmitter(64)}, nil })
	transcriber, err := sttrouter.New(sttrouter.Options{Config: stubConfig(), Registry: transcription, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(transcriber.Close)
	reasoning := llmrouter.NewRegistry()
	reasoning.Register("stub", func(routing.Spec) (llmrouter.Provider, error) { return &stubLLM{reply: "Hello."}, nil })
	reasoner, err := llmrouter.New(llmrouter.Options{Config: stubConfig(), Registry: reasoning, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(reasoner.Close)
	speech := ttsrouter.NewRegistry()
	speech.Register("stub", func(routing.Spec) (tts.TTS, error) { return voice, nil })
	speaker, err := ttsrouter.New(ttsrouter.Options{Config: stubConfig(), Registry: speech, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(speaker.Close)

	options := ManagerOptions{
		LLM: reasoner, STT: transcriber, TTS: speaker, Store: on, Conversations: conversations, Logger: logger,
		Edge: func(context.Context, Spec, streamapp.Bound, *slog.Logger) (agent.Edge, error) { return edge, nil },
	}
	if finder != nil {
		searching := searchrouter.NewRegistry()
		searching.Register("stub", func(routing.Spec) (search.Provider, error) { return finder, nil })
		options.Search, err = searchrouter.New(searchrouter.Options{Config: stubConfig(), Registry: searching, Logger: logger})
		s.Require().NoError(err)
	}
	m, err := NewManager(options)
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.Require().NoError(m.Shutdown()) })
	return m
}

func (s *RecordedCreateSuite) TestAChatReopenedInAnotherShardIsListedByTheRowItRunsIn() {
	here, there := s.twoShards()
	chat := chattest.Client(s.T())
	held := s.chatting(there, chat)
	first, err := held.Create(s.ctx, Spec{CustomerID: s.customer, Text: true, PersistConversation: true,
		LLMTarget: "en-low-latency", Caller: routing.Caller{UserID: "employee-1"}})
	s.Require().NoError(err)
	s.Require().NoError(first.Close())
	s.Require().NoError(held.records.Flush(s.ctx))
	_, err = there.Sessions.Pin(s.ctx, s.customer, first.ID(), true, time.Now().UTC())
	s.Require().NoError(err)

	carrying := s.chatting(here, chat)
	reopened, err := carrying.Create(s.ctx, Spec{ID: first.ID(), CustomerID: s.customer, Text: true, PersistConversation: true,
		ConversationID: first.Spec().ConversationID, Reopened: first.CreatedAt(), LLMTarget: "en-low-latency",
		Caller: routing.Caller{UserID: "employee-1"}})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = reopened.Close() })
	s.Require().NoError(carrying.records.Flush(s.ctx))

	running, err := here.Sessions.Get(s.ctx, s.customer, first.ID())
	s.Require().NoError(err)
	s.NotNil(running.PinnedAt, "the row it runs in lost the pin it had before")
	left, err := there.Sessions.Get(s.ctx, s.customer, first.ID())
	s.Require().NoError(err)
	s.Equal(store.SessionRunning, left.State, "read where it left, it is listed by its closed row there")
}

// twoShards are the stores of a router of each of two shards, over the suite's database.
func (s *RecordedCreateSuite) twoShards() (here, there *store.Store) {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	a, b := testenv.Database(dsn, "recorded_shard_a"), testenv.Database(dsn, "recorded_shard_b")
	open := func(own string) *store.Store {
		router, err := store.OpenShards(store.Options{DSN: s.dsn, ShardDSN: own, Shards: map[string]string{"a": a, "b": b}})
		s.Require().NoError(err)
		s.T().Cleanup(func() { s.Require().NoError(router.Close()) })
		s.Require().NoError(router.Migrate(s.ctx))
		return router
	}
	return open(a), open(b)
}

// chatting is a session manager over on that keeps its conversations in chat.
func (s *RecordedCreateSuite) chatting(on *store.Store, chat *getstream.Stream) *Manager {
	conversations := persistent.NewForChat(chat)
	s.T().Cleanup(conversations.Close)
	return s.managerOn(on, conversations, newQuietEdge(), &stubTTS{emitter: tts.NewEmitter(64)}, nil)
}

// gatedEdge is a call whose join says it began on joining and waits for joined.
type gatedEdge struct {
	*quietEdge
	joining, joined chan struct{}
}

func (e gatedEdge) Join(ctx context.Context) error {
	close(e.joining)
	<-e.joined
	return e.quietEdge.Join(ctx)
}

// refusingEdge is a call that will not have the agent.
type refusingEdge struct{ *quietEdge }

func (refusingEdge) Join(context.Context) error { return errors.New("the call refused the agent") }

// slowTTS takes 300 ms to start speaking, longer than Create takes to queue the row.
type slowTTS struct{ *stubTTS }

func (s slowTTS) Synthesize(request tts.Request) error {
	time.Sleep(300 * time.Millisecond)
	return s.stubTTS.Synthesize(request)
}

// stubSearch finds nothing.
type stubSearch struct{}

func (stubSearch) Search(context.Context, search.Query) (search.Result, error) {
	return search.Result{}, nil
}
func (stubSearch) Start(context.Context) error { return nil }
func (stubSearch) Close() error                { return nil }
func (stubSearch) Provider() string            { return "stub" }
func (stubSearch) Model() string               { return "stub-model" }
