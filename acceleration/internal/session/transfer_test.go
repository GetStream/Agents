//go:build integration

package session

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/audio"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation/chattest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/harness"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llmrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/metered"
	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/telnyx"
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

// TransferSuite hands a phone caller to a human: the number is in Postgres, its lines are
// made at an in-memory Stream, and Telnyx is a server that takes every call it is asked to
// place.
type TransferSuite struct {
	suite.Suite
	ctx      context.Context
	store    *store.Store
	customer string
	number   string
	model    *stubLLM
	ears     *stubSTT
	edge     *quietEdge
	manager  *Manager
	// stream is the Stream the number's lines are made at and its calls ended at.
	stream *chattest.Server
	// reads gives the manager a knowledge base and the database its pages are listed in, and
	// searchesWeb a search router.
	reads, searchesWeb bool
}

func TestTransferSuite(t *testing.T) {
	suite.Run(t, new(TransferSuite))
}

func (s *TransferSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN is not set")
	}
	s.ctx = context.Background()
	db, err := store.Open(testenv.Database(dsn, "session_transfer"))
	s.Require().NoError(err)
	s.Require().NoError(db.Migrate(s.ctx))
	s.store = db
	s.T().Cleanup(func() { s.Require().NoError(db.Close()) })
}

func (s *TransferSuite) SetupTest() {
	s.customer = "transfer-" + uuid.NewString()
	s.number = fmt.Sprintf("+1512%07d", rand.IntN(10_000_000))
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: s.number, Vendor: "telnyx", Country: "US", CustomerID: s.customer, PurchasedAt: time.Now().UTC(),
	}))
	s.reads, s.searchesWeb = false, false
	s.manager = s.manages()
}

// manages builds a manager whose sessions transfer through Telnyx, with a model that says
// goodbye and transfers to support on its first turn.
func (s *TransferSuite) manages() *Manager {
	logger := slog.New(slog.DiscardHandler)
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"call_sid":"v3:human","status":"queued"}`))
	}))
	s.T().Cleanup(vendor.Close)
	config, err := phone.DefaultConfig()
	s.Require().NoError(err)
	for i := range config.Vendors {
		if config.Vendors[i].Vendor == "telnyx" {
			config.Vendors[i].Credentials = nil
		}
	}
	registry := phone.NewRegistry(config)
	registry.Register("telnyx", func() (phone.Provider, error) {
		return telnyx.New(telnyx.Options{APIKey: "telnyx-key", ConnectionID: "texml-app", BaseURL: vendor.URL})
	})
	s.stream = chattest.NewServer(s.T())
	telephony, err := phone.NewService(phone.ServiceOptions{
		Registry: registry, Store: s.store, Stream: phone.NewStreamFromClient(s.stream.Client), Logger: logger,
	})
	s.Require().NoError(err)

	transcription := sttrouter.NewRegistry()
	s.ears = &stubSTT{emitter: stt.NewEmitter(64)}
	transcription.Register("stub", func(routing.Spec) (stt.STT, error) { return s.ears, nil })
	transcriber, err := sttrouter.New(sttrouter.Options{Config: stubConfig(), Registry: transcription, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(transcriber.Close)
	s.model = &stubLLM{reply: "Putting you through.", calls: []llm.ToolCall{{ID: "call-1", Name: "transfer", Arguments: `{"destination":"support"}`}}}
	reasoning := llmrouter.NewRegistry()
	var opened atomic.Int32
	reasoning.Register("stub", func(routing.Spec) (llmrouter.Provider, error) {
		if opened.Add(1) == 1 {
			return s.model, nil
		}
		return &stubLLM{reply: "Sure."}, nil
	})
	reasoner, err := llmrouter.New(llmrouter.Options{Config: stubConfig(), Registry: reasoning, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(reasoner.Close)
	speech := ttsrouter.NewRegistry()
	speech.Register("stub", func(routing.Spec) (tts.TTS, error) { return &stubTTS{emitter: tts.NewEmitter(64)}, nil })
	speaker, err := ttsrouter.New(ttsrouter.Options{Config: stubConfig(), Registry: speech, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(speaker.Close)

	managing := ManagerOptions{
		LLM: reasoner, STT: transcriber, TTS: speaker, Phone: telephony, Logger: logger,
		Edge: func(context.Context, Spec, streamapp.Bound, *slog.Logger) (agent.Edge, error) {
			s.edge = newQuietEdge(caller)
			return attendedEdge{s.edge}, nil
		},
	}
	if s.reads {
		managing.Store, managing.Knowledge = s.store, &metered.Knowledge{}
	}
	if s.searchesWeb {
		// search-fast is the search every session gets when its config names none.
		searching := searchrouter.NewRegistry()
		searching.Register("stub", func(routing.Spec) (search.Provider, error) { return stubSearch{}, nil })
		config := stubConfig()
		config.Aliases[defaultSearchTarget] = routing.Alias{Languages: []string{"en"}}
		managing.Search, err = searchrouter.New(searchrouter.Options{Config: config, Registry: searching, Logger: logger})
		s.Require().NoError(err)
	}
	m, err := NewManager(managing)
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.Require().NoError(m.Shutdown()) })
	return m
}

// answers opens the session of an agent answering a call to the suite's number, on a
// config that transfers to support and may hang up, so the session has a way to end the call.
func (s *TransferSuite) answers() *Session {
	spec := FromConfig(store.AgentConfig{ID: "cfg-1", Calls: store.AgentCalls{
		Transfer: &store.TransferSettings{
			Enabled: true, Destinations: []store.TransferDestination{{Name: "support", Number: "+15125550199"}},
		},
		EndCall: &store.EndCallSettings{Enabled: true},
	}})
	spec.CustomerID, spec.CallType, spec.CallID = s.customer, "default", "phone-"+uuid.NewString()
	spec.LLMTarget, spec.STTTarget, spec.TTSTarget = "en-low-latency", "en-low-latency", "en-low-latency"
	spec.Phone = &PhoneSpec{Number: s.number}
	created, err := s.manager.Create(s.ctx, spec)
	s.Require().NoError(err)
	return created
}

func (s *TransferSuite) TestACallTransferredColdEndsItsSessionAsTransferred() {
	transferred, dropped, failed := endCounter(EndTransferred), endCounter(EndAgentFailed), endCounter(EndModelFailed)
	before, droppedBefore, failedBefore := testutil.ToFloat64(transferred), testutil.ToFloat64(dropped), testutil.ToFloat64(failed)
	created := s.answers()
	events, detach := created.Watch()
	defer detach()

	// Somebody has spoken first: a reply to nobody yet heard is offered no tools, and a
	// transfer is one.
	s.model.mu.Lock()
	calls := s.model.calls
	s.model.calls = nil
	s.model.mu.Unlock()
	s.Require().Eventually(func() bool { return s.edge != nil }, settleFor, 10*time.Millisecond)
	s.edge.inbound <- agent.InboundAudio{Participant: caller, Audio: audio.PcmData{Samples: make([]int16, 320), SampleRate: 16_000, Channels: 1}}
	s.ears.emitter.Send(stt.Transcript{Participant: caller, Mode: stt.ModeFinal, Text: "hello", Language: "en", Confidence: 1})
	s.Require().Eventually(func() bool { return len(s.model.requests()) == 1 && !created.Busy() },
		settleFor, 10*time.Millisecond, "the caller was never answered")
	s.model.mu.Lock()
	s.model.calls = calls
	s.model.mu.Unlock()

	_, err := created.Respond(s.ctx, "can I talk to support please", nil, options.LLM{})
	s.Require().NoError(err)

	s.transferred(events)
	s.Require().Eventually(func() bool {
		return created.State() == Ended && !s.manager.Running(created.Spec().CustomerID, created.ID())
	}, settleFor, 20*time.Millisecond, "the session outlived the agent that left on the transfer")
	s.Equal(EndTransferred, created.EndReason())
	// The agent leaves the call by itself on a transfer, which is no agent dropping out.
	s.Equal(before+1, testutil.ToFloat64(transferred), "the transfer was not counted")
	s.Equal(droppedBefore, testutil.ToFloat64(dropped), "the transfer was counted as the agent dropping out")
	s.Equal(failedBefore, testutil.ToFloat64(failed), "the transfer was counted as the model failing")
	s.NotEmpty(s.asked("/sip/inbound_trunks"), "the transfer made no line at the suite's Stream")
	s.Empty(s.asked("/mark_ended"), "the call the caller was handed over on was ended")
}

// An agent reads text its operator did not write when its config binds a connector or its
// knowledge base is filled from a web page.
func (s *TransferSuite) TestASessionReadsOutsideTextWithAConnectorOrAKnowledgeBaseOfPages() {
	page := store.KnowledgeURL{CustomerID: s.customer, Namespace: "faq", URL: "https://acme.example/faq"}
	s.Require().NoError(s.store.KnowledgeURLs.Insert(s.ctx, &page))
	reading := &Manager{options: ManagerOptions{Store: s.store, Knowledge: &metered.Knowledge{}}}
	connector := []store.ConnectorBinding{{Name: "crm"}}
	for _, c := range []struct {
		name    string
		manager *Manager
		spec    Spec
		outside bool
	}{
		{"nothing", reading, Spec{CustomerID: s.customer}, false},
		{"a connector", &Manager{}, Spec{CustomerID: s.customer, ConnectorBindings: connector}, true},
		{"a knowledge base of pages", reading, Spec{CustomerID: s.customer, KnowledgeNamespace: "faq"}, true},
		{"a knowledge base of files", reading, Spec{CustomerID: s.customer, KnowledgeNamespace: "handbook"}, false},
		{"pages with no knowledge store", &Manager{options: ManagerOptions{Store: s.store}},
			Spec{CustomerID: s.customer, KnowledgeNamespace: "faq"}, false},
		{"no database", &Manager{options: ManagerOptions{Knowledge: &metered.Knowledge{}}},
			Spec{CustomerID: s.customer, KnowledgeNamespace: "faq"}, false},
	} {
		outside, err := c.manager.readsOutsideText(s.ctx, c.spec)
		s.Require().NoError(err, c.name)
		s.Equal(c.outside, outside, c.name)
	}
}

// A session tells its agent when its knowledge base is filled from a web page. A number the
// agent's own tool gave is then no number to transfer to, and one in its instructions still is.
// A search the deployment routes but the agent never ran changes nothing: every session can
// search unless its config says otherwise.
func (s *TransferSuite) TestAKnowledgeBaseOfPagesLeavesOnlyTheInstructionsToTransferTo() {
	page := store.KnowledgeURL{CustomerID: s.customer, Namespace: "faq", URL: "https://acme.example/faq"}
	s.Require().NoError(s.store.KnowledgeURLs.Insert(s.ctx, &page))
	s.reads = true
	for _, c := range []struct {
		name, namespace, instructions string
		searches, transfers           bool
	}{
		{"pages", "faq", "Be brief.", false, false},
		{"files", "handbook", "Be brief.", false, true},
		{"pages and the number in the instructions", "faq", "Hand callers to +1 512-555-0199.", false, true},
		{"a search it never ran", "", "Be brief.", true, true},
	} {
		s.Run(c.name, func() {
			s.searchesWeb = c.searches
			s.manager = s.manages()
			s.model.calls = nil
			spec := FromConfig(store.AgentConfig{ID: "cfg-1", Instructions: c.instructions, KnowledgeNamespace: c.namespace,
				ToolGroups: []store.ToolGroup{{Name: "duty", Tools: []string{"find_line"}}},
				Calls:      store.AgentCalls{Transfer: &store.TransferSettings{Enabled: true}}})
			spec.CustomerID, spec.CallType, spec.CallID = s.customer, "default", "phone-"+uuid.NewString()
			spec.LLMTarget, spec.STTTarget, spec.TTSTarget = "en-low-latency", "en-low-latency", "en-low-latency"
			spec.Phone = &PhoneSpec{Number: s.number}
			spec.Tools = []harness.Tool{{Name: "find_line", Description: "the number of the person on duty"}}
			created, err := s.manager.Create(s.ctx, spec)
			s.Require().NoError(err)
			events, detach := created.Watch()
			defer detach()
			s.Require().Eventually(func() bool { return s.edge != nil }, settleFor, 10*time.Millisecond)
			s.edge.inbound <- agent.InboundAudio{Participant: caller, Audio: audio.PcmData{Samples: make([]int16, 320), SampleRate: 16_000, Channels: 1}}
			s.ears.emitter.Send(stt.Transcript{Participant: caller, Mode: stt.ModeFinal, Text: "hello", Language: "en", Confidence: 1})
			s.Require().Eventually(func() bool { return len(s.model.requests()) == 1 && !created.Busy() },
				settleFor, 10*time.Millisecond, "the caller was never answered")
			if c.searches {
				s.Require().True(slices.ContainsFunc(s.model.requests()[0].Tools, func(tool llm.Tool) bool {
					return tool.Name == "search"
				}), "the agent was not offered search")
			}

			s.asks(llm.ToolCall{ID: "call-1", Name: "find_line", Arguments: "{}"})
			_, err = created.Respond(s.ctx, "can I talk to a person please", nil, options.LLM{})
			s.Require().NoError(err)
			found := awaitToolCall(events)
			s.Require().NotNil(found)
			s.asks(llm.ToolCall{ID: "call-2", Name: "transfer", Arguments: `{"to":"+15125550199"}`})
			s.True(created.ResolveTool(found.ID, "the person on duty is on (512) 555-0199", ""))

			if c.transfers {
				s.transferred(events)
				return
			}
			s.Contains(s.transferAnswer(events), "is not a number you may transfer to")
			s.Empty(s.asked("/sip/inbound_trunks"), "a number only the agent's tool gave was dialled")
		})
	}
}

// asks has the model call one tool on its next request that offers tools.
func (s *TransferSuite) asks(call llm.ToolCall) {
	s.model.mu.Lock()
	defer s.model.mu.Unlock()
	s.model.calls, s.model.called = []llm.ToolCall{call}, false
}

// transferAnswer waits for the agent to answer a transfer it did not make, and returns what
// it told the model.
func (s *TransferSuite) transferAnswer(events <-chan Event) string {
	deadline := time.After(settleFor)
	for {
		select {
		case event := <-events:
			if ran, ok := event.(agent.ToolRan); ok && ran.Tool == "transfer" {
				return ran.Result
			}
			if _, ok := event.(agent.Transferred); ok {
				s.FailNow("the agent transferred the caller")
			}
		case <-deadline:
			s.FailNow("the transfer was never answered")
		}
	}
}

// asked is the requests the suite's Stream was sent whose path ends in suffix.
func (s *TransferSuite) asked(suffix string) []string {
	var paths []string
	for _, request := range s.stream.Requests(s.stream.Key) {
		if strings.HasSuffix(request.Path, suffix) {
			paths = append(paths, request.Method+" "+request.Path)
		}
	}
	return paths
}

// transferred waits for the agent to have handed the caller over.
func (s *TransferSuite) transferred(events <-chan Event) {
	deadline := time.After(settleFor)
	for {
		select {
		case event := <-events:
			if _, ok := event.(agent.Transferred); ok {
				return
			}
		case <-deadline:
			s.FailNow("the agent never transferred the caller")
		}
	}
}
