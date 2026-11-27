//go:build integration

package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/memory/mem0"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// extractionGrace is how long mem0 is given to turn a conversation into memories, which it
// does on its own side after the conversation has moved on.
const extractionGrace = time.Minute

// recallEvery is how often a test waiting on mem0 opens another session to ask.
const recallEvery = 3 * time.Second

// extractionBound is how long mem0 may take to finish an exchange it was handed. It starts an
// exchange its worker lost again ten minutes after queueing it: of 775 adds read back from
// mem0's events on 2026-10-28, four started 10m00s late and were done within 10m10s (AI-2162).
const extractionBound = 12 * time.Minute

// extractionBudget is how long the suite may spend in all its waits for mem0 together. A
// package that runs past go test -timeout 30m (integration.yml) panics every suite in it, and
// the rest of the package takes about three minutes, so the waits end well before that.
const extractionBudget = 22 * time.Minute

// mem0Event is where mem0 says what became of an exchange it queued, by the event's id.
const mem0Event = "https://api.mem0.ai/v1/event/"

// MemorySuite runs the router against mem0 rather than a stand-in: whose memories a session
// reads is decided by the filters mem0 applies, so a stand-in would only prove itself. The
// model is the echo stub, so what a session recalled is what it answers with.
//
// It writes real memories, so it runs only when asked: RUN_MEM0_SUITE=1 as well as
// MEM0_API_KEY. A key in the repo's .env alone is not asking.
type MemorySuite struct {
	RouterSuite

	// known is a user of the standard app who told the agent they are allergic to peanuts,
	// under the label company=acme. Tests read what is known about them and add nothing.
	known *testClient
	// noting and echoing are the standard app's configs, by the model a session under one is
	// answered by: a device names no model, so a session takes its config's.
	noting, echoing string
	// mem0Log is the mem0 client's log, which names the event each exchange was queued as.
	mem0Log *lockedLog
	// mem0 is the client sessions remember through, whose HTTP client asks mem0 about events.
	mem0 *mem0.Store
	// told are the sessions told something since a test last waited for mem0 to learn it.
	told []string
	// extractionBy is when every wait for mem0 must be over, whichever test makes it.
	extractionBy time.Time
}

func TestMemorySuite(t *testing.T) {
	runSuite(t, new(MemorySuite))
}

func (s *MemorySuite) SetupSuite() {
	if os.Getenv("RUN_MEM0_SUITE") != "1" {
		s.T().Skip("RUN_MEM0_SUITE=1 runs this suite against the real mem0")
	}
	if os.Getenv("MEM0_API_KEY") == "" {
		s.T().Skip("MEM0_API_KEY must be set")
	}
	s.extractionBy = time.Now().Add(extractionBudget)
	s.mem0Log = &lockedLog{}
	memories, err := mem0.New(mem0.Options{
		Logger: slog.New(slog.NewJSONHandler(s.mem0Log, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	s.Require().NoError(err)
	s.memoryStore = memories
	s.mem0 = memories
	s.RouterSuite.SetupSuite()

	s.useFixture("standard")
	s.noting, s.echoing = s.answeredBy(s.serverClient, "noted/noted-model"), s.answeredBy(s.serverClient, "echo/echo-model")
	s.known = s.data.createUser()
	s.tell(s.known, s.remembering(s.known.userID, acme()), "I am allergic to peanuts.")
	s.learns(s.known, s.remembering(s.known.userID, acme()), "peanut")
}

func (s *MemorySuite) SetupTest() {
	s.told = nil
	s.useFixture("standard")
}

func (s *MemorySuite) TestWhatAUserToldThreeSessionsIsKnownInTheFourth() {
	owner := s.data.createUser()

	s.tell(owner, s.remembering(owner.userID, nil), "I am allergic to peanuts.")
	s.tell(owner, s.remembering(owner.userID, nil), "I live in Amsterdam.")
	s.tell(owner, s.remembering(owner.userID, nil), "My dog is called Rex.")

	s.learns(owner, s.remembering(owner.userID, nil), "peanut", "amsterdam", "rex")
}

func (s *MemorySuite) TestNothingIsRememberedAboutAnAnonymousCaller() {
	name := s.anonymousClient.userID

	// Refusing the session and keeping nothing from it are both an answer.
	opened, admitted := s.tries(s.anonymousClient, s.remembering(name, nil))
	if !admitted {
		return
	}
	s.Require().NotEmpty(s.ask(s.anonymousClient, opened, "I am allergic to peanuts."),
		"an exchange without an answer is not remembered")
	s.anonymousClient.stopSession(opened.Id)

	// The router hands an exchange over off the conversation's path, so it is given the
	// grace to do it. What it hands over is in the log, whether or not mem0 is late with it.
	handedOver := s.extraction()
	for deadline := time.Now().Add(extractionGrace); time.Now().Before(deadline); time.Sleep(recallEvery) {
		s.Require().Empty(handedOver.queuedAs(opened.Id), "the router handed an anonymous caller's exchange to mem0")
	}
}

func (s *MemorySuite) TestAnotherAppRecallsNothingAboutTheSameUser() {
	backend := s.data.backendOfAnotherApp()
	request := s.remembering(s.known.userID, nil)

	s.NotContains(s.recalledUnder(s.answeredBy(backend, "echo/echo-model"), backend.actingFor(s.known), request), "peanut")
}

func (s *MemorySuite) TestAUserCannotRecallAnotherUsersMemories() {
	stranger := s.data.createUser()

	// Refusing the session and opening it knowing nothing are both an answer.
	opened, admitted := s.tries(stranger, s.asking(s.remembering(s.known.userID, nil)))
	if !admitted {
		return
	}
	defer stranger.stopSession(opened.Id)
	s.NotContains(s.ask(stranger, opened, "What do you know about me?"), "peanut")
}

func (s *MemorySuite) TestAFilterNarrowsWhatIsRecalled() {
	elsewhere := s.remembering(s.known.userID, map[string]string{"company": "globex"})

	s.NotContains(s.recalled(s.known, elsewhere), "peanut")
}

func (s *MemorySuite) TestAFilterMatchingAnotherUsersLabelsRecallsNothingOfTheirs() {
	stranger := s.data.createUser()

	s.NotContains(s.recalled(stranger, s.remembering(stranger.userID, acme())), "peanut")
}

func (s *MemorySuite) TestAFilterNamingAnotherUserRecallsNothingOfTheirs() {
	stranger := s.data.createUser()
	impersonating := map[string]string{"user_id": s.known.userID, "app_id": s.customerID()}

	s.NotContains(s.recalled(stranger, s.remembering(stranger.userID, impersonating)), "peanut")
}

func (s *MemorySuite) TestAFilterNamingAnotherAppRecallsNothingOfItsMemories() {
	backend := s.data.backendOfAnotherApp()
	request := s.remembering(s.known.userID, map[string]string{"app_id": s.customerID(), "company": "acme"})
	request.Memory.AppId = pointerTo(s.customerID())

	s.NotContains(s.recalledUnder(s.answeredBy(backend, "echo/echo-model"), backend.actingFor(s.known), request), "peanut")
}

func (s *MemorySuite) TestDeletingASessionForgetsWhatItLearnedAndNothingElse() {
	owner := s.data.createUser()
	first := s.tell(owner, s.remembering(owner.userID, nil), "I am allergic to peanuts.")
	s.tell(owner, s.remembering(owner.userID, nil), "My dog is called Rex.")
	s.learns(owner, s.remembering(owner.userID, nil), "peanut", "rex")

	owner.deleteSession(first)

	recalled := s.recalled(owner, s.remembering(owner.userID, nil))
	s.NotContains(recalled, "peanut")
	s.Contains(recalled, "rex")
}

func (s *MemorySuite) TestAClaimedGuestsMemoriesMoveToTheUserWhoClaimedThem() {
	guest := s.data.createGuest()
	s.Require().NoError(s.store.Users.InsertGuest(context.Background(),
		&store.User{ID: guest.userID, CustomerID: s.customerID()}))
	s.tell(guest, s.remembering(guest.userID, nil), "I am allergic to peanuts.")
	s.learns(guest, s.remembering(guest.userID, nil), "peanut")
	account := s.data.createUser()

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/guests/claim",
		ClaimGuestRequest{GuestId: guest.userID, UserId: account.userID}, nil))

	s.learns(account, s.remembering(account.userID, nil), "peanut")
}

// tell says something in a session of its own and ends it, returning the session's id.
// What was said is handed to mem0 once the agent has answered.
func (s *MemorySuite) tell(as *testClient, request CreateSessionRequest, said string) string {
	opened := as.createSession(request)
	s.Require().NotEmpty(s.ask(as, opened, said), "an exchange without an answer is not remembered")
	as.stopSession(opened.Id)
	s.told = append(s.told, opened.Id)
	return opened.Id
}

// learns waits until a session opened with request knows every fact, since mem0 extracts
// what it was told after the conversation has moved on.
func (s *MemorySuite) learns(as *testClient, request CreateSessionRequest, facts ...string) {
	s.extracted()
	deadline := time.Now().Add(extractionGrace)
	for {
		recalled := s.recalled(as, request)
		missing := false
		for _, fact := range facts {
			missing = missing || !strings.Contains(recalled, fact)
		}
		if !missing {
			return
		}
		s.Require().True(time.Now().Before(deadline),
			"the agent never learned %v, though mem0 had extracted it; it knew %q", facts, recalled)
		time.Sleep(recallEvery)
	}
}

// extracted waits until mem0 has finished every exchange told since the last wait, within what
// is left of the suite's extraction time. It forgets those sessions first, so a session that
// failed its wait is not waited for again by the next test, whose failure it would blame.
func (s *MemorySuite) extracted() {
	told := s.told
	s.told = nil
	s.Require().NoError(s.extraction().until(told, s.extractionBy))
}

// extraction is the wait for mem0, over the mem0 client's log.
func (s *MemorySuite) extraction() mem0Extraction {
	return mem0Extraction{
		client: s.mem0.Client(), baseURL: mem0Event, apiKey: os.Getenv("MEM0_API_KEY"),
		log: s.mem0Log.String, grace: extractionGrace, bound: extractionBound, every: recallEvery,
	}
}

// recalled is what a session opened with request knew about the person before they said
// anything. The session writes nothing down, so asking does not teach mem0 the answer.
func (s *MemorySuite) recalled(as *testClient, request CreateSessionRequest) string {
	return s.recalledUnder(s.echoing, as, request)
}

// recalledUnder is recalled in a session under the config given, which is the echo stub's in
// an app other than the standard one.
func (s *MemorySuite) recalledUnder(config string, as *testClient, request CreateSessionRequest) string {
	request = s.asking(request)
	request.ConfigId = &config
	opened := as.createSession(request)
	defer as.stopSession(opened.Id)
	return s.ask(as, opened, "What do you know about me?")
}

// tries opens a session the router may refuse for the memory it asks for, reporting whether it
// was opened. A refusal of anything else is not an answer about memory.
func (s *MemorySuite) tries(as *testClient, request CreateSessionRequest) (Session, bool) {
	var opened Session
	status, body := as.call(http.MethodPost, "/v1/agents/sessions", request)
	if status == http.StatusBadRequest || status == http.StatusForbidden {
		s.Require().Contains(answered(s.T(), body).Message, "memory")
		return opened, false
	}
	s.Require().Equal(http.StatusCreated, status, string(body))
	s.Require().NoError(json.Unmarshal(body, &opened))
	return opened, true
}

// answeredBy is the id of an agent config of the backend's app answered by model.
func (s *MemorySuite) answeredBy(backend *testClient, model string) string {
	var created AgentConfig
	s.Require().Equal(http.StatusCreated, backend.do(http.MethodPost, "/v1/agents/configs",
		AgentConfigRequest{Name: "agent-" + s.utils.uuid(), Llm: &model}, &created))
	return created.Id
}

// ask says something in a session over its socket, the way a device does, and returns what
// the agent answered, in lower case.
func (s *MemorySuite) ask(as *testClient, opened Session, text string) string {
	connection := as.opens("/v1/agents/sessions/" + opened.Id + "/events")
	command := frame{"type": "respond", "text": text}
	// A conversation kept in Stream Chat takes each message once, by the id it is sent with.
	if value(opened.ConversationId) != "" {
		command["request_id"] = s.utils.uuid()
	}
	s.Require().NoError(connection.WriteJSON(command))

	for {
		s.Require().NoError(connection.SetReadDeadline(time.Now().Add(settleFor)))
		var received frame
		s.Require().NoError(connection.ReadJSON(&received))
		s.Require().NotEqual("error", received["type"], "the session reported %v", received)
		if received["type"] == "responded" && received["pending_work"] == false {
			answer, _ := received["text"].(string)
			return strings.ToLower(answer)
		}
	}
}

// remembering is a conversation in writing whose memories are about userID, narrowed by
// filter. Its model answers every turn, since only an exchange with an answer is kept.
func (s *MemorySuite) remembering(userID string, filter map[string]string) CreateSessionRequest {
	request := textSession(nil)
	request.ConfigId = &s.noting
	request.Memory = &SessionMemory{UserId: &userID}
	if filter != nil {
		request.Memory.Filter = &filter
	}
	return request
}

// asking is request as a session that recalls and keeps nothing, answered by the echo
// stub, which says what it was told before the conversation started.
func (s *MemorySuite) asking(request CreateSessionRequest) CreateSessionRequest {
	request.ConfigId = &s.echoing
	request.Incognito = pointerTo(true)
	return request
}

// acme is the label the known user's memories were written under.
func acme() map[string]string {
	return map[string]string{"company": "acme"}
}
