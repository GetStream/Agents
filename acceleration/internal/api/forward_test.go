//go:build integration

package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/dispatch"
)

// ForwardedSessionSuite covers what is asked of a session over HTTP when the request does
// not land on the node running the conversation.
//
// Every session a test here opens is opened on the other node and then asked about on the
// suite's own, which is what a load balancer with no reason to prefer either produces
// about half the time.
type ForwardedSessionSuite struct {
	RouterSuite

	// other is the node running every session a test here opens.
	other *httptest.Server
}

func TestForwardedSessionSuite(t *testing.T) {
	runSuite(t, new(ForwardedSessionSuite))
}

func (s *ForwardedSessionSuite) SetupSuite() {
	s.RouterSuite.SetupSuite()
	s.other = s.otherNode()
}

func (s *ForwardedSessionSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

func (s *ForwardedSessionSuite) TestASessionIsReadBackFromANodeThatIsNotRunningIt() {
	opened := s.onTheOtherNode(CreateSessionRequest{})

	read := s.serverClient.getSession(opened.Id)

	// The session keeps no row, so there is nothing here to answer from: this is the
	// running conversation or it is a 404.
	s.Equal(opened.Id, read.Id)
	s.Equal(opened.Llm, read.Llm)
}

func (s *ForwardedSessionSuite) TestASessionIsToldToAnswerFromANodeThatIsNotRunningIt() {
	answer := s.utils.uuid()
	opened := s.onTheOtherNode(CreateSessionRequest{
		Llm: pointerTo("echo/echo-model"), ConfigId: s.data.instructedAgent(answer),
	})
	// Watched from here too, which the relay serves. What is under test is that the
	// request to answer reached the conversation, and the answer is where that shows.
	watching := s.serverClient.opens("/v1/agents/sessions/" + opened.Id + "/events")

	s.Require().Equal(http.StatusNoContent, s.serverClient.do(http.MethodPost,
		"/v1/agents/sessions/"+opened.Id+"/respond", SayRequest{Text: "hello"}, nil))

	answered := s.await(watching, "responded")
	s.Contains(answered["text"], answer, "the conversation never heard what it was asked")
}

// Deleting is the operation that used to do damage from the wrong node: the row says the
// session is the caller's, so it was authorized here, and then the rows, the turns and
// what the session remembered were deleted while the node running it carried on against
// them. So this is recorded rather than incognito, which is what gives it rows to lose.
func (s *ForwardedSessionSuite) TestDeletingASessionFromAnotherNodeEndsItWhereItIsRunning() {
	opened := s.serverClient.on(s.other).createSession(CreateSessionRequest{
		Llm: pointerTo("llm-flow"),
	})

	s.serverClient.deleteSession(opened.Id)

	status, _ := s.serverClient.on(s.other).call(
		http.MethodGet, "/v1/agents/sessions/"+opened.Id, nil)
	s.Equal(http.StatusNotFound, status, "the conversation outlived the rows it was writing")
}

func (s *ForwardedSessionSuite) TestASessionNoNodeIsRunningIsAnsweredHere() {
	status, message := s.serverClient.failure(
		http.MethodGet, "/v1/agents/sessions/"+s.utils.uuid(), nil)

	s.Equal(http.StatusNotFound, status)
	s.Equal(errUnknownSession.Message, message)
}

// Who may touch a session is decided by the node running it, so this is the test that the
// node taking the request is not the one trusted to have asked.
func (s *ForwardedSessionSuite) TestAnotherAppCannotReachASessionOnAnotherNode() {
	opened := s.onTheOtherNode(CreateSessionRequest{})

	status, _ := s.data.backendOfAnotherApp().call(
		http.MethodGet, "/v1/agents/sessions/"+opened.Id, nil)

	s.Equal(http.StatusNotFound, status, "a stranger is not told the id is real")
}

// A message arriving where the conversation is not used to find nothing running and be
// handed to a worker, which is a second agent writing into a conversation the first is
// already answering in.
func (s *ForwardedSessionSuite) TestAMessageArrivingOnTheWrongNodeIsAnsweredByTheOneRunningTheConversation() {
	channel, written := "chat-"+s.utils.uuid(), "is anybody there?"
	// A message in writing is answered off the call rather than through a turn, so where
	// it shows is the model being asked rather than anything a watcher is sent.
	s.onTheOtherNode(CreateSessionRequest{
		AgentId: &channel, Llm: pointerTo("vision/vision-model"),
	})
	worker, release := s.dispatch.Register(s.customerID(), dispatch.Registration{Capacity: 1})
	defer release()

	s.Require().Equal(http.StatusOK,
		s.signedly("/v1/chat/hooks/stream", writtenTo(channel, written)))

	s.Require().Eventually(func() bool { return s.wasAsked(written) },
		settleFor, 10*time.Millisecond,
		"the message never reached the conversation on the node running it")
	// Whether to hand this to a worker was settled before the delivery was answered, so
	// there is nothing left to wait for.
	select {
	case message := <-worker.Messages():
		s.Failf("a second agent was started", "%s was given to a worker as well", message.ChannelID)
	default:
	}
}

// Two customers may each run a session of one id, each on a node of its own: a request reaches
// the session of the caller's own customer, wherever it lands.
func (s *ForwardedSessionSuite) TestARequestReachesTheSessionOfTheCallersCustomerOfAnIdTwoRun() {
	id := s.utils.uuid()
	other := s.data.backendOfAnotherApp()
	mine := s.serverClient.createSession(CreateSessionRequest{Id: &id, Incognito: pointerTo(true), Llm: pointerTo("llm-flow")})
	theirs := other.on(s.other).createSession(CreateSessionRequest{Id: &id, Incognito: pointerTo(true), Llm: pointerTo("echo/echo-model")})
	third := s.otherNode()

	s.Equal(theirs.Llm, other.getSession(id).Llm, "landing on the node running the other customer's")
	s.Equal(mine.Llm, s.serverClient.on(s.other).getSession(id).Llm, "landing on the node running the other customer's")
	s.Equal(theirs.Llm, other.on(third).getSession(id).Llm, "landing on a node running neither")
	s.Equal(mine.Llm, s.serverClient.on(third).getSession(id).Llm, "landing on a node running neither")
}

// A request with no credentials is answered here, with its 401, and not carried to the node
// running the session: from a node that cannot be reached it would be a 503, which tells a
// stranger the session is live.
func (s *ForwardedSessionSuite) TestARequestWithNoCredentialsIsRefusedHereRatherThanCarried() {
	id := s.utils.uuid()
	s.heldWhereNothingAnswers(id, "")
	path := "/v1/agents/sessions/" + id

	s.Require().Equal(http.StatusServiceUnavailable, s.serverClient.do(http.MethodGet, path, nil, nil),
		"the customer's own request is carried to the node, which cannot be reached")
	s.Equal(http.StatusUnauthorized, s.unauthenticatedClient.do(http.MethodGet, path, nil, nil))
}

// Two customers may each hold a conversation of one cid, each on a node of its own: an answer to a
// call held for approval is carried to the node of the caller's own customer's, whether it lands on
// the node running the other customer's or on one running neither.
func (s *ForwardedSessionSuite) TestAnApprovalReachesTheNodeOfTheCallersConversationOfACidTwoHold() {
	id := s.utils.uuid()
	cid := "agent:" + id
	s.heldWhereNothingAnswers(id, cid)
	theirs := s.data.backendOfAnotherApp().on(s.other).createSession(textSession(&id))
	s.Require().Equal(cid, value(theirs.ConversationId), "the other customer's conversation is of the cid")
	path := "/v1/agents/conversations/" + cid + "/tool_approvals"
	answer := map[string]any{"tool_call_id": "call", "request_id": "request", "allowed": true}

	s.Equal(http.StatusServiceUnavailable, s.serverClient.on(s.other).do(http.MethodPost, path, answer, nil),
		"landing on the node running the other customer's, the answer is carried to the caller's, which cannot be reached")
	s.Equal(http.StatusServiceUnavailable, s.serverClient.do(http.MethodPost, path, answer, nil),
		"landing on a node running neither")
}

// The same for an answer to a call held for approval, which names a conversation, not a session.
func (s *ForwardedSessionSuite) TestAnApprovalWithNoCredentialsIsRefusedHereRatherThanCarried() {
	cid := "messaging:" + s.utils.uuid()
	s.heldWhereNothingAnswers(s.utils.uuid(), cid)
	path := "/v1/agents/conversations/" + cid + "/tool_approvals"
	answer := map[string]any{"tool_call_id": "call", "request_id": "request", "allowed": true}

	s.Require().Equal(http.StatusServiceUnavailable, s.serverClient.do(http.MethodPost, path, answer, nil),
		"the customer's own answer is carried to the node, which cannot be reached")
	s.Equal(http.StatusUnauthorized, s.unauthenticatedClient.do(http.MethodPost, path, answer, nil))
}

// heldWhereNothingAnswers has the directory say a node that cannot be reached runs the
// customer's session id, answering in the conversation cid.
func (s *ForwardedSessionSuite) heldWhereNothingAnswers(id, cid string) {
	gone := httptest.NewUnstartedServer(nil)
	directory := s.nodeDirectory(gone, s.shard, slog.New(slog.DiscardHandler))
	gone.Close()
	directory.Hold(s.T().Context(), s.customerID(), id, 0, "", cid)
}

// wasAsked reports whether the model was given something to answer.
func (s *RouterSuite) wasAsked(text string) bool {
	for _, request := range s.vision.requests() {
		for _, message := range request.Input {
			if message.Content == text {
				return true
			}
		}
	}
	return false
}

// onTheOtherNode opens an incognito conversation in writing on the node the suite's
// clients do not send to.
//
// Incognito because a session with no row is one only the node running it can answer for,
// which is what makes a 200 here proof that the request was carried there.
func (s *ForwardedSessionSuite) onTheOtherNode(request CreateSessionRequest) Session {
	request.Incognito = pointerTo(true)
	if request.Llm == nil {
		request.Llm = pointerTo("llm-flow")
	}
	return s.serverClient.on(s.other).createSession(request)
}

// writtenTo is a message.new for a channel, as Stream would deliver it.
func writtenTo(channel, text string) string {
	return fmt.Sprintf(`{
  "type": "message.new",
  "cid": "agent:%s",
  "channel_id": "%s",
  "channel_type": "agent",
  "channel_custom": {},
  "message": {
    "id": "message-1",
    "text": "%s",
    "user": {"id": "sam", "name": "Sam"}
  }
}`, channel, channel, text)
}
