//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/dispatch"
	"github.com/GetStream/Vision-Agents/acceleration/internal/node"
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
	// logged is what the suite's own node logs.
	logged *lockedLog
}

func TestForwardedSessionSuite(t *testing.T) {
	runSuite(t, new(ForwardedSessionSuite))
}

func (s *ForwardedSessionSuite) SetupSuite() {
	s.logged = &lockedLog{}
	s.logs = s.logged
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
		s.signedly("/v1/chat/hooks/stream", writtenTo(channel, `{}`, written)))

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

// A message on a channel stamped with its customer reaches the node running that customer's
// conversation, though another customer has since said it runs a session writing as the agent.
func (s *ForwardedSessionSuite) TestAMessageReachesTheNodeOfTheChannelsCustomerThoughAnotherWritesAsItsAgent() {
	channel, written := "chat-"+s.utils.uuid(), "is anybody there? "+s.utils.uuid()
	s.onTheOtherNode(CreateSessionRequest{AgentId: &channel, Llm: pointerTo("vision/vision-model")})
	s.heldWhereNothingAnswers("another-customer", s.utils.uuid(), channel, "")

	stamped := fmt.Sprintf(`{%q: %q}`, conversation.CustomerField, s.customerID())
	s.Require().Equal(http.StatusOK,
		s.signedly("/v1/chat/hooks/stream", writtenTo(channel, stamped, written)))

	s.Require().Eventually(func() bool { return s.wasAsked(written) },
		settleFor, 10*time.Millisecond,
		"the message went to the node of the customer that wrote as the agent last")
}

// The same for a channel stamped with no customer, which the config it names says is whose.
func (s *ForwardedSessionSuite) TestAMessageReachesTheNodeOfTheCustomerWhoseConfigTheChannelNames() {
	channel, written := "chat-"+s.utils.uuid(), "is anybody there? "+s.utils.uuid()
	config := s.data.instructedAgent("answer whatever is asked")
	s.onTheOtherNode(CreateSessionRequest{AgentId: &channel, Llm: pointerTo("vision/vision-model")})
	s.heldWhereNothingAnswers("another-customer", s.utils.uuid(), channel, "")

	named := fmt.Sprintf(`{%q: %q}`, ConfigField, *config)
	s.Require().Equal(http.StatusOK,
		s.signedly("/v1/chat/hooks/stream", writtenTo(channel, named, written)))

	s.Require().Eventually(func() bool { return s.wasAsked(written) },
		settleFor, 10*time.Millisecond,
		"the message went to the node of the customer that wrote as the agent last")
}

// A message on a channel that names no customer, which sessions of two customers write in on
// other nodes, is answered by neither: nothing says whose it is.
func (s *ForwardedSessionSuite) TestAMessageNamingNoCustomerOnAChannelTwoCustomersWriteInIsAnsweredByNeither() {
	channel, written := "chat-"+s.utils.uuid(), "is anybody there? "+s.utils.uuid()
	s.onTheOtherNode(CreateSessionRequest{AgentId: &channel, Llm: pointerTo("vision/vision-model")})
	s.data.backendOfAnotherApp().on(s.otherNode()).createSession(CreateSessionRequest{
		AgentId: &channel, Llm: pointerTo("vision/vision-model"), Incognito: pointerTo(true),
	})

	s.Require().Equal(http.StatusOK,
		s.signedly("/v1/chat/hooks/stream", writtenTo(channel, `{}`, written)))

	s.Never(func() bool { return s.wasAsked(written) }, time.Second, 10*time.Millisecond,
		"one of the customers' sessions answered a message nothing says is theirs")
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
	s.heldWhereNothingAnswers(s.customerID(), id, "", "")
	path := "/v1/agents/sessions/" + id

	s.Require().Equal(http.StatusServiceUnavailable, s.serverClient.do(http.MethodGet, path, nil, nil),
		"the customer's own request is carried to the node, which cannot be reached")
	s.Equal(http.StatusUnauthorized, s.unauthenticatedClient.do(http.MethodGet, path, nil, nil))
}

// The same for an answer to a call held for approval, which names a conversation, not a session.
func (s *ForwardedSessionSuite) TestAnApprovalWithNoCredentialsIsRefusedHereRatherThanCarried() {
	cid := "messaging:" + s.utils.uuid()
	s.heldWhereNothingAnswers(s.customerID(), s.utils.uuid(), "", cid)
	path := approvalsIn(cid)

	s.Require().Equal(http.StatusServiceUnavailable, s.serverClient.do(http.MethodPost, path, anApproval, nil),
		"the customer's own answer is carried to the node, which cannot be reached")
	s.Equal(http.StatusUnauthorized, s.unauthenticatedClient.do(http.MethodPost, path, anApproval, nil))
}

// Another customer saying since that it answers in a conversation of the same cid does not move
// where the caller's answer goes. Carried to the caller's node, which cannot be reached, it is a
// 503.
func (s *ForwardedSessionSuite) TestAnApprovalIsCarriedToTheCallersNodeThoughAnotherCustomerHoldsItsCid() {
	cid := "messaging:" + s.utils.uuid()
	s.heldWhereNothingAnswers(s.customerID(), s.utils.uuid(), "", cid)
	s.nodeDirectory(s.server, s.shard, slog.New(slog.DiscardHandler)).
		Hold(s.T().Context(), "another-customer", s.utils.uuid(), "", cid)

	s.Equal(http.StatusServiceUnavailable, s.serverClient.do(http.MethodPost, approvalsIn(cid), anApproval, nil),
		"answered on the node of the customer that held the cid last")
}

// Another customer's session answering here in a conversation of the same cid is no reason to
// answer the caller here.
func (s *ForwardedSessionSuite) TestAnApprovalIsCarriedPastAnotherCustomersSessionInItsCid() {
	theirs := s.data.backendOfAnotherApp().createSession(CreateSessionRequest{Llm: pointerTo("llm-flow")})
	s.Require().NotNil(theirs.ConversationId)
	cid := *theirs.ConversationId
	s.heldWhereNothingAnswers(s.customerID(), s.utils.uuid(), "", cid)

	s.Equal(http.StatusServiceUnavailable, s.serverClient.do(http.MethodPost, approvalsIn(cid), anApproval, nil),
		"answered here, where only the other customer's session runs")
}

// A body is taken and refused by a node that forwards it as by the node running the session.
func (s *ForwardedSessionSuite) TestAQuestionOfTheMostItsOperationReadsIsCarried() {
	opened := s.onTheOtherNode(CreateSessionRequest{Llm: pointerTo("echo/echo-model")})

	status, answered := s.sentOfSize("/v1/agents/sessions/"+opened.Id+"/respond", 12<<20-1)

	s.Equal(http.StatusNoContent, status, answered)
}

func (s *ForwardedSessionSuite) TestAQuestionOverTheMostItsOperationReadsIsRefusedAsTheOwnerRefusesIt() {
	opened := s.onTheOtherNode(CreateSessionRequest{Llm: pointerTo("echo/echo-model")})

	s.refusedAsTooLarge("/v1/agents/sessions/"+opened.Id+"/respond", 12<<20)
	s.Regexp(`msg="served a request".* path=/v1/agents/sessions/`+opened.Id+`/respond status=413 .*code=payload_too_large`, s.logged.String())
}

func (s *ForwardedSessionSuite) TestALineToSpeakIsHeldToTheLimitOfItsOwnOperation() {
	opened := s.onTheOtherNode(CreateSessionRequest{Llm: pointerTo("echo/echo-model")})
	path := "/v1/agents/sessions/" + opened.Id + "/say"

	owner, _ := s.sentOfSizeTo(s.serverClient.on(s.other), path, 1<<20-1)
	status, answered := s.sentOfSize(path, 1<<20-1)

	s.Equal(owner, status, answered)
	s.NotEqual(http.StatusRequestEntityTooLarge, status)
	s.refusedAsTooLarge(path, 1<<20)
}

func (s *ForwardedSessionSuite) TestAnApprovalIsHeldToTheLimitOfItsOwnOperation() {
	cid := "messaging:" + s.utils.uuid()
	s.heldWhereNothingAnswers(s.customerID(), s.utils.uuid(), "", cid)

	carried, _ := s.sentOfSize(approvalsIn(cid), 1<<20-1)
	refused, _ := s.sentOfSize(approvalsIn(cid), 1<<20)

	s.Equal(http.StatusServiceUnavailable, carried, "carried to a node that cannot be reached")
	s.Equal(http.StatusRequestEntityTooLarge, refused)
}

// A caller that sends on without a length is answered once the limit is past, with the rest unread.
func (s *ForwardedSessionSuite) TestABodyOfNoStatedLengthIsReadNoFurtherThanItsOperationReadsIt() {
	opened := s.onTheOtherNode(CreateSessionRequest{Llm: pointerTo("echo/echo-model")})
	body := &spaces{}

	status, answered := s.sent(s.serverClient, http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/say", body)

	s.Equal(http.StatusRequestEntityTooLarge, status, answered)
	s.Less(body.read.Load(), int64(16<<20), "the body was read past its limit and the sockets' buffers")
}

func (s *ForwardedSessionSuite) TestABodyNotSentInTimeIsRefusedAsTheOwnerRefusesIt() {
	opened := s.onTheOtherNode(CreateSessionRequest{Llm: pointerTo("echo/echo-model")})
	path := "/v1/agents/sessions/" + opened.Id + "/say"
	unfinished := func() io.Reader {
		reader, writer := io.Pipe()
		ended := time.AfterFunc(20*time.Second, func() { _ = writer.CloseWithError(io.ErrUnexpectedEOF) })
		s.T().Cleanup(func() { ended.Stop(); _ = writer.Close() })
		return reader
	}

	ownerStatus, owner := s.sent(s.serverClient.on(s.other), http.MethodPost, path, unfinished())
	status, answered := s.sent(s.serverClient, http.MethodPost, path, unfinished())

	s.Equal(http.StatusBadRequest, status)
	s.Equal(ownerStatus, status)
	s.Equal(owner, answered)
}

func (s *ForwardedSessionSuite) TestABodySentToAnOperationThatTakesNoneIsNotRead() {
	opened := s.onTheOtherNode(CreateSessionRequest{})
	path := "/v1/agents/sessions/" + opened.Id

	owner, _ := s.sent(s.serverClient.on(s.other), http.MethodGet, path, strings.NewReader(strings.Repeat(" ", 2<<20)))
	status, answered := s.sent(s.serverClient, http.MethodGet, path, strings.NewReader(strings.Repeat(" ", 2<<20)))

	s.Equal(http.StatusOK, owner)
	s.Equal(http.StatusOK, status, answered)
}

func (s *ForwardedSessionSuite) TestAChangeToASessionIsCarriedWithItsBody() {
	opened := s.serverClient.on(s.other).createSession(CreateSessionRequest{Llm: pointerTo("llm-flow")})

	updated := s.serverClient.updateSession(opened.Id, UpdateSessionRequest{Title: pointerTo("renamed")})

	s.Equal(pointerTo("renamed"), updated.Title)
}

// A body cut short of its stated length is not carried, though what came of it would be read.
func (s *ForwardedSessionSuite) TestABodyCutShortIsNotCarried() {
	id, carried := s.heldByANodeThatAnswers(0)
	connection, err := net.Dial("tcp", s.server.Listener.Addr().String())
	s.Require().NoError(err)
	defer connection.Close()
	request, err := http.NewRequest(http.MethodPost, s.server.URL+"/v1/agents/sessions/"+id+"/say", nil)
	s.Require().NoError(err)
	request.Header = s.serverClient.header.Clone()
	request.Header.Set("Content-Type", "application/json")
	head := &bytes.Buffer{}
	s.Require().NoError(request.Header.Write(head))

	_, err = fmt.Fprintf(connection, "POST %s HTTP/1.1\r\nHost: router\r\nContent-Length: 100\r\n%s\r\n%s",
		request.URL.Path, head, `{"text":"hello"}`)
	s.Require().NoError(err)
	s.Require().NoError(connection.(*net.TCPConn).CloseWrite())
	s.Require().NoError(connection.SetReadDeadline(time.Now().Add(settleFor)))
	answered, err := io.ReadAll(connection)

	s.Require().NoError(err)
	s.Contains(string(answered), "500 Internal Server Error")
	s.Zero(carried.Load())
}

// The time a body is given to arrive does not end a request the other node is longer about.
func (s *ForwardedSessionSuite) TestARequestTheOtherNodeIsLongAboutIsAnswered() {
	id, carried := s.heldByANodeThatAnswers(7 * time.Second)

	// net/http reads on for the caller's hanging up, at once when there is no body.
	status, _ := s.sent(s.serverClient, http.MethodPost, "/v1/agents/sessions/"+id+"/say", http.NoBody)

	s.Equal(http.StatusTeapot, status)
	s.EqualValues(1, carried.Load())
}

// heldByANodeThatAnswers has the directory say a node that answers 418 after taking runs a
// session, and counts the requests carried there.
func (s *ForwardedSessionSuite) heldByANodeThatAnswers(taking time.Duration) (string, *atomic.Int64) {
	carried := &atomic.Int64{}
	_, accepted := s.sealer.Derive(node.CredentialPurpose)
	owner := httptest.NewServer(node.Serve(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		carried.Add(1)
		time.Sleep(taking)
		w.WriteHeader(http.StatusTeapot)
	}), accepted))
	s.T().Cleanup(owner.Close)
	id := s.utils.uuid()
	s.nodeDirectory(owner, s.shard, slog.New(slog.DiscardHandler)).Hold(s.T().Context(), s.customerID(), id, "", "")
	return id, carried
}

// refusedAsTooLarge checks a body of size sent to path is refused with the node running the
// session's own 413.
func (s *ForwardedSessionSuite) refusedAsTooLarge(path string, size int) {
	ownerStatus, owner := s.sentOfSizeTo(s.serverClient.on(s.other), path, size)
	status, answered := s.sentOfSize(path, size)

	s.Equal(http.StatusRequestEntityTooLarge, status, answered)
	s.Equal(ownerStatus, status)
	s.Equal(owner, answered)
	s.Contains(answered, `"payload_too_large"`)
}

// sentOfSize posts a question of exactly size bytes to the node not running the session.
func (s *ForwardedSessionSuite) sentOfSize(path string, size int) (int, string) {
	return s.sentOfSizeTo(s.serverClient, path, size)
}

func (s *ForwardedSessionSuite) sentOfSizeTo(client *testClient, path string, size int) (int, string) {
	question := `{"text":"hello"}`
	return s.sent(client, http.MethodPost, path, strings.NewReader(question+strings.Repeat(" ", size-len(question))))
}

// sent sends body as it is and returns what it was answered, without the duration and the
// request id, which are each answer's own.
func (s *ForwardedSessionSuite) sent(client *testClient, method, path string, body io.Reader) (int, string) {
	ctx, cancel := context.WithTimeout(s.T().Context(), 30*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, client.base()+path, body)
	s.Require().NoError(err)
	request.Header = client.header.Clone()
	request.Header.Set("Content-Type", "application/json")
	response, err := s.server.Client().Do(request)
	s.Require().NoError(err)
	defer response.Body.Close()
	answered, err := readAll(response)
	s.Require().NoError(err)
	var envelope ErrorResponse
	if json.Unmarshal(answered, &envelope) == nil && envelope.Error.Code != "" {
		answered, err = json.Marshal(envelope)
		s.Require().NoError(err)
	}
	return response.StatusCode, string(answered)
}

// spaces is a body of 64 MiB that says how much of it was read.
type spaces struct{ read atomic.Int64 }

func (b *spaces) Read(into []byte) (int, error) {
	if b.read.Load() >= 64<<20 {
		return 0, io.EOF
	}
	for i := range into {
		into[i] = ' '
	}
	b.read.Add(int64(len(into)))
	return len(into), nil
}

// anApproval is an answer to a call held for approval.
var anApproval = map[string]any{"tool_call_id": "call", "request_id": "request", "allowed": true}

// approvalsIn is where an answer to a call held for approval in a conversation is posted.
func approvalsIn(cid string) string {
	return "/v1/agents/conversations/" + cid + "/tool_approvals"
}

// heldWhereNothingAnswers has the directory say a node that cannot be reached runs a customer's
// session id, writing as agent and answering in the conversation cid.
func (s *RouterSuite) heldWhereNothingAnswers(customer, id, agent, cid string) {
	gone := httptest.NewUnstartedServer(nil)
	directory := s.nodeDirectory(gone, s.shard, slog.New(slog.DiscardHandler))
	gone.Close()
	directory.Hold(s.T().Context(), customer, id, agent, cid)
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

// writtenTo is a message.new for a channel with custom, a JSON object, as Stream would deliver
// it.
func writtenTo(channel, custom, text string) string {
	return fmt.Sprintf(`{
  "type": "message.new",
  "cid": "agent:%s",
  "channel_id": "%s",
  "channel_type": "agent",
  "channel_custom": %s,
  "message": {
    "id": "message-%s",
    "text": "%s",
    "user": {"id": "sam", "name": "Sam"}
  }
}`, channel, channel, custom, channel, text)
}
