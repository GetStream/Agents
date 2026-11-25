//go:build integration

package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/rueidis"

	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/dispatch"
	"github.com/GetStream/Vision-Agents/acceleration/internal/node"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv/redisready"
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

// The same when the message lands on the node running one of the two: that session is no more
// the channel's than the other.
func (s *ForwardedSessionSuite) TestAMessageNamingNoCustomerLandingBesideOneOfTwoCustomersWritingInItIsAnsweredByNeither() {
	channel, written := "chat-"+s.utils.uuid(), "is anybody there? "+s.utils.uuid()
	s.onTheOtherNode(CreateSessionRequest{AgentId: &channel, Llm: pointerTo("vision/vision-model")})
	s.data.backendOfAnotherApp().createSession(CreateSessionRequest{
		AgentId: &channel, Llm: pointerTo("vision/vision-model"), Incognito: pointerTo(true),
	})

	s.Require().Equal(http.StatusOK,
		s.signedly("/v1/chat/hooks/stream", writtenTo(channel, `{}`, written)))

	s.Never(func() bool { return s.wasAsked(written) }, time.Second, 10*time.Millisecond,
		"the session where the message landed answered a message nothing says is its customer's")
}

// A session that ended here leaves its claim on the agent behind until it lapses, and is no
// second customer writing in the channel: the session still running here answers.
func (s *ForwardedSessionSuite) TestAMessageNamingNoCustomerIsAnsweredHereThoughAnotherCustomersSessionEndedHere() {
	channel, written := "chat-"+s.utils.uuid(), "is anybody there? "+s.utils.uuid()
	another := s.data.backendOfAnotherApp()
	ended := another.createSession(CreateSessionRequest{
		AgentId: &channel, Llm: pointerTo("vision/vision-model"), Incognito: pointerTo(true),
	})
	another.stopSession(ended.Id)
	s.serverClient.createSession(CreateSessionRequest{
		AgentId: &channel, Llm: pointerTo("vision/vision-model"), Incognito: pointerTo(true),
	})

	s.Require().Equal(http.StatusOK,
		s.signedly("/v1/chat/hooks/stream", writtenTo(channel, `{}`, written)))

	s.Require().Eventually(func() bool { return s.wasAsked(written) },
		settleFor, 10*time.Millisecond, "the session running here was asked nothing")
}

// The customer of the session running here writing as the agent on another node too is no second
// customer writing in the channel: the session here answers.
func (s *ForwardedSessionSuite) TestAMessageNamingNoCustomerIsAnsweredHereThoughItsCustomerWritesElsewhereToo() {
	channel, written := "chat-"+s.utils.uuid(), "is anybody there? "+s.utils.uuid()
	s.serverClient.createSession(CreateSessionRequest{AgentId: &channel, Llm: pointerTo("vision/vision-model"), Incognito: pointerTo(true)})
	// Second, so the claim by customer and app names the other node.
	s.onTheOtherNode(CreateSessionRequest{AgentId: &channel, Llm: pointerTo("vision/vision-model")})

	s.Require().Equal(http.StatusOK, s.signedly("/v1/chat/hooks/stream", writtenTo(channel, `{}`, written)))

	s.Require().Eventually(func() bool { return s.wasAsked(written) },
		settleFor, 10*time.Millisecond, "the customer's session here was asked nothing")
}

// A directory that cannot be read says nobody writes as the agent elsewhere, as forwardedMessage
// answers here when it cannot find the node: the session running where the message landed
// answers it.
func (s *ForwardedSessionSuite) TestAMessageNamingNoCustomerIsAnsweredHereWhenTheDirectoryCannotBeRead() {
	channel, written := "chat-"+s.utils.uuid(), "is anybody there? "+s.utils.uuid()
	unread, _, _ := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler),
		func(options *Options) { options.Directory = s.unreadableDirectory() })
	s.serverClient.on(unread).createSession(CreateSessionRequest{
		AgentId: &channel, Llm: pointerTo("vision/vision-model"), Incognito: pointerTo(true),
	})

	s.Require().Equal(http.StatusOK, s.signedlyOn(unread, "/v1/chat/hooks/stream", writtenTo(channel, `{}`, written)))

	s.Require().Eventually(func() bool { return s.wasAsked(written) },
		settleFor, 10*time.Millisecond, "the session where the message landed was asked nothing")
}

// The same when Redis hangs rather than fails: the message is not held up by it.
func (s *ForwardedSessionSuite) TestAMessageNamingNoCustomerIsAnsweredHereInTimeWhenTheDirectoryHangs() {
	channel, written := "chat-"+s.utils.uuid(), "is anybody there? "+s.utils.uuid()
	proxy, redis := newRedisProxy(s.T())
	unread, _, _ := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler),
		func(options *Options) { options.Directory = s.directoryOver(redis, "here") })
	unread.Client().Timeout = 10 * time.Second
	s.serverClient.on(unread).createSession(CreateSessionRequest{
		AgentId: &channel, Llm: pointerTo("vision/vision-model"), Incognito: pointerTo(true),
	})
	proxy.hung.Store(true)

	started := time.Now()
	s.Require().Equal(http.StatusOK, s.signedlyOn(unread, "/v1/chat/hooks/stream", writtenTo(channel, `{}`, written)))

	s.Less(time.Since(started), 2*time.Second)
	s.Require().Eventually(func() bool { return s.wasAsked(written) },
		settleFor, 10*time.Millisecond, "the session where the message landed was asked nothing")
}

// The same while the node's renewals and its other reads of the directory, which have no limit,
// dial the connection the read waits for.
func (s *ForwardedSessionSuite) TestMessagesNamingNoCustomerAreAnsweredHereInTimeWhileOthersDialTheHungDirectory() {
	channel := "chat-" + s.utils.uuid()
	proxy, redis := newRedisProxy(s.T())
	directory := s.directoryOver(redis, "here")
	unread, _, _ := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler),
		func(options *Options) { options.Directory = directory })
	unread.Client().Timeout = 10 * time.Second
	s.serverClient.on(unread).createSession(CreateSessionRequest{
		AgentId: &channel, Llm: pointerTo("vision/vision-model"), Incognito: pointerTo(true),
	})
	// Held, so the directory has something to renew.
	directory.Hold(s.T().Context(), "another-customer", 0, s.utils.uuid(), "", "")
	proxy.hung.Store(true)

	reading, stop := context.WithCancel(s.T().Context())
	var others, messages sync.WaitGroup
	others.Go(func() {
		for reading.Err() == nil {
			others.Go(func() { _, _ = directory.Node(reading, "another-customer", "no-session") })
			select {
			case <-reading.Done():
			case <-time.After(3 * time.Second):
			}
		}
	})
	target, err := url.Parse(unread.URL + "/v1/chat/hooks/stream")
	s.Require().NoError(err)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for i := range 40 {
		body := strings.Replace(writtenTo(channel, `{}`, "is anybody there?"),
			"message-"+channel, fmt.Sprintf("message-%d-%s", i, channel), 1)
		request := s.request(target.Path, body, sign(body, suiteStreamSecret))
		request.URL, request.Host = target, target.Host
		messages.Go(func() {
			started := time.Now()
			response, err := unread.Client().Do(request)
			took := time.Since(started)
			if s.NoError(err) {
				s.Equal(http.StatusOK, response.StatusCode)
				s.NoError(response.Body.Close())
			}
			s.Less(took, 2*time.Second, "message %d", i)
		})
		<-ticker.C
	}
	messages.Wait()
	stop()
	others.Wait()
}

// A directory that is slow to answer is waited for: the other customer's session it names still
// keeps the one here from answering.
func (s *ForwardedSessionSuite) TestAMessageNamingNoCustomerIsAnsweredByNeitherThoughTheDirectoryIsSlow() {
	channel, written := "chat-"+s.utils.uuid(), "is anybody there? "+s.utils.uuid()
	proxy, redis := newRedisProxy(s.T())
	slow, _, _ := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler),
		func(options *Options) { options.Directory = s.directoryOver(redis, "here") })
	s.directoryOver(redis, "elsewhere").Hold(s.T().Context(), "another-customer", 0, s.utils.uuid(), channel, "")
	s.serverClient.on(slow).createSession(CreateSessionRequest{
		AgentId: &channel, Llm: pointerTo("vision/vision-model"), Incognito: pointerTo(true),
	})
	proxy.lag.Store(int64(700 * time.Millisecond))

	s.Require().Equal(http.StatusOK, s.signedlyOn(slow, "/v1/chat/hooks/stream", writtenTo(channel, `{}`, written)))

	s.Never(func() bool { return s.wasAsked(written) }, time.Second, 10*time.Millisecond,
		"the session where the message landed answered before the directory did")
}

// A channel stamped with its customer is answered in the hook's app: that customer's session
// writing as the agent in another app is not where the message goes, and with nothing of theirs
// running in this app, their worker here is handed it.
func (s *ForwardedSessionSuite) TestAMessageStampedWithItsCustomerIsNotCarriedToTheirSessionInAnotherApp() {
	channel := "chat-" + s.utils.uuid()
	config := s.data.instructedAgent("answer whatever is asked")
	// Another customer writing as the agent in this app is what has the stamp read at all.
	s.heldWhereNothingAnswers("another-customer", s.utils.uuid(), channel, "")
	s.heldInWhereNothingAnswers(streamAppID(), s.customerID(), s.utils.uuid(), channel, "")
	worker, release := s.dispatch.Register(s.customerID(), dispatch.Registration{Capacity: 1})
	defer release()

	stamped := fmt.Sprintf(`{%q: %q, %q: %q}`, conversation.CustomerField, s.customerID(), ConfigField, *config)
	s.Require().Equal(http.StatusOK,
		s.signedly("/v1/chat/hooks/stream", writtenTo(channel, stamped, "is anybody there?")))

	select {
	case message := <-worker.Messages():
		s.Equal(channel, message.ChannelID)
	case <-time.After(settleFor):
		s.Fail("the message was carried to the customer's session in another app")
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
		Hold(s.T().Context(), "another-customer", 0, s.utils.uuid(), "", cid)

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

// anApproval is an answer to a call held for approval.
var anApproval = map[string]any{"tool_call_id": "call", "request_id": "request", "allowed": true}

// approvalsIn is where an answer to a call held for approval in a conversation is posted.
func approvalsIn(cid string) string {
	return "/v1/agents/conversations/" + cid + "/tool_approvals"
}

// heldWhereNothingAnswers has the directory say a node that cannot be reached runs a customer's
// session id in the deployment's own app, writing as agent and answering in the conversation cid.
func (s *RouterSuite) heldWhereNothingAnswers(customer, id, agent, cid string) {
	s.heldInWhereNothingAnswers(0, customer, id, agent, cid)
}

// heldInWhereNothingAnswers is heldWhereNothingAnswers in a Stream app.
func (s *RouterSuite) heldInWhereNothingAnswers(app int64, customer, id, agent, cid string) {
	gone := httptest.NewUnstartedServer(nil)
	directory := s.nodeDirectory(gone, s.shard, slog.New(slog.DiscardHandler))
	gone.Close()
	directory.Hold(s.T().Context(), customer, app, id, agent, cid)
}

// unreadableDirectory is a node's directory over a Redis connection already closed, so every read
// of it fails, as one with Redis down does.
func (s *RouterSuite) unreadableDirectory() *node.Directory {
	redis, err := redisready.NewClient(rueidis.ClientOption{InitAddress: []string{s.redisAddr}, DisableCache: true})
	s.Require().NoError(err)
	redis.Close()
	return s.directoryOver(redis, "unreadable")
}

// directoryOver is a node's directory over a Redis connection of the test's own, at address.
func (s *RouterSuite) directoryOver(redis rueidis.Client, address string) *node.Directory {
	directory, err := node.NewDirectory(node.DirectoryOptions{
		Redis: redis, Address: address, Prefix: s.relayPrefix, Logger: slog.New(slog.DiscardHandler),
	})
	s.Require().NoError(err)
	s.T().Cleanup(directory.Close)
	return directory
}

// signedlyOn is signedly delivered to the node at rather than the suite's own.
func (s *RouterSuite) signedlyOn(at *httptest.Server, path, body string) int {
	request := s.request(path, body, sign(body, suiteStreamSecret))
	target, err := url.Parse(at.URL + path)
	s.Require().NoError(err)
	request.URL, request.Host = target, target.Host
	response, err := at.Client().Do(request)
	s.Require().NoError(err)
	s.Require().NoError(response.Body.Close())
	return response.StatusCode
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
