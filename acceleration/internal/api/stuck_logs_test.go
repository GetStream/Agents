//go:build integration

package api

import (
	"context"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/chatlog"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation/chattest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// A log line where a customer's caller or writer gets stuck names the customer, so the line
// in the log store says whose number it was.

// loggedOneLine is the pattern of one log line holding level and msg, then customer.
func loggedOneLine(level, msg, customer string) *regexp.Regexp {
	return regexp.MustCompile(`level=` + level + ` msg="` + regexp.QuoteMeta(msg) + `"[^\n]* app_id=` +
		regexp.QuoteMeta(customer) + `( |\n)`)
}

// Defect: the router rejects a call to a number of an app whose telephony is suspended and
// logs it at Info with only the number, so nobody can tell whose number was called.
func (s *TelnyxHookSuite) TestARejectedCallToASuspendedAppWarnsNamingTheApp() {
	s.attach(s.e164, "sip:"+s.e164+"@trunk.sip.example.test", "trunk-user", "trunk-password")
	s.Require().NoError(s.store.TelephonySuspensions.Upsert(context.Background(), &store.TelephonySuspension{
		CustomerID: s.customerID(), Reason: "fraud", SuspendedBy: "ops@example.test",
	}))

	status, _, body := s.deliver(s.call(s.e164), true)
	s.Require().Equal(http.StatusOK, status)
	s.Require().Contains(body, "<Reject")

	s.Regexp(loggedOneLine("WARN", "rejected a telnyx call to a number of an app whose telephony is suspended",
		s.customerID()), s.thisTest())
}

// Defect: a call to a number of an organization that may not pay is rejected and logged at Info
// without the app.
func (s *AdmissionSuite) TestATelnyxCallRefusedForMoneyWarnsNamingTheApp() {
	e164 := s.utils.number()
	s.line.attach(e164, "sip:"+e164+"@trunk.sip.example.test", "trunk-user", "trunk-password")
	s.refused(http.MethodPost, "/v1/search", SearchRequest{Query: "anything"})

	status, _, body := s.line.deliver(s.line.call(e164), true)
	s.Require().Equal(http.StatusOK, status)
	s.Require().Contains(body, "<Reject")

	s.Regexp(loggedOneLine("WARN", "rejected a telnyx call to a number of an organization that may not pay for it",
		s.customerID()), s.logged.String())
}

// Defect: a vendor fetching a placed call's plan that cannot be served logs the failure
// without the app whose call it is.
func (s *CallStatusSuite) TestAPlanThatCannotBeServedIsLoggedNamingTheCallsApp() {
	s.useApp(s.data.createApp())
	token := s.utils.uuid()
	s.Require().NoError(s.store.CallBridges.Insert(context.Background(), &store.CallBridge{
		Token: token, CustomerID: s.customerID(), Vendor: "twilio", TrunkURI: "sip:trunk@trunk.sip.example.test",
		CallID: s.utils.uuid(), ExpiresAt: time.Now().Add(time.Minute),
	}))

	// Twilio takes its plan on the request that places the call, so it has none to serve.
	s.Require().Equal(http.StatusNotFound, s.unauthenticatedClient.do(http.MethodGet, "/v1/phone/answer/"+token, nil, nil))

	s.Regexp(loggedOneLine("ERROR", "could not answer a placed call", s.customerID()), s.logged.String())
}

// Defect: a message on a line no agent answers on is logged with only the line, so the app
// that should name it in an agent's config is not known.
func (s *ChannelsSuite) TestAMessageOnALineNoAgentAnswersOnWarnsNamingTheApp() {
	line := s.connect(s.number())

	s.Require().Equal(http.StatusAccepted, s.deliver(line, s.writer(), "msg-"+s.utils.uuid(), "what is open?"))

	pattern := loggedOneLine("WARN", "a message arrived on a line no agent answers on", s.customerID())
	s.Require().Eventually(func() bool { return pattern.MatchString(s.logged.String()) },
		settleFor, 10*time.Millisecond, "the router never warned naming the app")
}

// Defect: a call that rang an app's number, which no attached number reaches, logged at Debug,
// which production does not keep.
func (s *CallHooksSuite) TestACallThatRangANumberNoNumberReachesIsWarnedAbout() {
	s.Require().Equal(http.StatusOK, s.arriveRingingNumber())

	s.True(s.loggedLine("level=WARN", `msg="no number reaches an arriving call"`, "stream_app="))
}

// Defect: every video call in the app, which arrives here too and no number is meant to reach,
// warned about as a caller stuck.
func (s *CallHooksSuite) TestAVideoCallNoNumberReachesIsNotWarnedAbout() {
	callID := "video-" + s.utils.uuid()

	s.Require().Equal(http.StatusOK, s.arrive("default:"+callID))

	s.True(s.loggedLine("level=DEBUG", `msg="no number reaches an arriving call"`, callID))
	s.False(s.loggedLine("level=WARN", `msg="no number reaches an arriving call"`))
}

// Defect: a call named for a number, which no number reaches, logged at Debug as a video call is.
func (s *CallHooksSuite) TestACallNamedForANumberNoNumberReachesIsWarnedAbout() {
	s.Require().Equal(http.StatusOK, s.arrive("default:"+s.callID))

	s.True(s.loggedLine("level=WARN", `msg="no number reaches an arriving call"`, s.loggedAs(s.callID)))
}

// ReadOnlyHooksSuite is AppHooksSuite with the fallback to the deployment's app off, so a
// customer with an app of its own may only read what it left in the deployment's app.
type ReadOnlyHooksSuite struct{ AppHooksSuite }

func TestReadOnlyHooksSuite(t *testing.T) { runSuite(t, new(ReadOnlyHooksSuite)) }

func (s *ReadOnlyHooksSuite) SetupSuite() {
	s.appRefuses = true
	s.AppHooksSuite.SetupSuite()
}

// BeforeTest runs this suite's own test alone: the rest are AppHooksSuite's, run there.
func (s *ReadOnlyHooksSuite) BeforeTest(suiteName, testName string) {
	if testName != "TestACallToTheNumberOfACustomerNoLongerWritingThereWarnsNamingTheCustomer" {
		s.T().Skip("an AppHooksSuite test, run there")
	}
	s.AppHooksSuite.BeforeTest(suiteName, testName)
}

// Defect: a call to a number a customer attached in the deployment's app, where it may only read
// now, warned naming the deployment's app, though the number says whose caller is stuck.
func (s *ReadOnlyHooksSuite) TestACallToTheNumberOfACustomerNoLongerWritingThereWarnsNamingTheCustomer() {
	s.chat.SetAppFor(suiteStreamKey, chattest.App{ID: suiteStreamApp})
	e164 := s.utils.number()
	call := "phone-" + e164
	s.attach(e164, call, 0)

	s.Require().Equal(http.StatusOK, s.hook("/v1/phone/hooks/stream", s.ringing(call, time.Now()), suiteStreamSecret, ""))

	s.Regexp(loggedOneLine("WARN", "no number reaches an arriving call", s.customerID()), s.logged.String())
}

// The same warning on a per-app router names the app the hook was verified to come from.
func (s *AppHooksSuite) TestACallNoNumberReachesInAnAppWarnsNamingTheApp() {
	call := "phone-" + s.utils.number()

	s.Require().Equal(http.StatusOK, s.fromApp(s.appPath("/v1/phone/hooks/stream"), s.ringing(call, time.Now())))

	s.Regexp(loggedOneLine("WARN", "no number reaches an arriving call", s.customerID()), s.logged.String())
}

// Defect: a channel the Router answers in whose session could not be opened logged the failure
// with only the channel, so nobody could tell whose agent stayed silent.
func (s *AppHooksSuite) TestAChannelSessionThatCannotBeOpenedIsLoggedNamingTheApp() {
	s.holdsType("callouts", memberWrites)
	s.answeringIn("callouts")
	channel := s.groupChannel("callouts", nil)
	s.chat.Fail(http.MethodPost, "/users")
	s.T().Cleanup(func() { s.chat.Heal(http.MethodPost, "/users") })

	s.Require().Equal(http.StatusOK, s.fromApp(s.appPath("/v1/chat/hooks/stream"), s.groupMessage("callouts", channel, nil)))

	pattern := loggedOneLine("ERROR", "could not open the conversation of a channel the Router answers in", s.customerID())
	s.Require().Eventually(func() bool { return pattern.MatchString(s.logged.String()) },
		settleFor, 10*time.Millisecond, "the router never logged the failure naming the app")
}

// Defect: a message to a session that leaves text to a worker, with no worker connected, was
// logged without the app. The session's own line is matched, as the line of a message no
// session takes says the same.
func (s *MessageHooksSuite) TestAMessageNoWorkerTakesForADispatchSessionIsLoggedNamingTheApp() {
	session := s.dispatching()

	s.Require().Equal(http.StatusOK, s.signedly("/v1/chat/hooks/stream", s.recordedMessage(s.ownedBy(s.customerID()), "is it fixed?")))

	line := `msg="nobody could answer an arriving message" channel=` + s.channelID + " session=" + session + " app_id=" + s.customerID() + " "
	s.Require().Eventually(func() bool { return strings.Contains(s.logged.String(), line) },
		settleFor, 10*time.Millisecond, "the router never logged the failure naming the app")
}

// Defect: a message a worker's session could not answer was logged without the app. The
// session's conversation is closed under it, so it refuses the follow-up.
func (s *MessageHooksSuite) TestAMessageAWorkersSessionCannotAnswerIsLoggedNamingTheApp() {
	handed := s.handedOut()
	opened := s.workersSession(handed)
	s.asked(opened.Id, handed.Text)
	held, found := s.manager.ByConversationWhere(chatlog.ChannelType+":"+s.channelID, nil)
	s.Require().True(found)
	s.Require().NoError(held.Close())

	s.inChannel("and what about Spain?")
	s.Require().Equal(http.StatusOK, s.signedly("/v1/chat/hooks/stream", s.again("and what about Spain?")))

	line := `msg="could not answer a message" channel=` + s.channelID + " session=" + opened.Id + " app_id=" + s.customerID() + " "
	s.Require().Eventually(func() bool { return strings.Contains(s.logged.String(), line) },
		settleFor, 10*time.Millisecond, "the router never logged the failure naming the app")
}

// Defect: a message on a line whose conversation Stream Chat could not open was logged without
// the app.
func (s *ChannelsSuite) TestAMessageWhoseConversationCannotBeOpenedIsLoggedNamingTheApp() {
	number := s.number()
	line := s.connect(number)
	s.agentOn(number, "")
	s.chat.Fail(http.MethodPost, "/query")
	s.T().Cleanup(func() { s.chat.Heal(http.MethodPost, "/query") })

	s.Require().Equal(http.StatusAccepted, s.deliver(line, s.writer(), "msg-"+s.utils.uuid(), "what is open?"))

	pattern := loggedOneLine("ERROR", "could not open a conversation for a channel message", s.customerID())
	s.Require().Eventually(func() bool { return pattern.MatchString(s.logged.String()) },
		settleFor, 10*time.Millisecond, "the router never logged the failure naming the app")
}
