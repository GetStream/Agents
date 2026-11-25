//go:build integration

package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/rueidis"

	"github.com/GetStream/Vision-Agents/acceleration/internal/activity"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/twiliosig"
)

// twilioCallPublicURL is the suite's ROUTER_PUBLIC_URL, with the trailing slash an operator may
// write; Twilio signs over it trimmed, then the path.
const twilioCallPublicURL = "http://localhost:8080/"

// TwilioHookSuite is Twilio asking the router where a call to one of its Twilio numbers goes,
// signed with the account's Auth Token as Twilio signs it.
type TwilioHookSuite struct {
	RouterSuite
	line   twilioRinger
	logged *lockedLog
	// since is how much of logged had been written when the current test started.
	since int
	e164  string
}

func TestTwilioHookSuite(t *testing.T) {
	runSuite(t, new(TwilioHookSuite))
}

func (s *TwilioHookSuite) SetupSuite() {
	s.twilioAuthToken = rand.Text()
	s.twilioAccountSID = twilioAccount()
	s.publicURL = twilioCallPublicURL
	s.logged = &lockedLog{}
	s.logs = s.logged
	s.RouterSuite.SetupSuite()
	s.line = twilioRinger{suite: &s.RouterSuite, token: s.twilioAuthToken, account: s.twilioAccountSID}
}

func (s *TwilioHookSuite) SetupTest() {
	s.since = len(s.logged.String())
	s.useApp(s.data.createApp())
	s.e164 = s.utils.number()
}

func (s *TwilioHookSuite) TestACallIsSentToTheNumbersOwnTrunkWithItsCredentialsInTheBodyOnly() {
	s.line.attach(s.e164, "sip:"+s.e164+"@trunk.sip.example.test", "trunk-user", "trunk-pa$$<word>")
	form := s.line.call(s.e164)

	status, contentType, body := s.line.deliver(form, s.line.sign(form))

	s.Require().Equal(http.StatusOK, status, body)
	s.Equal("application/xml", contentType)
	var parsed struct {
		Dial struct {
			Sip struct {
				URI      string `xml:",chardata"`
				Username string `xml:"username,attr"`
				Password string `xml:"password,attr"`
			} `xml:"Sip"`
		} `xml:"Dial"`
	}
	s.Require().NoError(xml.Unmarshal([]byte(body), &parsed))
	s.Equal("sip:"+s.e164+"@trunk.sip.example.test", parsed.Dial.Sip.URI)
	s.Equal("trunk-user", parsed.Dial.Sip.Username)
	s.Equal("trunk-pa$$<word>", parsed.Dial.Sip.Password)
	s.Require().Eventually(func() bool { return s.loggedLine(`msg="served a request"`, " path="+twilioCallHookPath+" ") },
		settleFor, 10*time.Millisecond)
	s.NotContains(s.logged.String(), "trunk-pa$$", "the password is never logged")
	s.NotContains(s.logged.String(), "trunk-pa$$&lt;word&gt;", "nor its TwiML")
}

func (s *TwilioHookSuite) TestACallNotSignedWithTheAccountsTokenOverThisURLIsRefused() {
	s.line.attach(s.e164, "sip:"+s.e164+"@trunk.sip.example.test", "trunk-user", "trunk-password")
	form := s.line.call(s.e164)
	// The signature is checked before the account: an unsigned call is not answered TwiML.
	onAnother := s.line.call(s.e164)
	onAnother.Set("AccountSid", twilioAccount())
	for name, sent := range map[string]struct {
		form      url.Values
		signature string
	}{
		"unsigned":                  {form, ""},
		"unsigned, another account": {onAnother, ""},
		"another token":             {form, twiliosig.Sign(rand.Text(), s.line.signedURL(twilioCallHookPath), form)},
		"another path":              {form, twiliosig.Sign(s.twilioAuthToken, s.line.signedURL(twilioEventsHookPath), form)},
		"another public URL":        {form, twiliosig.Sign(s.twilioAuthToken, "http://localhost:9090"+twilioCallHookPath, form)},
	} {
		status, _, body := s.line.deliver(sent.form, sent.signature)

		s.Equal(http.StatusUnauthorized, status, name)
		s.NotContains(body, "trunk-password", name)
		s.NotContains(body, "<Response>", name)
	}
	s.True(s.loggedLine("level=WARN", "refused a twilio call webhook"))
}

func (s *TwilioHookSuite) TestASignedCallOnAnotherAccountIsHungUp() {
	s.line.attach(s.e164, "sip:"+s.e164+"@trunk.sip.example.test", "trunk-user", "trunk-password")
	form := s.line.call(s.e164)
	other := twilioAccount()
	form.Set("AccountSid", other)

	status, _, body := s.line.deliver(form, s.line.sign(form))

	s.Equal(http.StatusOK, status)
	s.Contains(body, "<Response><Hangup/></Response>")
	s.NotContains(body, "trunk-password")
	s.True(s.loggedLine("another account than TWILIO_ACCOUNT_SID", "account_sid="+other))
}

// A signed call that names no account is not on TWILIO_ACCOUNT_SID's.
func (s *TwilioHookSuite) TestASignedCallThatNamesNoAccountIsHungUp() {
	s.line.attach(s.e164, "sip:"+s.e164+"@trunk.sip.example.test", "trunk-user", "trunk-password")
	form := s.line.call(s.e164)
	form.Del("AccountSid")

	status, _, body := s.line.deliver(form, s.line.sign(form))

	s.Equal(http.StatusOK, status)
	s.Contains(body, "<Response><Hangup/></Response>")
	s.NotContains(body, "trunk-password")
	s.True(s.loggedLine("another account than TWILIO_ACCOUNT_SID"))
}

// With no account configured, a signed call that names none must not match "".
func (s *TwilioHookSuite) TestWithoutAConfiguredAccountNoCallIsAnswered() {
	s.line.attach(s.e164, "sip:"+s.e164+"@trunk.sip.example.test", "trunk-user", "trunk-password")
	configured := s.router.twilioAccountSID
	s.router.twilioAccountSID = ""
	defer func() { s.router.twilioAccountSID = configured }()
	form := s.line.call(s.e164)
	form.Del("AccountSid")

	status, _, body := s.line.deliver(form, s.line.sign(form))

	s.Equal(http.StatusOK, status)
	s.Contains(body, "<Response><Hangup/></Response>")
	s.NotContains(body, "trunk-password")
	s.True(s.loggedLine("TWILIO_ACCOUNT_SID is not set"))
}

func (s *TwilioHookSuite) TestASignedCallThatIsNotInboundIsHungUp() {
	s.line.attach(s.e164, "sip:"+s.e164+"@trunk.sip.example.test", "trunk-user", "trunk-password")
	for logged, direct := range map[string]func(url.Values){
		"direction=outbound-api": func(form url.Values) { form.Set("Direction", "outbound-api") },
		`direction=""`:           func(form url.Values) { form.Del("Direction") },
	} {
		form := s.line.call(s.e164)
		direct(form)

		status, _, body := s.line.deliver(form, s.line.sign(form))

		s.Equal(http.StatusOK, status, logged)
		s.Contains(body, "<Hangup/>", logged)
		s.NotContains(body, "trunk-password", logged)
		s.True(s.loggedLine("not inbound", logged), logged)
	}
}

func (s *TwilioHookSuite) TestASignedCallThatNamesNoNumberIsHungUp() {
	form := s.line.call(s.e164)
	form.Del("To")

	status, _, body := s.line.deliver(form, s.line.sign(form))

	s.Equal(http.StatusOK, status)
	s.Contains(body, "<Hangup/>")
	s.True(s.loggedLine("hung up a twilio call that names no number"))
}

// The number is looked up as a Twilio one: the same number attached at Telnyx is not its trunk.
func (s *TwilioHookSuite) TestACallToANumberNotAttachedAtTwilioIsHungUp() {
	attachOwnTrunk(&s.RouterSuite, "telnyx", s.e164, "sip:"+s.e164+"@trunk.sip.example.test", "trunk-user", "trunk-password")
	form := s.line.call(s.e164)

	status, contentType, body := s.line.deliver(form, s.line.sign(form))

	s.Equal(http.StatusOK, status)
	s.Equal("application/xml", contentType)
	s.Contains(body, "<Response><Hangup/></Response>")
	s.NotContains(body, "trunk-password")
	s.True(s.loggedLine("hung up a twilio call to a number that is unknown, released or not attached", masked(s.e164)))
}

// A refusal of the pre-answer checks leaves the call unanswered and is counted refused.
func (s *TwilioHookSuite) TestACallToASuspendedAppIsRejectedUnansweredAndCountedRefused() {
	s.line.attach(s.e164, "sip:"+s.e164+"@trunk.sip.example.test", "trunk-user", "trunk-password")
	s.Require().NoError(s.store.TelephonySuspensions.Upsert(context.Background(), &store.TelephonySuspension{
		CustomerID: s.customerID(), Reason: "fraud", SuspendedBy: "ops@example.test",
	}))
	form := s.line.call(s.e164)

	status, _, body := s.line.deliver(form, s.line.sign(form))

	s.Require().Equal(http.StatusOK, status)
	s.Contains(body, `<Response><Reject reason="rejected"/></Response>`,
		"Reject as the first verb leaves the call unanswered, so no second of it is billed")
	s.NotContains(body, "trunk-password")
	s.True(s.loggedLine("rejected a twilio call to a number of an app whose telephony is suspended", masked(s.e164)))
	row := s.countedOnce(activity.AreaPhoneSMS, store.AuditSourceAPI)
	s.Equal(int64(1), row.refused)
	s.Equal(codeTelephonySuspended, row.code)
}

func (s *TwilioHookSuite) TestANumberAttachedBeforeItsTrunkWasKeptIsHungUpAndSaysWhatToDo() {
	s.line.attach(s.e164, "", "", "")
	form := s.line.call(s.e164)

	status, _, body := s.line.deliver(form, s.line.sign(form))

	s.Equal(http.StatusOK, status)
	s.Contains(body, "<Hangup/>")
	s.True(s.loggedLine(masked(s.e164), "hung up a twilio call", "attach the number again"))
}

func (s *TwilioHookSuite) TestWithoutAKeyringACallSaysWhatToSet() {
	s.line.attach(s.e164, "sip:"+s.e164+"@trunk.sip.example.test", "trunk-user", "trunk-password")
	sealed := s.router.phone
	s.router.phone = s.telephonySealedBy(nil, nil, slog.New(slog.NewTextHandler(s.logged, &slog.HandlerOptions{ReplaceAttr: store.RedactLogAttr})))
	defer func() { s.router.phone = sealed }()
	form := s.line.call(s.e164)

	status, _, body := s.line.deliver(form, s.line.sign(form))

	s.Equal(http.StatusOK, status)
	s.Contains(body, "<Response><Hangup/></Response>")
	s.NotContains(body, "trunk-password")
	s.True(s.loggedLine(masked(s.e164), "hung up a twilio call: this router has no key to open trunk passwords"))
}

// The router's own failure, here a trunk password its key cannot open, is a hang-up counted as
// a server error rather than an answered call.
func (s *TwilioHookSuite) TestATrunkPasswordThatCannotBeOpenedIsHungUpAndCountedRefused() {
	s.line.attach(s.e164, "sip:"+s.e164+"@trunk.sip.example.test", "trunk-user", "trunk-password")
	_, err := s.store.GlobalDB().NewUpdate().Table("stream_trunks").
		Set("bridge_password_sealed = ?", []byte("not sealed")).
		Where("customer_id = ?", s.customerID()).Exec(context.Background())
	s.Require().NoError(err)
	form := s.line.call(s.e164)

	status, _, body := s.line.deliver(form, s.line.sign(form))

	s.Equal(http.StatusOK, status)
	s.Contains(body, "<Response><Hangup/></Response>")
	s.True(s.loggedLine("hung up a twilio call: could not find its trunk", masked(s.e164)))
	row := s.countedOnce(activity.AreaPhoneSMS, store.AuditSourceAPI)
	s.Equal(int64(1), row.refused)
	s.Equal(internalError().Code, row.code)
}

func (s *TwilioHookSuite) TestWithoutTelephonyACallIsHungUp() {
	configured := s.router.phone
	s.router.phone = nil
	defer func() { s.router.phone = configured }()
	form := s.line.call(s.e164)

	status, _, body := s.line.deliver(form, s.line.sign(form))

	s.Equal(http.StatusOK, status)
	s.Contains(body, "<Hangup/>")
	s.True(s.loggedLine("hung up a twilio call: telephony is not configured"))
}

// Without the token or the public URL no signature can be checked, so nothing is answered.
func (s *TwilioHookSuite) TestWithoutATokenOrAPublicURLNoCallIsAnswered() {
	s.line.attach(s.e164, "sip:"+s.e164+"@trunk.sip.example.test", "trunk-user", "trunk-password")
	form := s.line.call(s.e164)
	signature := s.line.sign(form)
	token, public := s.router.twilioAuthToken, s.router.publicURL
	defer func() { s.router.twilioAuthToken, s.router.publicURL = token, public }()

	for name, unset := range map[string]func(){
		"no token":      func() { s.router.twilioAuthToken, s.router.publicURL = "", public },
		"no public URL": func() { s.router.twilioAuthToken, s.router.publicURL = token, "/" },
	} {
		unset()
		status, _, body := s.line.deliver(form, signature)

		s.Equal(http.StatusNotFound, status, name)
		s.Contains(body, "twilio calls are not configured", name)
		s.NotContains(body, "trunk-password", name)
	}
}

func (s *TwilioHookSuite) TestABodyThatIsNoFormOrTooLargeIsRefused() {
	for name, sent := range map[string]struct {
		body   string
		status int
	}{
		"not a form": {"To=%zz", http.StatusBadRequest},
		// The 64 KiB the spec declares, written out so a changed limit cannot pass.
		"too large": {"To=" + strings.Repeat("1", 64<<10), http.StatusRequestEntityTooLarge},
	} {
		request, err := http.NewRequest(http.MethodPost, s.server.URL+twilioCallHookPath, strings.NewReader(sent.body))
		s.Require().NoError(err)
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		response, err := http.DefaultClient.Do(request)
		s.Require().NoError(err)
		answered, err := io.ReadAll(response.Body)
		s.Require().NoError(response.Body.Close())
		s.Require().NoError(err)

		s.Equal(sent.status, response.StatusCode, name)
		s.NotContains(string(answered), "<Response>", name)
	}
}

// A copy of a signed call, Twilio's own or a replay, gets no second Dial once the first was
// answered: the signature has no time in it, so the copy would get the password again.
func (s *TwilioHookSuite) TestACallAnsweredOnceIsHungUpWithoutItsPasswordWhenSentAgain() {
	s.line.attach(s.e164, "sip:"+s.e164+"@trunk.sip.example.test", "trunk-user", "replayed-password")
	form := s.line.call(s.e164)
	signature := s.line.sign(form)
	_, _, first := s.line.deliver(form, signature)
	s.Require().Contains(first, `password="replayed-password"`)

	status, _, again := s.line.deliver(form, signature)

	s.Equal(http.StatusOK, status)
	s.Contains(again, "<Response><Hangup/></Response>")
	s.NotContains(again, "replayed-password")
	s.True(s.loggedLine("hung up a twilio call that was answered already", "call_sid="+form.Get("CallSid")))
	remembered, err := s.router.twilioCallSids.Do(context.Background(),
		s.router.twilioCallSids.B().Ttl().Key("phone:twilio:call:"+form.Get("CallSid")).Build()).AsInt64()
	s.Require().NoError(err)
	s.InDelta(7*24*60*60, remembered, 60, "a copy is refused for a week, written out so a changed TTL cannot pass")
}

// Eight copies of one signed call arrive at once: exactly one is answered with the password.
// A mark checked first and written after lets several copies pass the check together.
func (s *TwilioHookSuite) TestCopiesOfOneCallSentAtOnceAreAnsweredOnlyOnce() {
	s.line.attach(s.e164, "sip:"+s.e164+"@trunk.sip.example.test", "trunk-user", "parallel-password")
	for round := range 20 {
		form := s.line.call(s.e164)
		signature := s.line.sign(form)
		bodies := make([]string, 8)
		errs := make([]error, len(bodies))
		var wg sync.WaitGroup
		for i := range bodies {
			wg.Go(func() {
				response, err := http.DefaultClient.Do(s.line.request(context.Background(), form, signature))
				if err != nil {
					errs[i] = err
					return
				}
				defer response.Body.Close()
				answered, err := io.ReadAll(response.Body)
				bodies[i], errs[i] = string(answered), err
			})
		}
		wg.Wait()
		answered := 0
		for i, body := range bodies {
			s.Require().NoError(errs[i])
			if strings.Contains(body, "parallel-password") {
				answered++
			}
		}
		s.Equal(1, answered, "round %d: copies answered with the password", round)
	}
}

// Twilio names the number it rang in To; Called is the same number on a plain call, but a
// forwarded one names the original number there, and the credentials are those of To.
func (s *TwilioHookSuite) TestACallIsAnsweredForToWhateverCalledNames() {
	other := s.utils.number()
	s.line.attach(s.e164, "sip:"+s.e164+"@trunk.sip.example.test", "trunk-user", "to-password")
	s.line.attach(other, "sip:"+other+"@trunk.sip.example.test", "other-user", "other-password")
	form := s.line.call(s.e164)
	form.Set("Called", other)

	_, _, body := s.line.deliver(form, s.line.sign(form))

	s.Contains(body, `password="to-password"`)
	s.NotContains(body, "other-password")
}

// Twilio sends a call again when its answer never reached Twilio
// (https://www.twilio.com/docs/usage/webhooks/webhooks-connection-overrides), and that copy is
// answered; the one after it is not.
func (s *TwilioHookSuite) TestACallWhoseAnswerDidNotReachTwilioIsAnsweredWhenSentAgain() {
	s.line.attach(s.e164, "sip:"+s.e164+"@trunk.sip.example.test", "trunk-user", "retried-password")
	for name, lost := range map[string]func(cancel context.CancelFunc) *lostAnswer{
		"the answer could not be sent": func(context.CancelFunc) *lostAnswer {
			return &lostAnswer{ResponseRecorder: httptest.NewRecorder(), flush: errors.New("broken pipe")}
		},
		"Twilio hung up while it was written": func(cancel context.CancelFunc) *lostAnswer {
			return &lostAnswer{ResponseRecorder: httptest.NewRecorder(), onWrite: cancel}
		},
	} {
		form := s.line.call(s.e164)
		signature := s.line.sign(form)
		ctx, cancel := context.WithCancel(context.Background())
		answer := lost(cancel)
		s.router.receiveTwilioCall(answer, s.line.request(ctx, form, signature))
		cancel()
		s.Require().Contains(answer.Body.String(), `password="retried-password"`, name)

		_, _, retried := s.line.deliver(form, signature)
		_, _, replayed := s.line.deliver(form, signature)

		s.Contains(retried, `password="retried-password"`, name)
		s.Contains(replayed, "<Response><Hangup/></Response>", name)
		s.NotContains(replayed, "retried-password", name)
		s.True(s.loggedLine("level=WARN", "did not reach Twilio; its retry is answered", "call_sid="+form.Get("CallSid")), name)
	}
}

// A Redis that fails to forget a lost answer leaves the call marked: its retry is hung up, and
// the failure says so.
func (s *TwilioHookSuite) TestACallWhoseLostAnswerCannotBeForgottenIsHungUpWhenSentAgain() {
	s.line.attach(s.e164, "sip:"+s.e164+"@trunk.sip.example.test", "trunk-user", "kept-password")
	marks, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{s.redisAddr}, DisableCache: true})
	s.Require().NoError(err)
	sids := s.router.twilioCallSids
	s.T().Cleanup(func() { s.router.twilioCallSids = sids })
	s.router.twilioCallSids = marks
	form := s.line.call(s.e164)
	signature := s.line.sign(form)
	answer := &lostAnswer{ResponseRecorder: httptest.NewRecorder(), flush: errors.New("broken pipe"), onFlush: marks.Close}

	s.router.receiveTwilioCall(answer, s.line.request(context.Background(), form, signature))
	s.router.twilioCallSids = sids
	_, _, retried := s.line.deliver(form, signature)

	s.Require().Contains(answer.Body.String(), `password="kept-password"`)
	s.Contains(retried, "<Response><Hangup/></Response>")
	s.NotContains(retried, "kept-password")
	s.True(s.loggedLine("level=WARN", "could not forget a twilio call", "call_sid="+form.Get("CallSid"), "error="))
}

// Without a place to mark a call answered, or with one that fails, no call is answered: each
// copy would get the password.
func (s *TwilioHookSuite) TestWithoutAWorkingRedisNoCallIsAnswered() {
	s.line.attach(s.e164, "sip:"+s.e164+"@trunk.sip.example.test", "trunk-user", "unmarked-password")
	closed, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{s.redisAddr}, DisableCache: true})
	s.Require().NoError(err)
	closed.Close()
	sids := s.router.twilioCallSids
	s.T().Cleanup(func() { s.router.twilioCallSids = sids })

	for logged, client := range map[string]rueidis.Client{
		"there is no Redis to answer each call once": nil,
		"could not mark it answered":                 closed,
	} {
		s.router.twilioCallSids = client
		form := s.line.call(s.e164)

		status, _, body := s.line.deliver(form, s.line.sign(form))

		s.Equal(http.StatusOK, status, logged)
		s.Contains(body, "<Response><Hangup/></Response>", logged)
		s.NotContains(body, "unmarked-password", logged)
		s.True(s.loggedLine("level=WARN", "hung up a twilio call", logged), logged)
	}
}

// A call is marked by its CallSid, so one without a CallSid that can be marked is not answered.
func (s *TwilioHookSuite) TestACallWithNoCallSidOrOneTooLongIsHungUp() {
	s.line.attach(s.e164, "sip:"+s.e164+"@trunk.sip.example.test", "trunk-user", "trunk-password")
	for name, sid := range map[string]string{
		"no CallSid": "",
		// 65 bytes, one past the 64 marked, written out so a changed limit cannot pass.
		"too long": "CA" + strings.Repeat("0", 63),
	} {
		form := s.line.call(s.e164)
		form.Set("CallSid", sid)

		status, _, body := s.line.deliver(form, s.line.sign(form))

		s.Equal(http.StatusOK, status, name)
		s.Contains(body, "<Response><Hangup/></Response>", name)
		s.NotContains(body, "trunk-password", name)
		s.True(s.loggedLine("hung up a twilio call that names no CallSid, or one too long to mark"), name)
	}
}

// lostAnswer is a response that never reaches Twilio: its flush fails, or writing it calls
// onWrite, as Twilio hanging up ends the request's context.
type lostAnswer struct {
	*httptest.ResponseRecorder
	flush   error
	onWrite context.CancelFunc
	onFlush func()
}

func (a *lostAnswer) Write(body []byte) (int, error) {
	if a.onWrite != nil {
		a.onWrite()
	}
	return a.ResponseRecorder.Write(body)
}

func (a *lostAnswer) FlushError() error {
	if a.onFlush != nil {
		a.onFlush()
	}
	return a.flush
}

// loggedLine reports whether one line the current test logged holds every part.
func (s *TwilioHookSuite) loggedLine(parts ...string) bool {
	for _, line := range strings.Split(s.logged.String()[s.since:], "\n") {
		found := true
		for _, part := range parts {
			found = found && strings.Contains(line, part)
		}
		if found {
			return true
		}
	}
	return false
}

// twilioAccount is a Twilio account SID of the documented shape: AC and 32 hex digits
// (https://www.twilio.com/docs/voice/twiml, AccountSid).
func twilioAccount() string {
	digits := make([]byte, 16)
	_, _ = rand.Read(digits)
	return "AC" + hex.EncodeToString(digits)
}

// twilioRinger is Twilio calling the suite's router about one of its numbers, signing what it
// posts with token over the suite's public URL.
type twilioRinger struct {
	suite   *RouterSuite
	token   string
	account string
}

// call is the form Twilio posts when someone rings a number
// (https://www.twilio.com/docs/voice/twiml, "Twilio's request to your application").
func (l twilioRinger) call(to string) url.Values {
	return url.Values{
		"AccountSid": {l.account},
		"ApiVersion": {"2010-04-01"},
		"CallSid":    {"CA" + strings.ReplaceAll(l.suite.utils.uuid(), "-", "")},
		"CallStatus": {"ringing"},
		"Direction":  {"inbound"},
		"From":       {"+15550001111"},
		"To":         {to},
	}
}

// signedURL is the URL Twilio calls, and signs, for a path on the suite's router.
func (l twilioRinger) signedURL(path string) string {
	return strings.TrimRight(l.suite.publicURL, "/") + path
}

// sign is the X-Twilio-Signature Twilio sends with the form.
func (l twilioRinger) sign(form url.Values) string {
	return twiliosig.Sign(l.token, l.signedURL(twilioCallHookPath), form)
}

// request is the form posted as Twilio does, with signature unless it is empty.
func (l twilioRinger) request(ctx context.Context, form url.Values, signature string) *http.Request {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, l.suite.server.URL+twilioCallHookPath,
		strings.NewReader(form.Encode()))
	l.suite.Require().NoError(err)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if signature != "" {
		request.Header.Set(twiliosig.SignatureHeader, signature)
	}
	return request
}

// deliver posts the form as Twilio does, with signature unless it is empty, and answers with
// what came back.
func (l twilioRinger) deliver(form url.Values, signature string) (int, string, string) {
	response, err := http.DefaultClient.Do(l.request(context.Background(), form, signature))
	l.suite.Require().NoError(err)
	defer response.Body.Close()
	answered, err := io.ReadAll(response.Body)
	l.suite.Require().NoError(err)
	return response.StatusCode, response.Header.Get("Content-Type"), string(answered)
}

// attach holds the number at Twilio for the suite's app on a trunk of its own, as
// attachOwnTrunk does.
func (l twilioRinger) attach(e164, uri, username, password string) {
	attachOwnTrunk(l.suite, "twilio", e164, uri, username, password)
}
