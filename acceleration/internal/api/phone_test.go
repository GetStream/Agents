//go:build integration

package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	getstream "github.com/GetStream/getstream-go/v5"
	"github.com/redis/rueidis"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/node"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/outcome"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/telnyx"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/twilio"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/vendors"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
)

// PhoneSuite covers the telephony surface of a deployment that declares every vendor and
// holds credentials for none: which vendor can be asked for what, and what a request that
// could never reach a carrier is answered with.
type PhoneSuite struct {
	RouterSuite
}

func TestPhoneSuite(t *testing.T) {
	runSuite(t, new(PhoneSuite))
}

func (s *PhoneSuite) SetupTest() {
	s.useFixture("standard")
}

func (s *PhoneSuite) TestEveryVendorSaysWhichOperationsItSupports() {
	var vendors []PhoneVendor
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/phone/vendors", nil, &vendors))

	s.Require().NotEmpty(vendors)
	for _, vendor := range vendors {
		if !vendor.Implemented {
			s.Nil(vendor.Operations, vendor.Vendor+" claims operations it cannot do")
			continue
		}
		s.Require().NotNil(vendor.Operations, vendor.Vendor+" does not say what it can do")
		s.Contains(*vendor.Operations, PhoneOperationBuy)
	}
}

func (s *PhoneSuite) TestSearchingAtAVendorNobodyHasWrittenYetIsNotFound() {
	status, failure := s.serverClient.failure(http.MethodGet,
		"/v1/phone/numbers/available?vendor=bics&country=US", nil)

	s.Equal(http.StatusNotFound, status)
	s.Contains(failure, "not implemented")
	s.Contains(failure, "bics")
}

func (s *PhoneSuite) TestSearchingAtAVendorThatIsNotOneIsRefused() {
	status, failure := s.serverClient.failure(http.MethodGet,
		"/v1/phone/numbers/available?vendor=carrier-pigeon&country=US", nil)

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "not a known vendor")
}

func (s *PhoneSuite) TestSearchingEveryVendorSaysNoneCanBeAsked() {
	// Leaving the vendor out fans the search out, and no vendor here has credentials,
	// which is a different answer from having nothing for sale.
	status, payload := s.serverClient.call(http.MethodGet,
		"/v1/phone/numbers/available?country=US&administrative_area=CO", nil)

	var answered ErrorResponse
	s.Require().NoError(json.Unmarshal(payload, &answered), string(payload))
	s.Equal(http.StatusBadRequest, status)
	s.Equal(errPhoneNotConfigured.Message, answered.Error.Message)
	s.Equal(codeNotConfigured, answered.Error.Code)
}

func (s *PhoneSuite) TestAnAppThatHasBoughtNothingHoldsNoNumbers() {
	// Not the standard fixture: the campaign suites record numbers on it.
	s.useApp(s.data.createApp())
	var numbers []PhoneNumber
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/phone/numbers", nil, &numbers))

	s.Empty(numbers)
}

func (s *PhoneSuite) TestANumberCannotBeBoughtWithLabelsTheRollupsCannotCarry() {
	// A key longer than the rollups index is refused before the vendor is called, so a
	// number is never bought under a label nothing can be reported on.
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/phone/numbers", BuyNumberRequest{
		Vendor: "twilio", E164: "+15125551234",
		Tags: &map[string]string{strings.Repeat("x", 100): "v"},
	})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "tag")
}

func (s *PhoneSuite) TestASearchOutsideTheUSAndCanadaIsTheClientsToChange() {
	status, failure := s.serverClient.failure(http.MethodGet, "/v1/phone/numbers/available?country=GB", nil)

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "US and Canada")
}

func (s *PhoneSuite) TestANumberInJamaicaIsNotBoughtThoughTheRequestSaysUS() {
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/phone/numbers", BuyNumberRequest{
		Vendor: "twilio", E164: "+18765551234", Country: pointerTo("US"),
	})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "US and Canada")
}

func (s *PhoneSuite) TestBuyingWithoutSayingWhatToBuyIsRefused() {
	status, _ := s.serverClient.call(http.MethodPost, "/v1/phone/numbers", BuyNumberRequest{})

	s.Equal(http.StatusBadRequest, status)
}

func (s *PhoneSuite) TestACallCannotRingForLessThanNoTime() {
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/phone/calls", PlaceCallRequest{
		From: "+15125551234", To: "+15550001111", RingTimeoutSeconds: pointerTo(-5),
	})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "less than no time")
}

func (s *PhoneSuite) TestACallCannotNameTheNumberItRangUnder() {
	// The call hook dispatches the agent of whoever holds the number a call names, so a
	// customer naming somebody else's would hand their call to that customer's agent.
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/phone/calls", PlaceCallRequest{
		From: "+15125551234", To: "+15550001111", Custom: &map[string]string{"phone_number": "+15125550000"},
	})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "phone_number")
}

func (s *PhoneSuite) TestACallToAShortCodeIsRefused() {
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/phone/calls", PlaceCallRequest{
		From: "+15125551234", To: "911",
	})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "E.164")
}

func (s *PhoneSuite) TestACallOrATransferToAPremiumRateNumberIsForbidden() {
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/phone/calls", PlaceCallRequest{
		From: "+15125551234", To: "+19005550100",
	})
	s.Equal(http.StatusForbidden, status)
	s.Contains(failure, "premium-rate")

	status, failure = s.serverClient.failure(http.MethodPost, "/v1/phone/calls/transfer", TransferCallRequest{
		From: "+15125551234", To: "+19765550100", SessionId: s.data.storedSession(),
	})
	s.Equal(http.StatusForbidden, status)
	s.Contains(failure, "premium-rate")
}

// A gate that cannot read whether the callee opted out is neither a refusal nor a vendor
// failure: it is the router's own, a 500 that does not repeat what the database said.
func (s *PhoneSuite) TestACallWhoseGateCannotReadIsAServerError() {
	s.gateOnClosedStore()

	status, payload := s.serverClient.call(http.MethodPost, "/v1/phone/calls", PlaceCallRequest{
		From: "+15125551234", To: "+15550001111",
	})

	s.Equal(http.StatusInternalServerError, status, "%s", payload)
	s.Contains(string(payload), "something went wrong")
	s.NotContains(string(payload), "database is closed")
}

func (s *PhoneSuite) TestATransferWhoseGateCannotReadIsAServerError() {
	session := s.data.storedSession()
	s.gateOnClosedStore()

	status, payload := s.serverClient.call(http.MethodPost, "/v1/phone/calls/transfer", TransferCallRequest{
		From: "+15125551234", To: "+15550001111", SessionId: session,
	})

	s.Equal(http.StatusInternalServerError, status, "%s", payload)
	s.Contains(string(payload), "something went wrong")
	s.NotContains(string(payload), "database is closed")
}

// The session tests dial a premium-rate number, refused only once the session is let
// through, so a 403 is a session that passed.

func (s *PhoneSuite) TestACallMayBeRoutedIntoTheCallersOwnSession() {
	own := s.serverClient.createSession(textSession(nil)).Id

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/phone/calls", PlaceCallRequest{
		From: "+15125551234", To: "+19005550100", SessionId: &own,
	})

	s.Equal(http.StatusForbidden, status)
	s.Contains(failure, "premium-rate")
}

func (s *PhoneSuite) TestACallMayNameTheSessionItIsAboutToStart() {
	fresh := "session-" + s.utils.uuid()

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/phone/calls", PlaceCallRequest{
		From: "+15125551234", To: "+19005550100", SessionId: &fresh,
	})

	s.Equal(http.StatusForbidden, status)
	s.Contains(failure, "premium-rate")
}

func (s *PhoneSuite) TestACallCannotBeRoutedIntoAnotherCustomersSession() {
	theirs := s.data.backendOfAnotherApp().createSession(textSession(nil)).Id

	status, _ := s.serverClient.failure(http.MethodPost, "/v1/phone/calls", PlaceCallRequest{
		From: "+15125551234", To: "+19005550100", SessionId: &theirs,
	})

	s.Equal(http.StatusConflict, status)
}

func (s *PhoneSuite) TestAHumanCannotBeTransferredIntoAnotherCustomersSession() {
	theirs := s.data.backendOfAnotherApp().createSession(textSession(nil)).Id

	status, _ := s.serverClient.failure(http.MethodPost, "/v1/phone/calls/transfer", TransferCallRequest{
		From: "+15125551234", To: "+19765550100", SessionId: theirs,
	})

	s.Equal(http.StatusNotFound, status)
}

func (s *PhoneSuite) TestAHumanCannotBeTransferredIntoASessionThatNeverWas() {
	status, _ := s.serverClient.failure(http.MethodPost, "/v1/phone/calls/transfer", TransferCallRequest{
		From: "+15125551234", To: "+19765550100", SessionId: "session-" + s.utils.uuid(),
	})

	s.Equal(http.StatusNotFound, status)
}

func (s *PhoneSuite) TestTheAnswerPathIsReachedByAVendorThatHasNoCustomerToName() {
	// A telephony vendor fetching a call plan holds no credential of ours, so this path
	// has to be outside the middleware that requires one. Being unauthorized here would
	// mean a vendor bridging a live call to nowhere.
	status, failure := s.unauthenticatedClient.failure(
		http.MethodGet, "/v1/phone/answer/some-token", nil)

	s.Equal(http.StatusNotFound, status)
	s.Contains(failure, "not waiting to be answered")
}

func (s *PhoneSuite) TestOnlyTheAppsOwnBackendMayBuyNumbers() {
	s.assertPosture(serverOnly, func(as *testClient) int {
		status, _ := as.call(http.MethodGet, "/v1/phone/numbers", nil)
		return status
	})
}

func (s *PhoneSuite) TestANumberThatOnlyPlacedCallsIsNotListedAsAttached() {
	s.useApp(s.data.createApp())
	ctx := context.Background()
	number := s.utils.number()
	s.Require().NoError(s.store.PhoneNumbers.Insert(ctx, &store.PhoneNumber{
		E164: number, Vendor: "telnyx", Country: "US", CustomerID: s.customerID(), PurchasedAt: time.Now().UTC(),
	}))
	group := store.TrunkGroup{CustomerID: s.customerID(), Vendor: "telnyx"}
	s.Require().NoError(s.store.StreamTrunks.InGroup(ctx, group, func(_ context.Context, tx *store.TrunkGroupTx) error {
		return tx.SetLine(number, "trunk-"+s.utils.uuid(), "route-"+s.utils.uuid())
	}))

	var numbers []PhoneNumber
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/phone/numbers", nil, &numbers))

	s.Require().Len(numbers, 1)
	s.Nil(numbers[0].StreamTrunkId, "its callers reach no agent")
}

func (s *PhoneSuite) attachedNumber(customerID string) string {
	ctx := context.Background()
	number := s.utils.number()
	s.Require().NoError(s.store.PhoneNumbers.Insert(ctx, &store.PhoneNumber{
		E164: number, Vendor: "bics", Country: "US", CustomerID: customerID, PurchasedAt: time.Now().UTC(),
	}))
	s.Require().NoError(s.store.PhoneNumbers.Attach(ctx, customerID, number, store.NumberAttachment{
		TrunkID: "trunk-" + s.utils.uuid(), RouteID: "route-" + s.utils.uuid(), CallType: "default", CallID: "call-" + s.utils.uuid(),
	}))
	return number
}

func (s *PhoneSuite) TestADetachedNumberIsKeptButReachesNoAgent() {
	s.useApp(s.data.createApp())
	number := s.attachedNumber(s.customerID())

	s.Equal(http.StatusNoContent, s.serverClient.do(http.MethodDelete, "/v1/phone/numbers/"+number+"/attach", nil, nil))

	var numbers []PhoneNumber
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/phone/numbers", nil, &numbers))
	s.Require().Len(numbers, 1)
	s.Equal(number, numbers[0].E164)
	s.Nil(numbers[0].StreamTrunkId)
}

func (s *PhoneSuite) TestDetachingANumberTwiceChangesNothing() {
	s.useApp(s.data.createApp())
	number := s.attachedNumber(s.customerID())

	s.Equal(http.StatusNoContent, s.serverClient.do(http.MethodDelete, "/v1/phone/numbers/"+number+"/attach", nil, nil))
	s.Equal(http.StatusNoContent, s.serverClient.do(http.MethodDelete, "/v1/phone/numbers/"+number+"/attach", nil, nil))
}

func (s *PhoneSuite) TestAnotherCustomersNumberCannotBeDetached() {
	s.useApp(s.data.createApp())
	theirs := s.attachedNumber(s.data.createApp().app.ID)

	status, _ := s.serverClient.failure(http.MethodDelete, "/v1/phone/numbers/"+theirs+"/attach", nil)

	s.Equal(http.StatusNotFound, status)
}

// CallStatusSuite is the path a vendor posts a placed call's status to. Twilio and Telnyx are
// the real providers, opened with credentials no request is ever sent with: reading a status
// reaches no vendor.
type CallStatusSuite struct {
	RouterSuite
	// logged is what the router logged, at debug and up.
	logged *lockedLog
}

func TestCallStatusSuite(t *testing.T) {
	runSuite(t, new(CallStatusSuite))
}

// unreachableVendor is where the suite's vendors are pointed: port 1 on this host refuses
// at once, so a dial that gets past the router's own checks fails here and never reaches a
// real vendor with the suite's nonsense credentials.
const unreachableVendor = "http://127.0.0.1:1"

func (s *CallStatusSuite) SetupSuite() {
	s.phoneVendors = s.unreachableVendors()
	s.phoneOnChat = true
	s.sessionTelephony = true
	s.deploymentKnown = true
	s.logged = &lockedLog{}
	s.logs = s.logged
	s.RouterSuite.SetupSuite()
}

func (s *CallStatusSuite) SetupTest() {
	s.useFixture("standard")
}

// unreachableVendors are the vendors the router knows, twilio and telnyx pointed at
// unreachableVendor.
func (s *RouterSuite) unreachableVendors() *phone.Registry {
	config, err := phone.DefaultConfig()
	s.Require().NoError(err)
	// The suites share one process, so the credentials are not set in it: the two vendors
	// are declared as needing none, and handed some of their own.
	for i, vendor := range config.Vendors {
		if vendor.Vendor == "twilio" || vendor.Vendor == "telnyx" {
			config.Vendors[i].Credentials = nil
		}
	}
	registry := vendors.Registry(config)
	registry.Register("twilio", func() (phone.Provider, error) {
		return twilio.New(twilio.Options{AccountSID: "AC1", AuthToken: "t", BaseURL: unreachableVendor})
	})
	registry.Register("telnyx", func() (phone.Provider, error) {
		return telnyx.New(telnyx.Options{APIKey: "k", BaseURL: unreachableVendor})
	})
	return registry
}

// placed keeps a call placed at vendor and returns its status token and call id.
func (s *CallStatusSuite) placed(vendor string) (string, string) {
	callID := s.utils.uuid()
	token, err := s.router.phone.Outcomes().Placed(context.Background(), outcome.Call{
		Key: outcome.Key{
			CustomerID: s.customerID(), CallType: "default", CallID: callID,
			Local: s.utils.number(), Remote: s.utils.number(),
		},
		Vendor:  vendor,
		Reports: true,
	})
	s.Require().NoError(err)
	return token, callID
}

// post posts body to token's status path as a vendor does, with no credential of ours.
func (s *CallStatusSuite) post(token, contentType, body string) (int, string) {
	request, err := http.NewRequest(http.MethodPost, s.server.URL+"/v1/phone/status/"+token, strings.NewReader(body))
	s.Require().NoError(err)
	request.Header.Set("Content-Type", contentType)
	response, err := s.server.Client().Do(request)
	s.Require().NoError(err)
	defer response.Body.Close()
	answered, err := io.ReadAll(response.Body)
	s.Require().NoError(err)
	return response.StatusCode, string(answered)
}

// postForm posts form as a vendor's status.
func (s *CallStatusSuite) postForm(token string, form url.Values) int {
	status, _ := s.post(token, "application/x-www-form-urlencoded", form.Encode())
	return status
}

func (s *CallStatusSuite) read(callID string) outcome.Call {
	call, err := s.router.phone.Outcomes().Read(context.Background(), s.customerID(), callID)
	s.Require().NoError(err)
	return call
}

func (s *CallStatusSuite) TestACallIntoASessionWhoseCallCarriesOtherCustomIsAnInvalidRequest() {
	// The session's call keeps the custom it was made with, so the client is told what to
	// change rather than that something went wrong.
	s.useApp(s.data.createApp())
	ctx := context.Background()
	e164, session := s.utils.number(), s.utils.uuid()
	s.Require().NoError(s.store.PhoneNumbers.Insert(ctx, &store.PhoneNumber{
		E164: e164, Vendor: "twilio", Country: "US", CustomerID: s.customerID(), PurchasedAt: time.Now().UTC(),
	}))
	creator := "agent-1"
	_, err := s.chat.Client.Video().GetOrCreateCall(ctx, "agent", session, &getstream.GetOrCreateCallRequest{
		Data: &getstream.CallRequest{CreatedByID: &creator, Custom: map[string]any{"k": "first"}},
	})
	s.Require().NoError(err)

	status, payload := s.serverClient.call(http.MethodPost, "/v1/phone/calls", PlaceCallRequest{
		From: e164, To: "+15550001111", SessionId: &session, Custom: &map[string]string{"k": "second"},
	})

	s.Require().Equal(http.StatusBadRequest, status, "%s", payload)
	var answered ErrorResponse
	s.Require().NoError(json.Unmarshal(payload, &answered))
	s.Equal("invalid_request", answered.Error.Code)
	s.Contains(answered.Error.Message, "already carries other custom data")
}

func (s *CallStatusSuite) TestATwilioStatusIsReachedWithoutACustomer() {
	busy, busyCall := s.placed("twilio")
	noAnswer, noAnswerCall := s.placed("twilio")
	failed, failedCall := s.placed("twilio")
	completed, completedCall := s.placed("twilio")

	s.Equal(http.StatusNoContent, s.postForm(busy, url.Values{"CallStatus": {"busy"}}))
	s.Equal(http.StatusNoContent, s.postForm(noAnswer, url.Values{"CallStatus": {"no-answer"}}))
	s.Equal(http.StatusNoContent, s.postForm(failed, url.Values{"CallStatus": {"failed"}}))
	s.Equal(http.StatusNoContent, s.postForm(completed, url.Values{"CallStatus": {"completed"}}))

	s.Equal(outcome.Busy, s.read(busyCall).Outcome)
	s.Equal(outcome.NoAnswer, s.read(noAnswerCall).Outcome)
	s.Equal(outcome.Failed, s.read(failedCall).Outcome)
	s.Equal(outcome.Completed, s.read(completedCall).Outcome)
}

func (s *CallStatusSuite) TestATelnyxStatusIsRecorded() {
	token, callID := s.placed("telnyx")

	s.Equal(http.StatusNoContent, s.postForm(token, url.Values{"CallStatus": {"busy"}}))

	call := s.read(callID)
	s.Equal(outcome.Busy, call.Outcome)
	s.Equal("busy", call.VendorStatus)
}

// postMachine posts form as a vendor's machine report.
func (s *CallStatusSuite) postMachine(token string, form url.Values) (int, string) {
	request, err := http.NewRequest(http.MethodPost, s.server.URL+"/v1/phone/machine/"+token, strings.NewReader(form.Encode()))
	s.Require().NoError(err)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := s.server.Client().Do(request)
	s.Require().NoError(err)
	defer response.Body.Close()
	answered, err := io.ReadAll(response.Body)
	s.Require().NoError(err)
	return response.StatusCode, string(answered)
}

func (s *CallStatusSuite) TestATelnyxMachineReportReachesTheMachineHookWithoutACustomer() {
	token, callID := s.placed("telnyx")
	var told []string
	s.router.phone.OnMachine(func(_ context.Context, customerID, sessionID string, machine phone.Machine) {
		told = append(told, customerID+" "+sessionID+" "+string(machine))
	})
	s.T().Cleanup(func() { s.router.phone.OnMachine(s.router.tellMachine) })

	status, _ := s.postMachine(token, url.Values{"AnsweredBy": {"machine_end_beep"}})

	s.Equal(http.StatusNoContent, status)
	s.Equal([]string{s.customerID() + " " + callID + " greeting_ended"}, told)
}

func (s *CallStatusSuite) TestAMachineReportForAnUnknownTokenIs404() {
	status, failure := s.postMachine(s.utils.uuid(), url.Values{"AnsweredBy": {"human"}})

	s.Equal(http.StatusNotFound, status)
	s.Contains(failure, "that call is not waiting for its status")
}

func (s *CallStatusSuite) TestAMachineReportThatCannotBeReadIs400() {
	token, _ := s.placed("telnyx")

	status, failure := s.postMachine(token, url.Values{"CallSid": {"CA1"}})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "AnsweredBy")
}

// hangsUpOnMachines opens on sessions a placed call's session whose plan is to hang up on an
// answering machine, with a call kept for it, and returns its id and the call's token.
func (s *CallStatusSuite) hangsUpOnMachines(sessions *session.Manager) (string, string) {
	opened, err := sessions.Create(context.Background(), session.Spec{CustomerID: s.customerID(),
		Phone: &session.PhoneSpec{VendorCallID: "CA1"}, Voicemail: &session.VoicemailPlan{},
		Attribution: routing.Attribution{APIKey: "key-phone"}})
	s.Require().NoError(err)
	token, _ := s.placedFor(opened.ID())
	return opened.ID(), token
}

// endsForVoicemail waits for sessions to stop holding id.
func (s *CallStatusSuite) endsForVoicemail(sessions *session.Manager, id string) {
	s.Require().Eventually(func() bool { return !sessions.Running(s.customerID(), id) }, 10*time.Second, 20*time.Millisecond,
		"the session was kept on the answering machine")
}

func (s *CallStatusSuite) TestAMachineReportHangsUpTheSessionHere() {
	id, token := s.hangsUpOnMachines(s.manager)

	status, _ := s.postMachine(token, url.Values{"AnsweredBy": {"machine_end_beep"}})

	s.Equal(http.StatusNoContent, status)
	s.endsForVoicemail(s.manager, id)
}

func (s *CallStatusSuite) TestAMachineReportHangsUpTheSessionOnTheNodeHoldingIt() {
	other, _, sessions := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler))
	s.Require().NotEqual(other.Listener.Addr().String(), s.router.directory.Address())
	id, token := s.hangsUpOnMachines(sessions)

	status, _ := s.postMachine(token, url.Values{"AnsweredBy": {"machine_end_beep"}})

	s.Equal(http.StatusNoContent, status)
	s.endsForVoicemail(sessions, id)
}

func (s *CallStatusSuite) TestAMachineReportForASessionThatEndedIsTakenAndChangesNothing() {
	id, token := s.hangsUpOnMachines(s.manager)
	s.Require().Equal(http.StatusNoContent, func() int {
		status, _ := s.postMachine(token, url.Values{"AnsweredBy": {"machine_end_beep"}})
		return status
	}())
	s.endsForVoicemail(s.manager, id)

	again, failure := s.postMachine(token, url.Values{"AnsweredBy": {"machine_end_beep"}})

	s.Equal(http.StatusNoContent, again, "body: %s", failure)
}

func (s *CallStatusSuite) TestAPersonPickingUpKeepsTheSession() {
	id, token := s.hangsUpOnMachines(s.manager)

	status, _ := s.postMachine(token, url.Values{"AnsweredBy": {"human"}})

	s.Equal(http.StatusNoContent, status)
	s.Never(func() bool { return !s.manager.Running(s.customerID(), id) }, time.Second, 50*time.Millisecond,
		"the session was ended for a person")
}

func (s *CallStatusSuite) TestAnUnknownStatusTokenIs404() {
	status, failure := s.post(s.utils.uuid(), "application/x-www-form-urlencoded", "CallStatus=busy")

	s.Equal(http.StatusNotFound, status)
	s.Contains(failure, "that call is not waiting for its status")
}

func (s *CallStatusSuite) TestAStatusThatCannotBeReadIs400() {
	token, callID := s.placed("twilio")

	empty, _ := s.post(token, "application/x-www-form-urlencoded", "")
	notAForm, _ := s.post(token, "application/json", `{"CallStatus":"busy"}`)

	s.Equal(http.StatusBadRequest, empty)
	s.Equal(http.StatusBadRequest, notAForm)
	call := s.read(callID)
	s.Empty(call.Outcome)
	s.Empty(call.VendorStatus)
}

func (s *CallStatusSuite) TestAStatusOver64KiBIs413() {
	token, _ := s.placed("twilio")

	status, _ := s.post(token, "application/x-www-form-urlencoded", "CallStatus=busy&pad="+strings.Repeat("a", 64<<10))

	s.Equal(http.StatusRequestEntityTooLarge, status)
}

// placedFor keeps a call placed at Twilio for session and returns its status token and key.
func (s *CallStatusSuite) placedFor(session string) (string, outcome.Key) {
	key := outcome.Key{
		CustomerID: s.customerID(), CallType: "default", CallID: session,
		Local: s.utils.number(), Remote: s.utils.number(),
	}
	token, err := s.router.phone.Outcomes().Placed(context.Background(), outcome.Call{Key: key, Vendor: "twilio", Reports: true})
	s.Require().NoError(err)
	return token, key
}

// busy records a busy call placed for session through the keeper, not the status path: the
// path also ends the session, and one ended a moment ago is not found until its row is written.
func (s *CallStatusSuite) busy(session string) {
	token, _ := s.placedFor(session)
	_, err := s.router.phone.Outcomes().Reported(context.Background(), token, outcome.Report{Outcome: outcome.Busy, Word: "busy"})
	s.Require().NoError(err)
}

func (s *CallStatusSuite) TestASessionShowsHowItsPlacedCallEnded() {
	opened := s.serverClient.createSession(textSession(nil))
	s.busy(opened.Id)

	read := s.serverClient.getSession(opened.Id)

	s.Require().NotNil(read.CallOutcome)
	s.Equal(CallOutcomeBusy, value(read.CallOutcome.Outcome))
	s.Nil(read.CallOutcome.AnsweredAt)
	s.Nil(read.CallOutcome.DurationSeconds)
}

func (s *CallStatusSuite) TestASessionShowsWhenTheCallWasAnsweredAndForHowLong() {
	opened := s.serverClient.createSession(textSession(nil))
	_, key := s.placedFor(opened.Id)
	answered := time.Now().UTC().Truncate(time.Second)
	s.Require().NoError(s.router.phone.Outcomes().Answered(context.Background(), key, answered))
	s.Require().NoError(s.router.phone.Outcomes().Ended(context.Background(), key, answered.Add(75*time.Second), outcome.Leg{}))

	read := s.serverClient.getSession(opened.Id)

	s.Require().NotNil(read.CallOutcome)
	s.Equal(CallOutcomeCompleted, value(read.CallOutcome.Outcome))
	s.Require().NotNil(read.CallOutcome.AnsweredAt)
	s.True(answered.Equal(*read.CallOutcome.AnsweredAt))
	s.Equal(75, value(read.CallOutcome.DurationSeconds))
}

func (s *CallStatusSuite) TestASessionWithoutAPlacedCallHasNoCallOutcome() {
	opened := s.serverClient.createSession(textSession(nil))

	s.Nil(s.serverClient.getSession(opened.Id).CallOutcome)
}

func (s *CallStatusSuite) TestADeviceIsNotShownTheCallOutcome() {
	backend := s.serverClient.actingFor(s.client)
	opened := backend.createSession(textSession(nil))
	s.busy(opened.Id)

	s.Nil(s.client.getSession(opened.Id).CallOutcome)
	s.NotNil(backend.getSession(opened.Id).CallOutcome, "the backend is shown the outcome of the same session")
}

func (s *CallStatusSuite) TestABusyStatusEndsItsSessionBeforeTheStartTimeout() {
	// The suite's vendors have no credentials to place a call with, so the session is opened
	// as the phone service opens one, by the manager.
	opened, err := s.manager.Create(context.Background(), session.Spec{CustomerID: s.customerID(),
		Phone: &session.PhoneSpec{VendorCallID: "CA1"}, Attribution: routing.Attribution{APIKey: "key-phone"}})
	s.Require().NoError(err)
	token, _ := s.placedFor(opened.ID())

	s.Require().Equal(http.StatusNoContent, s.postForm(token, url.Values{"CallStatus": {"busy"}}))

	s.Require().Eventually(func() bool { return !s.manager.Running(s.customerID(), opened.ID()) }, 10*time.Second, 20*time.Millisecond,
		"the session waits for its start timeout")
}

// openedHere is a session on the suite's node opened as the phone service opens one for a
// placed call. The suite's vendors have no credentials to place a call with.
func (s *CallStatusSuite) openedHere() string {
	opened, err := s.manager.Create(context.Background(), session.Spec{CustomerID: s.customerID(),
		Phone: &session.PhoneSpec{VendorCallID: "CA1"}, Attribution: routing.Attribution{APIKey: "key-phone"}})
	s.Require().NoError(err)
	return opened.ID()
}

// keptBy checks that the node logging to logged decided to keep the session id, which sessions
// still runs.
func (s *CallStatusSuite) keptBy(logged *lockedLog, sessions *session.Manager, id string) {
	s.Require().Eventually(func() bool {
		for line := range strings.Lines(logged.String()) {
			if strings.Contains(line, "kept the session of an unanswered call") && strings.Contains(line, "session="+id) {
				return true
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond, "the node did not say it kept the session")
	s.True(sessions.Running(s.customerID(), id), "the session was ended")
}

// A session can have two calls placed into it. A report that the older went unanswered does
// not end the session while the newer still rings.
func (s *CallStatusSuite) TestABusyOlderCallKeepsASessionANewerCallStillRingsInto() {
	id := s.openedHere()
	older, _ := s.placedFor(id)
	s.placedFor(id)

	s.Require().Equal(http.StatusNoContent, s.postForm(older, url.Values{"CallStatus": {"busy"}}))

	s.keptBy(s.logged, s.manager, id)
}

func (s *CallStatusSuite) TestABusyNewerCallEndsASessionAnOlderCallStillRingsInto() {
	id := s.openedHere()
	s.placedFor(id)
	newer, _ := s.placedFor(id)

	s.Require().Equal(http.StatusNoContent, s.postForm(newer, url.Values{"CallStatus": {"busy"}}))

	s.Require().Eventually(func() bool { return !s.manager.Running(s.customerID(), id) }, 10*time.Second, 20*time.Millisecond,
		"the session waits for its start timeout")
}

// The nodes a test makes share the suite's phone service; the vendor's status still reaches
// the suite's node, as in a deployment where each node has its own.
func (s *CallStatusSuite) TestAnotherNodeDoesNotTakeTheSuitesUnansweredCalls() {
	logged := &lockedLog{}
	s.peerHolding(logged)
	id := s.openedHere()
	older, _ := s.placedFor(id)
	s.placedFor(id)

	s.Require().Equal(http.StatusNoContent, s.postForm(older, url.Values{"CallStatus": {"busy"}}))

	s.keptBy(s.logged, s.manager, id)
	s.NotContains(logged.String(), id)
}

// peerHolding is a second node of the deployment, logging to logged, holding a session opened
// as the phone service opens one for a placed call. Each of adjust changes the node's options.
func (s *CallStatusSuite) peerHolding(logged *lockedLog, adjust ...func(*Options)) (*httptest.Server, *session.Manager, string) {
	other, _, sessions := s.otherNodeHolding(nil, s.sealer, slog.New(slog.NewTextHandler(logged, nil)), adjust...)
	opened, err := sessions.Create(context.Background(), session.Spec{CustomerID: s.customerID(),
		Phone: &session.PhoneSpec{VendorCallID: "CA1"}, Attribution: routing.Attribution{APIKey: "key-phone"}})
	s.Require().NoError(err)
	return other, sessions, opened.ID()
}

// askPeerToEnd asks other to end the session of an unanswered call the way a peer does.
func (s *CallStatusSuite) askPeerToEnd(other *httptest.Server, asked string) *node.ForwardResponse {
	request, err := http.NewRequest(http.MethodPost, unansweredPeerPath, strings.NewReader(asked))
	s.Require().NoError(err)
	credential, _ := s.sealer.Derive(node.CredentialPurpose)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	answer, err := node.NewForwarder(nil, credential).Ask(ctx, other.Listener.Addr().String(), request, []byte(asked))
	s.Require().NoError(err)
	return answer
}

// askedFor is the ask a peer makes for customerID's session id.
func (s *CallStatusSuite) askedFor(customerID, id string) string {
	asked, err := json.Marshal(unansweredPeerRequest{CustomerID: customerID, SessionID: id})
	s.Require().NoError(err)
	return string(asked)
}

// refusesToEnd checks that other answered an ask as no such call, said why, and still holds id.
func (s *CallStatusSuite) refusesToEnd(answer *node.ForwardResponse, logged *lockedLog, sessions *session.Manager, id string) {
	s.Equal(int32(http.StatusNotFound), answer.GetStatus(), "body: %s", answer.GetBody())
	s.Contains(string(answer.GetBody()), `"code":"`+codeCallNotFound+`"`)
	s.Contains(logged.String(), "refused a peer's ask to end a session")
	s.Never(func() bool { return !sessions.Running(s.customerID(), id) }, time.Second, 50*time.Millisecond,
		"the session was ended")
}

func (s *CallStatusSuite) TestABusyStatusEndsItsSessionOnTheNodeHoldingIt() {
	other, sessions, id := s.peerHolding(&lockedLog{})
	s.Require().NotEqual(other.Listener.Addr().String(), s.router.directory.Address())
	token, _ := s.placedFor(id)

	s.Require().Equal(http.StatusNoContent, s.postForm(token, url.Values{"CallStatus": {"busy"}}))

	s.Require().Eventually(func() bool { return !sessions.Running(s.customerID(), id) }, 10*time.Second, 20*time.Millisecond,
		"the node holding the session kept it")
}

func (s *CallStatusSuite) TestAPeersAskForACallStoredAsUnansweredEndsItsSession() {
	other, sessions, id := s.peerHolding(&lockedLog{})
	s.busy(id)

	answer := s.askPeerToEnd(other, s.askedFor(s.customerID(), id))

	s.Equal(int32(http.StatusNoContent), answer.GetStatus(), "body: %s", answer.GetBody())
	s.False(sessions.Running(s.customerID(), id), "the session waits for its start timeout")
}

func (s *CallStatusSuite) TestAPeersAskForACallThatStillRingsIsRefused() {
	logged := &lockedLog{}
	other, sessions, id := s.peerHolding(logged)
	s.placedFor(id)

	s.refusesToEnd(s.askPeerToEnd(other, s.askedFor(s.customerID(), id)), logged, sessions, id)
}

// A session can have two calls placed into it; the stored call is the newest, and the asking
// node decided on the one its vendor reported. An ask is refused unless the newest says
// nobody answered, since ending the session would strand a call that still rings.
func (s *CallStatusSuite) TestAPeersAskWhileANewerCallInTheSessionStillRingsIsRefused() {
	logged := &lockedLog{}
	other, sessions, id := s.peerHolding(logged)
	s.busy(id)
	s.placedFor(id)

	s.refusesToEnd(s.askPeerToEnd(other, s.askedFor(s.customerID(), id)), logged, sessions, id)
}

func (s *CallStatusSuite) TestAPeersAskWhileAnOlderCallInTheSessionStillRingsEndsItsSession() {
	other, sessions, id := s.peerHolding(&lockedLog{})
	s.placedFor(id)
	s.busy(id)

	answer := s.askPeerToEnd(other, s.askedFor(s.customerID(), id))

	s.Equal(int32(http.StatusNoContent), answer.GetStatus(), "body: %s", answer.GetBody())
	s.False(sessions.Running(s.customerID(), id), "the session waits for its start timeout")
}

func (s *CallStatusSuite) TestAPeersAskForACallSomebodyAnsweredIsRefused() {
	logged := &lockedLog{}
	other, sessions, id := s.peerHolding(logged)
	token, key := s.placedFor(id)
	s.Require().NoError(s.router.phone.Outcomes().Answered(context.Background(), key, time.Now()))
	_, err := s.router.phone.Outcomes().Reported(context.Background(), token, outcome.Report{Outcome: outcome.Busy, Word: "busy"})
	s.Require().NoError(err)

	s.refusesToEnd(s.askPeerToEnd(other, s.askedFor(s.customerID(), id)), logged, sessions, id)
}

func (s *CallStatusSuite) TestAPeersAskForACallThatIsNotKeptIsRefused() {
	logged := &lockedLog{}
	other, sessions, id := s.peerHolding(logged)

	s.refusesToEnd(s.askPeerToEnd(other, s.askedFor(s.customerID(), id)), logged, sessions, id)
}

func (s *CallStatusSuite) TestAPeerWithoutAPhoneServiceRefusesAnAsk() {
	logged := &lockedLog{}
	other, sessions, id := s.peerHolding(logged, func(options *Options) { options.Phone = nil })
	s.busy(id)

	s.refusesToEnd(s.askPeerToEnd(other, s.askedFor(s.customerID(), id)), logged, sessions, id)
}

func (s *CallStatusSuite) TestAPeerWhosePhoneServiceHasNoStoreRefusesAnAsk() {
	storeless, err := phone.NewService(phone.ServiceOptions{Registry: s.phoneVendors})
	s.Require().NoError(err)
	logged := &lockedLog{}
	other, sessions, id := s.peerHolding(logged, func(options *Options) { options.Phone = storeless })
	s.busy(id)

	s.refusesToEnd(s.askPeerToEnd(other, s.askedFor(s.customerID(), id)), logged, sessions, id)
}

// phoneOnAClosedStore is a phone service whose database is down.
func (s *CallStatusSuite) phoneOnAClosedStore() *phone.Service {
	closed, err := store.Open(testDatabase(s.T(), os.Getenv("ROUTER_POSTGRES_DSN")))
	s.Require().NoError(err)
	s.Require().NoError(closed.Close())
	service, err := phone.NewService(phone.ServiceOptions{Registry: s.phoneVendors, Store: closed})
	s.Require().NoError(err)
	return service
}

func (s *CallStatusSuite) TestAPeerThatCannotReadTheStoredCallKeepsTheSession() {
	closed := s.phoneOnAClosedStore()
	other, sessions, id := s.peerHolding(&lockedLog{}, func(options *Options) { options.Phone = closed })
	s.busy(id)

	answer := s.askPeerToEnd(other, s.askedFor(s.customerID(), id))

	s.Equal(int32(http.StatusInternalServerError), answer.GetStatus(), "body: %s", answer.GetBody())
	s.True(sessions.Running(s.customerID(), id), "the session was ended")
}

// The phone service hands a node an unanswered call once its row is written, so only a
// database that goes down in between keeps the node from reading it back.
func (s *CallStatusSuite) TestANodeThatCannotReadTheStoredCallKeepsTheSession() {
	logged := &lockedLog{}
	closed := s.phoneOnAClosedStore()
	_, server, sessions := s.otherNodeHolding(nil, s.sealer, slog.New(slog.NewTextHandler(logged, nil)),
		func(options *Options) { options.Phone = closed })
	opened, err := sessions.Create(context.Background(), session.Spec{CustomerID: s.customerID(),
		Phone: &session.PhoneSpec{VendorCallID: "CA1"}, Attribution: routing.Attribution{APIKey: "key-phone"}})
	s.Require().NoError(err)
	s.busy(opened.ID())

	server.endUnansweredNow(context.Background(), s.customerID(), opened.ID())

	s.Contains(logged.String(), "could not read an unanswered call")
	s.True(sessions.Running(s.customerID(), opened.ID()), "the session was ended")
}

func (s *CallStatusSuite) TestAPeersAskNamingAnotherCustomerIsRefused() {
	logged := &lockedLog{}
	other, sessions, id := s.peerHolding(logged)
	s.busy(id)

	s.refusesToEnd(s.askPeerToEnd(other, s.askedFor("another-"+s.utils.uuid(), id)), logged, sessions, id)
}

func (s *CallStatusSuite) TestAPeersAskNamingNoCustomerIsRefused() {
	logged := &lockedLog{}
	other, sessions, id := s.peerHolding(logged)
	s.busy(id)

	s.refusesToEnd(s.askPeerToEnd(other, s.askedFor("", id)), logged, sessions, id)
}

func (s *CallStatusSuite) TestAnEmptyPeerAskIsRefused() {
	logged := &lockedLog{}
	other, sessions, id := s.peerHolding(logged)
	s.busy(id)

	s.refusesToEnd(s.askPeerToEnd(other, `{}`), logged, sessions, id)
	s.refusesToEnd(s.askPeerToEnd(other, `null`), logged, sessions, id)
}

func (s *CallStatusSuite) TestAPeerAskOfTheWrongShapeIsRefused() {
	other, sessions, id := s.peerHolding(&lockedLog{})
	s.busy(id)

	for _, asked := range []string{`{"customer_id":1,"session_id":"` + id + `"}`, `[]`, `not json`} {
		answer := s.askPeerToEnd(other, asked)
		s.Equal(int32(http.StatusBadRequest), answer.GetStatus(), "%s: %s", asked, answer.GetBody())
	}
	s.True(sessions.Running(s.customerID(), id), "the session was ended")
}

// openPhoneSession opens the session id for a placed call as an SDK does, after placing it.
func (s *CallStatusSuite) openPhoneSession(id string) (int, ErrorResponse) {
	status, payload := s.serverClient.call(http.MethodPost, "/v1/agents/sessions", CreateSessionRequest{
		Id: &id, StartVoice: pointerTo(true), Phone: &SessionPhone{Number: s.utils.number()},
	})
	var answered ErrorResponse
	if status >= http.StatusBadRequest {
		s.Require().NoError(json.Unmarshal(payload, &answered), string(payload))
	}
	return status, answered
}

// openWithVoicemail opens the session id for a placed call with plan, as an SDK does.
func (s *CallStatusSuite) openWithVoicemail(id string, plan SessionVoicemail) (int, string) {
	status, payload := s.serverClient.call(http.MethodPost, "/v1/agents/sessions", CreateSessionRequest{
		Id: &id, StartVoice: pointerTo(true), Phone: &SessionPhone{Number: s.utils.number(), Voicemail: &plan},
	})
	return status, string(payload)
}

// endedForVoicemail waits for the session id to show it ended for an answering machine.
func (s *CallStatusSuite) endedForVoicemail(id string) {
	s.Require().Eventually(func() bool {
		shown := s.serverClient.getSession(id)
		return shown.EndReason != nil && *shown.EndReason == "voicemail"
	}, 10*time.Second, 50*time.Millisecond, "the session never showed it ended for voicemail")
}

func (s *CallStatusSuite) TestASessionAskedToHangUpOnAMachineDoesWhenOneAnswers() {
	id := s.utils.uuid()
	token, _ := s.placedFor(id)
	status, payload := s.openWithVoicemail(id, SessionVoicemail{Action: VoicemailActionHangUp})
	s.Require().Equal(http.StatusCreated, status, payload)

	answered, _ := s.postMachine(token, url.Values{"AnsweredBy": {"machine_end_silence"}})

	s.Equal(http.StatusNoContent, answered)
	s.endedForVoicemail(id)
}

func (s *CallStatusSuite) TestASessionAskedToLeaveAMessageLeavesItWhenAMachineAnswers() {
	id := s.utils.uuid()
	token, _ := s.placedFor(id)
	status, payload := s.openWithVoicemail(id, SessionVoicemail{
		Action: VoicemailActionLeaveMessage, Message: pointerTo("Hi Ali, this is Northwind Dental."),
	})
	s.Require().Equal(http.StatusCreated, status, payload)

	answered, _ := s.postMachine(token, url.Values{"AnsweredBy": {"machine_end_beep"}})

	s.Equal(http.StatusNoContent, answered)
	s.endedForVoicemail(id)
}

func (s *CallStatusSuite) TestAVoicemailPlanWithAnUnknownActionIsRefused() {
	status, payload := s.openWithVoicemail(s.utils.uuid(), SessionVoicemail{Action: "call_back"})

	s.Equal(http.StatusBadRequest, status, payload)
	s.Contains(payload, "action")
}

func (s *CallStatusSuite) TestLeavingAMessageWithNoMessageIsRefused() {
	status, payload := s.openWithVoicemail(s.utils.uuid(), SessionVoicemail{Action: VoicemailActionLeaveMessage})

	s.Equal(http.StatusBadRequest, status, payload)
	s.Contains(payload, "message")
}

func (s *CallStatusSuite) TestLeavingAMessageOfOnlySpacesIsRefused() {
	status, payload := s.openWithVoicemail(s.utils.uuid(), SessionVoicemail{
		Action: VoicemailActionLeaveMessage, Message: pointerTo("  \n\t "),
	})

	s.Equal(http.StatusBadRequest, status, payload)
	s.Contains(payload, "message")
}

func (s *CallStatusSuite) TestLeavingAMessageOnASpeechToSpeechAgentIsRefused() {
	plan := SessionVoicemail{Action: VoicemailActionLeaveMessage, Message: pointerTo("Hi Ali, this is Northwind Dental.")}
	status, payload := s.serverClient.call(http.MethodPost, "/v1/agents/sessions", CreateSessionRequest{
		Id: pointerTo(s.utils.uuid()), StartVoice: pointerTo(true), Sts: pointerTo("openai/gpt-realtime-2"),
		Phone: &SessionPhone{Number: s.utils.number(), Voicemail: &plan},
	})

	s.Equal(http.StatusBadRequest, status, "%s", payload)
	s.Contains(string(payload), "cannot say a fixed voicemail message")
}

func (s *CallStatusSuite) TestAVoicemailMessageOver1000CharactersIsRefused() {
	status, payload := s.openWithVoicemail(s.utils.uuid(), SessionVoicemail{
		Action: VoicemailActionLeaveMessage, Message: pointerTo(strings.Repeat("é", 1001)),
	})

	s.Equal(http.StatusBadRequest, status, payload)
	s.Contains(payload, "message")
}

func (s *CallStatusSuite) TestAVoicemailMessageOf1000CharactersIsTaken() {
	id := s.utils.uuid()
	status, payload := s.openWithVoicemail(id, SessionVoicemail{
		Action: VoicemailActionLeaveMessage, Message: pointerTo(strings.Repeat("é", 1000)),
	})
	defer s.serverClient.stopSession(id)

	s.Equal(http.StatusCreated, status, payload)
}

func (s *CallStatusSuite) TestOpeningASessionAfterItsCallWasBusyIsRefused() {
	id := s.utils.uuid()
	token, _ := s.placedFor(id)
	s.Require().Equal(http.StatusNoContent, s.postForm(token, url.Values{"CallStatus": {"busy"}}))

	status, answered := s.openPhoneSession(id)

	s.Equal(http.StatusConflict, status)
	s.Equal("call_unanswered", answered.Error.Code)
	s.Contains(answered.Error.Message, "busy")
	s.False(s.manager.Running(s.customerID(), id))
	s.Equal(outcome.Busy, s.read(id).Outcome)
}

func (s *CallStatusSuite) TestASessionOpensAfterItsCallCompleted() {
	id := s.utils.uuid()
	token, _ := s.placedFor(id)
	s.Require().Equal(http.StatusNoContent, s.postForm(token, url.Values{"CallStatus": {"completed"}}))

	status, answered := s.openPhoneSession(id)
	defer s.serverClient.stopSession(id)

	s.Equal(http.StatusCreated, status, answered.Error.Message)
	s.True(s.manager.Running(s.customerID(), id))
}

func (s *CallStatusSuite) TestASessionOpensWhileItsCallRings() {
	id := s.utils.uuid()
	s.placedFor(id)

	status, answered := s.openPhoneSession(id)
	defer s.serverClient.stopSession(id)

	s.Equal(http.StatusCreated, status, answered.Error.Message)
	s.True(s.manager.Running(s.customerID(), id))
}

// attachedForSessions is a number attached the way attaching does today: every caller gets a
// session of their own, so no one call is recorded.
func (s *PhoneSuite) attachedForSessions(customerID string) string {
	ctx := context.Background()
	number := s.utils.number()
	s.Require().NoError(s.store.PhoneNumbers.Insert(ctx, &store.PhoneNumber{
		E164: number, Vendor: "bics", Country: "US", CustomerID: customerID, PurchasedAt: time.Now().UTC(),
	}))
	s.Require().NoError(s.store.PhoneNumbers.Attach(ctx, customerID, number, store.NumberAttachment{
		TrunkID: "trunk-" + s.utils.uuid(), RouteID: "route-" + s.utils.uuid(), CallType: "agent",
	}))
	return number
}

func (s *PhoneSuite) listedNumber() PhoneNumber {
	var numbers []PhoneNumber
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/phone/numbers", nil, &numbers))
	s.Require().Len(numbers, 1)
	return numbers[0]
}

func (s *PhoneSuite) TestANumberCanBeGivenAnAgent() {
	s.useApp(s.data.createApp())
	number := s.attachedForSessions(s.customerID())
	config := s.config("echo/local-model")

	var answered PhoneNumber
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/phone/numbers/"+number+"/agent",
		SetNumberAgentRequest{ConfigId: &config}, &answered))

	s.Require().NotNil(answered.AgentConfigId)
	s.Equal(config, *answered.AgentConfigId)
	listed := s.listedNumber()
	s.Require().NotNil(listed.AgentConfigId)
	s.Equal(config, *listed.AgentConfigId)
}

func (s *PhoneSuite) TestANumberCanBeGivenAnAgentByName() {
	s.useApp(s.data.createApp())
	number := s.attachedForSessions(s.customerID())
	name := "agent-" + s.utils.uuid()
	var created AgentConfig
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs",
		AgentConfigRequest{Name: name, Llm: pointerTo("echo/local-model")}, &created))

	var answered PhoneNumber
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/phone/numbers/"+number+"/agent",
		SetNumberAgentRequest{Agent: &name}, &answered))

	s.Require().NotNil(answered.AgentConfigId)
	s.Equal(created.Id, *answered.AgentConfigId)
}

func (s *PhoneSuite) TestGivingANumberAnotherAgentReplacesIt() {
	s.useApp(s.data.createApp())
	number := s.attachedForSessions(s.customerID())
	first, second := s.config("echo/local-model"), s.config("echo/local-model")

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/phone/numbers/"+number+"/agent",
		SetNumberAgentRequest{ConfigId: &first}, nil))
	var answered PhoneNumber
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/phone/numbers/"+number+"/agent",
		SetNumberAgentRequest{ConfigId: &second}, &answered))

	s.Require().NotNil(answered.AgentConfigId)
	s.Equal(second, *answered.AgentConfigId)
}

func (s *PhoneSuite) TestANumbersAgentCanBeRemoved() {
	s.useApp(s.data.createApp())
	number := s.attachedForSessions(s.customerID())
	config := s.config("echo/local-model")
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/phone/numbers/"+number+"/agent",
		SetNumberAgentRequest{ConfigId: &config}, nil))

	s.Equal(http.StatusNoContent, s.serverClient.do(http.MethodDelete, "/v1/phone/numbers/"+number+"/agent", nil, nil))

	s.Nil(s.listedNumber().AgentConfigId)
	s.Equal(http.StatusNoContent, s.serverClient.do(http.MethodDelete, "/v1/phone/numbers/"+number+"/agent", nil, nil))
}

func (s *PhoneSuite) TestAConfigANumberAnswersWithCannotBeDeleted() {
	s.useApp(s.data.createApp())
	number := s.attachedForSessions(s.customerID())
	config := s.config("echo/local-model")
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/phone/numbers/"+number+"/agent",
		SetNumberAgentRequest{ConfigId: &config}, nil))

	status, failure := s.serverClient.failure(http.MethodDelete, "/v1/agents/configs/"+config, nil)

	s.Equal(http.StatusConflict, status)
	s.Contains(failure, number)
	s.Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/configs/"+config, nil, nil))
}

func (s *PhoneSuite) TestAConfigCanBeDeletedOnceNoNumberAnswersWithIt() {
	s.useApp(s.data.createApp())
	number := s.attachedForSessions(s.customerID())
	config := s.config("echo/local-model")
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/phone/numbers/"+number+"/agent",
		SetNumberAgentRequest{ConfigId: &config}, nil))
	s.Require().Equal(http.StatusNoContent, s.serverClient.do(http.MethodDelete, "/v1/phone/numbers/"+number+"/agent", nil, nil))

	s.Equal(http.StatusNoContent, s.serverClient.do(http.MethodDelete, "/v1/agents/configs/"+config, nil, nil))
}

func (s *PhoneSuite) TestANumberDetachedByAnOlderRouterDoesNotKeepItsConfig() {
	// A router from before agents on numbers detaches without clearing the number's agent.
	s.useApp(s.data.createApp())
	ctx := context.Background()
	number := s.attachedForSessions(s.customerID())
	config := s.config("echo/local-model")
	s.Require().NoError(s.store.PhoneNumbers.SetAgent(ctx, s.customerID(), number, config))
	_, err := s.store.GlobalDB().NewUpdate().Model((*store.PhoneNumber)(nil)).
		Set("attached_at = NULL").Where("e164 = ?", number).Exec(ctx)
	s.Require().NoError(err)

	s.Equal(http.StatusNoContent, s.serverClient.do(http.MethodDelete, "/v1/agents/configs/"+config, nil, nil))
}

func (s *PhoneSuite) TestTheRefusedDeleteSaysHowToFreeTheConfig() {
	s.useApp(s.data.createApp())
	first, second := s.attachedForSessions(s.customerID()), s.attachedForSessions(s.customerID())
	config := s.config("echo/local-model")
	s.Require().NoError(s.store.PhoneNumbers.SetAgent(context.Background(), s.customerID(), first, config))

	_, failure := s.serverClient.failure(http.MethodDelete, "/v1/agents/configs/"+config, nil)
	s.Contains(failure, "number "+first+" answers")
	s.Contains(failure, "DELETE /v1/phone/numbers/{e164}/agent")

	s.Require().NoError(s.store.PhoneNumbers.SetAgent(context.Background(), s.customerID(), second, config))
	_, failure = s.serverClient.failure(http.MethodDelete, "/v1/agents/configs/"+config, nil)
	s.Contains(failure, "numbers ")
	s.Contains(failure, first)
	s.Contains(failure, second)
}

func (s *PhoneSuite) TestANumberThatIsNotAttachedCannotBeGivenAnAgent() {
	s.useApp(s.data.createApp())
	number := s.utils.number()
	s.Require().NoError(s.store.PhoneNumbers.Insert(context.Background(), &store.PhoneNumber{
		E164: number, Vendor: "bics", Country: "US", CustomerID: s.customerID(), PurchasedAt: time.Now().UTC(),
	}))
	config := s.config("echo/local-model")

	status, failure := s.serverClient.failure(http.MethodPut, "/v1/phone/numbers/"+number+"/agent",
		SetNumberAgentRequest{ConfigId: &config})

	s.Equal(http.StatusConflict, status)
	s.Contains(failure, "attach the number first")
}

func (s *PhoneSuite) TestANumberAttachedToACallOfItsOwnCannotBeGivenAnAgent() {
	s.useApp(s.data.createApp())
	number := s.attachedNumber(s.customerID())
	config := s.config("echo/local-model")

	status, failure := s.serverClient.failure(http.MethodPut, "/v1/phone/numbers/"+number+"/agent",
		SetNumberAgentRequest{ConfigId: &config})

	s.Equal(http.StatusConflict, status)
	s.Contains(failure, "attach the number again")
}

func (s *PhoneSuite) TestAnAgentMustBeOneOfTheCustomersConfigs() {
	s.useApp(s.data.createApp())
	number := s.attachedForSessions(s.customerID())
	unknown := "config-" + s.utils.uuid()

	status, failure := s.serverClient.failure(http.MethodPut, "/v1/phone/numbers/"+number+"/agent",
		SetNumberAgentRequest{ConfigId: &unknown})
	s.Equal(http.StatusNotFound, status)
	s.Contains(failure, errUnknownConfig.Message)

	status, _ = s.serverClient.failure(http.MethodPut, "/v1/phone/numbers/"+number+"/agent", SetNumberAgentRequest{})
	s.Equal(http.StatusBadRequest, status)
}

func (s *PhoneSuite) TestAnotherCustomersNumberCannotBeGivenAnAgent() {
	s.useApp(s.data.createApp())
	theirs := s.attachedForSessions(s.data.createApp().app.ID)
	config := s.config("echo/local-model")

	status, _ := s.serverClient.failure(http.MethodPut, "/v1/phone/numbers/"+theirs+"/agent",
		SetNumberAgentRequest{ConfigId: &config})

	s.Equal(http.StatusNotFound, status)
}

func (s *PhoneSuite) TestDetachingANumberRemovesItsAgent() {
	s.useApp(s.data.createApp())
	number := s.attachedForSessions(s.customerID())
	config := s.config("echo/local-model")
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/phone/numbers/"+number+"/agent",
		SetNumberAgentRequest{ConfigId: &config}, nil))

	s.Require().Equal(http.StatusNoContent, s.serverClient.do(http.MethodDelete, "/v1/phone/numbers/"+number+"/attach", nil, nil))

	answering, err := s.store.PhoneNumbers.AnsweredBy(context.Background(), s.customerID(), config)
	s.Require().NoError(err)
	s.Empty(answering)
	s.Nil(s.listedNumber().AgentConfigId)
}

func (s *PhoneSuite) TestReleasingANumberRemovesItsAgent() {
	s.useApp(s.data.createApp())
	ctx := context.Background()
	number := s.attachedForSessions(s.customerID())
	config := s.config("echo/local-model")
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/phone/numbers/"+number+"/agent",
		SetNumberAgentRequest{ConfigId: &config}, nil))

	group := store.TrunkGroup{CustomerID: s.customerID(), Vendor: "bics"}
	s.Require().NoError(s.store.StreamTrunks.InGroup(ctx, group, func(_ context.Context, tx *store.TrunkGroupTx) error {
		return tx.ReleaseNumber(number, time.Time{})
	}))

	answering, err := s.store.PhoneNumbers.AnsweredBy(ctx, s.customerID(), config)
	s.Require().NoError(err)
	s.Empty(answering)
	held, err := s.store.PhoneNumbers.Select(ctx, s.customerID(), store.PhoneNumberFilter{IncludeReleased: true})
	s.Require().NoError(err)
	s.Require().Len(held, 1)
	s.Empty(held[0].ConfigID, "a released row keeps no agent")
}

func (s *PhoneSuite) TestReattachingANumberKeepsItsAgent() {
	s.useApp(s.data.createApp())
	number := s.attachedForSessions(s.customerID())
	config := s.config("echo/local-model")
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/phone/numbers/"+number+"/agent",
		SetNumberAgentRequest{ConfigId: &config}, nil))

	s.Require().NoError(s.store.PhoneNumbers.Attach(context.Background(), s.customerID(), number, store.NumberAttachment{
		TrunkID: "trunk-" + s.utils.uuid(), RouteID: "route-" + s.utils.uuid(), CallType: "agent",
	}))

	listed := s.listedNumber()
	s.Require().NotNil(listed.AgentConfigId)
	s.Equal(config, *listed.AgentConfigId)
}

func (s *PhoneSuite) TestClearingANumbersLineRemovesItsAgent() {
	s.useApp(s.data.createApp())
	ctx := context.Background()
	number := s.attachedForSessions(s.customerID())
	config := s.config("echo/local-model")
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/phone/numbers/"+number+"/agent",
		SetNumberAgentRequest{ConfigId: &config}, nil))

	group := store.TrunkGroup{CustomerID: s.customerID(), Vendor: "bics"}
	s.Require().NoError(s.store.StreamTrunks.InGroup(ctx, group, func(_ context.Context, tx *store.TrunkGroupTx) error {
		return tx.ClearLine(number)
	}))

	answering, err := s.store.PhoneNumbers.AnsweredBy(ctx, s.customerID(), config)
	s.Require().NoError(err)
	s.Empty(answering)
	s.Nil(s.listedNumber().AgentConfigId)
}

// arrivedFor opens the leg of a caller whose call was dispatched to a customer's worker.
func (s *CallStatusSuite) arrivedFor(customer string) string {
	call := s.utils.uuid()
	s.router.phone.OpenLeg(context.Background(), routing.Owner{CustomerID: customer}, store.CallLeg{
		CallType: "agent", CallID: call, StreamSessionID: "stream-" + call, Vendor: "twilio", Direction: store.Inbound,
		LocalNumber: s.utils.number(), RemoteNumber: s.utils.number(), DialedAt: time.Now().Add(-time.Minute),
	}, true)
	return call
}

// callInto places a call into a session from a number the suite's app holds.
func (s *CallStatusSuite) callInto(session string) (int, string) {
	e164 := s.utils.number()
	s.Require().NoError(s.store.PhoneNumbers.Insert(context.Background(), &store.PhoneNumber{
		E164: e164, Vendor: "twilio", Country: "US", CustomerID: s.customerID(), PurchasedAt: time.Now().UTC(),
	}))
	status, payload := s.serverClient.call(http.MethodPost, "/v1/phone/calls", PlaceCallRequest{
		From: e164, To: "+15550001111", SessionId: &session,
	})
	return status, string(payload)
}

func (s *CallStatusSuite) TestACallCannotBeRoutedIntoACallDispatchedToAnotherCustomer() {
	s.useApp(s.data.createApp())
	theirs := s.arrivedFor("customer-" + s.utils.uuid())

	status, payload := s.callInto(theirs)

	s.Equal(http.StatusConflict, status, payload)
	s.Contains(payload, "session_id is taken")
}

// A call to a vendor nothing answers is phone_vendor_failed: one let past the router's own checks.
func (s *CallStatusSuite) TestACallMayBeRoutedIntoACallDispatchedToTheCaller() {
	s.useApp(s.data.createApp())

	status, payload := s.callInto(s.arrivedFor(s.customerID()))

	s.Equal(http.StatusServiceUnavailable, status, payload)
	s.Contains(payload, "phone_vendor_failed")
}

// An incognito session has no row, so only the node running it says whose its call is.
func (s *CallStatusSuite) TestACallCannotBeRoutedIntoAnotherCustomersSessionOnAnotherNode() {
	s.useApp(s.data.createApp())
	_, _, sessions := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler))
	opened, err := sessions.Create(context.Background(), session.Spec{CustomerID: "customer-" + s.utils.uuid(), Text: true,
		Incognito: true, Attribution: routing.Attribution{APIKey: "key"}})
	s.Require().NoError(err)

	status, payload := s.callInto(opened.ID())

	s.Equal(http.StatusConflict, status, payload)
}

func (s *CallStatusSuite) TestACallMayBeRoutedIntoTheCallersSessionOnAnotherNode() {
	s.useApp(s.data.createApp())
	_, _, sessions := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler))
	opened, err := sessions.Create(context.Background(), session.Spec{CustomerID: s.customerID(), Text: true,
		Incognito: true, Attribution: routing.Attribution{APIKey: "key"}})
	s.Require().NoError(err)

	status, payload := s.callInto(opened.ID())

	s.Equal(http.StatusServiceUnavailable, status, payload)
	s.Contains(payload, "phone_vendor_failed")
}

func (s *CallStatusSuite) TestASessionCannotBeCreatedOnACallDispatchedToAnotherCustomer() {
	s.useApp(s.data.createApp())
	theirs := s.arrivedFor("customer-" + s.utils.uuid())

	s.Equal(http.StatusConflict, s.serverClient.do(http.MethodPost, "/v1/agents/sessions", textSession(&theirs), nil))
}

func (s *CallStatusSuite) TestASessionMayBeCreatedOnACallDispatchedToItsCustomer() {
	s.useApp(s.data.createApp())
	mine := s.arrivedFor(s.customerID())

	s.Equal(mine, s.serverClient.createSession(textSession(&mine)).Id)
}

func (s *CallStatusSuite) TestASessionCannotBeCreatedUnderTheIdOfAnotherCustomersOnAnotherNode() {
	s.useApp(s.data.createApp())
	_, _, sessions := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler))
	opened, err := sessions.Create(context.Background(), session.Spec{CustomerID: "customer-" + s.utils.uuid(), Text: true,
		Incognito: true, Attribution: routing.Attribution{APIKey: "key"}})
	s.Require().NoError(err)
	id := opened.ID()

	s.Equal(http.StatusConflict, s.serverClient.do(http.MethodPost, "/v1/agents/sessions", textSession(&id), nil))
}

// A customer in a Stream app of its own names its sessions without regard to another app's.
func (s *CallStatusSuite) TestASessionMayBeCreatedUnderTheNameOfAnotherAppsCall() {
	s.useApp(s.data.createApp())
	theirs := s.arrivedFor("customer-" + s.utils.uuid())
	s.giveApp(s.customerID(), streamAppID(), "key-"+s.customerID())
	_, _, sessions := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler))
	opened, err := sessions.Create(context.Background(), session.Spec{CustomerID: "customer-" + s.utils.uuid(), Text: true,
		Incognito: true, Attribution: routing.Attribution{APIKey: "key"}})
	s.Require().NoError(err)

	s.serverClient.createSession(textSession(&theirs))
	s.Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/sessions", textSession(pointerTo(opened.ID())), nil))
}

// A customer whose work is pinned to the deployment's app's id and one pinned to none act in one
// app.
func (s *CallStatusSuite) TestACallCannotBeRoutedIntoASessionOfTheDeploymentsAppWhicheverWayEitherIsPinned() {
	s.useApp(s.data.createApp())
	theirs := "customer-" + s.utils.uuid()
	s.giveApp(theirs, suiteStreamApp, "key-"+theirs)
	_, _, sessions := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler))
	hold := func(customer string) string {
		opened, err := sessions.Create(context.Background(), session.Spec{CustomerID: customer, Text: true,
			Incognito: true, Attribution: routing.Attribution{APIKey: "key"}})
		s.Require().NoError(err)
		return opened.ID()
	}

	status, payload := s.callInto(hold(theirs))
	s.Equal(http.StatusConflict, status, payload)

	unpinned := hold("customer-" + s.utils.uuid())
	s.giveApp(s.customerID(), suiteStreamApp, "key-"+s.customerID())
	status, payload = s.callInto(unpinned)
	s.Equal(http.StatusConflict, status, payload)
	s.Equal(http.StatusConflict, s.serverClient.do(http.MethodPost, "/v1/agents/sessions", textSession(&unpinned), nil))
}

// Customers of a deployment in app mode with no app to act in share no call.
func (s *CallStatusSuite) TestASessionOfNoStreamAppIsOnNoCallOfAnothers() {
	s.useApp(s.data.createApp())
	theirs := "customer-" + s.utils.uuid()
	s.setApps(theirs, func(apps *suiteApps) {
		apps.perApp = true
		apps.nowhere = map[string]bool{theirs: true, s.customerID(): true}
	})
	s.stream.Invalidate(s.customerID())
	s.T().Cleanup(func() { s.setApps(theirs, func(apps *suiteApps) { apps.perApp = false; apps.nowhere = nil }) })
	_, _, sessions := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler))
	opened, err := sessions.Create(context.Background(), session.Spec{CustomerID: theirs, Text: true,
		Incognito: true, Attribution: routing.Attribution{APIKey: "key"}})
	s.Require().NoError(err)
	request := textSession(pointerTo(opened.ID()))
	request.Incognito = pointerTo(true)

	s.Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/sessions", request, nil))
}

func (s *CallStatusSuite) TestASessionMayBeCreatedUnderTheIdOfTheCallersOnAnotherNode() {
	s.useApp(s.data.createApp())
	_, _, sessions := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler))
	opened, err := sessions.Create(context.Background(), session.Spec{CustomerID: s.customerID(), Text: true,
		Incognito: true, Attribution: routing.Attribution{APIKey: "key"}})
	s.Require().NoError(err)
	request := textSession(pointerTo(opened.ID()))
	request.Incognito = pointerTo(true)

	s.Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/sessions", request, nil))
}

// heldOn opens an incognito session of a customer's with sessions, in writing.
func heldOn(sessions *session.Manager, customer, id string) error {
	_, err := sessions.Create(context.Background(), session.Spec{CustomerID: customer, ID: id, Text: true,
		Incognito: true, Attribution: routing.Attribution{APIKey: "key"}})
	return err
}

func (s *CallStatusSuite) TestOfTwoCustomersCreatingOneIdAtOnceOnTwoNodesOneIsLetIn() {
	s.useApp(s.data.createApp())
	_, _, first := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler))
	_, _, second := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler))
	for range 20 {
		id := s.utils.uuid()
		var errs [2]error
		var wg sync.WaitGroup
		for i, sessions := range []*session.Manager{first, second} {
			wg.Go(func() { errs[i] = heldOn(sessions, "customer-"+s.utils.uuid(), id) })
		}
		wg.Wait()

		s.True(errors.Is(errs[0], session.ErrSessionExists) != errors.Is(errs[1], session.ErrSessionExists), "%v", errs)
	}
}

// A call being kept and a session opening under its id are one before the other, however long
// the leg takes to keep.
func (s *CallStatusSuite) TestASessionOpenedWhileAnotherCustomersCallIsPlacedIntoItsIdIsRefused() {
	s.useApp(s.data.createApp())
	_, _, sessions := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler))
	ctx := context.Background()
	_, err := s.store.GlobalDB().ExecContext(ctx, "CREATE OR REPLACE FUNCTION slow_call() RETURNS trigger AS $$ BEGIN IF NEW.customer_id = '"+s.customerID()+
		"' THEN PERFORM pg_sleep(1); END IF; RETURN NEW; END $$ LANGUAGE plpgsql")
	s.Require().NoError(err)
	_, err = s.store.GlobalDB().ExecContext(ctx, "CREATE OR REPLACE TRIGGER slow_call BEFORE INSERT ON call_legs FOR EACH ROW EXECUTE FUNCTION slow_call()")
	s.Require().NoError(err)
	defer func() {
		_, err := s.store.GlobalDB().ExecContext(ctx, "DROP TRIGGER slow_call ON call_legs; DROP FUNCTION slow_call()")
		s.Require().NoError(err)
	}()
	id := s.utils.uuid()
	called := make(chan int, 1)
	go func() {
		status, _ := s.callInto(id)
		called <- status
	}()
	s.Require().Eventually(func() bool {
		var asleep bool
		s.Require().NoError(s.store.GlobalDB().NewSelect().ColumnExpr("EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event = 'PgSleep' AND datname = current_database())").Scan(ctx, &asleep))
		return asleep
	}, settleFor, 5*time.Millisecond)

	err = heldOn(sessions, "customer-"+s.utils.uuid(), id)

	s.Equal(http.StatusServiceUnavailable, <-called, "the call is let in, to a vendor nothing answers")
	s.ErrorIs(err, session.ErrSessionExists)
}

// A session carrying on a conversation is on the call of the app the conversation is kept in.
func (s *CallStatusSuite) TestASessionResumedInAnotherAppIsRefusedACallOfAnotherCustomersThere() {
	s.useApp(s.data.createApp())
	_, _, sessions := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler))
	spec := session.Spec{CustomerID: s.customerID(), Text: true, PersistConversation: true, Caller: routing.Caller{UserID: "user-" + s.utils.uuid()},
		AgentID: "agent-" + s.utils.uuid(), Attribution: routing.Attribution{APIKey: "key"}}
	first, err := sessions.Create(context.Background(), spec)
	s.Require().NoError(err)
	spec.ConversationID, spec.AgentID = first.Spec().ConversationID, first.Spec().AgentID
	s.Require().True(sessions.Close(first.ID(), session.OwnerOf(first.Spec())))
	spec.ID = s.arrivedFor("customer-" + s.utils.uuid())
	app := streamAppID()
	s.giveApp(s.customerID(), app, "key-"+s.customerID())

	_, err = sessions.Create(context.Background(), spec)
	s.ErrorIs(err, session.ErrSessionExists)

	spec.ID = s.utils.uuid()
	resumed, err := sessions.Create(context.Background(), spec)
	s.Require().NoError(err)
	s.Equal(http.StatusConflict, s.data.backendOfAnotherApp().do(http.MethodPost, "/v1/agents/sessions", textSession(pointerTo(resumed.ID())), nil),
		"the session holds its call in the app it acts in")
	_, err = s.router.directory.HoldCall(context.Background(), "another-customer", app, resumed.ID())
	s.NoError(err, "and no longer in the one it was to act in")
}

// A session that ends, or fails to open, leaves its call to whoever opens it next.
func (s *CallStatusSuite) TestACallIsLeftWhenItsSessionEnds() {
	s.useApp(s.data.createApp())
	_, _, sessions := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler))
	id, mine := s.utils.uuid(), "customer-"+s.utils.uuid()
	s.Require().NoError(heldOn(sessions, mine, id))
	s.ErrorIs(heldOn(s.manager, "customer-"+s.utils.uuid(), id), session.ErrSessionExists)
	s.Require().True(sessions.Close(id, session.Owner{CustomerID: mine, Kind: auth.KindServer}))
	failing := session.Spec{CustomerID: mine, ID: id, Text: true, PersistConversation: true, ConversationID: "agent:" + s.utils.uuid(), Attribution: routing.Attribution{APIKey: "key"}}
	_, err := sessions.Create(context.Background(), failing)
	s.Require().Error(err)

	s.NoError(heldOn(s.manager, "customer-"+s.utils.uuid(), id))
}

func (s *CallStatusSuite) TestASessionOfAMintedIdOrReopenedIsAskedNothing() {
	s.useApp(s.data.createApp())
	theirs := s.arrivedFor("customer-" + s.utils.uuid())
	spec := session.Spec{CustomerID: s.customerID(), ID: theirs, Text: true, Incognito: true, Attribution: routing.Attribution{APIKey: "key"}}
	_, err := s.manager.Create(context.Background(), spec)
	s.Require().ErrorIs(err, session.ErrSessionExists)

	spec.MintedID = true
	minted, err := s.manager.Create(context.Background(), spec)
	s.Require().NoError(err)
	s.Require().True(s.manager.Close(minted.ID(), session.OwnerOf(minted.Spec())))
	kept := session.Spec{CustomerID: s.customerID(), Text: true, PersistConversation: true, Caller: routing.Caller{UserID: "user-" + s.utils.uuid()},
		AgentID: "agent-" + s.utils.uuid(), Attribution: routing.Attribution{APIKey: "key"}}
	first, err := s.manager.Create(context.Background(), kept)
	s.Require().NoError(err)
	s.Require().True(s.manager.Close(first.ID(), session.OwnerOf(first.Spec())))
	kept.ConversationID, kept.AgentID = first.Spec().ConversationID, first.Spec().AgentID
	kept.Reopened, kept.ID = time.Now(), s.arrivedFor("customer-"+s.utils.uuid())
	_, err = s.manager.Create(context.Background(), kept)
	s.NoError(err)
}

// deafRedis is the suite's Redis until deaf is set: from then on it takes what is sent it and
// answers nothing, as a Redis cut off by the network does.
//
// A connection it is sent any of deafTo on goes deaf by itself.
func (s *CallStatusSuite) deafRedis(deaf *atomic.Bool, deafTo ...string) rueidis.Client {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			upstream, err := net.Dial("tcp", os.Getenv("ROUTER_REDIS_ADDR"))
			if err != nil {
				_ = conn.Close()
				continue
			}
			var cut atomic.Bool
			go func() {
				defer upstream.Close()
				buffer := make([]byte, 64<<10)
				for {
					n, err := conn.Read(buffer)
					if err != nil {
						return
					}
					for _, word := range deafTo {
						if strings.Contains(string(buffer[:n]), word) {
							cut.Store(true)
						}
					}
					if _, err := upstream.Write(buffer[:n]); err != nil {
						return
					}
				}
			}()
			go func() {
				buffer := make([]byte, 4096)
				for {
					n, err := upstream.Read(buffer)
					if err != nil {
						return
					}
					if deaf.Load() || cut.Load() {
						continue
					}
					if _, err := conn.Write(buffer[:n]); err != nil {
						return
					}
				}
			}()
		}
	}()
	client, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{listener.Addr().String()}, DisableCache: true, ForceSingleClient: true})
	s.Require().NoError(err)
	s.T().Cleanup(client.Close)
	return client
}

// With the directory not answering, a session of a chosen id is refused at once, since nobody can
// say whose its call is, and any other opens.
func (s *CallStatusSuite) TestADirectoryThatDoesNotAnswerHoldsUpNoSession() {
	s.useApp(s.data.createApp())
	var deaf atomic.Bool
	logger := slog.New(slog.DiscardHandler)
	directory, err := node.NewDirectory(node.DirectoryOptions{Redis: s.deafRedis(&deaf), Address: "node-deaf:1", Prefix: s.relayPrefix, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(directory.Close)
	sessions := s.sessionsWith(directory)
	s.Require().NoError(heldOn(sessions, s.customerID(), s.utils.uuid()))
	deaf.Store(true)

	began := time.Now()
	err = heldOn(sessions, s.customerID(), s.utils.uuid())
	s.ErrorIs(err, store.ErrCallUnchecked)
	s.Contains(s.logged.String(), "refused a session: could not tell whose its call is")
	s.Less(time.Since(began), 2900*time.Millisecond, "asked for a second, not for as long as the claim has")
	var answer APIError
	s.Require().ErrorAs(refusedRequest(err), &answer)
	s.Equal(ErrorTypeUnavailable, answer.Type)
	s.NoError(heldOn(sessions, s.customerID(), ""))
	_, err = sessions.Create(context.Background(), session.Spec{CustomerID: s.customerID(), ID: s.utils.uuid(), MintedID: true, Text: true,
		Incognito: true, Attribution: routing.Attribution{APIKey: "key"}})
	s.NoError(err)
	s.Less(time.Since(began), 10*time.Second)

	// Letting go of a call is no slower.
	began = time.Now()
	release, err := directory.HoldCall(context.Background(), s.customerID(), 0, s.utils.uuid())
	s.Require().Error(err)
	release(context.Background())
	s.Less(time.Since(began), 2900*time.Millisecond)

	answering := s.router.directory
	s.router.directory = directory
	s.T().Cleanup(func() { s.router.directory = answering })
	began = time.Now()
	status, payload := s.callInto(s.utils.uuid())
	s.Less(time.Since(began), 2900*time.Millisecond)
	s.Equal(http.StatusServiceUnavailable, status, payload)
	s.Contains(payload, "could not tell whose call")
	s.Contains(s.logged.String(), "refused a phone call: could not tell whose its call is")
}

// sessionsWith is a node's sessions, said to be there by directory.
func (s *CallStatusSuite) sessionsWith(directory *node.Directory) *session.Manager {
	logger := slog.New(slog.NewTextHandler(s.logged, nil))
	sessions, err := session.NewManager(session.ManagerOptions{
		LLM: s.streams.LLM, Conversations: s.conversations, Stream: s.stream, Store: s.store, Directory: directory, Logger: logger,
		Writer: s.routerWriter(s.store, s.live, nil, logger),
		Edge: func(context.Context, session.Spec, streamapp.Bound, *slog.Logger) (agent.Edge, error) {
			return &silentEdge{inbound: make(chan agent.InboundAudio, 4)}, nil
		},
	})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = sessions.Shutdown() })
	return sessions
}

// Sessions of chosen ids that the directory does not answer for hold up no other placement.
func (s *CallStatusSuite) TestSessionsNotAnsweredForHoldUpNoOtherPlacement() {
	s.useApp(s.data.createApp())
	var deaf atomic.Bool
	directory, err := node.NewDirectory(node.DirectoryOptions{Redis: s.deafRedis(&deaf), Address: "node-deaf:2", Prefix: s.relayPrefix})
	s.Require().NoError(err)
	s.T().Cleanup(directory.Close)
	sessions := s.sessionsWith(directory)
	deaf.Store(true)
	var wg sync.WaitGroup
	began := time.Now()
	for range 64 {
		wg.Go(func() { s.ErrorIs(heldOn(sessions, s.customerID(), s.utils.uuid()), store.ErrCallUnchecked) })
	}

	at := time.Now()
	s.Require().NoError(s.store.CallLegs.PlaceWithin(context.Background(), &store.CallLeg{Token: s.utils.uuid(), CustomerID: s.customerID(),
		OrganizationID: s.utils.uuid(), CallType: "agent", CallID: s.utils.uuid(), Vendor: "telnyx", Direction: store.Outbound,
		LocalNumber: s.utils.number(), RemoteNumber: s.utils.number(), PlacedAt: &at}, store.CallsAtOnce{Limit: 9, Ringing: time.Hour}))

	s.Less(time.Since(at), time.Second)
	wg.Wait()
	s.Less(time.Since(began), 2900*time.Millisecond)
}

// A session refused after it said it is on its call says so no longer.
func (s *CallStatusSuite) TestASessionRefusedAfterItHeldItsCallLeavesIt() {
	s.useApp(s.data.createApp())
	var deaf atomic.Bool
	// Deaf to the hold, which Redis takes all the same.
	directory, err := node.NewDirectory(node.DirectoryOptions{Redis: s.deafRedis(&deaf, "HSET"), Address: "node-deaf:3", Prefix: s.relayPrefix})
	s.Require().NoError(err)
	s.T().Cleanup(directory.Close)
	id := s.utils.uuid()
	s.Require().ErrorIs(heldOn(s.sessionsWith(directory), "customer-"+s.utils.uuid(), id), store.ErrCallUnchecked)

	s.NoError(heldOn(s.manager, s.customerID(), id))
}

// Sessions closed as the router stops do not each wait for a directory that does not answer.
func (s *CallStatusSuite) TestARouterStopsThoughTheDirectoryDoesNotAnswer() {
	s.useApp(s.data.createApp())
	var deaf atomic.Bool
	directory, err := node.NewDirectory(node.DirectoryOptions{Redis: s.deafRedis(&deaf), Address: "node-deaf:4", Prefix: s.relayPrefix})
	s.Require().NoError(err)
	s.T().Cleanup(directory.Close)
	sessions := s.sessionsWith(directory)
	for range 5 {
		s.Require().NoError(heldOn(sessions, s.customerID(), s.utils.uuid()))
	}
	deaf.Store(true)

	began := time.Now()
	s.NoError(sessions.Shutdown())

	s.Less(time.Since(began), 2900*time.Millisecond)
}

// A router that stops takes back what its sessions said of their calls.
func (s *CallStatusSuite) TestACallIsLeftWhenItsRouterStops() {
	s.useApp(s.data.createApp())
	var deaf atomic.Bool
	directory, err := node.NewDirectory(node.DirectoryOptions{Redis: s.deafRedis(&deaf), Address: "node-stops:1", Prefix: s.relayPrefix})
	s.Require().NoError(err)
	s.T().Cleanup(directory.Close)
	sessions, other, id := s.sessionsWith(directory), s.sessionsWith(directory), s.utils.uuid()
	s.Require().NoError(heldOn(sessions, "customer-"+s.utils.uuid(), id))
	s.Require().ErrorIs(heldOn(other, "customer-"+s.utils.uuid(), id), session.ErrSessionExists)

	s.Require().NoError(sessions.Shutdown())

	s.Eventually(func() bool { return heldOn(other, "customer-"+s.utils.uuid(), id) == nil }, 5*time.Second, 50*time.Millisecond)
}
