//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GetStream/getstream-go/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation/chattest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/telnyx"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/twilio"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/vendors"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// moveTestPublicURL is the router's public URL in these tests; the hook URL is it plus the path.
const (
	moveTestPublicURL = "https://router.example.test"
	moveTestHookURL   = moveTestPublicURL + "/v1/phone/hooks/twilio"
	// moveTestOldURL stands for the voice URL a number attached before the webhook has.
	moveTestOldURL = "https://twimlets.com/echo?Twiml=old"
	moveTestKEK    = "phone move suite"
)

// PhoneMoveSuite runs router phone move-twilio against Postgres, a fake Stream and a fake
// Twilio, with numbers attached as the router attaches them.
type PhoneMoveSuite struct {
	suite.Suite
	ctx      context.Context
	store    *store.Store
	chat     *chattest.Server
	twilio   *fakeTwilio
	telnyx   *fakeTwilio
	token    string
	customer string
	// owners is the customer each number attachedEarlier gave out is held by.
	owners map[string]string
	// updatesBefore is how many requests Stream was sent when the last number was attached.
	updatesBefore int
}

func TestPhoneMoveSuite(t *testing.T) {
	suite.Run(t, new(PhoneMoveSuite))
}

func (s *PhoneMoveSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN not set")
	}
	s.ctx = context.Background()
	settings := config.Defaults()
	settings.Postgres.DSN = testenv.Database(dsn, "phone_move")
	opened, err := openStore(s.ctx, settings)
	s.Require().NoError(err)
	s.store = opened
	s.T().Cleanup(func() { s.Require().NoError(opened.Close()) })
}

func (s *PhoneMoveSuite) SetupTest() {
	// The database is the suite's own, and a run lists every customer's numbers: none of an
	// earlier test's may be left attached.
	s.releaseAll()
	// A token exported in the shell would let a test of a router without one pass.
	s.T().Setenv("TWILIO_WEBHOOK_AUTH_TOKEN", "")
	s.chat = chattest.NewServer(s.T())
	s.twilio = newFakeTwilio(s.T())
	s.telnyx = newFakeTwilio(s.T())
	s.token = rand.Text()
	s.customer = uuid.NewString()
	s.owners = map[string]string{}
}

// A dry run, the default, says what it would do with each number and writes nothing at Twilio.
func (s *PhoneMoveSuite) TestADryRunSaysWhatItWouldMoveAndWritesNothing() {
	first, second := s.attachedEarlier(), s.attachedEarlier()
	kept := s.attachedWithNoStoredBridge()

	out, err := s.move(s.ready())

	s.Require().NoError(err)
	s.Contains(out, second+"\tapp deployment\twould move\twould set allowlist\n")
	s.Contains(out, kept+"\tapp deployment\tno stored bridge, needs a re-attach\n")
	s.Contains(out, "0 moved, 2 to move, 0 already moved, 1 with no stored bridge\n")
	s.Contains(out, "nothing was written at Twilio or Stream")
	s.Empty(s.twilio.posted(), "nothing was written at Twilio")
	s.Equal(moveTestOldURL, s.twilio.voiceURL(first))
	s.Contains(out, first+"\tapp deployment\twould move\twould set allowlist\n")
	s.Contains(out, "allowlist, per number: 0 set, 2 to set, 0 already set\n")
	s.Zero(s.trunkUpdates(), "nothing was written at Stream")
	s.Empty(s.addresses(first))
}

// --apply points each number at the webhook once, by POST; a second run finds them there and
// writes nothing. A number whose trunk was not kept is not asked about at all.
func (s *PhoneMoveSuite) TestApplyMovesEachNumberOnceAndAgainMovesNone() {
	first, second := s.attachedEarlier(), s.attachedEarlier()
	kept := s.attachedWithNoStoredBridge()
	passwordBefore, _ := s.chat.TrunkAuth(s.trunkOf(first))

	out, err := s.move(s.ready(), "--apply")

	s.Require().NoError(err)
	s.Contains(out, "2 moved, 0 to move, 0 already moved, 1 with no stored bridge\n")
	s.NotContains(out, "nothing was written")
	posted := s.twilio.posted()
	s.Require().Len(posted, 2)
	for _, form := range posted {
		s.Equal(url.Values{"VoiceUrl": {moveTestHookURL}, "VoiceMethod": {http.MethodPost}}, form)
	}
	s.Equal(moveTestHookURL, s.twilio.voiceURL(first))
	s.Equal(moveTestHookURL, s.twilio.voiceURL(second))
	s.NotContains(strings.Join(s.twilio.asked(), "\n"), url.QueryEscape(kept))
	for _, trunk := range s.chat.Trunks() {
		password, _ := s.chat.TrunkAuth(trunk)
		s.Require().NotEmpty(password)
		s.NotContains(out, password, "the output names numbers only")
	}
	// Both numbers are on the customer's one shared trunk, which is written once, keeping its
	// numbers and password.
	s.Equal(s.trunkOf(first), s.trunkOf(second))
	s.Contains(out, first+"\tapp deployment\tmoved\tallowlist set\n")
	s.Contains(out, second+"\tapp deployment\tmoved\tallowlist already set\n")
	s.Contains(out, "allowlist, per number: 1 set, 0 to set, 1 already set\n")
	s.Equal(1, s.trunkUpdates())
	s.Equal(s.twilioRanges(), s.addresses(first))
	password, _ := s.chat.TrunkAuth(s.trunkOf(first))
	s.Equal(passwordBefore, password)
	s.ElementsMatch([]string{first, second}, s.chat.TrunkNumbers(s.trunkOf(first)))

	again, err := s.move(s.ready(), "--apply")

	s.Require().NoError(err)
	s.Contains(again, first+"\tapp deployment\talready moved\tallowlist already set\n")
	s.Contains(again, "0 moved, 0 to move, 2 already moved, 1 with no stored bridge\n")
	s.Contains(again, "allowlist, per number: 0 set, 0 to set, 2 already set\n")
	s.Len(s.twilio.posted(), 2, "nothing more was written")
	s.Equal(1, s.trunkUpdates(), "nothing more was written at Stream")
}

// Attaching a number that joins a live shared Twilio trunk leaves the trunk's addresses as they
// are: only a new trunk and --apply set them.
func (s *PhoneMoveSuite) TestAJoinLeavesALiveTrunksAddressesAlone() {
	first := s.attachedEarlier()
	second := moveTestNumber()
	s.twilio.hold(second)
	s.hold(second, "twilio")

	_, err := s.service(s.ready()).Attach(s.ctx, phone.Attachment{CustomerID: s.customer, E164: second})

	s.Require().NoError(err)
	s.owners[second] = s.customer
	s.Require().Equal(s.trunkOf(first), s.trunkOf(second), "joined the live shared trunk")
	s.ElementsMatch([]string{first, second}, s.chat.TrunkNumbers(s.trunkOf(first)))
	s.Empty(s.addresses(first), "a join gave a live trunk addresses it did not have")
}

// A number that joins the shared trunk while a run is between its read of the trunk and its
// update stays on the trunk at Stream: the run holds the trunk's group, as a join does.
func (s *PhoneMoveSuite) TestANumberJoiningDuringARunStaysOnTheTrunk() {
	first := s.attachedEarlier()
	trunk := s.trunkOf(first)
	waiting, release := s.chat.Hold("/sip/inbound_trunks/" + trunk)
	moved := make(chan error, 1)
	go func() {
		_, err := s.move(s.ready(), "--apply", "--one", first)
		moved <- err
	}()
	<-waiting
	second := moveTestNumber()
	s.twilio.hold(second)
	s.hold(second, "twilio")
	attached := make(chan error, 1)
	go func() {
		_, err := s.service(s.ready()).Attach(s.ctx, phone.Attachment{CustomerID: s.customer, E164: second})
		attached <- err
	}()
	// An attach that does not wait for the run joins well within this; one that waits only
	// ends once the run is let go.
	select {
	case err := <-attached:
		attached <- err
	case <-time.After(3 * time.Second):
	}

	release()

	s.Require().NoError(<-moved)
	s.Require().NoError(<-attached)
	s.owners[second] = s.customer
	s.Require().Equal(trunk, s.trunkOf(second))
	s.ElementsMatch([]string{first, second}, s.chat.TrunkNumbers(trunk), "the run's update dropped the number that joined")
	s.Equal(s.twilioRanges(), s.addresses(first))
}

// --apply keeps the trunk's name, which the run reads from Stream and sends back.
func (s *PhoneMoveSuite) TestApplyKeepsTheTrunksName() {
	number := s.attachedEarlier()

	_, err := s.move(s.ready(), "--apply")

	s.Require().NoError(err)
	listed, err := s.chat.Client.Video().ListSIPTrunks(s.ctx, &getstream.ListSIPTrunksRequest{})
	s.Require().NoError(err)
	i := slices.IndexFunc(listed.Data.SipTrunks, func(t getstream.SIPTrunkResponse) bool { return t.ID == s.trunkOf(number) })
	s.Require().GreaterOrEqual(i, 0)
	s.Equal("made earlier", listed.Data.SipTrunks[i].Name)
	s.Equal(s.twilioRanges(), s.addresses(number))
}

// A dry run over a trunk that has the addresses already says so, not that it would set them.
func (s *PhoneMoveSuite) TestADryRunOverATrunkThatHasTheAddressesSaysTheyAreSet() {
	number := s.attachedEarlier()
	trunk := s.trunkOf(number)
	s.Require().NoError(phone.NewStreamFromClient(s.chat.Client).UpdateTrunk(s.ctx, trunk,
		phone.Trunk{Name: "made earlier", Numbers: s.chat.TrunkNumbers(trunk), AllowedIPs: s.twilioRanges()}))

	out, err := s.move(s.ready())

	s.Require().NoError(err)
	s.Contains(out, number+"\tapp deployment\twould move\tallowlist already set\n")
	s.Contains(out, "allowlist, per number: 0 set, 0 to set, 1 already set\n")
}

// A trunk that has Twilio's addresses in another order is left alone.
func (s *PhoneMoveSuite) TestATrunkWithTheAddressesInAnotherOrderIsLeftAlone() {
	number := s.attachedEarlier()
	trunk := s.trunkOf(number)
	reversed := slices.Clone(s.twilioRanges())
	slices.Reverse(reversed)
	s.Require().NoError(phone.NewStreamFromClient(s.chat.Client).UpdateTrunk(s.ctx, trunk,
		phone.Trunk{Name: "made earlier", Numbers: s.chat.TrunkNumbers(trunk), AllowedIPs: reversed}))

	out, err := s.move(s.ready(), "--apply")

	s.Require().NoError(err)
	s.Contains(out, number+"\tapp deployment\tmoved\tallowlist already set\n")
}

// A run reads past one page of the store, and reports each number once.
func (s *PhoneMoveSuite) TestARunReportsEveryNumberPastOnePageOnce() {
	numbers := map[string]bool{}
	for range 101 {
		numbers[s.attachedWithNoStoredBridge()] = true
	}

	out, err := s.move(s.ready())

	s.Require().NoError(err)
	s.Contains(out, "0 moved, 0 to move, 0 already moved, 101 with no stored bridge\n")
	for number := range numbers {
		s.Equal(1, strings.Count(out, number+"\t"), number)
	}
}

// A number on the webhook by GET is moved: Twilio would ask it without the body the webhook reads.
func (s *PhoneMoveSuite) TestANumberOnTheWebhookByGetIsMoved() {
	number := s.attachedEarlier()
	s.twilio.point(number, moveTestHookURL, http.MethodGet)

	out, err := s.move(s.ready())

	s.Require().NoError(err)
	s.Contains(out, number+"\tapp deployment\twould move\twould set allowlist\n")
}

// --one moves that number and sets that number's trunk only, as the first step of a move.
func (s *PhoneMoveSuite) TestOneMovesOnlyThatNumber() {
	first := s.attachedEarlier()
	// Another customer's number, on a shared trunk of its own.
	s.customer = uuid.NewString()
	second := s.attachedEarlier()
	s.Require().NotEqual(s.trunkOf(first), s.trunkOf(second))

	out, err := s.move(s.ready(), "--apply", "--one", second)

	s.Require().NoError(err)
	s.Equal(second+"\tapp deployment\tmoved\tallowlist set\n"+
		"1 moved, 0 to move, 0 already moved, 0 with no stored bridge\nallowlist, per number: 1 set, 0 to set, 0 already set\n", out)
	s.Equal(moveTestOldURL, s.twilio.voiceURL(first))
	s.Equal(moveTestHookURL, s.twilio.voiceURL(second))
	s.Empty(s.addresses(first))
	s.Equal(s.twilioRanges(), s.addresses(second))
}

// --app moves the numbers made in that Stream app only.
func (s *PhoneMoveSuite) TestAppMovesOnlyTheNumbersOfThatApp() {
	deployments, theApps := s.attachedEarlier(), s.attachedEarlier()
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE phone_numbers SET stream_app_pk = 4242 WHERE e164 = ?", theApps)
	s.Require().NoError(err)

	out, err := s.move(s.ready(), "--apply", "--app", "4242")

	s.Require().NoError(err)
	s.Equal(theApps+"\tapp 4242\tmoved\tallowlist set\n1 moved, 0 to move, 0 already moved, 0 with no stored bridge\n"+
		"allowlist, per number: 1 set, 0 to set, 0 already set\n", out)
	s.Equal(moveTestOldURL, s.twilio.voiceURL(deployments))
}

// A run reads and writes a number's trunk in the Stream app the number is lined in, never in
// another one.
func (s *PhoneMoveSuite) TestARunWritesTheTrunkInTheNumbersOwnApp() {
	number := s.attachedEarlier()
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE phone_numbers SET stream_app_pk = 4242 WHERE e164 = ?", number)
	s.Require().NoError(err)
	other := chattest.NewServer(s.T())
	apps := s.ready().with(func(m *moverSettings) { m.apps = oneApp{app: 4242, theApp: s.chat, other: other} })

	out, err := s.move(apps, "--apply", "--app", "4242")

	s.Require().NoError(err, out)
	s.Contains(out, number+"\tapp 4242\tmoved\tallowlist set\n")
	s.Equal(s.twilioRanges(), s.addresses(number))
	for _, request := range other.Requests(other.Key) {
		s.False(request.Method == http.MethodPut && strings.Contains(request.Path, "/sip/inbound_trunks/"),
			"no trunk is written in another app: %s %s", request.Method, request.Path)
	}
}

// oneApp answers app with the Stream app theApp, and every other app with other.
type oneApp struct {
	app           int64
	theApp, other *chattest.Server
}

func (a oneApp) For(context.Context, string) (*phone.Stream, int64, error) {
	return phone.NewStreamFromClient(a.other.Client), 0, nil
}

func (a oneApp) ForApp(_ context.Context, _ string, app int64) (*phone.Stream, error) {
	if app == a.app {
		return phone.NewStreamFromClient(a.theApp.Client), nil
	}
	return phone.NewStreamFromClient(a.other.Client), nil
}

func (a oneApp) ForAppRemoving(ctx context.Context, customer string, app int64) (*phone.Stream, error) {
	return a.ForApp(ctx, customer, app)
}

func (a oneApp) Placed(context.Context, string) (int64, error) { return 0, nil }

func (a oneApp) DeploymentApp() int64 { return 0 }

// Without what the webhook needs to answer, nothing is moved and Twilio is asked nothing, so no
// number is pointed at a webhook that would refuse its calls.
func (s *PhoneMoveSuite) TestARouterThatCouldNotAnswerTheWebhookMovesNothing() {
	ready := s.ready()
	for name, test := range map[string]struct {
		settings moverSettings
		refusal  string
	}{
		"no webhook token": {settings: ready.with(func(m *moverSettings) { m.token = "" }), refusal: "TWILIO_WEBHOOK_AUTH_TOKEN"},
		"no public URL":    {settings: ready.with(func(m *moverSettings) { m.publicURL = "" }), refusal: "ROUTER_PUBLIC_URL"},
		"a plain http URL": {settings: ready.with(func(m *moverSettings) { m.publicURL = "http://router.example.test" }), refusal: "https"},
		"no account SID":   {settings: ready.with(func(m *moverSettings) { m.account = "" }), refusal: "TWILIO_ACCOUNT_SID"},
		"no keyring":       {settings: ready.with(func(m *moverSettings) { m.sealed = false }), refusal: "stored trunk"},
	} {
		s.Run(name, func() {
			s.releaseAll()
			kept := s.attachedWithNoStoredBridge()
			number := s.attachedEarlier()

			out, err := s.move(test.settings, "--apply")

			s.ErrorContains(err, test.refusal)
			s.NotContains(out, kept, "refused before any number was read")
			s.Empty(s.twilio.asked(), "twilio was asked nothing")
			s.Equal(moveTestOldURL, s.twilio.voiceURL(number))
		})
	}
}

// A Telnyx number is not listed, and Telnyx is asked nothing; the service refuses to move a
// vendor whose numbers do not ask the router's webhook.
func (s *PhoneMoveSuite) TestATelnyxNumberIsNeverTouched() {
	twilioNumber := s.attachedEarlier()
	telnyxNumber := moveTestNumber()
	s.telnyx.hold(telnyxNumber)
	s.hold(telnyxNumber, "telnyx")
	s.Require().NoError(s.store.PhoneNumbers.Attach(s.ctx, s.customer, telnyxNumber,
		store.NumberAttachment{TrunkID: "telnyx-trunk-" + uuid.NewString(), CallType: "default"}))

	out, err := s.move(s.ready(), "--apply")

	s.Require().NoError(err)
	s.Contains(out, twilioNumber+"\tapp deployment\tmoved\tallowlist set\n")
	s.NotContains(out, telnyxNumber)
	s.Empty(s.telnyx.asked())

	err = s.service(s.ready()).MoveToCallHook(s.ctx, phone.CallHookMove{Vendor: "telnyx", Apply: true},
		func(phone.MovedNumber) { s.Fail("a telnyx number was reported") })
	s.ErrorIs(err, phone.ErrInvalid)
	s.Empty(s.telnyx.asked())
}

// A number or an app written wrong is refused before anything is read.
func (s *PhoneMoveSuite) TestAFlagWrittenWrongIsRefused() {
	s.attachedEarlier()
	for name, args := range map[string][]string{
		"a number not in E.164":    {"--one", "15125551234"},
		"an app that is not an id": {"--app", "my-app"},
		"an app id of zero":        {"--app", "0"},
		"an unknown flag":          {"--dry"},
	} {
		s.Run(name, func() {
			out, err := s.move(s.ready(), append([]string{"--apply"}, args...)...)

			s.Error(err)
			s.Empty(out)
			s.Empty(s.twilio.asked())
		})
	}
}

// Attaching a number attached earlier again points it at the webhook, so an app can move a
// number itself; attaching it once more writes nothing. Its trunk's addresses are the run's to
// set, not an attach's.
func (s *PhoneMoveSuite) TestAttachingANumberAttachedEarlierAgainMovesIt() {
	number := s.attachedEarlier()

	_, err := s.service(s.ready()).Attach(s.ctx, phone.Attachment{CustomerID: s.customer, E164: number})

	s.Require().NoError(err)
	s.Equal(moveTestHookURL, s.twilio.voiceURL(number))
	s.Len(s.twilio.posted(), 1)
	s.Empty(s.addresses(number))

	_, err = s.service(s.ready()).Attach(s.ctx, phone.Attachment{CustomerID: s.customer, E164: number})

	s.Require().NoError(err)
	s.Len(s.twilio.posted(), 1, "a number on the webhook is not written again")
}

// A run stops at the first number it could not move, naming it, and moves none after it: a
// password the keyring does not open, a number Twilio does not hold, an update Twilio refused.
func (s *PhoneMoveSuite) TestARunStopsAtTheFirstNumberItCouldNotMove() {
	for name, test := range map[string]struct {
		breakNumber func(number string) moverSettings
		failure     string
		dryRun      bool
	}{
		"a password the keyring does not open": {breakNumber: func(string) moverSettings {
			return s.ready().with(func(m *moverSettings) { m.kek = "another key" })
		}, failure: "open the trunk password"},
		"a number twilio does not hold": {breakNumber: func(number string) moverSettings {
			s.twilio.release(number)
			return s.ready()
		}, failure: "not one of this account's numbers"},
		"an update twilio refused": {breakNumber: func(number string) moverSettings {
			s.twilio.refuse(number)
			return s.ready()
		}, failure: "500"},
		"a number twilio does not hold, in a dry run": {breakNumber: func(number string) moverSettings {
			s.twilio.release(number)
			return s.ready()
		}, failure: "not one of this account's numbers", dryRun: true},
		"a trunk stream does not have": {breakNumber: func(number string) moverSettings {
			// Its rule first: Stream keeps a trunk a rule names.
			held, err := s.store.PhoneNumbers.GetByE164(s.ctx, s.owners[number], number)
			s.Require().NoError(err)
			stream := phone.NewStreamFromClient(s.chat.Client)
			s.Require().NoError(stream.DeleteRoute(s.ctx, held.StreamRouteID))
			s.Require().NoError(stream.DeleteTrunk(s.ctx, held.StreamTrunkID))
			return s.ready()
		}, failure: "stream has no sip trunk"},
		"trunks stream would not list": {breakNumber: func(string) moverSettings {
			s.chat.Fail(http.MethodGet, "/sip/inbound_trunks")
			return s.ready()
		}, failure: "list sip trunks", dryRun: true},
		"an update stream refused": {breakNumber: func(number string) moverSettings {
			s.chat.Fail(http.MethodPut, "/sip/inbound_trunks/"+s.trunkOf(number))
			return s.ready()
		}, failure: "update sip trunk"},
	} {
		s.Run(name, func() {
			s.releaseAll()
			s.chat = chattest.NewServer(s.T())
			broken := s.attachedEarlier()
			// On a trunk of its own, which a broken trunk leaves as it is.
			s.customer = uuid.NewString()
			after := s.attachedEarlier()
			settings := test.breakNumber(broken)
			args := []string{"--apply"}
			if test.dryRun {
				args = nil
			}

			_, err := s.move(settings, args...)

			s.ErrorContains(err, test.failure)
			s.ErrorContains(err, broken)
			s.Equal(moveTestOldURL, s.twilio.voiceURL(after), "no number after it was moved")
			s.Empty(s.addresses(after), "no trunk after it was set")
		})
	}
}

// Attaching an attached number again with the router unable to answer the webhook neither asks
// Twilio nor logs; a move that fails is logged, and the attach still answers.
func (s *PhoneMoveSuite) TestAttachingAgainNeverFailsOverTheMove() {
	number := s.attachedEarlier()
	var logs bytes.Buffer
	unready := s.ready().with(func(m *moverSettings) { m.token, m.logs = "", &logs })

	_, err := s.service(unready).Attach(s.ctx, phone.Attachment{CustomerID: s.customer, E164: number})

	s.Require().NoError(err)
	s.Empty(s.twilio.asked())
	s.Empty(logs.String())

	s.twilio.refuse(number)
	_, err = s.service(s.ready().with(func(m *moverSettings) { m.logs = &logs })).
		Attach(s.ctx, phone.Attachment{CustomerID: s.customer, E164: number})

	s.Require().NoError(err)
	s.Contains(logs.String(), "could not point an attached number at the router's call webhook")
	s.Equal(moveTestOldURL, s.twilio.voiceURL(number))
}

// A run whose attached numbers cannot be read fails saying so, rather than reporting none to move.
func (s *PhoneMoveSuite) TestARunWhoseNumbersCannotBeReadFails() {
	s.attachedEarlier()
	settings := config.Defaults()
	settings.Postgres.DSN = testenv.Database(os.Getenv("ROUTER_POSTGRES_DSN"), "phone_move")
	closed, err := openStore(s.ctx, settings)
	s.Require().NoError(err)
	s.Require().NoError(closed.Close())

	var out bytes.Buffer
	err = moveTwilioNumbers(s.ctx, []string{"--apply"}, s.serviceOn(closed, s.ready()), &out)

	s.ErrorContains(err, "attached numbers")
	s.Empty(s.twilio.asked())
}

// Attaching an attached number again answers within a short deadline when Twilio never answers
// the move, and the number stays attached.
func (s *PhoneMoveSuite) TestAttachingAgainAnswersWhenTwilioNeverDoes() {
	number := s.attachedEarlier()
	s.twilio.hanging.Store(true)
	var logs bytes.Buffer
	service := s.service(s.ready().with(func(m *moverSettings) { m.logs = &logs }))

	start := time.Now()
	attached, err := service.Attach(s.ctx, phone.Attachment{CustomerID: s.customer, E164: number})

	s.Require().NoError(err)
	s.Less(time.Since(start), 15*time.Second, "the attach waited on Twilio's client timeout")
	s.NotEmpty(attached.TrunkID)
	s.Contains(logs.String(), "could not point an attached number at the router's call webhook")
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, number)
	s.Require().NoError(err)
	s.NotNil(held.AttachedAt)
}

// The command is built from the router's config: without a keyring it refuses, and with one it
// gets as far as the public URL, which this config lacks.
func (s *PhoneMoveSuite) TestTheCommandRunsTheChecksOnTheRoutersConfig() {
	settings := config.Defaults()
	settings.Postgres.DSN = testenv.Database(os.Getenv("ROUTER_POSTGRES_DSN"), "phone_move")
	settings.PublicURL = ""
	s.T().Setenv("TWILIO_ACCOUNT_SID", "AC"+strings.Repeat("0", 32))
	s.T().Setenv("TWILIO_AUTH_TOKEN", "api-secret")
	s.T().Setenv("TWILIO_WEBHOOK_AUTH_TOKEN", s.token)
	s.T().Setenv(devKEKEnvVar, "")

	err := dispatchCommand("phone", []string{"move-twilio"}, settings, slog.New(slog.DiscardHandler))
	s.ErrorContains(err, "stored trunk")

	s.T().Setenv(devKEKEnvVar, moveTestKEK)
	err = dispatchCommand("phone", []string{"move-twilio"}, settings, slog.New(slog.DiscardHandler))
	s.ErrorContains(err, "ROUTER_PUBLIC_URL")

	s.ErrorContains(dispatchCommand("phone", nil, settings, nil), "move-twilio")
	s.ErrorContains(dispatchCommand("phone", []string{"move"}, settings, nil), `unknown phone command "move"`)
}

// releaseAll gives back every number an earlier test or subtest left held.
func (s *PhoneMoveSuite) releaseAll() {
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE phone_numbers SET released_at = now() WHERE released_at IS NULL")
	s.Require().NoError(err)
}

// move runs router phone move-twilio with args on a service with settings.
func (s *PhoneMoveSuite) move(settings moverSettings, args ...string) (string, error) {
	var out bytes.Buffer
	err := moveTwilioNumbers(s.ctx, args, s.service(settings), &out)
	return out.String(), err
}

// moverSettings are what a deployment has for its Twilio numbers.
type moverSettings struct {
	token, account, publicURL string
	// sealed is a deployment with a keyring, of kek.
	sealed bool
	kek    string
	// logs, when set, is where the service logs.
	logs *bytes.Buffer
	// apps, when set, answers which Stream app each number's trunk is in, in place of the
	// suite's one fake Stream.
	apps phone.Apps
	// dataKeys, when set, are the apps' data keys the service opens and moves trunk passwords with.
	dataKeys *appkeys.Keys
}

func (s *PhoneMoveSuite) ready() moverSettings {
	return moverSettings{
		token: s.token, account: "AC" + strings.Repeat("0", 32), publicURL: moveTestPublicURL, sealed: true, kek: moveTestKEK,
	}
}

// with is the settings with change made to a copy.
func (m moverSettings) with(change func(*moverSettings)) moverSettings {
	change(&m)
	return m
}

// service is a phone service with Twilio and Telnyx at the suite's fakes and the settings given.
func (s *PhoneMoveSuite) service(settings moverSettings) *phone.Service {
	return s.serviceOn(s.store, settings)
}

// serviceOn is service on the store given.
func (s *PhoneMoveSuite) serviceOn(db *store.Store, settings moverSettings) *phone.Service {
	vendorConfig, err := phone.DefaultConfig()
	s.Require().NoError(err)
	for i := range vendorConfig.Vendors {
		vendorConfig.Vendors[i].Credentials = nil
	}
	registry := vendors.Registry(vendorConfig)
	registry.Register("twilio", func() (phone.Provider, error) {
		return twilio.New(twilio.Options{
			AccountSID: settings.account, AuthToken: "api-secret", WebhookAuthToken: settings.token, BaseURL: s.twilio.URL,
		})
	})
	registry.Register("telnyx", func() (phone.Provider, error) {
		return telnyx.New(telnyx.Options{APIKey: "k", ConnectionID: "texml-1", BaseURL: s.telnyx.URL})
	})
	var sealer *auth.Sealer
	if settings.sealed {
		sealer, err = auth.NewSealer(settings.kek)
		s.Require().NoError(err)
	}
	logger := slog.New(slog.DiscardHandler)
	if settings.logs != nil {
		logger = slog.New(slog.NewTextHandler(settings.logs, &slog.HandlerOptions{Level: slog.LevelError}))
	}
	service, err := phone.NewService(phone.ServiceOptions{
		Registry: registry, Store: db, Stream: phone.NewStreamFromClient(s.chat.Client), Apps: settings.apps, Sealer: sealer,
		DataKeys: settings.dataKeys, PublicURL: settings.publicURL, Logger: logger,
	})
	s.Require().NoError(err)
	return service
}

// attachedEarlier is a Twilio number attached as the router attaches one, then left on the voice
// URL a number attached before the webhook has, with what Twilio was asked forgotten. A trunk
// the attach made is given the addresses one made before Twilio's were declared has: none. One
// it joined is left as the join left it.
func (s *PhoneMoveSuite) attachedEarlier() string {
	before := s.chat.Trunks()
	number := moveTestNumber()
	s.twilio.hold(number)
	s.hold(number, "twilio")
	_, err := s.service(s.ready()).Attach(s.ctx, phone.Attachment{CustomerID: s.customer, E164: number})
	s.Require().NoError(err)
	s.owners[number] = s.customer
	s.twilio.point(number, moveTestOldURL, http.MethodGet)
	s.twilio.forget()
	if trunk := s.trunkOf(number); !slices.Contains(before, trunk) {
		s.Require().NoError(phone.NewStreamFromClient(s.chat.Client).UpdateTrunk(s.ctx, trunk,
			phone.Trunk{Name: "made earlier", Numbers: s.chat.TrunkNumbers(trunk)}))
	}
	s.Require().Empty(s.addresses(number), "a join gave a live trunk addresses it did not have")
	s.updatesBefore = len(s.chat.Requests(s.chat.Key))
	return number
}

// twilioRanges are the signalling addresses Twilio is declared with.
func (s *PhoneMoveSuite) twilioRanges() []string {
	declared, err := phone.DefaultConfig()
	s.Require().NoError(err)
	twilio, _ := declared.Lookup("twilio")
	s.Require().NotEmpty(twilio.Signalling)
	return twilio.Signalling
}

// trunkOf is the id of the Stream trunk the number is on.
func (s *PhoneMoveSuite) trunkOf(e164 string) string {
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, s.owners[e164], e164)
	s.Require().NoError(err)
	return held.StreamTrunkID
}

// addresses are the addresses the trunk the number is on takes calls from.
func (s *PhoneMoveSuite) addresses(e164 string) []string {
	_, allowed := s.chat.TrunkAuth(s.trunkOf(e164))
	return allowed
}

// trunkUpdates is how many trunk updates Stream was sent since the last number was attached.
func (s *PhoneMoveSuite) trunkUpdates() int {
	updates := 0
	for _, request := range s.chat.Requests(s.chat.Key)[s.updatesBefore:] {
		if request.Method == http.MethodPut && strings.Contains(request.Path, "/sip/inbound_trunks/") {
			updates++
		}
	}
	return updates
}

// attachedWithNoStoredBridge is a Twilio number attached with no trunk credentials kept, as one
// that arrived by a data move is.
func (s *PhoneMoveSuite) attachedWithNoStoredBridge() string {
	number := moveTestNumber()
	s.twilio.hold(number)
	s.twilio.point(number, moveTestOldURL, http.MethodGet)
	s.hold(number, "twilio")
	s.Require().NoError(s.store.PhoneNumbers.Attach(s.ctx, s.customer, number,
		store.NumberAttachment{TrunkID: "moved-trunk-" + uuid.NewString(), CallType: "default"}))
	return number
}

// hold records the number as the suite's customer's, bought from vendor.
func (s *PhoneMoveSuite) hold(e164, vendor string) {
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: e164, Vendor: vendor, Country: "US", CustomerID: s.customer, PurchasedAt: time.Now().UTC(),
	}))
}

// moveTestNumber is a US number of its own for one test.
func moveTestNumber() string {
	digits, err := rand.Int(rand.Reader, big.NewInt(10_000_000))
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("+1555%07d", digits.Int64())
}

// fakeTwilio is Twilio's numbers API for the numbers it holds: a list read by PhoneNumber that
// answers each number's voice URL and method, and an update that sets them, as Twilio's does.
// Telnyx's tests use it only to record that nothing was asked.
type fakeTwilio struct {
	*httptest.Server
	mu       sync.Mutex
	numbers  map[string]*fakeTwilioNumber
	requests []string
	forms    []url.Values
	// hanging holds every request until its caller gives up.
	hanging atomic.Bool
}

type fakeTwilioNumber struct {
	sid, voiceURL, voiceMethod string
	// refused answers its updates 500, and gone leaves it out of the list.
	refused, gone bool
}

func newFakeTwilio(t *testing.T) *fakeTwilio {
	fake := &fakeTwilio{numbers: map[string]*fakeTwilioNumber{}}
	fake.Server = httptest.NewServer(http.HandlerFunc(fake.serve))
	t.Cleanup(fake.Close)
	return fake
}

func (f *fakeTwilio) serve(w http.ResponseWriter, r *http.Request) {
	if f.hanging.Load() {
		<-r.Context().Done()
		return
	}
	_ = r.ParseForm()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, r.Method+" "+r.URL.RequestURI())
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/IncomingPhoneNumbers.json"):
		listed := []map[string]string{}
		if number, ok := f.numbers[r.URL.Query().Get("PhoneNumber")]; ok && !number.gone {
			listed = append(listed, map[string]string{
				"sid": number.sid, "phone_number": r.URL.Query().Get("PhoneNumber"),
				"voice_url": number.voiceURL, "voice_method": number.voiceMethod,
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"incoming_phone_numbers": listed})
	case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/IncomingPhoneNumbers/"):
		f.forms = append(f.forms, r.PostForm)
		for _, number := range f.numbers {
			if strings.HasSuffix(r.URL.Path, "/"+number.sid+".json") {
				if number.refused {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				number.voiceURL, number.voiceMethod = r.PostForm.Get("VoiceUrl"), r.PostForm.Get("VoiceMethod")
				_ = json.NewEncoder(w).Encode(map[string]string{"sid": number.sid})
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// hold makes the number one the account holds.
func (f *fakeTwilio) hold(e164 string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.numbers[e164] = &fakeTwilioNumber{sid: "PN" + strings.ToLower(rand.Text())}
}

// point sets the number's voice URL and method, as a change at Twilio would.
func (f *fakeTwilio) point(e164, voiceURL, method string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.numbers[e164].voiceURL, f.numbers[e164].voiceMethod = voiceURL, method
}

// release makes the account no longer hold the number, keeping its voice URL to read.
func (f *fakeTwilio) release(e164 string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.numbers[e164].gone = true
}

// refuse makes Twilio answer the number's updates 500.
func (f *fakeTwilio) refuse(e164 string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.numbers[e164].refused = true
}

func (f *fakeTwilio) voiceURL(e164 string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.numbers[e164].voiceURL
}

// forget drops what the fake was asked so far.
func (f *fakeTwilio) forget() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests, f.forms = nil, nil
}

// asked is every request, as method, path and query.
func (f *fakeTwilio) asked() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.requests...)
}

// posted is the form of every update.
func (f *fakeTwilio) posted() []url.Values {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]url.Values(nil), f.forms...)
}

// The service the command builds opens a trunk password under the app's data key. Without the
// data keys it opens under the router's key only, and the number cannot be moved.
func (s *PhoneMoveSuite) TestTheCommandOpensATrunkPasswordUnderTheAppsDataKey() {
	fake := serveFakeCloudKMS(s.T())
	number := s.attachedEarlier()
	trunk := s.trunkOf(number)
	key := appkeystest.AddKey(s.T(), s.store, fake.kms.KMS, s.customer, 1)
	aad := appkeystest.RecordAAD("stream_trunk", s.customer, []byte("stream_trunks/"+s.customer+"/"+trunk))
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE stream_trunks SET bridge_password_sealed = ?, "+
		"bridge_password_kek_version = 0, bridge_password_data_key_version = 1 WHERE id = ?",
		appkeystest.Seal(s.T(), key, []byte("data-key-password"), aad), trunk)
	s.Require().NoError(err)
	settings := config.Defaults()
	settings.Postgres.DSN = testenv.Database(os.Getenv("ROUTER_POSTGRES_DSN"), "phone_move")
	settings.PublicURL = moveTestPublicURL
	settings.Auth.AppKeysKMSKey = testKMSKey
	s.T().Setenv(devKEKEnvVar, moveTestKEK)

	service, db, closeKeys, err := numberMover(s.ctx, settings, slog.New(slog.DiscardHandler))
	s.Require().NoError(err)
	defer db.Close()
	defer closeKeys()
	bridge, err := service.InboundBridge(s.ctx, "twilio", number)

	s.Require().NoError(err)
	s.Equal("data-key-password", bridge.Password)
}

// A dry run opens each trunk password as the webhook would, and moves none of them to the app's
// data key, so the trunk's row is left byte for byte as it was. Only --apply moves one.
func (s *PhoneMoveSuite) TestADryRunWithSealingOnLeavesATrunkPasswordUnderTheRoutersKey() {
	number := s.attachedEarlier()
	kms := appkeystest.NewKMS()
	options := appkeystest.Options()
	options.Seal = true
	keys, err := appkeys.New(s.store, kms, options)
	s.Require().NoError(err)
	sealing := s.ready().with(func(m *moverSettings) { m.dataKeys = keys })
	row := func() string {
		var row string
		s.Require().NoError(s.store.GlobalDB().NewSelect().ColumnExpr("to_jsonb(t)::text").
			TableExpr("stream_trunks AS t").Where("t.id = ?", s.trunkOf(number)).Scan(s.ctx, &row))
		return row
	}
	before := row()
	s.Require().Contains(before, `"bridge_password_data_key_version": null`)

	out, err := s.move(sealing)

	s.Require().NoError(err)
	s.Contains(out, number+"\tapp deployment\twould move\twould set allowlist\n")
	s.Equal(before, row(), "a dry run wrote the trunk's row")
	s.Zero(kms.Wraps(), "a dry run made no data key")

	_, err = s.move(sealing, "--apply")

	s.Require().NoError(err)
	s.NotContains(row(), `"bridge_password_data_key_version": null`, "--apply moves the password to the data key")
}
