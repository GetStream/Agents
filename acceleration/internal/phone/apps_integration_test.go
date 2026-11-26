//go:build integration

package phone

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	getstream "github.com/GetStream/getstream-go/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation/chattest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/outcome"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/sipbridge"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// AppsSuite runs the phone service against two Stream apps in memory, the deployment's and
// a customer's own, with a stub vendor, and checks that every trunk and rule is made and
// removed in the app the call it serves is in.
type AppsSuite struct {
	suite.Suite
	ctx   context.Context
	store *store.Store

	deployment *chattest.Server
	own        *chattest.Server
	apps       *testApps
	service    *Service
	vendor     *stub
	registry   *Registry
	customer   string
	e164       string
}

func TestAppsSuite(t *testing.T) { suite.Run(t, new(AppsSuite)) }

func (s *AppsSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN not set")
	}
	s.ctx = context.Background()
	opened, err := store.Open(testenv.Database(dsn, "phone_apps"))
	s.Require().NoError(err)
	s.Require().NoError(opened.Migrate(s.ctx))
	// A number an earlier run left held would refuse the same number now: the database is the
	// suite's own and is kept between runs.
	_, err = opened.GlobalDB().ExecContext(s.ctx, "UPDATE phone_numbers SET released_at = now() WHERE released_at IS NULL")
	s.Require().NoError(err)
	s.store = opened
	s.T().Cleanup(func() { s.Require().NoError(opened.Close()) })
}

func (s *AppsSuite) SetupTest() {
	s.deployment, s.own = chattest.NewServer(s.T()), chattest.NewServer(s.T())
	s.apps = &testApps{deployment: s.deployment, own: map[string]*chattest.Server{}, pins: map[string]int64{}, blocked: map[string]bool{}, cutOff: map[string]bool{}, unread: map[string]bool{}}
	s.customer = "customer-" + uuid.NewString()
	s.e164 = aNumber()

	vendor := &stub{vendor: "twilio", leg: StreamLegShared}
	s.vendor = vendor
	registry := NewRegistry(s.config())
	registry.Register(vendor.vendor, func() (Provider, error) { return vendor, nil })
	s.registry = registry
	for _, name := range s.credentials(vendor.vendor) {
		s.T().Setenv(name, "set")
	}
	s.service = s.serviceAt("https://router.example")

	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: s.e164, Vendor: vendor.vendor, Country: "US", CustomerID: s.customer, PurchasedAt: time.Now().UTC(),
	}))
}

// serviceAt is the suite's service reachable from the internet at a url.
func (s *AppsSuite) serviceAt(publicURL string) *Service {
	service, err := NewService(ServiceOptions{Registry: s.registry, Store: s.store, Apps: s.apps, PublicURL: publicURL})
	s.Require().NoError(err)
	return service
}

// austinNumbers is how many Austin numbers there are: +1 512, then seven digits whose first is
// 2 to 9. The North American Numbering Plan's exchange code is NXX, N being 2 to 9, and
// Country (region.Of) finds no country for a number whose exchange starts with 0 or 1.
const austinNumbers = 8_000_000

// numbersFrom and numbersGiven make aNumber's numbers: the next one each time, from a random
// start so that runs on a kept database start in different places.
var (
	numbersFrom  = rand.Int64N(austinNumbers)
	numbersGiven atomic.Int64
)

// aNumber is a number no other call in this process gave out: a well-formed Austin number,
// since a number in no country is not sold. A number drawn at random could be one the suite
// already holds, and a held number is unique (phone_numbers_held_idx).
func aNumber() string {
	return fmt.Sprintf("+1512%07d", 2_000_000+(numbersFrom+numbersGiven.Add(1))%austinNumbers)
}

func (s *AppsSuite) config() Config {
	config, err := DefaultConfig()
	s.Require().NoError(err)
	// The suite's stub vendor is named twilio but dials the trunk it is given, as Twilio did
	// before its numbers asked the router where each call goes, so what the service does with
	// no keyring is still exercised. Twilio as declared is attached in the api package's
	// TwilioHookSuite.
	for i, vendor := range config.Vendors {
		if vendor.Vendor == "twilio" {
			config.Vendors[i].AsksForTrunk = false
		}
	}
	return config
}

func (s *AppsSuite) credentials(vendor string) []string {
	declared, ok := s.config().Lookup(vendor)
	s.Require().True(ok)
	return declared.Credentials
}

// ownApp gives the suite's customer an app of its own from now on.
func (s *AppsSuite) ownApp() {
	s.apps.give(s.customer, 4242, s.own)
}

// onAnAllowlist is a number held on a vendor that cannot send a trunk password, and the
// addresses that vendor calls from.
type onAnAllowlist struct {
	e164       string
	signalling []string
}

// vendorOnAnAllowlist registers vonage, which authenticates by address, and gives the
// suite's customer a number on it.
func (s *AppsSuite) vendorOnAnAllowlist() onAnAllowlist {
	declared, ok := s.config().Lookup("vonage")
	s.Require().True(ok)
	s.Require().NotEmpty(declared.Signalling, "the test needs a vendor that declares its addresses")
	return onAnAllowlist{e164: s.numberOn("vonage"), signalling: declared.Signalling}
}

// numberOn registers a stub for the vendor next to the suite's own and gives the suite's
// customer a number on it.
func (s *AppsSuite) numberOn(vendor string) string {
	other := s.vendorBeside(vendor)
	e164 := aNumber()
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: e164, Vendor: other.vendor, Country: "US", CustomerID: s.customer, PurchasedAt: time.Now().UTC(),
	}))
	return e164
}

// vendorBeside registers a stub for the vendor next to the suite's own.
func (s *AppsSuite) vendorBeside(vendor string) *stub {
	other := &stub{vendor: vendor}
	registry := NewRegistry(s.config())
	registry.Register(s.vendor.vendor, func() (Provider, error) { return s.vendor, nil })
	registry.Register(other.vendor, func() (Provider, error) { return other, nil })
	for _, name := range append(s.credentials(s.vendor.vendor), s.credentials(other.vendor)...) {
		s.T().Setenv(name, "set")
	}
	service, err := NewService(ServiceOptions{Registry: registry, Store: s.store, Apps: s.apps})
	s.Require().NoError(err)
	s.service = service
	return other
}

// planStub is a vendor that fetches its call plan when the call is answered, and renders
// the bridge it is given into the plan so a test can read it back.
type planStub struct {
	stub
	rendered Bridge
}

func (p *planStub) Answer(bridge Bridge, _ string) (Plan, error) {
	p.rendered = bridge
	return Plan{ContentType: "text/plain", Body: []byte(bridge.Password)}, nil
}

func (s *AppsSuite) TestACallsTrunkHasAPasswordTheVendorIsGiven() {
	_, err := s.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550001111",
	})
	s.Require().NoError(err)

	trunks := s.deployment.Trunks()
	s.Require().Len(trunks, 1)
	password, allowed := s.deployment.TrunkAuth(trunks[0])
	s.NotEmpty(password, "every Twilio account calls from the same addresses, so they alone let in anybody's call")
	s.Equal(s.twilioSignalling(), allowed)
	s.Equal(password, s.vendor.dialed.Bridge.Password, "the vendor answers the challenge with it")
}

// twilioSignalling are the addresses Twilio is declared to call from.
func (s *AppsSuite) twilioSignalling() []string {
	declared, ok := s.config().Lookup("twilio")
	s.Require().True(ok)
	s.Require().NotEmpty(declared.Signalling)
	return declared.Signalling
}

// A vendor that sends a password and declares no addresses gets a trunk as before: a password
// and no allowlist.
func (s *AppsSuite) TestACallOnAPasswordVendorWithNoAddressesHasAPasswordAndNoAllowlist() {
	telnyx := s.numberOn("telnyx")

	_, err := s.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: telnyx, To: "+15550001111",
	})
	s.Require().NoError(err)

	trunks := s.deployment.Trunks()
	s.Require().Len(trunks, 1)
	password, allowed := s.deployment.TrunkAuth(trunks[0])
	s.NotEmpty(password)
	s.Empty(allowed)
}

// A shared trunk of a vendor that cannot send a password has its addresses and no password.
func (s *AppsSuite) TestASharedTrunkOfAVendorThatCannotSendAPasswordHasNoPassword() {
	vonage := s.vendorOnAnAllowlist()
	s.sealed()

	line, err := s.lineFor(s.customer, vonage.e164)

	s.Require().NoError(err)
	password, allowed := s.deployment.TrunkAuth(line.TrunkID)
	s.Empty(password, "Stream would challenge a vendor that has no way to answer")
	s.Equal(vonage.signalling, allowed)
}

// A number joining a shared trunk of a vendor that cannot send a password gives the trunk the
// vendor's declared addresses, whatever it had.
func (s *AppsSuite) TestAJoinGivesAnAddressOnlyVendorsTrunkItsDeclaredAddresses() {
	vonage := s.vendorOnAnAllowlist()
	s.sealed()
	first, err := s.lineFor(s.customer, vonage.e164)
	s.Require().NoError(err)
	s.Require().NoError(NewStreamFromClient(s.deployment.Client).UpdateTrunk(s.ctx, first.TrunkID,
		Trunk{Name: "edited", Numbers: s.deployment.TrunkNumbers(first.TrunkID), AllowedIPs: []string{"192.0.2.0/30"}}))

	second, err := s.lineFor(s.customer, s.anotherNumber(s.customer, "vonage"))

	s.Require().NoError(err)
	s.Require().Equal(first.TrunkID, second.TrunkID)
	_, allowed := s.deployment.TrunkAuth(first.TrunkID)
	s.Equal(vonage.signalling, allowed)
}

// A shared Twilio trunk has Twilio's addresses and a password when it is made, and keeps both
// when another number joins it.
func (s *AppsSuite) TestASharedTwilioTrunkHasTwiliosAddressesAndAPassword() {
	s.sealed()
	first, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	made, _ := s.deployment.TrunkAuth(first.TrunkID)
	second, err := s.lineFor(s.customer, s.anotherNumber(s.customer, s.vendor.vendor))
	s.Require().NoError(err)

	s.Require().Equal(first.TrunkID, second.TrunkID)
	password, allowed := s.deployment.TrunkAuth(first.TrunkID)
	s.NotEmpty(made)
	s.Equal(made, password)
	s.Equal(s.twilioSignalling(), allowed)
}

// A join whose read of the trunk's addresses fails writes nothing: Stream takes the list whole,
// so a write sent without them would clear the live trunk's.
func (s *AppsSuite) TestAJoinWhoseTrunkReadFailsWritesNothing() {
	s.sealed()
	first, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	second := s.anotherNumber(s.customer, s.vendor.vendor)
	_, before := s.deployment.TrunkAuth(first.TrunkID)
	s.Require().NotEmpty(before)
	s.deployment.Fail(http.MethodGet, "/sip/inbound_trunks")
	n := len(s.deployment.Requests(s.deployment.Key))

	_, err = s.lineFor(s.customer, second)

	s.Require().Error(err)
	for _, asked := range s.askedSince(n) {
		s.NotContains(asked, "PUT ", "a failed read was followed by a write")
	}
	_, after := s.deployment.TrunkAuth(first.TrunkID)
	s.Equal(before, after)
	s.Equal([]string{s.e164}, s.deployment.TrunkNumbers(first.TrunkID))
}

// A number leaving the shared trunk of a vendor that authenticates by address, whose
// addresses the registry no longer declares, leaves the trunk's addresses as they are: an
// empty list would let in every caller of the trunk's other numbers.
func (s *AppsSuite) TestALeaveWithoutDeclaredAddressesSendsNoEmptyList() {
	vonage := s.vendorOnAnAllowlist()
	s.sealed()
	first, err := s.lineFor(s.customer, vonage.e164)
	s.Require().NoError(err)
	secondE164 := s.anotherNumber(s.customer, "vonage")
	_, err = s.lineFor(s.customer, secondE164)
	s.Require().NoError(err)
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, secondE164)
	s.Require().NoError(err)
	config := s.config()
	for i := range config.Vendors {
		if config.Vendors[i].Vendor == "vonage" {
			config.Vendors[i].Signalling = nil
		}
	}
	registry := NewRegistry(config)
	for name, provider := range map[string]Provider{s.vendor.vendor: s.vendor, "vonage": &stub{vendor: "vonage"}} {
		registry.Register(name, func() (Provider, error) { return provider, nil })
	}
	s.service.registry = registry
	n := len(s.deployment.Requests(s.deployment.Key))

	s.Require().NoError(s.service.unline(s.ctx, held, "test", time.Time{}))

	for _, asked := range s.askedSince(n) {
		s.NotContains(asked, "PUT ", "an address-only vendor's trunk was written without addresses")
	}
	_, allowed := s.deployment.TrunkAuth(first.TrunkID)
	s.Equal(vonage.signalling, allowed)
	stored, err := s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, secondE164)
	s.Require().NoError(err)
	s.Empty(stored.StreamTrunkID, "the number is off the trunk in the router's rows")
}

// madeEarlier gives the trunk the addresses a trunk made before Twilio's were declared has:
// none.
func (s *AppsSuite) madeEarlier(trunkID string) {
	s.Require().NoError(NewStreamFromClient(s.deployment.Client).UpdateTrunk(s.ctx, trunkID,
		Trunk{Name: "made earlier", Numbers: s.deployment.TrunkNumbers(trunkID)}))
}

// A number joining a shared Twilio trunk made before Twilio's addresses were declared leaves
// the trunk's addresses as they are: only a new trunk and the mover set them.
func (s *AppsSuite) TestAJoinLeavesATwilioTrunksAddressesAlone() {
	s.sealed()
	first, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	s.madeEarlier(first.TrunkID)

	second, err := s.lineFor(s.customer, s.anotherNumber(s.customer, s.vendor.vendor))

	s.Require().NoError(err)
	s.Require().Equal(first.TrunkID, second.TrunkID)
	s.Require().Len(s.deployment.TrunkNumbers(first.TrunkID), 2)
	_, allowed := s.deployment.TrunkAuth(first.TrunkID)
	s.Empty(allowed, "a join gave a live trunk addresses it did not have")
}

// A number leaving a shared Twilio trunk made before Twilio's addresses were declared leaves
// the trunk's addresses as they are.
func (s *AppsSuite) TestALeaveLeavesATwilioTrunksAddressesAlone() {
	s.sealed()
	second := s.anotherNumber(s.customer, s.vendor.vendor)
	first, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	_, err = s.lineFor(s.customer, second)
	s.Require().NoError(err)
	s.madeEarlier(first.TrunkID)
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, second)
	s.Require().NoError(err)

	s.Require().NoError(s.service.unline(s.ctx, held, "test", time.Time{}))

	s.Require().Equal([]string{s.e164}, s.deployment.TrunkNumbers(first.TrunkID))
	_, allowed := s.deployment.TrunkAuth(first.TrunkID)
	s.Empty(allowed, "a leave gave a live trunk addresses it did not have")
}

// The undo of a join that failed leaves a shared Twilio trunk made before Twilio's addresses
// were declared as it was.
func (s *AppsSuite) TestAFailedJoinsUndoLeavesATwilioTrunksAddressesAlone() {
	s.sealed()
	first, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	s.madeEarlier(first.TrunkID)
	s.deployment.Fail(http.MethodPost, "/sip/inbound_routing_rules")

	_, err = s.lineFor(s.customer, s.anotherNumber(s.customer, s.vendor.vendor))

	s.Require().Error(err)
	s.Require().Equal([]string{s.e164}, s.deployment.TrunkNumbers(first.TrunkID))
	_, allowed := s.deployment.TrunkAuth(first.TrunkID)
	s.Empty(allowed, "the undo gave a live trunk addresses it did not have")
}

// A number leaving a shared Twilio trunk that has Twilio's addresses leaves them on it.
func (s *AppsSuite) TestALeaveKeepsATwilioTrunksAddresses() {
	s.sealed()
	second := s.anotherNumber(s.customer, s.vendor.vendor)
	first, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	_, err = s.lineFor(s.customer, second)
	s.Require().NoError(err)
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, second)
	s.Require().NoError(err)

	s.Require().NoError(s.service.unline(s.ctx, held, "test", time.Time{}))

	s.Require().Equal([]string{s.e164}, s.deployment.TrunkNumbers(first.TrunkID))
	_, allowed := s.deployment.TrunkAuth(first.TrunkID)
	s.Equal(s.twilioSignalling(), allowed)
}

// The undo of a join that failed leaves a shared Twilio trunk's addresses on it.
func (s *AppsSuite) TestAFailedJoinsUndoKeepsATwilioTrunksAddresses() {
	s.sealed()
	first, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	s.deployment.Fail(http.MethodPost, "/sip/inbound_routing_rules")

	_, err = s.lineFor(s.customer, s.anotherNumber(s.customer, s.vendor.vendor))

	s.Require().Error(err)
	s.Require().Equal([]string{s.e164}, s.deployment.TrunkNumbers(first.TrunkID))
	_, allowed := s.deployment.TrunkAuth(first.TrunkID)
	s.Equal(s.twilioSignalling(), allowed)
}

// A number of a vendor that cannot send a password, attached on a trunk of its own, gets the
// vendor's addresses and no password.
func (s *AppsSuite) TestAnAttachedNumberOfAnAddressOnlyVendorHasNoPassword() {
	vonage := s.vendorOnAnAllowlist()

	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: vonage.e164})

	s.Require().NoError(err)
	password, allowed := s.deployment.TrunkAuth(attached.TrunkID)
	s.Empty(password, "Stream would challenge a vendor that has no way to answer")
	s.Equal(vonage.signalling, allowed)
}

func (s *AppsSuite) TestEveryCallsTrunkHasAPasswordOfItsOwn() {
	call := func() string {
		_, err := s.service.Call(s.ctx, CallRequest{
			Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550001111",
		})
		s.Require().NoError(err)
		return s.vendor.dialed.Bridge.Password
	}

	s.NotEqual(call(), call())
}

func (s *AppsSuite) TestAnAttachedNumbersTrunkHasAPasswordTheVendorIsGiven() {
	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)

	password, allowed := s.deployment.TrunkAuth(attached.TrunkID)
	s.NotEmpty(password)
	s.Equal(s.twilioSignalling(), allowed)
	s.Equal(password, s.vendor.inbound.Bridge.Password)
}

func (s *AppsSuite) TestACallOnAVendorThatCannotSendAPasswordHasItsAddressesAndNoPassword() {
	vonage := s.vendorOnAnAllowlist()

	_, err := s.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: vonage.e164, To: "+15550001111",
	})
	s.Require().NoError(err)

	trunks := s.deployment.Trunks()
	s.Require().Len(trunks, 1)
	password, allowed := s.deployment.TrunkAuth(trunks[0])
	s.Equal(vonage.signalling, allowed)
	s.Empty(password, "Stream would challenge a vendor that has no way to answer")
}

// A vendor that fetches its call plan on answer needs the router reachable, which is the
// deployment's setting, not the caller's.
func (s *AppsSuite) TestACallOnAVendorThatFetchesItsPlanNeedsAPublicURL() {
	plivo := &planStub{stub: stub{vendor: "plivo"}}
	registry := NewRegistry(s.config())
	registry.Register(plivo.vendor, func() (Provider, error) { return plivo, nil })
	for _, name := range s.credentials(plivo.vendor) {
		s.T().Setenv(name, "set")
	}
	sealer, err := auth.NewSealer("apps suite")
	s.Require().NoError(err)
	service, err := NewService(ServiceOptions{Registry: registry, Store: s.store, Apps: s.apps, Sealer: sealer})
	s.Require().NoError(err)
	e164 := aNumber()
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: e164, Vendor: plivo.vendor, Country: "US", CustomerID: s.customer, PurchasedAt: time.Now().UTC(),
	}))

	_, err = service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: e164, To: "+15550001111",
	})

	s.ErrorContains(err, "public url")
	s.ErrorIs(err, ErrNotConfigured)
	s.Empty(plivo.dialed.To, "a call nobody could fetch the plan of is not dialled")
}

// The bridge parked until the vendor fetches its plan keeps the trunk's password sealed and the
// fetch's token only hashed, and the fetch still carries the password.
func (s *AppsSuite) TestACallPlanFetchedOnAnswerCarriesTheTrunksPasswordKeptSealed() {
	plivo := &planStub{stub: stub{vendor: "plivo"}}
	registry := NewRegistry(s.config())
	registry.Register(plivo.vendor, func() (Provider, error) { return plivo, nil })
	for _, name := range s.credentials(plivo.vendor) {
		s.T().Setenv(name, "set")
	}
	sealer, err := auth.NewSealer("apps suite")
	s.Require().NoError(err)
	service, err := NewService(ServiceOptions{
		Registry: registry, Store: s.store, Apps: s.apps, Sealer: sealer, PublicURL: "https://router.example.test",
	})
	s.Require().NoError(err)
	e164 := aNumber()
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: e164, Vendor: plivo.vendor, Country: "US", CustomerID: s.customer, PurchasedAt: time.Now().UTC(),
	}))

	_, err = service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: e164, To: "+15550001111",
	})
	s.Require().NoError(err)
	token := plivo.dialed.AnswerURL[strings.LastIndex(plivo.dialed.AnswerURL, "/")+1:]
	trunks := s.deployment.Trunks()
	s.Require().Len(trunks, 1)
	password, _ := s.deployment.TrunkAuth(trunks[0])
	s.Require().NotEmpty(password)

	var rows []string
	s.Require().NoError(s.store.ShardDB().NewSelect().ColumnExpr("to_jsonb(cb)::text").
		TableExpr("call_bridges AS cb").Where("cb.customer_id = ?", s.customer).Scan(s.ctx, &rows))
	s.Require().Len(rows, 1)
	s.NotContains(rows[0], password, "the parked bridge holds the trunk's password in the clear")
	s.NotContains(rows[0], token, "the parked bridge holds the token its fetch presents")

	_, err = service.Answer(s.ctx, token)
	s.Require().NoError(err)

	s.Equal(password, plivo.rendered.Password)
}

// onAPlanVendor is a service whose vendor fetches its call plan on answer, reachable at a
// public url, and a number the suite's customer holds on it.
func (s *AppsSuite) onAPlanVendor(vendor string, config Config, sealer *auth.Sealer) (*planStub, *Service, string) {
	fetcher := &planStub{stub: stub{vendor: vendor}}
	registry := NewRegistry(config)
	registry.Register(fetcher.vendor, func() (Provider, error) { return fetcher, nil })
	for _, name := range s.credentials(fetcher.vendor) {
		s.T().Setenv(name, "set")
	}
	service, err := NewService(ServiceOptions{
		Registry: registry, Store: s.store, Apps: s.apps, Sealer: sealer, PublicURL: "https://router.example.test",
	})
	s.Require().NoError(err)
	e164 := aNumber()
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: e164, Vendor: fetcher.vendor, Country: "US", CustomerID: s.customer, PurchasedAt: time.Now().UTC(),
	}))
	return fetcher, service, e164
}

// A parked bridge's sealed password opens only for the customer and the token it was sealed
// for: moved to another bridge row, or presented with another token, it does not.
func (s *AppsSuite) TestAParkedPasswordOpensOnlyForItsCustomerAndToken() {
	sealer, err := auth.NewSealer("apps suite")
	s.Require().NoError(err)
	plivo, service, e164 := s.onAPlanVendor("plivo", s.config(), sealer)
	_, err = service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: e164, To: "+15550001111",
	})
	s.Require().NoError(err)
	token := plivo.dialed.AnswerURL[strings.LastIndex(plivo.dialed.AnswerURL, "/")+1:]
	var parked store.CallBridge
	s.Require().NoError(s.store.ShardDB().NewSelect().Model(&parked).Where("customer_id = ?", s.customer).Scan(s.ctx))
	s.Require().NotEmpty(parked.TrunkPasswordSealed)
	moved := func(customerID, token string) store.CallBridge {
		return store.CallBridge{
			Token: token, CustomerID: customerID, Vendor: parked.Vendor, TrunkURI: parked.TrunkURI,
			TrunkUsername: parked.TrunkUsername, TrunkPasswordSealed: parked.TrunkPasswordSealed,
			TrunkKEKVersion: parked.TrunkKEKVersion, CallID: parked.CallID, ExpiresAt: parked.ExpiresAt,
		}
	}

	_, err = service.Answer(s.ctx, token)
	s.Require().NoError(err)
	s.Require().NotEmpty(plivo.rendered.Password)

	for name, bridge := range map[string]store.CallBridge{
		"another customer's row": moved("customer-"+uuid.NewString(), token),
		"another token":          moved(s.customer, uuid.NewString()),
	} {
		plivo.rendered = Bridge{}
		s.Require().NoError(s.store.CallBridges.Insert(s.ctx, &bridge))
		_, err := service.Answer(s.ctx, bridge.Token)
		s.Error(err, name)
		s.Empty(plivo.rendered.Password, name)
	}
}

// A bridge parked before the key rotated is opened with the key that sealed it, so a call
// ringing through a rotation is still answered with its trunk's password.
func (s *AppsSuite) TestAParkedPasswordOpensWithTheKeyThatSealedItAfterARotation() {
	sealed, err := auth.NewSealerWithKeyring(1, map[int]string{1: "apps suite key one"})
	s.Require().NoError(err)
	plivo, service, e164 := s.onAPlanVendor("plivo", s.config(), sealed)
	_, err = service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: e164, To: "+15550001111",
	})
	s.Require().NoError(err)
	token := plivo.dialed.AnswerURL[strings.LastIndex(plivo.dialed.AnswerURL, "/")+1:]
	trunks := s.deployment.Trunks()
	s.Require().Len(trunks, 1)
	password, _ := s.deployment.TrunkAuth(trunks[0])
	s.Require().NotEmpty(password)

	service.sealer, err = auth.NewSealerWithKeyring(2, map[int]string{1: "apps suite key one", 2: "apps suite key two"})
	s.Require().NoError(err)
	_, err = service.Answer(s.ctx, token)

	s.Require().NoError(err)
	s.Equal(password, plivo.rendered.Password)
}

// A vendor that sends the trunk's password on answer gets it from a sealed bridge, so on a
// deployment with no keyring its call is refused before anything is made at Stream.
func (s *AppsSuite) TestACallThatSendsAPasswordIsRefusedWithoutAKeyringBeforeAnythingIsMade() {
	plivo, service, e164 := s.onAPlanVendor("plivo", s.config(), nil)
	before := len(s.deployment.Requests(s.deployment.Key))

	_, err := service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: e164, To: "+15550001111",
	})

	s.ErrorIs(err, ErrNoSealerToCall)
	for _, request := range s.deployment.Requests(s.deployment.Key)[before:] {
		s.False(request.Writes(), "a call that cannot be placed made %s %s at Stream", request.Method, request.Path)
	}
	s.Empty(plivo.dialed.To, "the call is not dialled")
	count, err := s.store.ShardDB().NewSelect().Model((*store.CallBridge)(nil)).Where("customer_id = ?", s.customer).Count(s.ctx)
	s.Require().NoError(err)
	s.Zero(count, "no bridge is parked")
}

// Bandwidth authenticates by address and never sends a password, so its call needs no
// keyring and its parked bridge keeps no password.
func (s *AppsSuite) TestACallOnAVendorThatSendsNoPasswordNeedsNoKeyringAndParksNone() {
	config := s.config()
	i := slices.IndexFunc(config.Vendors, func(v Vendor) bool { return v.Vendor == "bandwidth" })
	s.Require().GreaterOrEqual(i, 0)
	s.Require().Equal(TrunkAllowlist, config.Vendors[i].Authenticates())
	config.Vendors[i].Signalling = []string{"192.0.2.0/24"}
	bandwidth, service, e164 := s.onAPlanVendor("bandwidth", config, nil)

	_, err := service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: e164, To: "+15550001111",
	})
	s.Require().NoError(err)

	var parked store.CallBridge
	s.Require().NoError(s.store.ShardDB().NewSelect().Model(&parked).Where("customer_id = ?", s.customer).Scan(s.ctx))
	s.Empty(parked.TrunkPasswordSealed)
	token := bandwidth.dialed.AnswerURL[strings.LastIndex(bandwidth.dialed.AnswerURL, "/")+1:]
	_, err = service.Answer(s.ctx, token)
	s.Require().NoError(err)
	s.NotEmpty(bandwidth.rendered.URI, "the plan bridges to the trunk")
	s.Empty(bandwidth.rendered.Password)
}

func (s *AppsSuite) number() store.PhoneNumber {
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, s.e164)
	s.Require().NoError(err)
	return held
}

// trunk is the stored trunk of the suite's number.
func (s *AppsSuite) trunk() store.StreamTrunk {
	trunk, err := s.store.StreamTrunks.Get(s.ctx, s.customer, s.number().StreamTrunkID)
	s.Require().NoError(err)
	return trunk
}

func (s *AppsSuite) TestATrunkIsCreatedInTheAppAttachingTheNumber() {
	s.ownApp()

	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)

	s.Equal([]string{attached.TrunkID}, s.own.Trunks())
	s.Equal([]string{attached.RouteID}, s.own.Rules())
	s.Empty(s.deployment.Trunks())
	held := s.number()
	s.Equal(int64(4242), held.StreamAppPK)
	s.Equal(attached.RouteID, held.StreamRouteID)
}

func (s *AppsSuite) TestReleasingANumberDeletesItsTrunkAndRouteInItsApp() {
	// A trunk left behind is billed, and points a number nobody holds at a call.
	s.ownApp()
	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)

	s.Require().NoError(s.service.Release(s.ctx, s.customer, s.e164))

	s.Empty(s.own.Trunks())
	s.Empty(s.own.Rules())
}

func (s *AppsSuite) TestAnImportedNumberIsNeverGivenBackAtTheVendor() {
	telnyx := s.vendorBeside("telnyx")
	e164 := aNumber()
	row, err := json.Marshal(map[string]any{
		"id": rand.Int64N(1 << 50), "e164": e164, "vendor": telnyx.vendor, "country": "US",
		"capabilities": []string{"voice"}, "monthly_cost_micros": 0, "customer_id": s.customer,
		"tags": map[string]string{}, "purchased_at": time.Now().UTC(),
	})
	s.Require().NoError(err)
	s.Require().NoError(s.store.ImportRow(s.ctx, s.customer, "phone_numbers", row))

	s.ErrorIs(s.service.Release(s.ctx, s.customer, e164), store.ErrNoNumber)
	s.Empty(telnyx.released, "the vendor is not asked to give it back")
	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})
	s.ErrorIs(err, store.ErrNoNumber, "nor are its calls pointed at an agent")
	s.Empty(s.deployment.Trunks())
}

func (s *AppsSuite) TestReleasingStopsTheVendorSendingCallsFirst() {
	s.ownApp()
	s.sealed()
	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)

	s.Require().NoError(s.service.Release(s.ctx, s.customer, s.e164))

	s.Equal([]string{s.e164}, s.vendor.stopped)
	_, err = s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, s.e164)
	s.ErrorIs(err, store.ErrNoNumber, "the number is released")
}

func (s *AppsSuite) TestAReleaseTheVendorWillNotStopKeepsTheNumber() {
	s.ownApp()
	s.sealed()
	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	s.vendor.err = errors.New("the vendor refused")

	s.Require().Error(s.service.Release(s.ctx, s.customer, s.e164))

	s.NotNil(s.number().AttachedAt, "the vendor may still send callers, so the agent still answers")
	s.Equal([]string{s.e164}, s.own.TrunkNumbers(attached.TrunkID))
}

// TestAReleaseRefusedForANumberJustBoughtSaysItIsSettling: Telnyx refused a release 4 seconds
// after the purchase and took the same release 13 seconds later, so a refusal then is said to
// be the vendor setting the number up, and the number is still held for the retry.
func (s *AppsSuite) TestAReleaseRefusedForANumberJustBoughtSaysItIsSettling() {
	s.vendor.releaseErr = &VendorError{Vendor: s.vendor.vendor, Path: "/numbers", Status: http.StatusServiceUnavailable}

	err := s.service.Release(s.ctx, s.customer, s.e164)

	s.ErrorIs(err, ErrNumberSettling)
	s.ErrorAs(err, new(*VendorError), "the vendor's answer is kept for the log")
	_, err = s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, s.e164)
	s.NoError(err, "still held, so it can be released again")
	s.vendor.releaseErr = nil
	s.Require().NoError(s.service.Release(s.ctx, s.customer, s.e164))
}

func (s *AppsSuite) TestAReleaseRefusedForANumberHeldAWhileIsTheVendorsFailure() {
	s.vendor.releaseErr = &VendorError{Vendor: s.vendor.vendor, Path: "/numbers", Status: http.StatusServiceUnavailable}
	later, err := NewService(ServiceOptions{Registry: s.registry, Store: s.store, Apps: s.apps, PublicURL: "https://router.example",
		Now: func() time.Time { return time.Now().Add(2 * NumberSettles) }})
	s.Require().NoError(err)

	err = later.Release(s.ctx, s.customer, s.e164)

	s.ErrorAs(err, new(*VendorError))
	s.NotErrorIs(err, ErrNumberSettling)
}

func (s *AppsSuite) TestReattachingAfterRegistrationMovesTheTrunk() {
	// The number was attached while the customer acted in the deployment's app, and is
	// attached again once it has its own: the old trunk goes from the old app.
	first, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	s.Require().Equal([]string{first.TrunkID}, s.deployment.Trunks())
	s.ownApp()

	second, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)

	s.Empty(s.deployment.Trunks())
	s.Empty(s.deployment.Rules())
	s.Equal([]string{second.TrunkID}, s.own.Trunks())
}

func (s *AppsSuite) TestReattachingInAnotherAppKeepsTheNumbersAgent() {
	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	config := &store.AgentConfig{CustomerID: s.customer, Name: "answering"}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, config))
	s.Require().NoError(s.store.PhoneNumbers.SetAgent(s.ctx, s.customer, s.e164, config.ID))
	s.ownApp()

	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)

	held := s.number()
	s.Equal(int64(4242), held.StreamAppPK)
	s.Equal(config.ID, held.ConfigID, "moving the number to another app took its agent away")
}

func (s *AppsSuite) TestDetachingKeepsTheNumberOnItsTrunk() {
	s.ownApp()
	s.sealed()
	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)

	s.Require().NoError(s.service.Detach(s.ctx, s.customer, s.e164))

	held := s.number()
	s.Nil(held.AttachedAt, "callers no longer reach the agent")
	s.Equal(attached.TrunkID, held.StreamTrunkID)
	s.Equal([]string{attached.TrunkID}, s.own.Trunks())
	s.Equal([]string{attached.RouteID}, s.own.Rules())
	s.Equal([]string{s.e164}, s.own.TrunkNumbers(attached.TrunkID))
	s.Equal([]string{s.e164}, s.vendor.stopped, "the vendor stops sending callers to the trunk")
}

func (s *AppsSuite) TestDetachingANumberAttachedInTheDeploymentsApp() {
	// The deployment's app is stored as no app at all, and its trunk group is app zero.
	s.sealed()
	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)

	s.Require().NoError(s.service.Detach(s.ctx, s.customer, s.e164))

	held := s.number()
	s.Nil(held.AttachedAt)
	s.Equal(attached.TrunkID, held.StreamTrunkID)
	s.Equal([]string{attached.TrunkID}, s.deployment.Trunks())
	s.Equal([]string{attached.RouteID}, s.deployment.Rules())
	s.Equal([]string{s.e164}, s.deployment.TrunkNumbers(attached.TrunkID))
	s.Equal([]string{s.e164}, s.vendor.stopped)
}

func (s *AppsSuite) TestADetachedNumberStillPlacesCalls() {
	s.ownApp()
	s.sealed()
	s.own.OneTrunkPerNumber()
	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	s.Require().NoError(s.service.Detach(s.ctx, s.customer, s.e164))

	_, err = s.callFrom(s.e164)

	s.Require().NoError(err)
	s.Equal([]string{attached.TrunkID}, s.own.Trunks())
}

func (s *AppsSuite) TestDetachingANumberNotAttachedChangesNothing() {
	s.sealed()
	before := s.number()

	s.Require().NoError(s.service.Detach(s.ctx, s.customer, s.e164))

	s.Equal(before, s.number())
	s.Equal([]string{s.e164}, s.vendor.stopped, "the vendor is told anyway, in case it still sends callers")
}

func (s *AppsSuite) TestDetachingAnotherCustomersNumberFails() {
	s.sealed()
	other := "customer-" + uuid.NewString()
	theirs := s.anotherNumber(other, s.vendor.vendor)
	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: other, E164: theirs})
	s.Require().NoError(err)

	err = s.service.Detach(s.ctx, s.customer, theirs)

	s.ErrorContains(err, "is not a number")
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, other, theirs)
	s.Require().NoError(err)
	s.NotNil(held.AttachedAt)
	s.Empty(s.vendor.stopped)
}

func (s *AppsSuite) TestAVendorThatRefusesLeavesTheNumberAttached() {
	s.sealed()
	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	s.vendor.err = errors.New("the vendor refused")

	s.Require().Error(s.service.Detach(s.ctx, s.customer, s.e164))
	s.NotNil(s.number().AttachedAt, "the vendor may still send callers, so the agent still answers")

	s.vendor.err = nil
	s.Require().NoError(s.service.Detach(s.ctx, s.customer, s.e164))
	s.Nil(s.number().AttachedAt)
}

func (s *AppsSuite) TestAttachingADetachedNumberUsesTheSameTrunk() {
	s.ownApp()
	s.sealed()
	s.own.OneTrunkPerNumber()
	first, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	s.Require().NoError(s.service.Detach(s.ctx, s.customer, s.e164))
	s.vendor.inbound = Inbound{}

	again, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})

	s.Require().NoError(err)
	s.Equal(first.TrunkID, again.TrunkID)
	s.Equal(first.RouteID, again.RouteID)
	s.NotNil(s.number().AttachedAt)
	s.Equal(s.e164, s.vendor.inbound.E164, "the vendor is pointed back at the trunk")
}

func (s *AppsSuite) TestAttachingADetachedNumberWithoutAKeyWorks() {
	s.own.OneTrunkPerNumber()
	s.ownApp()
	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	s.Require().NoError(s.service.Detach(s.ctx, s.customer, s.e164))

	again, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})

	s.Require().NoError(err, "Stream lets a number be on one trunk only")
	s.NotNil(s.number().AttachedAt)
	s.Equal([]string{again.TrunkID}, s.own.Trunks())
	s.Equal([]string{s.e164}, s.own.TrunkNumbers(again.TrunkID))
	s.Equal([]string{again.RouteID}, s.own.Rules())
}

func (s *AppsSuite) TestDetachingLeavesALiveCallAlone() {
	s.ownApp()
	s.sealed()
	s.own.OneTrunkPerNumber()
	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	placed, err := s.callFrom(s.e164)
	s.Require().NoError(err)
	scope := store.AppScope{App: 4242}
	live, err := s.store.CallResources.Select(s.ctx, scope, placed.CallType, placed.CallID)
	s.Require().NoError(err)
	s.Require().Len(live, 1)

	s.Require().NoError(s.service.Detach(s.ctx, s.customer, s.e164))

	kept, err := s.store.CallResources.Select(s.ctx, scope, placed.CallType, placed.CallID)
	s.Require().NoError(err)
	s.Equal(live, kept)
	s.Equal([]string{attached.TrunkID}, s.own.Trunks())
	s.Equal([]string{attached.RouteID}, s.own.Rules())

	s.Require().NoError(s.service.ReleaseCall(s.ctx, scope, placed.CallType, placed.CallID))
	ended, err := s.store.CallResources.Select(s.ctx, scope, placed.CallType, placed.CallID)
	s.Require().NoError(err)
	s.Empty(ended)
	s.Equal([]string{attached.TrunkID}, s.own.Trunks(), "the number's own line outlives its calls")
}

func (s *AppsSuite) TestDetachingASipTrunkNumberChangesNothing() {
	sealer, err := auth.NewSealer("apps suite")
	s.Require().NoError(err)
	service, err := NewService(ServiceOptions{
		Registry: NewRegistry(s.config()), Store: s.store, Apps: s.apps, Sealer: sealer,
		SIPTrunks: &stub{vendor: SIPTrunkVendor},
	})
	s.Require().NoError(err)
	password := "trunk-s3cret"
	trunk, err := service.CreateSIPTrunk(s.ctx, s.customer, SIPTrunkSettings{
		Name: "main", Host: "trunk.example.com", Username: "trunk-user", Password: &password,
	})
	s.Require().NoError(err)
	e164 := aNumber()
	_, err = service.AddTrunkNumber(s.ctx, TrunkNumber{
		Owner: routing.Owner{CustomerID: s.customer}, TrunkID: trunk.ID, E164: e164, Country: "us",
	})
	s.Require().NoError(err)
	before, err := s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, e164)
	s.Require().NoError(err)

	s.Require().NoError(service.Detach(s.ctx, s.customer, e164), "sip_trunk is no vendor of the registry")

	after, err := s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, e164)
	s.Require().NoError(err)
	s.Equal(before, after)
}

func (s *AppsSuite) TestAttachingAnAttachedNumberAgainChangesNothing() {
	// The SDKs attach on every WaitForCall, so the same request arrives on every run.
	s.ownApp()
	s.own.OneTrunkPerNumber()
	first, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	s.vendor.inbound = Inbound{}

	again, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})

	s.Require().NoError(err)
	s.Equal(first.TrunkID, again.TrunkID)
	s.Equal(first.RouteID, again.RouteID)
	s.Equal(first.Bridge.URI, again.Bridge.URI)
	s.Equal([]string{first.TrunkID}, s.own.Trunks())
	s.Equal([]string{first.RouteID}, s.own.Rules())
	s.Empty(s.vendor.inbound.E164, "the vendor is not told anything new")
}

func (s *AppsSuite) TestAttachingAnAttachedNumberAgainInTheDeploymentsAppChangesNothing() {
	// The deployment's app is stored as no app at all, and read back as zero.
	s.deployment.OneTrunkPerNumber()
	first, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	s.vendor.inbound = Inbound{}

	again, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})

	s.Require().NoError(err)
	s.Equal(first.TrunkID, again.TrunkID)
	s.Equal(first.RouteID, again.RouteID)
	s.Equal([]string{first.TrunkID}, s.deployment.Trunks())
	s.Equal([]string{first.RouteID}, s.deployment.Rules())
	s.Empty(s.vendor.inbound.E164, "the vendor is not told anything new")
}

// Numbers attached before every caller had a call of their own route into one fixed call,
// which attaching again used to replace.
func (s *AppsSuite) TestAttachingANumberThatStillNamesAFixedCallChangesNothing() {
	s.ownApp()
	first, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	held := s.number()
	s.Require().NoError(s.store.PhoneNumbers.Attach(s.ctx, s.customer, s.e164, store.NumberAttachment{
		TrunkID: held.StreamTrunkID, RouteID: held.StreamRouteID, StreamAppPK: held.StreamAppPK,
		CallType: held.StreamCallType, CallID: "phone-" + s.e164,
	}))
	s.vendor.inbound = Inbound{}

	again, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})

	s.Require().NoError(err)
	s.Equal(first.TrunkID, again.TrunkID)
	s.Equal([]string{first.TrunkID}, s.own.Trunks())
	s.Equal([]string{first.RouteID}, s.own.Rules())
	s.Empty(s.vendor.inbound.E164, "the vendor is not told anything new")
	s.Equal("phone-"+s.e164, s.number().StreamCallID, "the row is left as it was")
}

func (s *AppsSuite) TestAttachingATelnyxNumberWhosePasswordWasNotKeptChangesNothing() {
	telnyx := &stub{vendor: "telnyx"}
	registry := NewRegistry(s.config())
	registry.Register(telnyx.vendor, func() (Provider, error) { return telnyx, nil })
	for _, name := range s.credentials(telnyx.vendor) {
		s.T().Setenv(name, "set")
	}
	sealer, err := auth.NewSealer("apps suite")
	s.Require().NoError(err)
	service, err := NewService(ServiceOptions{Registry: registry, Store: s.store, Apps: s.apps, Sealer: sealer})
	s.Require().NoError(err)
	e164 := aNumber()
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: e164, Vendor: telnyx.vendor, Country: "US", CustomerID: s.customer, PurchasedAt: time.Now().UTC(),
	}))
	first, err := service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})
	s.Require().NoError(err)
	_, err = s.store.GlobalDB().NewUpdate().Table("stream_trunks").Set("bridge_password_sealed = ''::bytea").
		Where("id = ?", first.TrunkID).Exec(s.ctx)
	s.Require().NoError(err)
	telnyx.inbound = Inbound{}

	again, err := service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Require().NoError(err)
	s.Equal(first.TrunkID, again.TrunkID)
	s.Equal([]string{first.TrunkID}, s.deployment.Trunks())
	s.Equal([]string{first.RouteID}, s.deployment.Rules())
	s.Empty(telnyx.inbound.E164, "telnyx is not told anything new")
}

func (s *AppsSuite) TestReleasingAfterRegistrationDeletesTheLinesLeftInTheDeploymentApp() {
	// The customer may no longer add to the deployment's app, and what the router made
	// there for it is still the router's to take down.
	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	s.Require().Len(s.deployment.Trunks(), 1)
	s.ownApp()

	s.Require().NoError(s.service.Release(s.ctx, s.customer, s.e164))

	s.Empty(s.deployment.Trunks())
	s.Empty(s.deployment.Rules())
}

func (s *AppsSuite) TestACallEndedAfterRegistrationDeletesItsLinesInTheDeploymentApp() {
	placed, err := s.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550001111",
	})
	s.Require().NoError(err)
	s.Require().Len(s.deployment.Trunks(), 1)
	s.ownApp()

	s.Require().NoError(s.service.ReleaseCall(s.ctx, store.AppScope{App: 1, Unpinned: true}, placed.CallType, placed.CallID))

	s.Empty(s.deployment.Trunks())
	s.Empty(s.deployment.Rules())
}

func (s *AppsSuite) TestACampaignCallAndItsSessionShareAnAppAndACall() {
	s.ownApp()

	placed, err := s.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550001111",
	})
	s.Require().NoError(err)

	s.Equal(int64(4242), placed.StreamApp)
	s.Equal(placed.SessionID, placed.CallID, "the answered leg lands in the session's own call")
	s.Equal("agent", placed.CallType)
	s.Len(s.own.Trunks(), 1)
	pin, found, err := s.store.CallResources.Pin(s.ctx, s.customer, "agent", placed.SessionID)
	s.Require().NoError(err)
	s.True(found)
	s.Equal(int64(4242), pin, "the session joining it finds the app the call's lines are in")
}

func (s *AppsSuite) TestReleasingDeletesEachTrunkInTheAppThatMadeIt() {
	s.ownApp()
	placed, err := s.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550001111",
	})
	s.Require().NoError(err)

	s.Require().NoError(s.service.ReleaseCall(s.ctx, store.AppScope{App: 4242}, placed.CallType, placed.CallID))

	s.Empty(s.own.Trunks())
	s.Empty(s.own.Rules())
}

func (s *AppsSuite) TestAnotherAppsEndedCallReleasesNothing() {
	// A call's id is only unique within an app. An event about a call in the deployment's
	// app must not take down the lines of a call of the same name in a customer's.
	s.ownApp()
	placed, err := s.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550001111",
	})
	s.Require().NoError(err)

	s.Require().NoError(s.service.ReleaseCall(s.ctx, store.AppScope{App: 1, Unpinned: true}, placed.CallType, placed.CallID))

	s.Len(s.own.Trunks(), 1)
}

func (s *AppsSuite) TestALegacyNumberStillReleasesInTheDeploymentApp() {
	placed, err := s.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550001111",
	})
	s.Require().NoError(err)
	s.Require().Len(s.deployment.Trunks(), 1)

	s.Require().NoError(s.service.ReleaseCall(s.ctx, store.AppScope{App: 1, Unpinned: true}, placed.CallType, placed.CallID))

	s.Empty(s.deployment.Trunks())
}

func (s *AppsSuite) TestAMidCallTransferCreatesItsTrunkInTheSessionsApp() {
	s.ownApp()

	_, err := s.service.Transfer(s.ctx, TransferRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550002222",
		CallID: "call-1", StreamApp: 4242,
	})
	s.Require().NoError(err)

	s.Len(s.own.Trunks(), 1)
	s.Empty(s.deployment.Trunks())
}

// testApps is the apps the suite's customers make lines in: their own when given one, the
// deployment's otherwise. As app mode does once it no longer falls back, a customer given an
// app of its own can read what it left in the deployment's app and not add to it.
type testApps struct {
	mu         sync.Mutex
	deployment *chattest.Server
	own        map[string]*chattest.Server
	pins       map[string]int64
	blocked    map[string]bool
	// cutOff are the customers whose lines cannot be reached to be taken down.
	cutOff map[string]bool
	// unread are the customers whose placement cannot be read for now, as when a database does
	// not answer.
	unread map[string]bool
}

// errPlacedUnread is where a customer's lines are made, not read for now.
var errPlacedUnread = errors.New("apps: where the customer's lines are made could not be read")

func (a *testApps) cannotRead(customer string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.unread[customer] = true
}

func (a *testApps) block(customer string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.blocked[customer] = true
}

func (a *testApps) give(customer string, pin int64, server *chattest.Server) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.own[customer], a.pins[customer] = server, pin
}

func (a *testApps) For(_ context.Context, customer string) (*Stream, int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.blocked[customer] {
		return nil, 0, streamapp.ErrStreamAppDisconnected
	}
	if server, ok := a.own[customer]; ok {
		return NewStreamFromClient(server.Client), a.pins[customer], nil
	}
	return NewStreamFromClient(a.deployment.Client), 0, nil
}

func (a *testApps) Placed(ctx context.Context, customer string) (int64, error) {
	a.mu.Lock()
	unread := a.unread[customer]
	a.mu.Unlock()
	if unread {
		return 0, errPlacedUnread
	}
	_, pin, err := a.For(ctx, customer)
	return pin, err
}

func (a *testApps) ForApp(ctx context.Context, customer string, app int64) (*Stream, error) {
	a.mu.Lock()
	_, owns := a.own[customer]
	a.mu.Unlock()
	if owns && app == 0 {
		return nil, streamapp.ErrReadOnly
	}
	return a.ForAppRemoving(ctx, customer, app)
}

func (a *testApps) ForAppRemoving(_ context.Context, customer string, app int64) (*Stream, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cutOff[customer] {
		return nil, errors.New("apps: the stream app cannot be reached")
	}
	if server, ok := a.own[customer]; ok && a.pins[customer] == app {
		return NewStreamFromClient(server.Client), nil
	}
	return NewStreamFromClient(a.deployment.Client), nil
}

func (s *AppsSuite) purchase() Purchase {
	return Purchase{Vendor: s.vendor.vendor, E164: aNumber(), Country: "US", Owner: routing.Owner{CustomerID: s.customer}}
}

// A number bought but not saved says which vendor sold it and under what id, so support can
// find it there, and names no phone number, since the failure is logged.
func (s *AppsSuite) TestANumberBoughtButNotSavedNamesTheVendorAndItsIDAndNotTheNumber() {
	s.vendor.vendorID = "order-" + uuid.NewString()
	purchase := s.purchase()
	_, err := s.service.Buy(s.ctx, purchase)
	s.Require().NoError(err)

	// The vendor sells it again, and the row the first purchase saved holds it.
	_, err = s.service.Buy(s.ctx, purchase)

	s.Require().ErrorContains(err, "was not recorded")
	s.ErrorContains(err, s.vendor.vendor)
	s.ErrorContains(err, s.vendor.vendorID)
	s.NotContains(err.Error(), purchase.E164)
	s.NotContains(err.Error(), strings.TrimPrefix(purchase.E164, "+"))
}

func (s *AppsSuite) TestBuyingANumberPointsTheCallHookOfTheAppItIsBoughtIn() {
	s.ownApp()

	_, err := s.service.Buy(s.ctx, s.purchase())

	s.Require().NoError(err)
	hooks := s.own.EventHooks(s.own.Key)
	s.Require().Len(hooks, 1)
	s.Equal("https://router.example/v1/phone/hooks/stream/4242", value(hooks[0].WebhookUrl))
	s.Empty(s.deployment.EventHooks(s.deployment.Key))
}

func (s *AppsSuite) TestABuyOutsideTheUSAndCanadaLeavesTheAppsHookAlone() {
	s.ownApp()
	purchase := s.purchase()
	purchase.E164 = "+18765551234"

	_, err := s.service.Buy(s.ctx, purchase)

	s.ErrorIs(err, ErrInvalid)
	s.Empty(s.own.EventHooks(s.own.Key), "a refused buy changes nothing in the customer's app")
}

func (s *AppsSuite) TestABuyWhoseHookCannotBePointedIsNotPaidFor() {
	s.ownApp()
	s.service = s.serviceAt("ftp://router.example")

	_, err := s.service.Buy(s.ctx, s.purchase())

	s.Error(err)
	s.Zero(s.vendor.ordered, "the vendor is not paid for a number whose calls would never arrive")
}

func (s *AppsSuite) TestWithoutAPublicURLABuyPointsNoHook() {
	s.service = s.serviceAt("")

	_, err := s.service.Buy(s.ctx, s.purchase())

	s.Require().NoError(err)
	s.Empty(s.deployment.EventHooks(s.deployment.Key))
}

func (s *AppsSuite) TestAttachingANumberPointsTheCallHookOfTheAppItIsAttachedIn() {
	s.ownApp()

	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})

	s.Require().NoError(err)
	hooks := s.own.EventHooks(s.own.Key)
	s.Require().Len(hooks, 1)
	s.Equal("https://router.example/v1/phone/hooks/stream/4242", value(hooks[0].WebhookUrl))
}

func (s *AppsSuite) TestAttachingAnAttachedNumberAgainRepairsARemovedHook() {
	s.ownApp()
	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	s.own.SetEventHooks(s.own.Key)

	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})

	s.Require().NoError(err)
	hooks := s.own.EventHooks(s.own.Key)
	s.Require().Len(hooks, 1)
	s.Equal("https://router.example/v1/phone/hooks/stream/4242", value(hooks[0].WebhookUrl))
}

func (s *AppsSuite) TestAnAttachWhoseHookCannotBePointedLeavesTheNumberDetached() {
	s.ownApp()
	s.service = s.serviceAt("ftp://router.example")

	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})

	s.Error(err)
	held := s.number()
	s.Nil(held.AttachedAt)
	s.Empty(held.StreamTrunkID)
}

func (s *AppsSuite) TestAnAttachedNumberAttachedAgainStillAnswersWhenItsHookCannotBePointed() {
	s.ownApp()
	first, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	s.service = s.serviceAt("ftp://router.example")

	again, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})

	s.Require().NoError(err)
	s.Equal(first.TrunkID, again.TrunkID)
}

func (s *AppsSuite) TestANumberInAnotherFormIsNotBought() {
	_, err := s.service.Buy(s.ctx, Purchase{
		Vendor: s.vendor.vendor, E164: strings.TrimPrefix(aNumber(), "+"), Owner: routing.Owner{CustomerID: s.customer},
	})

	s.ErrorIs(err, store.ErrNotE164)
	s.Zero(s.vendor.ordered, "the vendor is not paid for a number that would not be recorded")
}

func (s *AppsSuite) TestAnotherFormOfANumberReachesNothingAtTheVendor() {
	// Another customer's row for the suite's number without its plus, as one could be written
	// before every write checked the form. The vendor would read it as the suite's number.
	other, variant := "customer-"+uuid.NewString(), strings.TrimPrefix(s.e164, "+")
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "INSERT INTO phone_numbers (e164, vendor, country, customer_id)"+
		" VALUES (?, ?, 'US', ?)", variant, s.vendor.vendor, other)
	s.Require().NoError(err)

	s.ErrorIs(s.service.Release(s.ctx, other, variant), store.ErrNoNumber)
	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: other, E164: variant})
	s.ErrorIs(err, store.ErrNoNumber)
	_, err = s.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: other}, From: variant, To: "+15550001111"})
	s.ErrorIs(err, store.ErrNoNumber)

	s.Empty(s.vendor.released)
	s.Zero(s.vendor.inbound)
	s.Zero(s.vendor.dialed)
	_, err = s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, s.e164)
	s.NoError(err, "the number stays with who held it")
}

func (s *AppsSuite) TestAnAppStreamBlockedCannotBuyANumber() {
	s.apps.block(s.customer)

	_, err := s.service.Buy(s.ctx, Purchase{Vendor: s.vendor.vendor, E164: aNumber(), Owner: routing.Owner{CustomerID: s.customer}})

	s.ErrorIs(err, streamapp.ErrStreamAppDisconnected)
}

func (s *AppsSuite) TestTheVendorIsGivenATrunkAddressStreamCanFindByTheCallingNumber() {
	_, err := s.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550001111",
	})
	s.Require().NoError(err)

	trunks := s.deployment.Trunks()
	s.Require().Len(trunks, 1)
	s.Equal("sip:"+s.e164+"@sip.example.test", s.vendor.dialed.Bridge.URI)
}

func (s *AppsSuite) TestATransferGivesTheVendorATrunkAddressStreamCanFindByTheCallingNumber() {
	_, err := s.service.Transfer(s.ctx, TransferRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550002222", CallID: "call-1",
	})
	s.Require().NoError(err)

	s.Equal("sip:"+s.e164+"@sip.example.test", s.vendor.dialed.Bridge.URI)
}

func (s *AppsSuite) TestAttachingGivesTheVendorATrunkAddressStreamCanFindByTheNumber() {
	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)

	want := "sip:" + s.e164 + "@sip.example.test"
	s.Equal(want, s.vendor.inbound.Bridge.URI)
	s.Equal(want, attached.Bridge.URI, "the api shows the address the vendor was given")
}

func (s *AppsSuite) TestAnAttachedNumbersCallersEachLandInACallOfTheirOwn() {
	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)

	rule := s.deployment.Rule(attached.RouteID)
	s.Require().NotNil(rule)
	direct := rule["direct_routing_configs"].(map[string]any)
	s.Equal(lineCallTemplate, direct["call_id"], "each caller's call is named for the session answering them")
	s.Equal("agent", direct["call_type"])
	custom := rule["call_configs"].(map[string]any)["custom_data"].(map[string]any)
	s.Equal(s.e164, custom[NumberKey], "the call says which number it rang")
}

func (s *AppsSuite) TestAPlacedCallLandsInTheCallOfTheSessionItWasPlacedFor() {
	id := "0b9f2a52-5c1e-4d7a-9a3e-0f6b8f1c2d3e"

	placed, err := s.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550001111", SessionID: id,
	})
	s.Require().NoError(err)

	s.Equal(id, placed.SessionID)
	s.Equal(id, placed.CallID)
}

func (s *AppsSuite) callPlacedRows(placed Placed) []store.AgentLog {
	return s.eventRows(placed, "call_placed")
}

func (s *AppsSuite) eventRows(placed Placed, eventType string) []store.AgentLog {
	rows, err := s.store.AgentLogs.Select(s.ctx, s.customer, store.AgentLogFilter{SessionID: placed.SessionID, Limit: 250})
	s.Require().NoError(err)
	var found []store.AgentLog
	for _, row := range rows {
		if row.EventType == eventType {
			found = append(found, row)
		}
	}
	return found
}

func (s *AppsSuite) TestAPlacedCallWritesACallPlacedRow() {
	placed, err := s.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550001111",
	})
	s.Require().NoError(err)

	rows := s.callPlacedRows(placed)
	s.Require().Len(rows, 1)
	row, err := s.store.AgentLogs.Get(s.ctx, s.customer, rows[0].ID)
	s.Require().NoError(err)
	s.Equal(BridgeHost(s.vendor.dialed.Bridge.URI), row.Details["stream_sip_host"])
	s.NotEmpty(row.Details["stream_sip_host"])
	s.Equal("twilio", row.Details["vendor"])
	s.Equal(placed.VendorCallID, row.Details["vendor_call_id"])
	s.NotContains(row.Details, "customer_trunk_host", "the call went out through a vendor, not a customer's trunk")
	for key, value := range row.Details {
		s.NotContains(value, s.vendor.dialed.Bridge.Password, key)
	}
}

func (s *AppsSuite) TestASlowCallPlacedRowDoesNotFailTheCallOrLoseItsResourceRow() {
	// Holding the customer's log lock makes the call_placed write wait.
	locked, unlock := make(chan struct{}), make(chan struct{})
	go func() {
		_ = s.store.GlobalDB().RunInTx(s.ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			_, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended(?, 731))", s.customer)
			close(locked)
			<-unlock
			return err
		})
	}()
	<-locked

	started := time.Now()
	placed, err := s.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550001111",
	})
	waited := time.Since(started)
	close(unlock)

	s.Require().NoError(err)
	s.Less(waited, 2*agentLogTimeout, "the reply waits for the row only up to its timeout")
	s.Empty(s.callPlacedRows(placed))
	resources, err := s.store.CallResources.Select(s.ctx, store.AppScope{App: 1, Unpinned: true}, placed.CallType, placed.CallID)
	s.Require().NoError(err)
	s.Len(resources, 1)
}

// trunkCall is a call placed through a customer's own trunk, with what its service logged and
// the vendor that was asked to place it.
type trunkCall struct {
	placed   Placed
	service  *Service
	vendor   *stub
	logged   *bytes.Buffer
	password string
}

func (s *AppsSuite) callsThroughATrunk() trunkCall {
	return s.callsThroughATrunkOn(StreamLegPerCall)
}

// callsThroughATrunkOn is callsThroughATrunk where the stand-in for the sip_trunk provider
// has the given kind of Stream leg.
func (s *AppsSuite) callsThroughATrunkOn(leg StreamLeg) trunkCall {
	// One no other customer holds, as a number on a trunk has to be.
	e164 := aNumber()
	call := s.onATrunk(s.customer, e164, leg)

	placed, err := call.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: e164, To: "+15550001111",
	})
	s.Require().NoError(err)
	call.placed = placed
	return call
}

// onATrunk is a service that dials through customers' own trunks with a stand-in for the
// sip_trunk provider of the given kind of Stream leg, and the number added to a trunk of the
// customer's own.
func (s *AppsSuite) onATrunk(customer, e164 string, leg StreamLeg) trunkCall {
	call := s.dialsTrunks(leg)
	s.Require().NoError(s.addOnATrunk(call, customer, e164))
	return call
}

// dialsTrunks is a service that dials through customers' own trunks with a stand-in for the
// sip_trunk provider of the given kind of Stream leg.
func (s *AppsSuite) dialsTrunks(leg StreamLeg) trunkCall {
	sealer, err := auth.NewSealer("apps suite")
	s.Require().NoError(err)
	logged := &bytes.Buffer{}
	vendor := &stub{vendor: SIPTrunkVendor, leg: leg}
	service, err := NewService(ServiceOptions{
		Registry: NewRegistry(s.config()), Store: s.store, Apps: s.apps, Sealer: sealer,
		SIPTrunks: vendor, Logger: slog.New(slog.NewTextHandler(logged, nil)),
	})
	s.Require().NoError(err)
	return trunkCall{service: service, vendor: vendor, logged: logged, password: uuid.NewString()}
}

// addOnATrunk adds a number to a trunk made for the customer.
func (s *AppsSuite) addOnATrunk(call trunkCall, customer, e164 string) error {
	password := call.password
	trunk, err := call.service.CreateSIPTrunk(s.ctx, customer, SIPTrunkSettings{
		Name: "main", Host: "trunk.example.com", Username: "trunk-user", Password: &password,
	})
	s.Require().NoError(err)
	_, err = call.service.AddTrunkNumber(s.ctx, TrunkNumber{
		Owner: routing.Owner{CustomerID: customer}, TrunkID: trunk.ID, E164: e164, Country: "us",
	})
	return err
}

func (s *AppsSuite) TestACallThroughACustomersTrunkLandsInItsSessionsCallThroughTheNumbersRule() {
	s.deployment.OneTrunkPerNumber()

	call := s.callsThroughATrunkOn(StreamLegShared)

	from := call.vendor.dialed.From
	s.Equal("sip:"+from+"@sip.example.test", call.vendor.dialed.Bridge.URI)
	s.Equal(call.placed.CallID, call.vendor.dialed.CallID, "the bridge names the session's call on its leg")
	trunks, rules := s.deployment.Trunks(), s.deployment.Rules()
	s.Require().Len(trunks, 1)
	s.Require().Len(rules, 1)
	s.Equal([]string{from}, s.deployment.TrunkNumbers(trunks[0]))
	rule := s.deployment.Rule(rules[0])
	s.Equal([]any{from}, rule["called_numbers"])
	s.Equal([]any{trunks[0]}, rule["trunk_ids"])
	direct := rule["direct_routing_configs"].(map[string]any)
	s.Equal(lineCallTemplate, direct["call_id"], "the leg lands in the call its header names")
}

// The customer that registered the number on its trunk did so first, before the other bought it,
// and has placed no call from it since.
func (s *AppsSuite) TestANumberAnotherCustomerBoughtIsNotLinedFromATrunkItWasAddedToFirst() {
	s.sealed()
	s.deployment.OneTrunkPerNumber()
	e164, first := aNumber(), "customer-"+uuid.NewString()
	trunk := s.onATrunk(first, e164, StreamLegShared)
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: e164, Vendor: s.vendor.vendor, Country: "US", CustomerID: s.customer, PurchasedAt: time.Now().UTC(),
	}))

	_, err := trunk.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: first}, From: e164, To: "+15550001111",
	})

	s.ErrorIs(err, store.ErrNumberHeldElsewhere)
	s.NotErrorIs(err, ErrNotDialed, "a campaign does not try it again")
	s.EqualError(err, store.ErrNumberHeldElsewhere.Error(), "naming nobody")
	s.Empty(trunk.vendor.dialed.From, "the trunk was not dialled")
	s.Empty(s.deployment.Trunks(), "nothing was made at Stream")
	s.Empty(s.deployment.Rules())
	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})
	s.Require().NoError(err, "the buyer attaches it in the app both share")
	_, err = s.callFrom(e164)
	s.NoError(err, "and calls from it")
}

// squatted is a number another customer added to its own trunk and placed a call from, in the
// deployment's app, before the suite's customer bought it. The call need not have connected: the
// line is made before the trunk is dialled. It answers that customer, its trunk service and its
// row as the call left it.
func (s *AppsSuite) squatted() (string, string, trunkCall, store.PhoneNumber) {
	s.sealed()
	s.deployment.OneTrunkPerNumber()
	e164, squatter := aNumber(), "customer-"+uuid.NewString()
	trunk := s.onATrunk(squatter, e164, StreamLegShared)
	_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: e164, To: "+15550001111"})
	s.Require().NoError(err)
	line, err := s.store.PhoneNumbers.GetByE164(s.ctx, squatter, e164)
	s.Require().NoError(err)
	s.Require().NotEmpty(line.StreamTrunkID, "the call lined it")
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: e164, Vendor: s.vendor.vendor, Country: "US", CustomerID: s.customer, PurchasedAt: time.Now().UTC(),
	}))
	return e164, squatter, trunk, line
}

// takenOff checks the squatter's line for the number is gone, here and at Stream.
func (s *AppsSuite) takenOff(squatter string, line store.PhoneNumber, stream *chattest.Server) {
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, squatter, line.E164)
	s.Require().NoError(err)
	s.Empty(held.StreamTrunkID, "its line is forgotten")
	s.NotContains(stream.Trunks(), line.StreamTrunkID, "its trunk went with its only number")
	s.Nil(stream.Rule(line.StreamRouteID))
}

// leftAsItWas checks the squatter's line for the number is still there, here and at Stream.
func (s *AppsSuite) leftAsItWas(squatter string, line store.PhoneNumber, stream *chattest.Server) {
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, squatter, line.E164)
	s.Require().NoError(err)
	s.Equal(line.StreamTrunkID, held.StreamTrunkID)
	s.Equal(line.StreamRouteID, held.StreamRouteID)
	s.Equal(line.StreamAppPK, held.StreamAppPK)
	s.Contains(stream.TrunkNumbers(line.StreamTrunkID), line.E164)
}

func (s *AppsSuite) TestABuyerAttachesANumberAnotherCustomerLinedOnItsTrunkBeforeItWasBought() {
	e164, squatter, trunk, line := s.squatted()

	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Require().NoError(err)
	s.takenOff(squatter, line, s.deployment)
	_, err = s.callFrom(e164)
	s.NoError(err, "and the buyer calls from it")
	_, err = trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: e164, To: "+15550001111"})
	s.ErrorIs(err, store.ErrNumberHeldElsewhere, "the other customer cannot line it again")
}

func (s *AppsSuite) TestABuyerCallsFromANumberAnotherCustomerLinedOnItsTrunkBeforeItWasBought() {
	e164, squatter, _, line := s.squatted()

	_, err := s.callFrom(e164)

	s.Require().NoError(err)
	s.takenOff(squatter, line, s.deployment)
}

func (s *AppsSuite) TestABuyerTransfersFromANumberAnotherCustomerLinedOnItsTrunkBeforeItWasBought() {
	e164, squatter, _, line := s.squatted()

	_, err := s.service.Transfer(s.ctx, TransferRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: e164, To: "+15550002222", CallID: "call-1",
	})

	s.Require().NoError(err)
	s.takenOff(squatter, line, s.deployment)
}

// Both customers' lines are made in one Stream app that is not the deployment's, so the
// transfer's line goes where the other customer's is.
func (s *AppsSuite) TestABuyerTransfersInAnAppItSharesFromANumberAnotherCustomerLinedThere() {
	s.sealed()
	shared, e164, squatter := chattest.NewServer(s.T()), aNumber(), "customer-"+uuid.NewString()
	shared.OneTrunkPerNumber()
	s.apps.give(squatter, 6262, shared)
	s.apps.give(s.customer, 6262, shared)
	trunk := s.onATrunk(squatter, e164, StreamLegShared)
	_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: e164, To: "+15550001111"})
	s.Require().NoError(err)
	line, err := s.store.PhoneNumbers.GetByE164(s.ctx, squatter, e164)
	s.Require().NoError(err)
	s.Require().Equal(int64(6262), line.StreamAppPK, "the call lined it in the app both share")
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: e164, Vendor: s.vendor.vendor, Country: "US", CustomerID: s.customer, PurchasedAt: time.Now().UTC(),
	}))

	_, err = s.service.Transfer(s.ctx, TransferRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: e164, To: "+15550002222", CallID: "call-1", StreamApp: 6262,
	})

	s.Require().NoError(err)
	s.takenOff(squatter, line, shared)
}

// The other customer lined the number in the deployment's app and has since connected an app of
// its own, so it may no longer add to what it left in the deployment's: its line there is still
// taken down.
func (s *AppsSuite) TestABuyerTakesDownALineWhoseCustomerHasSinceConnectedAnAppOfItsOwn() {
	e164, squatter, _, line := s.squatted()
	s.apps.give(squatter, 7171, chattest.NewServer(s.T()))

	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Require().NoError(err)
	s.takenOff(squatter, line, s.deployment)
}

// A second customer's row for the number on its own trunk, as one written before a number was
// held by one customer at a time leaves it: nobody sold it the number either.
func (s *AppsSuite) TestACallFromANumberOnATrunkTakesNoOtherCustomersLine() {
	s.sealed()
	e164, squatter := aNumber(), "customer-"+uuid.NewString()
	trunk := s.onATrunk(squatter, e164, StreamLegShared)
	_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: e164, To: "+15550001111"})
	s.Require().NoError(err)
	line, err := s.store.PhoneNumbers.GetByE164(s.ctx, squatter, e164)
	s.Require().NoError(err)
	second := "customer-" + uuid.NewString()
	password := trunk.password
	theirs, err := trunk.service.CreateSIPTrunk(s.ctx, second, SIPTrunkSettings{
		Name: "main", Host: "trunk.example.com", Username: "trunk-user", Password: &password,
	})
	s.Require().NoError(err)
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: e164, Vendor: SIPTrunkVendor, Country: "US", CustomerID: second, SIPTrunkID: theirs.ID, PurchasedAt: time.Now().UTC(),
	}))

	_, err = trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: second}, From: e164, To: "+15550001111"})

	s.Require().NoError(err)
	s.leftAsItWas(squatter, line, s.deployment)
	s.NotNil(s.deployment.Rule(line.StreamRouteID))
}

func (s *AppsSuite) TestALineForTheNumberInAnotherAppIsLeftAlone() {
	s.sealed()
	s.deployment.OneTrunkPerNumber()
	e164, squatter := aNumber(), "customer-"+uuid.NewString()
	elsewhere := chattest.NewServer(s.T())
	s.apps.give(squatter, 4343, elsewhere)
	trunk := s.onATrunk(squatter, e164, StreamLegShared)
	_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: e164, To: "+15550001111"})
	s.Require().NoError(err)
	line, err := s.store.PhoneNumbers.GetByE164(s.ctx, squatter, e164)
	s.Require().NoError(err)
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: e164, Vendor: s.vendor.vendor, Country: "US", CustomerID: s.customer, PurchasedAt: time.Now().UTC(),
	}))

	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Require().NoError(err)
	s.leftAsItWas(squatter, line, elsewhere)
}

// Taken off here and left at Stream, the line would go on refusing the buyer with nothing here
// to take it down by.
func (s *AppsSuite) TestALineStreamWillNotDeleteTheRuleOfIsLeftAsItWas() {
	e164, squatter, _, line := s.squatted()
	s.deployment.Fail(http.MethodDelete, "/sip/inbound_routing_rules/"+line.StreamRouteID)

	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Error(err)
	s.leftAsItWas(squatter, line, s.deployment)
}

func (s *AppsSuite) TestALineWhoseAppCannotBeReachedIsLeftAsItWas() {
	e164, squatter, _, line := s.squatted()
	s.apps.mu.Lock()
	s.apps.cutOff[squatter] = true
	s.apps.mu.Unlock()

	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Error(err)
	s.leftAsItWas(squatter, line, s.deployment)
}

// The other customer's trunk at Stream lists another of its numbers too, so the number is taken
// off it rather than deleted with it.
func (s *AppsSuite) TestALineOnATrunkStreamWillNotTakeTheNumberOffIsLeftAsItWas() {
	e164, squatter, trunk, line := s.squatted()
	other := aNumber()
	s.Require().NoError(s.addOnATrunk(trunk, squatter, other))
	_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: other, To: "+15550001111"})
	s.Require().NoError(err)
	s.Require().ElementsMatch([]string{e164, other}, s.deployment.TrunkNumbers(line.StreamTrunkID))
	s.deployment.Fail(http.MethodPut, "/sip/inbound_trunks/"+line.StreamTrunkID)

	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Error(err)
	s.leftAsItWas(squatter, line, s.deployment)
}

// The other customer's shared trunk is gone at Stream, which lists the number on it no more.
func (s *AppsSuite) TestALineOnATrunkStreamNoLongerHasIsForgotten() {
	e164, squatter, trunk, line := s.squatted()
	other := aNumber()
	s.Require().NoError(s.addOnATrunk(trunk, squatter, other))
	_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: other, To: "+15550001111"})
	s.Require().NoError(err)
	s.goneAtStream(line.StreamTrunkID)

	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Require().NoError(err)
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, squatter, e164)
	s.Require().NoError(err)
	s.Empty(held.StreamTrunkID)
}

// The other customer's line names a trunk whose row was dropped (forgetLostTrunk), which Stream
// still has: it is deleted there with the number's rule, or not forgotten at all.
func (s *AppsSuite) TestALineOnATrunkWithNoRowIsLeftAsItWasWhenStreamKeepsItsRule() {
	e164, squatter, _, line := s.squatted()
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "DELETE FROM stream_trunks WHERE id = ?", line.StreamTrunkID)
	s.Require().NoError(err)
	s.deployment.Fail(http.MethodDelete, "/sip/inbound_routing_rules/"+line.StreamRouteID)

	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Error(err)
	s.leftAsItWas(squatter, line, s.deployment)
}

func (s *AppsSuite) TestALineOnATrunkWithNoRowIsDeletedAtStream() {
	e164, squatter, _, line := s.squatted()
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "DELETE FROM stream_trunks WHERE id = ?", line.StreamTrunkID)
	s.Require().NoError(err)

	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Require().NoError(err)
	s.takenOff(squatter, line, s.deployment)
}

// Forgotten here and left at Stream, the trunk would go on listing the number with nothing here
// to take it down by.
func (s *AppsSuite) TestALineOnATrunkWithNoRowStreamWillNotDeleteIsLeftAsItWas() {
	e164, squatter, _, line := s.squatted()
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "DELETE FROM stream_trunks WHERE id = ?", line.StreamTrunkID)
	s.Require().NoError(err)
	s.deployment.Fail(http.MethodDelete, "/sip/inbound_trunks/"+line.StreamTrunkID)

	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Error(err)
	s.leftAsItWas(squatter, line, s.deployment)
}

// lineChanges are the changes a data move replays that leave a lined number no row with its line.
var lineChanges = []string{"deleted", "given back", "another number's"}

// moved replays a change onto the line's row.
func (s *AppsSuite) moved(line store.PhoneNumber, change string) {
	s.Require().NoError(s.store.ApplyChanges(s.ctx, line.CustomerID, []store.DataChange{s.lineChange(line, change)}))
}

func (s *AppsSuite) lineChange(line store.PhoneNumber, change string) store.DataChange {
	if change == "deleted" {
		key, err := json.Marshal(map[string]any{"id": line.ID, "customer_id": line.CustomerID})
		s.Require().NoError(err)
		return store.DataChange{Table: "phone_numbers", Op: store.ChangeDelete, Key: key}
	}
	set := map[string]any{"released_at": time.Now().UTC()}
	if change == "another number's" {
		set = map[string]any{"e164": aNumber()}
	}
	fields, err := json.Marshal(set)
	s.Require().NoError(err)
	var row json.RawMessage
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
		"SELECT to_jsonb(pn) || ?::jsonb FROM phone_numbers AS pn WHERE id = ?", string(fields), line.ID).Scan(&row))
	return store.DataChange{Table: "phone_numbers", Op: store.ChangeUpdate, Payload: row}
}

// forgotten checks Stream has nothing left of the line, nor a trunk row here.
func (s *AppsSuite) forgotten(line store.PhoneNumber) {
	s.NotContains(s.deployment.Trunks(), line.StreamTrunkID)
	s.Nil(s.deployment.Rule(line.StreamRouteID))
	_, err := s.store.StreamTrunks.Get(s.ctx, line.CustomerID, line.StreamTrunkID)
	s.ErrorIs(err, store.ErrNoStreamTrunk)
}

func (s *AppsSuite) TestABuyerAttachesANumberWhoseLineAMoveLeftWithoutARow() {
	for _, change := range lineChanges {
		s.Run(change, func() {
			e164, _, _, line := s.squatted()
			s.moved(line, change)
			s.moved(line, "deleted")

			_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

			s.Require().NoError(err)
			s.forgotten(line)
			_, err = s.callFrom(e164)
			s.NoError(err, "and calls from it")
		})
	}
}

func (s *AppsSuite) TestABuyerCallsFromANumberWhoseLineAMoveLeftWithoutARow() {
	for _, change := range lineChanges {
		s.Run(change, func() {
			e164, _, _, line := s.squatted()
			s.moved(line, change)

			_, err := s.callFrom(e164)

			s.Require().NoError(err)
			s.forgotten(line)
		})
	}
}

// Copied in full again after the change and before the number was bought, the row is back with no
// line.
func (s *AppsSuite) TestABuyerAttachesANumberWhoseRowAMoveWroteAgain() {
	for _, change := range lineChanges {
		s.Run(change, func() {
			e164, squatter, _, line := s.squatted()
			bought, err := s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, e164)
			s.Require().NoError(err)
			_, err = s.store.GlobalDB().ExecContext(s.ctx, "DELETE FROM phone_numbers WHERE id = ?", bought.ID)
			s.Require().NoError(err)
			var row json.RawMessage
			s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
				"SELECT to_jsonb(pn) FROM phone_numbers AS pn WHERE id = ?", line.ID).Scan(&row))
			s.moved(line, change)
			s.Require().NoError(s.store.ImportRow(s.ctx, squatter, "phone_numbers", row))
			again, err := s.store.PhoneNumbers.GetByE164(s.ctx, squatter, e164)
			s.Require().NoError(err)
			s.Require().Empty(again.StreamTrunkID)
			s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
				E164: e164, Vendor: s.vendor.vendor, Country: "US", CustomerID: s.customer, PurchasedAt: time.Now().UTC(),
			}))

			_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

			s.Require().NoError(err)
			s.forgotten(line)
		})
	}
}

func (s *AppsSuite) TestABuyerTransfersFromANumberWhoseLineAMoveLeftWithoutARow() {
	e164, _, _, line := s.squatted()
	s.moved(line, "deleted")

	_, err := s.service.Transfer(s.ctx, TransferRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: e164, To: "+15550002222", CallID: "call-1",
	})

	s.Require().NoError(err)
	s.forgotten(line)
}

// The buyer's number is lined in the app of its own it had, and its lines are now made where the
// other customer's forgotten one is.
func (s *AppsSuite) TestABuyerAttachesInAnAppItMovedToANumberWhoseLineAMoveLeftThere() {
	e164, _, _, line := s.squatted()
	s.ownApp()
	_, err := s.callFrom(e164)
	s.Require().NoError(err)
	s.moved(line, "deleted")
	s.apps.give(s.customer, 0, s.deployment)

	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Require().NoError(err)
	s.forgotten(line)
}

// The number is now on another customer's trunk, which adding it here allowed: nobody held it.
func (s *AppsSuite) TestACustomerCallsFromATrunkNumberWhoseLineAMoveLeftWithoutARow() {
	e164, squatter, trunk, line := s.squatted()
	s.moved(line, "deleted")
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "DELETE FROM phone_numbers WHERE e164 = ?", e164)
	s.Require().NoError(err)
	second := "customer-" + uuid.NewString()
	s.Require().NotEqual(squatter, second)
	s.Require().NoError(s.addOnATrunk(trunk, second, e164))

	_, err = trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: second}, From: e164, To: "+15550001111"})

	s.Require().NoError(err)
	s.forgotten(line)
}

// Another number of its customer's is still on the trunk, so only this one is taken off it.
func (s *AppsSuite) TestAForgottenLineLeavesTheOtherNumbersOfItsTrunk() {
	e164, squatter, trunk, line := s.squatted()
	other := aNumber()
	s.Require().NoError(s.addOnATrunk(trunk, squatter, other))
	_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: other, To: "+15550001111"})
	s.Require().NoError(err)
	kept, err := s.store.PhoneNumbers.GetByE164(s.ctx, squatter, other)
	s.Require().NoError(err)
	s.Require().Equal(line.StreamTrunkID, kept.StreamTrunkID)
	s.moved(line, "deleted")

	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Require().NoError(err)
	s.Equal([]string{other}, s.deployment.TrunkNumbers(line.StreamTrunkID))
	s.Nil(s.deployment.Rule(line.StreamRouteID))
	s.NotNil(s.deployment.Rule(kept.StreamRouteID))
	before := s.deployment.Trunks()
	_, err = trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: other, To: "+15550001111"})
	s.NoError(err, "it still calls on its line")
	s.Equal(before, s.deployment.Trunks())
}

func (s *AppsSuite) TestAForgottenLineStreamWillNotTakeDownIsFoundAgain() {
	for _, refused := range []string{"rule", "trunk"} {
		s.Run(refused, func() {
			e164, squatter, _, line := s.squatted()
			s.moved(line, "deleted")
			path := "/sip/inbound_trunks/" + line.StreamTrunkID
			if refused == "rule" {
				path = "/sip/inbound_routing_rules/" + line.StreamRouteID
			}
			s.deployment.Fail(http.MethodDelete, path)

			_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

			s.Error(err)
			_, err = s.store.StreamTrunks.Get(s.ctx, squatter, line.StreamTrunkID)
			s.Require().NoError(err)
			s.deployment.Heal(http.MethodDelete, path)
			_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})
			s.Require().NoError(err)
			s.forgotten(line)
		})
	}
}

func (s *AppsSuite) TestAForgottenLineOnATrunkStreamWillNotTakeTheNumberOffFailsTheAttach() {
	e164, squatter, trunk, line := s.squatted()
	other := aNumber()
	s.Require().NoError(s.addOnATrunk(trunk, squatter, other))
	_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: other, To: "+15550001111"})
	s.Require().NoError(err)
	s.moved(line, "deleted")
	s.deployment.Fail(http.MethodPut, "/sip/inbound_trunks/"+line.StreamTrunkID)

	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.ErrorContains(err, "update sip trunk")
	s.Contains(s.deployment.TrunkNumbers(line.StreamTrunkID), other)
}

// A trunk no row here names was made by hand or by another router on the app, and a rule of more
// than the number and its trunk was not made by this one.
func (s *AppsSuite) TestAForgottenLineLeavesWhatTheRouterDidNotMake() {
	e164, squatter, trunk, line := s.squatted()
	other := aNumber()
	s.Require().NoError(s.addOnATrunk(trunk, squatter, other))
	_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: other, To: "+15550001111"})
	s.Require().NoError(err)
	s.moved(line, "deleted")
	byHand, rules := s.madeByHand(line)

	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Require().NoError(err)
	s.Nil(s.deployment.Rule(line.StreamRouteID))
	s.Equal([]string{other}, s.deployment.TrunkNumbers(line.StreamTrunkID))
	s.Contains(s.deployment.Trunks(), byHand)
	for _, rule := range rules {
		s.NotNil(s.deployment.Rule(rule))
	}
}

// Stream keeps the trunk, with its number, for as long as a rule made by hand names it.
func (s *AppsSuite) TestAForgottenTrunkARuleMadeByHandNamesIsKeptUntilTheRuleIsGone() {
	e164, squatter, _, line := s.squatted()
	s.moved(line, "deleted")
	byHand, rules := s.madeByHand(line)

	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.ErrorContains(err, "delete sip trunk")
	_, err = s.store.StreamTrunks.Get(s.ctx, squatter, line.StreamTrunkID)
	s.Require().NoError(err)
	stream := NewStreamFromClient(s.deployment.Client)
	for _, rule := range rules {
		s.Require().NotNil(s.deployment.Rule(rule))
		s.Require().NoError(stream.DeleteRoute(s.ctx, rule))
	}
	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})
	s.Require().NoError(err)
	s.forgotten(line)
	s.Contains(s.deployment.Trunks(), byHand)
}

// madeByHand makes a trunk at Stream, and rules that name the line's number and trunk without
// being the line's own.
func (s *AppsSuite) madeByHand(line store.PhoneNumber) (string, []string) {
	stream := NewStreamFromClient(s.deployment.Client)
	byHand, _, err := stream.CreateTrunk(s.ctx, Trunk{Name: "by hand", Numbers: []string{aNumber()}})
	s.Require().NoError(err)
	var rules []string
	for _, route := range []Route{
		{Name: "two numbers", TrunkIDs: []string{line.StreamTrunkID}, CalledNumbers: []string{line.E164, aNumber()}},
		{Name: "two trunks", TrunkIDs: []string{line.StreamTrunkID, byHand}, CalledNumbers: []string{line.E164}},
		{Name: "another trunk", TrunkIDs: []string{byHand}, CalledNumbers: []string{line.E164}},
	} {
		rule, err := stream.CreateRoute(s.ctx, route)
		s.Require().NoError(err)
		rules = append(rules, rule)
	}
	return byHand, rules
}

// readded is a number whose row a move deleted, added again by the same customer and called from:
// its trunk has the rule of the line before beside its own.
func (s *AppsSuite) readded() (string, string, trunkCall, store.PhoneNumber) {
	s.sealed()
	s.deployment.OneTrunkPerNumber()
	e164, customer := aNumber(), "customer-"+uuid.NewString()
	trunk := s.onATrunk(customer, e164, StreamLegShared)
	call := CallRequest{Owner: routing.Owner{CustomerID: customer}, From: e164, To: "+15550001111"}
	_, err := trunk.service.Call(s.ctx, call)
	s.Require().NoError(err)
	line, err := s.store.PhoneNumbers.GetByE164(s.ctx, customer, e164)
	s.Require().NoError(err)
	s.moved(line, "deleted")
	s.Require().NoError(s.addOnATrunk(trunk, customer, e164))
	_, err = trunk.service.Call(s.ctx, call)
	s.Require().NoError(err)
	return e164, customer, trunk, line
}

func (s *AppsSuite) TestANumberAddedAgainAfterAMoveIsGivenBack() {
	e164, customer, trunk, line := s.readded()

	s.Require().NoError(trunk.service.Release(s.ctx, customer, e164))

	s.forgotten(line)
	s.Empty(s.deployment.Trunks())
	s.Empty(s.deployment.Rules())
}

// Whatever keeps its trunk at Stream, the number stays held, and giving it back again works.
func (s *AppsSuite) TestANumberWhoseTrunkStreamKeepsIsGivenBackLater() {
	for failing, says := range map[string]string{
		"the list of rules":  "list sip routing rules",
		"the rule left":      "delete sip routing rule",
		"the trunk, for now": "delete sip trunk",
	} {
		s.Run(failing, func() {
			e164, customer, trunk, line := s.readded()
			method, path, status := http.MethodDelete, "/sip/inbound_routing_rules/"+line.StreamRouteID, http.StatusInternalServerError
			switch failing {
			case "the list of rules":
				method, path = http.MethodGet, "/sip/inbound_routing_rules"
			case "the trunk, for now":
				path, status = "/sip/inbound_trunks/"+line.StreamTrunkID, http.StatusTooManyRequests
			}
			s.deployment.FailWith(method, path, status)
			before := s.lists()

			s.ErrorContains(trunk.service.Release(s.ctx, customer, e164), says)

			held, err := s.store.PhoneNumbers.GetByE164(s.ctx, customer, e164)
			s.Require().NoError(err)
			s.Equal(line.StreamTrunkID, held.StreamTrunkID)
			if status == http.StatusTooManyRequests {
				s.Equal(before, s.lists(), "not refused for a rule")
			}
			s.deployment.Heal(method, path)
			s.Require().NoError(trunk.service.Release(s.ctx, customer, e164))
			s.forgotten(line)
		})
	}
}

// The trunk keeps another number, so nothing but its rule stands in the way.
func (s *AppsSuite) TestAForgottenLineWhoseRuleStreamKeepsOnASharedTrunkFailsTheAttach() {
	e164, squatter, trunk, line := s.squatted()
	other := aNumber()
	s.Require().NoError(s.addOnATrunk(trunk, squatter, other))
	_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: other, To: "+15550001111"})
	s.Require().NoError(err)
	s.moved(line, "deleted")
	path := "/sip/inbound_routing_rules/" + line.StreamRouteID
	s.deployment.Fail(http.MethodDelete, path)

	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.ErrorContains(err, "delete sip routing rule")
	s.deployment.Heal(http.MethodDelete, path)
	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})
	s.Require().NoError(err)
	s.Nil(s.deployment.Rule(line.StreamRouteID))
}

func (s *AppsSuite) TestANumberAddedAgainAfterAMoveIsAttachedByItsBuyer() {
	e164, _, _, line := s.readded()
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: e164, Vendor: s.vendor.vendor, Country: "US", CustomerID: s.customer, PurchasedAt: time.Now().UTC(),
	}))

	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Require().NoError(err)
	s.forgotten(line)
	_, err = s.callFrom(e164)
	s.NoError(err)
}

// The move deleted one number of a trunk, whose rule keeps the trunk when its other number goes.
func (s *AppsSuite) TestATrunkGoesWithItsLastNumberPastARuleAMoveLeft() {
	for _, then := range []string{"given back", "bought"} {
		s.Run(then, func() {
			e164, squatter, trunk, line := s.squatted()
			other := aNumber()
			s.Require().NoError(s.addOnATrunk(trunk, squatter, other))
			_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: other, To: "+15550001111"})
			s.Require().NoError(err)
			s.moved(line, "deleted")

			if then == "given back" {
				s.Require().NoError(trunk.service.Release(s.ctx, squatter, other))
			} else {
				s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
					E164: other, Vendor: s.vendor.vendor, Country: "US", CustomerID: s.customer, PurchasedAt: time.Now().UTC(),
				}))
				_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: other})
				s.Require().NoError(err)
			}

			s.forgotten(line)
			_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})
			s.NoError(err)
		})
	}
}

// Two numbers' rows name a trunk with no row, so what else is on it is not known here: only the
// rules of the number taken off it go, and Stream keeps the trunk for the other.
func (s *AppsSuite) TestATrunkWithNoRowKeepsTheRuleOfItsOtherNumber() {
	for _, then := range []string{"given back", "bought"} {
		s.Run(then, func() {
			e164, squatter, trunk, line := s.squatted()
			other := aNumber()
			s.Require().NoError(s.addOnATrunk(trunk, squatter, other))
			_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: other, To: "+15550001111"})
			s.Require().NoError(err)
			kept, err := s.store.PhoneNumbers.GetByE164(s.ctx, squatter, other)
			s.Require().NoError(err)
			_, err = s.store.GlobalDB().ExecContext(s.ctx, "DELETE FROM stream_trunks WHERE id = ?", line.StreamTrunkID)
			s.Require().NoError(err)

			if then == "given back" {
				s.NoError(trunk.service.Release(s.ctx, squatter, e164))
			} else {
				_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})
				s.Error(err)
			}

			s.NotNil(s.deployment.Rule(kept.StreamRouteID))
			s.Contains(s.deployment.TrunkNumbers(line.StreamTrunkID), other)
		})
	}
}

// The number has a second rule on the trunk with no row.
func (s *AppsSuite) TestATrunkWithNoRowGoesWithEveryRuleOfItsNumber() {
	for _, then := range []string{"given back", "bought"} {
		s.Run(then, func() {
			e164, squatter, trunk, line := s.squatted()
			_, err := NewStreamFromClient(s.deployment.Client).CreateRoute(s.ctx, Route{
				Name: "again", TrunkIDs: []string{line.StreamTrunkID}, CalledNumbers: []string{e164},
			})
			s.Require().NoError(err)
			_, err = s.store.GlobalDB().ExecContext(s.ctx, "DELETE FROM stream_trunks WHERE id = ?", line.StreamTrunkID)
			s.Require().NoError(err)

			if then == "given back" {
				err = trunk.service.Release(s.ctx, squatter, e164)
			} else {
				_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})
			}

			s.Require().NoError(err)
			s.NotContains(s.deployment.Trunks(), line.StreamTrunkID)
		})
	}
}

// A trunk made for one call has another rule beside the call's.
func (s *AppsSuite) TestACallsTrunkGoesWithEveryRuleOnIt() {
	for _, how := range []string{"ended", "swept"} {
		s.Run(how, func() {
			s.sealed()
			s.vendor.leg = StreamLegPerCall
			number := s.anotherNumber(s.customer, s.vendor.vendor)
			before := s.deployment.Trunks()
			placed, err := s.callFrom(number)
			s.Require().NoError(err)
			made := slices.DeleteFunc(s.deployment.Trunks(), func(id string) bool { return slices.Contains(before, id) })
			s.Require().Len(made, 1)
			_, err = NewStreamFromClient(s.deployment.Client).CreateRoute(s.ctx, Route{
				Name: "again", TrunkIDs: made, CalledNumbers: []string{number},
			})
			s.Require().NoError(err)

			if how == "ended" {
				s.Require().NoError(s.service.ReleaseCall(s.ctx, store.AppScope{Unpinned: true}, defaultCallType, placed.CallID))
			} else {
				_, err = s.service.SweepCallResources(s.ctx, time.Now().Add(time.Hour))
				s.Require().NoError(err)
			}

			s.NotContains(s.deployment.Trunks(), made[0])
		})
	}
}

// The trunk's row has moved to another group, its pin filled in, since the join that failed: the
// numbers on it are not this group's to know, and keep their rules.
func (s *AppsSuite) TestAFailedLineLeavesTheRulesOfATrunkWhoseRowMoved() {
	_, squatter, trunk, line := s.squatted()
	other, joiner := aNumber(), aNumber()
	s.Require().NoError(s.addOnATrunk(trunk, squatter, other))
	_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: other, To: "+15550001111"})
	s.Require().NoError(err)
	second, err := s.store.PhoneNumbers.GetByE164(s.ctx, squatter, other)
	s.Require().NoError(err)
	_, err = s.store.GlobalDB().ExecContext(s.ctx, "UPDATE stream_trunks SET stream_app_pk = 777 WHERE id = ?", line.StreamTrunkID)
	s.Require().NoError(err)

	trunk.service.leave(s.ctx, NewStreamFromClient(s.deployment.Client), store.TrunkGroup{CustomerID: squatter, Vendor: SIPTrunkVendor},
		&joining{e164: joiner, trunkID: line.StreamTrunkID})

	s.NotNil(s.deployment.Rule(line.StreamRouteID))
	s.NotNil(s.deployment.Rule(second.StreamRouteID))
	s.ElementsMatch([]string{line.E164, other}, s.deployment.TrunkNumbers(line.StreamTrunkID))
}

// The row moves while the number joins its trunk, which fails the line.
func (s *AppsSuite) TestALineWhoseTrunksRowMovedMeanwhileLeavesItsOtherNumbersRules() {
	_, squatter, trunk, line := s.squatted()
	other := aNumber()
	s.Require().NoError(s.addOnATrunk(trunk, squatter, other))
	waiting, release := s.deployment.Hold("/sip/inbound_routing_rules")
	failed := make(chan error, 1)
	go func() {
		_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: other, To: "+15550001111"})
		failed <- err
	}()
	<-waiting
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE stream_trunks SET stream_app_pk = 777 WHERE id = ?", line.StreamTrunkID)
	release()
	s.Require().NoError(err)

	s.Require().ErrorContains(<-failed, "another group's")

	s.NotNil(s.deployment.Rule(line.StreamRouteID))
	s.Contains(s.deployment.TrunkNumbers(line.StreamTrunkID), line.E164)
	rules, err := NewStreamFromClient(s.deployment.Client).rulesOn(s.ctx, line.StreamTrunkID)
	s.Require().NoError(err)
	s.Equal(map[string]string{line.StreamRouteID: line.E164}, rules, "its own went")
}

// interrupted is a vendor that runs something, then refuses, when it is told of a number's trunk.
type interrupted struct {
	*stub
	during func()
}

func (v interrupted) ConfigureInbound(context.Context, Inbound) error {
	v.during()
	return errors.New("refused")
}

func (v interrupted) Dial(context.Context, Outbound) (Dialed, error) {
	v.during()
	return Dialed{}, errors.New("refused")
}

// Stream does not delete the number's rule at first, which keeps the trunk until it does. Another
// number's rule on the trunk is not the undone one's to delete.
func (s *AppsSuite) TestAnUndoneTrunkOfItsOwnGoesWithItsNumbersRuleOnly() {
	for _, what := range []string{"attach", "call", "transfer"} {
		for _, shared := range []bool{false, true} {
			s.Run(fmt.Sprintf("%s, another number's rule there: %t", what, shared), func() {
				s.service.sealer = nil
				before := s.deployment.Trunks()
				number, other := s.anotherNumber(s.customer, s.vendor.vendor), ""
				var made, rules []string
				vendor := interrupted{stub: s.vendor, during: func() {
					made = slices.DeleteFunc(s.deployment.Trunks(), func(id string) bool { return slices.Contains(before, id) })
					s.Require().Len(made, 1)
					rules = s.deployment.Rules()
					if shared {
						rule, err := NewStreamFromClient(s.deployment.Client).CreateRoute(s.ctx, Route{
							Name: "another", TrunkIDs: made, CalledNumbers: []string{aNumber()},
						})
						s.Require().NoError(err)
						other = rule
					}
					s.deployment.FailNextRuleDelete()
				}}
				s.registry.Register(s.vendor.vendor, func() (Provider, error) { return vendor, nil })

				var err error
				switch what {
				case "attach":
					_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: number})
				case "call":
					_, err = s.callFrom(number)
				case "transfer":
					_, err = s.service.Transfer(s.ctx, TransferRequest{
						Owner: routing.Owner{CustomerID: s.customer}, From: number, To: "+15550002222", CallID: "call-1", CallType: "support",
					})
				}

				s.Require().Error(err)
				s.Require().NotEmpty(rules, "the trunk had a rule")
				left := s.deployment.Rules()
				if shared {
					s.Equal([]string{other}, slices.DeleteFunc(left, func(id string) bool { return id != other }), "another number's rule stays")
					s.Contains(s.deployment.Trunks(), made[0])
				} else {
					s.NotContains(s.deployment.Trunks(), made[0])
				}
				numbers, err := NewStreamFromClient(s.deployment.Client).rulesOn(s.ctx, made[0])
				s.Require().NoError(err)
				s.NotContains(slices.Collect(maps.Values(numbers)), number, "the number's own rule is gone")
			})
		}
	}
}

// The trunk takes calls from some addresses only, which taking the number off it keeps.
func (s *AppsSuite) TestAForgottenLineLeavesItsTrunksAddresses() {
	e164, squatter, trunk, line := s.squatted()
	other := aNumber()
	s.Require().NoError(s.addOnATrunk(trunk, squatter, other))
	_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: other, To: "+15550001111"})
	s.Require().NoError(err)
	addresses := []string{"192.0.2.10", "198.51.100.0/24"}
	s.Require().NoError(NewStreamFromClient(s.deployment.Client).UpdateTrunk(s.ctx, line.StreamTrunkID, Trunk{
		Name: "phone-sip_trunk-" + squatter, Numbers: []string{e164, other}, AllowedIPs: addresses,
	}))
	s.moved(line, "deleted")

	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Require().NoError(err)
	s.Equal([]string{other}, s.deployment.TrunkNumbers(line.StreamTrunkID))
	_, allowed := s.deployment.TrunkAuth(line.StreamTrunkID)
	s.Equal(addresses, allowed)
}

// Stream no longer has the trunk by the time its numbers are written again, so the number is on
// it no more.
func (s *AppsSuite) TestAForgottenLineOnATrunkStreamLostMeanwhileIsCleared() {
	e164, squatter, trunk, line := s.squatted()
	other := aNumber()
	s.Require().NoError(s.addOnATrunk(trunk, squatter, other))
	_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: other, To: "+15550001111"})
	s.Require().NoError(err)
	s.moved(line, "deleted")
	row, err := s.store.StreamTrunks.Get(s.ctx, squatter, line.StreamTrunkID)
	s.Require().NoError(err)
	s.goneAtStream(line.StreamTrunkID)

	cleared, err := s.service.clearForgotten(s.ctx, NewStreamFromClient(s.deployment.Client), row, e164)

	s.Require().NoError(err)
	s.True(cleared)
}

// The rule a failed line made would not go at first.
func (s *AppsSuite) TestAnUndoneTrunkGoesWithItsRule() {
	s.sealed()
	_, err := s.callFrom(s.e164)
	s.Require().NoError(err)
	held := s.number()
	_, err = s.store.GlobalDB().ExecContext(s.ctx, "DELETE FROM stream_trunks WHERE id = ?", held.StreamTrunkID)
	s.Require().NoError(err)
	group := store.TrunkGroup{CustomerID: s.customer, Vendor: held.Vendor}

	s.service.leave(s.ctx, NewStreamFromClient(s.deployment.Client), group, &joining{e164: s.e164, trunkID: held.StreamTrunkID})

	s.Empty(s.deployment.Trunks())
}

// A number is forgotten on one trunk and still held on another: it is taken off the first, and put
// once more; held on its only trunk, it is not.
func (s *AppsSuite) TestANumberStillHeldIsPutOnceMoreOnlyWhenALineWasCleared() {
	s.sealed()
	s.deployment.OneTrunkPerNumber()
	e164, filler := aNumber(), aNumber()
	var lines []store.PhoneNumber
	var dials trunkCall
	for _, number := range []string{e164, filler} {
		customer := "customer-" + uuid.NewString()
		dials = s.onATrunk(customer, number, StreamLegShared)
		_, err := dials.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: customer}, From: number, To: "+15550001111"})
		s.Require().NoError(err)
		line, err := s.store.PhoneNumbers.GetByE164(s.ctx, customer, number)
		s.Require().NoError(err)
		lines = append(lines, line)
	}
	puts := func() int {
		count := 0
		for _, request := range s.deployment.Requests(s.deployment.Key) {
			if request.Method == http.MethodPost && strings.HasSuffix(request.Path, "/sip/inbound_trunks") {
				count++
			}
		}
		return count
	}
	_, pin, err := s.service.streamFor(s.ctx, s.customer)
	s.Require().NoError(err)
	stream := NewStreamFromClient(s.deployment.Client)
	put := func() error {
		_, _, err := s.service.createTrunk(s.ctx, stream, pin, Trunk{Name: "put", Numbers: []string{e164}})
		return err
	}

	before := puts()
	s.ErrorContains(put(), "numbers already used")
	s.Equal(1, puts()-before, "held on its only trunk")

	// The trunks are read in the order of their ids: the first is the forgotten one.
	slices.SortFunc(lines, func(a, b store.PhoneNumber) int { return strings.Compare(a.StreamTrunkID, b.StreamTrunkID) })
	forgotten, held := lines[0], lines[1]
	for _, line := range lines {
		s.Require().NoError(s.deployment.PutOnTrunk(line.StreamTrunkID, e164))
	}
	_, err = s.store.GlobalDB().ExecContext(s.ctx, "DELETE FROM phone_numbers WHERE id = ?", forgotten.ID)
	s.Require().NoError(err)
	_, err = s.store.GlobalDB().ExecContext(s.ctx, "UPDATE phone_numbers SET e164 = ? WHERE id = ?", e164, held.ID)
	s.Require().NoError(err)
	before = puts()
	s.ErrorContains(put(), "numbers already used")
	s.Equal(2, puts()-before)
	s.NotContains(s.deployment.Trunks(), forgotten.StreamTrunkID)
	s.NotNil(s.deployment.Rule(held.StreamRouteID))
	s.Contains(s.deployment.TrunkNumbers(held.StreamTrunkID), e164)
}

// Another number's forgotten line is for whoever needs that number.
func (s *AppsSuite) TestOnlyTheTrunksListingANumberAreCleared() {
	e164, _, _, line := s.squatted()
	_, _, _, another := s.squatted()
	s.moved(line, "deleted")
	s.moved(another, "deleted")

	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Require().NoError(err)
	s.forgotten(line)
	s.Contains(s.deployment.TrunkNumbers(another.StreamTrunkID), another.E164)
	s.NotNil(s.deployment.Rule(another.StreamRouteID))
}

// The trunk's row went since it was read.
func (s *AppsSuite) TestATrunkWhoseRowIsGoneIsLeftAtStream() {
	e164, squatter, _, line := s.squatted()
	trunk, err := s.store.StreamTrunks.Get(s.ctx, squatter, line.StreamTrunkID)
	s.Require().NoError(err)
	_, err = s.store.GlobalDB().ExecContext(s.ctx, "DELETE FROM stream_trunks WHERE id = ?", trunk.ID)
	s.Require().NoError(err)

	cleared, err := s.service.clearForgotten(s.ctx, NewStreamFromClient(s.deployment.Client), trunk, e164)

	s.Require().NoError(err)
	s.False(cleared)
	s.Contains(s.deployment.Trunks(), trunk.ID)
	s.NotNil(s.deployment.Rule(line.StreamRouteID))
}

func (s *AppsSuite) TestANumberOnATrunkMadeByHandIsLeftThere() {
	s.squatted()
	e164 := s.anotherNumber(s.customer, s.vendor.vendor)
	byHand, _, err := NewStreamFromClient(s.deployment.Client).CreateTrunk(s.ctx, Trunk{Name: "by hand", Numbers: []string{e164}})
	s.Require().NoError(err)

	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.ErrorContains(err, "numbers already used by another SIP trunk")
	s.Equal([]string{e164}, s.deployment.TrunkNumbers(byHand))
}

// lists is how often the deployment's app was asked for its trunks or rules.
func (s *AppsSuite) lists() int {
	count := 0
	for _, request := range s.deployment.Requests(s.deployment.Key) {
		if request.Method == http.MethodGet && strings.Contains(request.Path, "/sip/") {
			count++
		}
	}
	return count
}

func (s *AppsSuite) TestANumberStreamTakesAsksForNoLists() {
	s.squatted()
	for _, leg := range []StreamLeg{StreamLegShared, StreamLegPerCall} {
		s.vendor.leg = leg
		before := s.lists()
		_, err := s.callFrom(s.anotherNumber(s.customer, s.vendor.vendor))
		s.Require().NoError(err)
		s.Equal(before, s.lists())
	}
}

// A refusal nothing here explains is answered as it was, whether or not the lists can be read.
func (s *AppsSuite) TestANumberStreamRefusesIsRefusedWhenItsListsFail() {
	for _, path := range []string{"/sip/inbound_trunks", "none"} {
		s.Run(path, func() {
			_, _, _, line := s.squatted()
			e164 := s.anotherNumber(s.customer, s.vendor.vendor)
			byHand, _, err := NewStreamFromClient(s.deployment.Client).CreateTrunk(s.ctx, Trunk{Name: "by hand", Numbers: []string{e164}})
			s.Require().NoError(err)
			s.deployment.FailWith(http.MethodGet, path, http.StatusTooManyRequests)

			_, err = s.callFrom(e164)

			s.deployment.Heal(http.MethodGet, path)
			s.ErrorContains(err, "numbers already used by another SIP trunk")
			s.Equal([]string{e164}, s.deployment.TrunkNumbers(byHand))
			s.Contains(s.deployment.Trunks(), line.StreamTrunkID)
		})
	}
}

func (s *AppsSuite) TestANumberRefusedInAnAppWithNoTrunkLinesAsksForNoLists() {
	s.sealed()
	s.ownApp()
	s.own.OneTrunkPerNumber()
	_, _, err := NewStreamFromClient(s.own.Client).CreateTrunk(s.ctx, Trunk{Name: "by hand", Numbers: []string{s.e164}})
	s.Require().NoError(err)
	_, err = s.store.GlobalDB().ExecContext(s.ctx, "DELETE FROM stream_trunks WHERE stream_app_pk = 4242")
	s.Require().NoError(err)

	_, err = s.callFrom(s.e164)

	s.ErrorContains(err, "numbers already used by another SIP trunk")
	made := 0
	for _, request := range s.own.Requests(s.own.Key) {
		s.False(request.Method == http.MethodGet && strings.Contains(request.Path, "/sip/"), request.Path)
		if request.Method == http.MethodPost && strings.HasSuffix(request.Path, "/sip/inbound_trunks") {
			made++
		}
	}
	s.Equal(2, made, "the one by hand, and the call's, which is not tried again")
}

// Another customer's row still holds the number on the trunk that has it, as one written before a
// number was held by one customer at a time does.
func (s *AppsSuite) TestANumberStreamRefusesLeavesALineStillHeld() {
	e164, squatter, trunk, line := s.squatted()
	second := "customer-" + uuid.NewString()
	bought, err := s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, e164)
	s.Require().NoError(err)
	_, err = s.store.GlobalDB().ExecContext(s.ctx, "DELETE FROM phone_numbers WHERE id = ?", bought.ID)
	s.Require().NoError(err)
	password := trunk.password
	theirs, err := trunk.service.CreateSIPTrunk(s.ctx, second, SIPTrunkSettings{
		Name: "main", Host: "trunk.example.com", Username: "trunk-user", Password: &password,
	})
	s.Require().NoError(err)
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: e164, Vendor: SIPTrunkVendor, Country: "US", CustomerID: second, SIPTrunkID: theirs.ID, PurchasedAt: time.Now().UTC(),
	}))

	_, err = trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: second}, From: e164, To: "+15550001111"})

	s.ErrorContains(err, "numbers already used by another SIP trunk")
	s.leftAsItWas(squatter, line, s.deployment)
	s.NotNil(s.deployment.Rule(line.StreamRouteID))
}

func (s *AppsSuite) TestAFailureOtherThanARefusalAsksForNoLists() {
	e164, _, _, line := s.squatted()
	s.moved(line, "deleted")
	s.deployment.FailWith(http.MethodPost, "/sip/inbound_trunks", http.StatusTooManyRequests)
	before := s.lists()

	_, err := s.callFrom(e164)

	s.Error(err)
	s.Equal(before, s.lists())
}

func (s *AppsSuite) TestAForgottenLineWhoseRulesStreamWillNotListFailsTheCall() {
	e164, _, _, line := s.squatted()
	s.moved(line, "deleted")
	s.deployment.Fail(http.MethodGet, "/sip/inbound_routing_rules")

	_, err := s.callFrom(e164)

	s.ErrorContains(err, "list sip routing rules")
	s.Contains(s.deployment.TrunkNumbers(line.StreamTrunkID), e164)
}

// A trunk of the number's own, rather than a shared line, is made past the line too.
func (s *AppsSuite) TestANumberGetsATrunkOfItsOwnPastAForgottenLine() {
	makes := map[string]func(e164 string) error{
		"call": func(e164 string) error {
			s.vendor.leg = StreamLegPerCall
			_, err := s.callFrom(e164)
			return err
		},
		"transfer": func(e164 string) error {
			_, err := s.service.Transfer(s.ctx, TransferRequest{
				Owner: routing.Owner{CustomerID: s.customer}, From: e164, To: "+15550002222", CallID: "call-1", CallType: "support",
			})
			return err
		},
		"attach": func(e164 string) error {
			s.service.sealer = nil
			_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})
			return err
		},
	}
	for what, make := range makes {
		s.Run(what, func() {
			s.vendor.leg = StreamLegShared
			e164, _, _, line := s.squatted()
			s.moved(line, "deleted")

			s.Require().NoError(make(e164))

			s.forgotten(line)
		})
	}
}

// boughtPastAMove gives the suite's customer a number another customer lined in the app with the pin, whose
// row a move then deleted.
func (s *AppsSuite) boughtPastAMove(pin int64) (string, store.PhoneNumber) {
	s.sealed()
	s.own.OneTrunkPerNumber()
	e164, other := aNumber(), "customer-"+uuid.NewString()
	s.apps.give(other, pin, s.own)
	s.apps.give(s.customer, pin, s.own)
	trunk := s.onATrunk(other, e164, StreamLegShared)
	_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: other}, From: e164, To: "+15550001111"})
	s.Require().NoError(err)
	line, err := s.store.PhoneNumbers.GetByE164(s.ctx, other, e164)
	s.Require().NoError(err)
	s.Require().Equal(pin, line.StreamAppPK)
	s.moved(line, "deleted")
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: e164, Vendor: s.vendor.vendor, Country: "US", CustomerID: s.customer, PurchasedAt: time.Now().UTC(),
	}))
	return e164, line
}

func (s *AppsSuite) TestAForgottenLineInACustomersOwnAppIsTakenDownThere() {
	e164, line := s.boughtPastAMove(4242)

	_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Require().NoError(err)
	s.NotContains(s.own.Trunks(), line.StreamTrunkID)
	s.Nil(s.own.Rule(line.StreamRouteID))
	_, err = s.store.StreamTrunks.Get(s.ctx, line.CustomerID, line.StreamTrunkID)
	s.ErrorIs(err, store.ErrNoStreamTrunk)
}

func (s *AppsSuite) TestANumberInACustomersOwnAppGetsATrunkOfItsOwnPastAForgottenLine() {
	for _, what := range []string{"transfer", "call", "attach"} {
		s.Run(what, func() {
			e164, line := s.boughtPastAMove(4242)
			s.vendor.leg = StreamLegShared

			var err error
			switch what {
			case "transfer":
				_, err = s.service.Transfer(s.ctx, TransferRequest{
					Owner: routing.Owner{CustomerID: s.customer}, From: e164, To: "+15550002222", CallID: "call-1", CallType: "support", StreamApp: 4242,
				})
			case "call":
				s.vendor.leg = StreamLegPerCall
				_, err = s.callFrom(e164)
			case "attach":
				s.service.sealer = nil
				_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})
			}

			s.Require().NoError(err)
			s.NotContains(s.own.Trunks(), line.StreamTrunkID)
		})
	}
}

// Two numbers of a trunk lost their rows: the first one needed deletes the trunk with both their
// rules, and a rule of the number made twice goes twice.
func (s *AppsSuite) TestAForgottenTrunkIsDeletedWithTheRulesOfItsNumbers() {
	e164, squatter, trunk, line := s.squatted()
	other := aNumber()
	s.Require().NoError(s.addOnATrunk(trunk, squatter, other))
	_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: other, To: "+15550001111"})
	s.Require().NoError(err)
	second, err := s.store.PhoneNumbers.GetByE164(s.ctx, squatter, other)
	s.Require().NoError(err)
	again, err := NewStreamFromClient(s.deployment.Client).CreateRoute(s.ctx, Route{
		Name: "again", TrunkIDs: []string{line.StreamTrunkID}, CalledNumbers: []string{e164},
	})
	s.Require().NoError(err)
	s.moved(line, "deleted")
	s.moved(second, "deleted")

	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Require().NoError(err)
	s.forgotten(line)
	s.Nil(s.deployment.Rule(second.StreamRouteID))
	s.Nil(s.deployment.Rule(again))
}

// Where an app lets a number be on two trunks, every one it was forgotten on is cleared, the first
// the app lists as well as the last.
func (s *AppsSuite) TestANumberForgottenOnTwoTrunksIsTakenOffBoth() {
	e164, _, trunk, line := s.squatted()
	s.moved(line, "deleted")
	second, other := "customer-"+uuid.NewString(), aNumber()
	s.Require().NoError(s.addOnATrunk(trunk, second, other))
	_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: second}, From: other, To: "+15550001111"})
	s.Require().NoError(err)
	kept, err := s.store.PhoneNumbers.GetByE164(s.ctx, second, other)
	s.Require().NoError(err)
	s.NoError(s.deployment.PutOnTrunk(kept.StreamTrunkID, other, e164))

	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Require().NoError(err)
	s.forgotten(line)
	s.Equal([]string{other}, s.deployment.TrunkNumbers(kept.StreamTrunkID))
}

// The trunk lists another number before the forgotten one.
func (s *AppsSuite) TestAForgottenLineIsFoundWhereverItsTrunkListsIt() {
	e164, squatter, trunk, line := s.squatted()
	other := aNumber()
	s.Require().NoError(s.addOnATrunk(trunk, squatter, other))
	_, err := trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: other, To: "+15550001111"})
	s.Require().NoError(err)
	s.NoError(s.deployment.PutOnTrunk(line.StreamTrunkID, other, e164))
	s.moved(line, "deleted")

	_, err = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.Require().NoError(err)
	s.Equal([]string{other}, s.deployment.TrunkNumbers(line.StreamTrunkID))
	s.Equal("phone-sip_trunk-"+squatter, s.deployment.TrunkName(line.StreamTrunkID))
}

// However a replayed delete, another call from the trunk and the buyer's attach fall, a later
// attach works and Stream lists the number once.
func (s *AppsSuite) TestABuyerAttachesWhileAMoveDeletesTheNumbersRow() {
	for range 5 {
		e164, squatter, trunk, line := s.squatted()
		deletion := s.lineChange(line, "deleted")
		var applied error
		var running sync.WaitGroup
		running.Go(func() { applied = s.store.ApplyChanges(s.ctx, squatter, []store.DataChange{deletion}) })
		running.Go(func() {
			_, _ = trunk.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: squatter}, From: e164, To: "+15550001111"})
		})
		for range 2 {
			running.Go(func() { _, _ = s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164}) })
		}
		running.Wait()
		s.Require().NoError(applied)

		_, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

		s.Require().NoError(err)
		listing := 0
		for _, id := range s.deployment.Trunks() {
			if slices.Contains(s.deployment.TrunkNumbers(id), e164) {
				listing++
			}
		}
		s.Equal(1, listing)
	}
}

func (s *AppsSuite) TestANumberACustomerInTheSameAppBoughtCannotBeAddedToATrunk() {
	err := s.addOnATrunk(s.dialsTrunks(StreamLegShared), "customer-"+uuid.NewString(), s.e164)

	s.ErrorIs(err, store.ErrNumberHeldElsewhere)
}

func (s *AppsSuite) TestANumberOnTheTrunkOfACustomerInTheSameAppCannotBeAddedToAnotherTrunk() {
	e164 := aNumber()
	trunks := s.onATrunk("customer-"+uuid.NewString(), e164, StreamLegShared)

	err := s.addOnATrunk(trunks, "customer-"+uuid.NewString(), e164)

	s.ErrorIs(err, store.ErrNumberHeldElsewhere)
}

// Stream lists a number on one trunk per app, and these are two apps.
func (s *AppsSuite) TestTwoCustomersInAppsOfTheirOwnCanAddOneNumberToTheirTrunks() {
	e164, first, second := aNumber(), "customer-"+uuid.NewString(), "customer-"+uuid.NewString()
	s.apps.give(first, 5151, s.own)
	s.apps.give(second, 5252, chattest.NewServer(s.T()))
	trunks := s.dialsTrunks(StreamLegShared)

	s.Require().NoError(s.addOnATrunk(trunks, first, e164))
	s.Require().NoError(s.addOnATrunk(trunks, second, e164))

	for _, customer := range []string{first, second} {
		held, err := s.store.PhoneNumbers.GetByE164(s.ctx, customer, e164)
		s.Require().NoError(err)
		s.Equal(SIPTrunkVendor, held.Vendor)
	}
}

func (s *AppsSuite) TestANumberBoughtInAnotherAppCanBeAddedToATrunkAndCalledFrom() {
	s.ownApp()
	theirs := "customer-" + uuid.NewString()
	trunks := s.dialsTrunks(StreamLegShared)

	s.Require().NoError(s.addOnATrunk(trunks, theirs, s.e164))

	_, err := trunks.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: theirs}, From: s.e164, To: "+15550001111"})
	s.NoError(err)
}

// The holder's app is disconnected, so where its lines are made cannot be read.
func (s *AppsSuite) TestAHolderWhoseAppCannotBeReadIsTakenToShareTheApp() {
	s.apps.block(s.customer)
	theirs := "customer-" + uuid.NewString()
	s.apps.give(theirs, 5353, s.own)

	err := s.addOnATrunk(s.dialsTrunks(StreamLegShared), theirs, s.e164)

	s.ErrorIs(err, store.ErrNumberHeldElsewhere)
}

func (s *AppsSuite) TestACustomerWhoseAppCannotBeReadIsTakenToShareTheHoldersApp() {
	s.ownApp()
	theirs := "customer-" + uuid.NewString()
	s.apps.block(theirs)

	err := s.addOnATrunk(s.dialsTrunks(StreamLegShared), theirs, s.e164)

	s.ErrorIs(err, store.ErrNumberHeldElsewhere)
}

// Added to the trunk while the buyer's app was readable, and in another app; called from once
// it no longer is.
func (s *AppsSuite) TestATrunkNumberABuyerWhoseAppCannotBeReadHoldsIsNotCalledFrom() {
	s.ownApp()
	theirs := "customer-" + uuid.NewString()
	s.apps.give(theirs, 5454, chattest.NewServer(s.T()))
	trunks := s.dialsTrunks(StreamLegShared)
	s.Require().NoError(s.addOnATrunk(trunks, theirs, s.e164))
	s.apps.block(s.customer)

	_, err := trunks.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: theirs}, From: s.e164, To: "+15550001111"})

	s.ErrorIs(err, store.ErrNumberHeldElsewhere)
}

// trunkNumberBought is a number another customer added to its own trunk, which the suite's
// customer has since bought, and that customer's trunk service.
func (s *AppsSuite) trunkNumberBought() (string, string, trunkCall) {
	e164, first := aNumber(), "customer-"+uuid.NewString()
	trunks := s.onATrunk(first, e164, StreamLegShared)
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: e164, Vendor: s.vendor.vendor, Country: "US", CustomerID: s.customer, PurchasedAt: time.Now().UTC(),
	}))
	return e164, first, trunks
}

// One failed read of where the buyer's lines are made says nothing about the number: refused as
// held elsewhere, the call would be lasting, and a campaign would never try the contact again.
func (s *AppsSuite) TestACallFromATrunkNumberWhoseBuyersAppWasNotReadIsTriedAgain() {
	e164, first, trunks := s.trunkNumberBought()
	s.apps.cannotRead(s.customer)

	_, err := trunks.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: first}, From: e164, To: "+15550001111"})

	s.ErrorIs(err, errPlacedUnread)
	s.ErrorIs(err, ErrNotDialed, "a campaign tries it again")
	s.NotErrorIs(err, store.ErrNumberHeldElsewhere)
	s.Empty(trunks.vendor.dialed.From, "the trunk was not dialled")
	s.Empty(s.deployment.Trunks(), "nothing was made at Stream")
}

// The buyer's policies could not be read, so with the fallback on, where its lines are made is
// not known: no answer, rather than no app.
func (s *AppsSuite) TestACallFromATrunkNumberWhoseBuyersPolicyWasNotReadIsTriedAgain() {
	e164, first, trunks := s.trunkNumberBought()
	deployment := streamapp.NewDeployment(streamapp.DeploymentOptions{
		APIKey: "deploy-key", Secret: "deploy-secret", App: 1, Strict: true, BaseURL: s.deployment.URL,
	})
	stored, err := streamapp.NewStored(streamapp.StoredOptions{Store: s.store, Deployment: deployment, FallbackToDeployment: true})
	s.Require().NoError(err)
	stored.SetFloor(func(_ context.Context, customer string) (bool, error) {
		if customer == s.customer {
			return false, errors.New("the policies could not be read")
		}
		return false, nil
	})
	trunks.service.apps = AppsOf(streamapp.NewClients(stored, streamapp.ClientsOptions{}))

	_, err = trunks.service.Call(s.ctx, CallRequest{Owner: routing.Owner{CustomerID: first}, From: e164, To: "+15550001111"})

	s.ErrorIs(err, ErrNotDialed, "a campaign tries it again")
	s.NotErrorIs(err, store.ErrNumberHeldElsewhere)
	s.Empty(trunks.vendor.dialed.From, "the trunk was not dialled")
}

// One failed read of where the holder's lines are made says nothing about the number: the add
// fails to be tried again, and once read, the holder is in another app.
func (s *AppsSuite) TestAddingANumberWhoseHoldersAppWasNotReadIsTriedAgain() {
	s.ownApp()
	s.apps.cannotRead(s.customer)
	theirs := "customer-" + uuid.NewString()
	trunks := s.dialsTrunks(StreamLegShared)

	err := s.addOnATrunk(trunks, theirs, s.e164)

	s.ErrorIs(err, errPlacedUnread)
	s.NotErrorIs(err, store.ErrNumberHeldElsewhere)
	_, err = s.store.PhoneNumbers.GetByE164(s.ctx, theirs, s.e164)
	s.ErrorIs(err, store.ErrNoNumber, "nothing was added")
	s.apps.mu.Lock()
	delete(s.apps.unread, s.customer)
	s.apps.mu.Unlock()
	s.NoError(s.addOnATrunk(trunks, theirs, s.e164), "asked again, it is let in")
}

func (s *AppsSuite) TestAddingANumberWhereTheCustomersAppWasNotReadIsTriedAgain() {
	s.ownApp()
	theirs := "customer-" + uuid.NewString()
	s.apps.cannotRead(theirs)

	err := s.addOnATrunk(s.dialsTrunks(StreamLegShared), theirs, s.e164)

	s.ErrorIs(err, errPlacedUnread)
	s.NotErrorIs(err, store.ErrNumberHeldElsewhere)
}

// The session's call was made in the deployment's app before calls were pinned, so it has no
// pin, and the buyer's lines are made in the deployment's app, named by its id: one app.
func (s *AppsSuite) TestATransferInAnUnpinnedCallIsRefusedLikeOnePinnedToTheDeploymentsApp() {
	s.store.SetStreamPins(store.StreamPins{Deployment: func() int64 { return 1 }})
	s.T().Cleanup(func() { s.store.SetStreamPins(store.StreamPins{}) })
	e164, first, trunks := s.trunkNumberBought()
	s.apps.give(s.customer, 1, s.deployment)

	for _, pin := range []int64{0, 1} {
		_, err := trunks.service.Transfer(s.ctx, TransferRequest{
			Owner: routing.Owner{CustomerID: first}, From: e164, To: "+15550002222", CallID: "call-1", StreamApp: pin,
		})

		s.ErrorIs(err, store.ErrNumberHeldElsewhere, "the call pinned %d", pin)
	}
	s.Empty(s.deployment.Trunks(), "nothing was made at Stream")
}

// Both customers' lines are made in one Stream app that is not the deployment's.
func (s *AppsSuite) TestATransferInAnAppTwoCustomersShareIsNotMadeFromATrunkNumberTheOtherBought() {
	shared := chattest.NewServer(s.T())
	e164, first, trunks := s.trunkNumberBought()
	s.apps.give(first, 6161, shared)
	s.apps.give(s.customer, 6161, shared)

	_, err := trunks.service.Transfer(s.ctx, TransferRequest{
		Owner: routing.Owner{CustomerID: first}, From: e164, To: "+15550002222", CallID: "call-1", StreamApp: 6161,
	})

	s.ErrorIs(err, store.ErrNumberHeldElsewhere)
	s.Empty(shared.Trunks(), "nothing was made at Stream")
}

// In app mode a customer with no app of its own falls back to the deployment's; asking where
// its lines go on another's behalf is not a use of the fallback by it.
func (s *AppsSuite) TestAskingWhereAHoldersLinesAreMadeCountsNoUseOfTheFallbackForIt() {
	deployment := streamapp.NewDeployment(streamapp.DeploymentOptions{
		APIKey: "deploy-key", Secret: "deploy-secret", App: 1, Strict: true, BaseURL: s.deployment.URL,
	})
	stored, err := streamapp.NewStored(streamapp.StoredOptions{Store: s.store, Deployment: deployment, FallbackToDeployment: true})
	s.Require().NoError(err)
	holder := strconv.FormatInt(int64(uuid.New().ID())+1_000_000, 10)

	app, err := AppsOf(streamapp.NewClients(stored, streamapp.ClientsOptions{})).Placed(s.ctx, holder)

	s.Require().NoError(err)
	s.Equal(int64(1), app)
	uses, err := s.store.StreamFallbackUses.Select(s.ctx, time.Unix(0, 0))
	s.Require().NoError(err)
	for _, use := range uses {
		s.NotEqual(holder, use.CustomerID)
	}
}

func (s *AppsSuite) TestACallThroughACustomersTrunkNamesTheTrunksHostOnly() {
	call := s.callsThroughATrunk()

	rows := s.callPlacedRows(call.placed)
	s.Require().Len(rows, 1)
	row, err := s.store.AgentLogs.Get(s.ctx, s.customer, rows[0].ID)
	s.Require().NoError(err)
	s.Equal("trunk.example.com", row.Details["customer_trunk_host"])
	s.Equal(SIPTrunkVendor, row.Details["vendor"])
	for key, value := range row.Details {
		s.NotContains(value, "trunk-user", key)
		s.NotContains(value, call.password, key)
	}
	s.Contains(call.logged.String(), "customer_trunk_host=trunk.example.com")
	s.NotContains(call.logged.String(), "trunk-user")
	s.NotContains(call.logged.String(), call.password)
}

func (s *AppsSuite) TestACallThroughACustomersTrunkKeepsWhereEachSidesAudioComesFrom() {
	call := s.callsThroughATrunk()
	s.Require().NotNil(call.vendor.dialed.Connected, "the bridge has nobody to tell")

	call.vendor.dialed.Connected(sipbridge.MediaAddrs{Customer: "198.51.100.7", Stream: "203.0.113.10"})

	rows := s.eventRows(call.placed, "bridge_media")
	s.Require().Len(rows, 1)
	row, err := s.store.AgentLogs.Get(s.ctx, s.customer, rows[0].ID)
	s.Require().NoError(err)
	s.Equal(map[string]any{"stream_media_addr": "203.0.113.10", "customer_media_addr": "198.51.100.7"}, row.Details)
}

func (s *AppsSuite) TestACallThroughACustomersTrunkKeepsHowItEnded() {
	call := s.callsThroughATrunk()
	s.Require().NotNil(call.vendor.dialed.Ended, "the bridge has nobody to tell")

	call.vendor.dialed.Ended(CallStatus{Result: CallBusy, Word: "486"})

	kept, err := call.service.Outcomes().Read(s.ctx, s.customer, call.placed.CallID)
	s.Require().NoError(err)
	s.Equal(outcome.Busy, kept.Outcome)
	s.Equal("486", kept.VendorStatus)
	s.Empty(call.vendor.dialed.StatusURL, "the bridge tells the router itself")
}

func (s *AppsSuite) TestACallThroughACustomersTrunkOnItsOwnStreamTrunkCannotBeResumed() {
	call := s.callsThroughATrunk()

	// Its Stream trunk's password was never stored, and Stream challenges a re-INVITE.
	s.Nil(call.vendor.dialed.Resume)
}

func (s *AppsSuite) TestACallThroughACustomersTrunkOnASharedLineCanBeResumed() {
	call := s.callsThroughATrunkOn(StreamLegShared)

	resume := call.vendor.dialed.Resume
	s.Require().NotNil(resume)
	number, err := s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, call.vendor.dialed.From)
	s.Require().NoError(err)
	s.NotEmpty(number.StreamTrunkID)
	s.Equal(TrunkCallRef{
		CustomerID: s.customer, SIPTrunkID: number.SIPTrunkID, StreamTrunkID: number.StreamTrunkID,
		From: call.vendor.dialed.From, CallID: call.placed.CallID,
		Token: resume.Token,
	}, *resume)
	byToken, err := call.service.Outcomes().ByToken(s.ctx, resume.Token)
	s.Require().NoError(err)
	s.Equal(call.placed.CallID, byToken.CallID, "the token is the placed call's")

	resumed, err := call.service.ResumeTrunkCall(s.ctx, *resume)
	s.Require().NoError(err)
	s.Equal(call.vendor.dialed.Bridge, resumed.Bridge, "a takeover dials the bridge the call was placed on")
}

func (s *AppsSuite) TestABridgeThatSawNoAddressWritesNoRow() {
	call := s.callsThroughATrunk()
	s.Require().NotNil(call.vendor.dialed.Connected)

	call.vendor.dialed.Connected(sipbridge.MediaAddrs{})

	s.Empty(s.eventRows(call.placed, "bridge_media"))
}

func (s *AppsSuite) TestACallThroughAVendorHasNoBridgeToHearFrom() {
	_, err := s.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550001111",
	})
	s.Require().NoError(err)

	s.Nil(s.vendor.dialed.Connected)
	s.Nil(s.vendor.dialed.Resume)
}

func (s *AppsSuite) TestACallTheVendorRefusedWritesNoCallPlacedRow() {
	s.vendor.err = errors.New("the vendor refused")

	_, err := s.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550001111", SessionID: "0b9f2a52-5c1e-4d7a-9a3e-0f6b8f1c2d3f",
	})
	s.Require().Error(err)

	s.Empty(s.callPlacedRows(Placed{SessionID: "0b9f2a52-5c1e-4d7a-9a3e-0f6b8f1c2d3f"}))
}

func (s *AppsSuite) TestAnAttachedNumbersTrunkCredentialsAreKeptForItsCalls() {
	sealer, err := auth.NewSealer("apps suite")
	s.Require().NoError(err)
	s.service.sealer = sealer

	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)

	bridge, err := s.service.InboundBridge(s.ctx, "twilio", s.e164)
	s.Require().NoError(err)
	s.Equal("sip:"+s.e164+"@sip.example.test", bridge.URI, "the address names the number")
	s.Equal(attached.TrunkID, bridge.Username)
	password, _ := s.deployment.TrunkAuth(attached.TrunkID)
	s.Require().NotEmpty(password)
	s.Equal(password, bridge.Password, "the password the trunk was made with")
	s.NotContains(string(s.trunk().BridgePasswordSealed), password, "the password is stored sealed")
}

func (s *AppsSuite) TestWithoutASealerANumberStillAttachesButKeepsNoCredentials() {
	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)

	_, err = s.service.InboundBridge(s.ctx, "twilio", s.e164)

	s.ErrorIs(err, ErrNoSealer)
	s.Empty(s.trunk().BridgePasswordSealed)
	s.Equal(attached.Bridge.URI, s.trunk().BridgeURI, "the address is not secret, so it is kept")
}

// Telnyx asks per call where to send a caller, and the answer is the stored trunk, so a
// Telnyx number attached with nowhere to keep the trunk's password could never be answered.
func (s *AppsSuite) TestWithoutASealerATelnyxNumberIsNotAttached() {
	telnyx := &stub{vendor: "telnyx"}
	registry := NewRegistry(s.config())
	registry.Register(telnyx.vendor, func() (Provider, error) { return telnyx, nil })
	for _, name := range s.credentials(telnyx.vendor) {
		s.T().Setenv(name, "set")
	}
	service, err := NewService(ServiceOptions{Registry: registry, Store: s.store, Apps: s.apps})
	s.Require().NoError(err)
	e164 := aNumber()
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: e164, Vendor: telnyx.vendor, Country: "US", CustomerID: s.customer, PurchasedAt: time.Now().UTC(),
	}))

	_, err = service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: e164})

	s.ErrorIs(err, ErrNoSealer)
	s.ErrorContains(err, "set ROUTER_DEV_KEK (local), or ROUTER_AUTH_KEK_V1_WRAPPED with ROUTER_AUTH_KMS_KEY (hosted)", "the error names what to set")
	s.Empty(s.deployment.Trunks(), "nothing is made in Stream")
	s.Empty(telnyx.inbound.E164, "telnyx is not told to send calls anywhere")
}

func (s *AppsSuite) TestATransfersTrunkHasAPasswordTheVendorIsGiven() {
	_, err := s.service.Transfer(s.ctx, TransferRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550002222", CallID: "call-1",
	})
	s.Require().NoError(err)

	trunks := s.deployment.Trunks()
	s.Require().Len(trunks, 1)
	password, allowed := s.deployment.TrunkAuth(trunks[0])
	s.NotEmpty(password)
	s.Equal(s.twilioSignalling(), allowed)
	s.Equal(password, s.vendor.dialed.Bridge.Password)
}

func (s *AppsSuite) TestATransferOnAVendorThatCannotSendAPasswordHasItsAddressesAndNoPassword() {
	vonage := s.vendorOnAnAllowlist()

	_, err := s.service.Transfer(s.ctx, TransferRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: vonage.e164, To: "+15550002222", CallID: "call-1",
	})
	s.Require().NoError(err)

	trunks := s.deployment.Trunks()
	s.Require().Len(trunks, 1)
	password, allowed := s.deployment.TrunkAuth(trunks[0])
	s.Equal(vonage.signalling, allowed, "without them the vendor's leg is challenged and cannot answer")
	s.Empty(password)
}

func (s *AppsSuite) TestATransferOnAVendorWithNoAddressesAndNoPasswordIsRefusedBeforeAnythingIsMade() {
	declared, ok := s.config().Lookup("sinch")
	s.Require().True(ok)
	s.Require().Empty(declared.Signalling)
	sinch := s.numberOn("sinch")

	_, err := s.service.Transfer(s.ctx, TransferRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: sinch, To: "+15550002222", CallID: "call-1",
	})
	s.Require().ErrorContains(err, "signalling addresses")
	s.ErrorIs(err, ErrNotConfigured, "the addresses are the deployment's to declare")
	s.Empty(s.deployment.Trunks())
}

// sealed gives the suite's service a key to seal trunk passwords with, which lines need.
func (s *AppsSuite) sealed() {
	sealer, err := auth.NewSealer("apps suite")
	s.Require().NoError(err)
	s.service.sealer = sealer
}

// anotherNumber gives a customer one more number on a vendor.
func (s *AppsSuite) anotherNumber(customer, vendor string) string {
	e164 := aNumber()
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: e164, Vendor: vendor, Country: "US", CustomerID: customer, PurchasedAt: time.Now().UTC(),
	}))
	return e164
}

// lineFor puts a customer's number on a trunk in the app the customer makes lines in.
func (s *AppsSuite) lineFor(customer, e164 string) (TrunkLine, error) {
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, customer, e164)
	s.Require().NoError(err)
	stream, pin, err := s.service.streamFor(s.ctx, customer)
	s.Require().NoError(err)
	return s.service.line(s.ctx, stream, pin, held)
}

func (s *AppsSuite) TestTwoNumbersOfOneVendorShareATrunk() {
	s.ownApp()
	s.sealed()
	second := s.anotherNumber(s.customer, s.vendor.vendor)

	first, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	other, err := s.lineFor(s.customer, second)
	s.Require().NoError(err)

	s.Equal([]string{first.TrunkID}, s.own.Trunks())
	s.Equal(first.TrunkID, other.TrunkID)
	s.Equal([]string{s.e164, second}, s.own.TrunkNumbers(first.TrunkID))
	s.Equal("sip:"+second+"@sip.example.test", other.Bridge.URI, "the address names the number")
	password, _ := s.own.TrunkAuth(first.TrunkID)
	s.Equal(password, other.Bridge.Password, "a number joining later is given the trunk's password")
}

func (s *AppsSuite) TestTheFiftyFirstNumberOpensASecondTrunk() {
	s.sealed()
	first, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	for range trunkCapacity - 1 {
		_, err := s.lineFor(s.customer, s.anotherNumber(s.customer, s.vendor.vendor))
		s.Require().NoError(err)
	}

	fiftyFirst, err := s.lineFor(s.customer, s.anotherNumber(s.customer, s.vendor.vendor))
	s.Require().NoError(err)

	s.Len(s.deployment.Trunks(), 2)
	s.NotEqual(first.TrunkID, fiftyFirst.TrunkID)
	s.Len(s.deployment.TrunkNumbers(first.TrunkID), trunkCapacity)
	s.Len(s.deployment.TrunkNumbers(fiftyFirst.TrunkID), 1)
}

func (s *AppsSuite) TestCustomersNeverShareATrunk() {
	s.sealed()
	other := "customer-" + uuid.NewString()
	theirs := s.anotherNumber(other, s.vendor.vendor)

	ours, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	their, err := s.lineFor(other, theirs)
	s.Require().NoError(err)

	s.NotEqual(ours.TrunkID, their.TrunkID)
	s.Equal([]string{s.e164}, s.deployment.TrunkNumbers(ours.TrunkID))
	s.Equal([]string{theirs}, s.deployment.TrunkNumbers(their.TrunkID))
}

func (s *AppsSuite) TestVendorsNeverShareATrunk() {
	vonage := s.vendorOnAnAllowlist()
	s.sealed()

	twilio, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	other, err := s.lineFor(s.customer, vonage.e164)
	s.Require().NoError(err)

	s.NotEqual(twilio.TrunkID, other.TrunkID)
	_, allowed := s.deployment.TrunkAuth(other.TrunkID)
	s.Equal(vonage.signalling, allowed, "each vendor's trunk lets in that vendor")
}

func (s *AppsSuite) TestEachNumberHasARuleOfItsOwnThatReadsTheHeader() {
	s.sealed()
	second := s.anotherNumber(s.customer, s.vendor.vendor)
	first, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	other, err := s.lineFor(s.customer, second)
	s.Require().NoError(err)

	s.NotEqual(first.RouteID, other.RouteID)
	for e164, routeID := range map[string]string{s.e164: first.RouteID, second: other.RouteID} {
		rule := s.deployment.Rule(routeID)
		s.Require().NotNil(rule)
		s.Equal(lineCallTemplate, rule["direct_routing_configs"].(map[string]any)["call_id"])
		s.Equal([]any{e164}, rule["called_numbers"])
		custom := rule["call_configs"].(map[string]any)["custom_data"].(map[string]any)
		s.Equal(e164, custom[NumberKey], "the call says which number it rang")
	}
	held := s.number()
	s.Equal(first.TrunkID, held.StreamTrunkID)
	s.Equal(first.RouteID, held.StreamRouteID)
}

// onItsOwnTrunk gives the suite's number a trunk and a rule of its own in the deployment's
// app, attached as a number was before trunks were shared, and answers the app, what was
// stored and the bridge.
func (s *AppsSuite) onItsOwnTrunk() (*Stream, store.NumberAttachment, Bridge) {
	stream, pin, err := s.service.streamFor(s.ctx, s.customer)
	s.Require().NoError(err)
	trunkID, bridge, err := stream.CreateTrunk(s.ctx, Trunk{Name: "phone-" + s.e164, Numbers: []string{s.e164}})
	s.Require().NoError(err)
	routeID, err := stream.CreateRoute(s.ctx, Route{
		Name: "phone-" + s.e164, TrunkIDs: []string{trunkID}, CalledNumbers: []string{s.e164},
		CallID: "{{uuid}}", CallType: defaultCallType, Custom: map[string]string{NumberKey: s.e164},
	})
	s.Require().NoError(err)
	bridge, err = bridge.WithNumber(s.e164)
	s.Require().NoError(err)
	sealed, err := s.service.sealer.SealWithAAD(bridge.Password, bridgeAAD(s.customer, s.e164))
	s.Require().NoError(err)
	attached := store.NumberAttachment{
		TrunkID: trunkID, RouteID: routeID, StreamAppPK: pin, CallType: defaultCallType,
	}
	group := store.TrunkGroup{CustomerID: s.customer, StreamAppPK: pin, Vendor: s.vendor.vendor}
	s.Require().NoError(s.store.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *store.TrunkGroupTx) error {
		err := tx.SaveTrunk(store.StreamTrunk{
			ID: trunkID, BridgeURI: bridge.URI, BridgeUsername: bridge.Username,
			BridgePasswordSealed: sealed, BridgePasswordKEKVersion: s.service.sealer.CurrentVersion(), Own: true,
		})
		if err != nil {
			return err
		}
		return tx.Attach(s.e164, attached)
	}))
	return stream, attached, bridge
}

func (s *AppsSuite) TestANumberAttachedBeforeSharedTrunksIsAdoptedAsItIs() {
	s.sealed()
	_, attached, bridge := s.onItsOwnTrunk()
	trunkID, routeID := attached.TrunkID, attached.RouteID

	line, err := s.lineFor(s.customer, s.e164)

	s.Require().NoError(err)
	s.Equal(trunkID, line.TrunkID)
	s.Equal(routeID, line.RouteID)
	s.Equal(bridge, line.Bridge)
	s.Equal([]string{trunkID}, s.deployment.Trunks(), "no new trunk")
	rule := s.deployment.Rule(routeID)
	direct := rule["direct_routing_configs"].(map[string]any)
	s.Equal(lineCallTemplate, direct["call_id"])
	s.Equal(defaultCallType, direct["call_type"])
	s.Equal([]any{s.e164}, rule["called_numbers"])
	s.Equal([]any{trunkID}, rule["trunk_ids"])
	custom := rule["call_configs"].(map[string]any)["custom_data"].(map[string]any)
	s.Equal(s.e164, custom[NumberKey], "the update keeps the number on the call")

	joined, err := s.lineFor(s.customer, s.anotherNumber(s.customer, s.vendor.vendor))
	s.Require().NoError(err)
	s.Equal(trunkID, joined.TrunkID, "the adopted trunk takes the customer's next number")
}

// The requests are the ones the router made before a lost rule or trunk was looked for, and
// the read of the trunk's addresses a join keeps.
func (s *AppsSuite) TestAWorkingLineAsksStreamWhatItAskedBefore() {
	s.sealed()
	_, attached, _ := s.onItsOwnTrunk()
	before := len(s.deployment.Requests(s.deployment.Key))

	_, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	_, err = s.lineFor(s.customer, s.anotherNumber(s.customer, s.vendor.vendor))
	s.Require().NoError(err)

	s.Equal([]string{
		"PUT /api/v2/video/sip/inbound_routing_rules/" + attached.RouteID,
		"GET /api/v2/video/sip/inbound_trunks",
		"PUT /api/v2/video/sip/inbound_trunks/" + attached.TrunkID,
		"POST /api/v2/video/sip/inbound_routing_rules",
	}, s.askedSince(before))
}

func (s *AppsSuite) TestAnAdoptedNumberWhoseRuleStreamLostGetsANewOne() {
	s.sealed()
	// Another number's trunk and rule, which a rule made again must not take for the number's.
	other, err := s.lineFor(s.customer, s.anotherNumber(s.customer, s.vendor.vendor))
	s.Require().NoError(err)
	stream, attached, bridge := s.onItsOwnTrunk()
	s.Require().NoError(stream.DeleteRoute(s.ctx, attached.RouteID))

	line, err := s.lineFor(s.customer, s.e164)

	s.Require().NoError(err)
	s.Equal(attached.TrunkID, line.TrunkID)
	s.Equal(bridge, line.Bridge)
	s.NotEqual(attached.RouteID, line.RouteID)
	s.ElementsMatch([]string{other.RouteID, line.RouteID}, s.deployment.Rules())
	rule := s.deployment.Rule(line.RouteID)
	s.Equal([]any{attached.TrunkID}, rule["trunk_ids"])
	s.Equal([]any{s.e164}, rule["called_numbers"])
	s.Equal(lineCallTemplate, rule["direct_routing_configs"].(map[string]any)["call_id"])
	s.Equal(line.RouteID, s.number().StreamRouteID, "the new rule is stored")
}

func (s *AppsSuite) TestACallFromANumberWhoseRuleStreamLostPlaces() {
	s.sealed()
	stream, attached, _ := s.onItsOwnTrunk()
	s.Require().NoError(stream.DeleteRoute(s.ctx, attached.RouteID))

	_, err := s.callFrom(s.e164)

	s.Require().NoError(err)
	held := s.number()
	s.NotEqual(attached.RouteID, held.StreamRouteID)
	s.Equal([]string{held.StreamRouteID}, s.deployment.Rules())
}

func (s *AppsSuite) TestAttachingANumberWhoseRuleStreamLostGivesItANewOne() {
	s.sealed()
	stream, attached, _ := s.onItsOwnTrunk()
	s.Require().NoError(s.service.Detach(s.ctx, s.customer, s.e164))
	s.Require().NoError(stream.DeleteRoute(s.ctx, attached.RouteID))

	again, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})

	s.Require().NoError(err)
	s.Equal(attached.TrunkID, again.TrunkID)
	s.Equal([]string{again.RouteID}, s.deployment.Rules())
	s.NotEqual(attached.RouteID, again.RouteID)
	s.NotNil(s.number().AttachedAt)
}

func (s *AppsSuite) TestANumberStreamRoutesThroughAnotherRuleIsNotGivenASecond() {
	s.sealed()
	stream, attached, _ := s.onItsOwnTrunk()
	s.Require().NoError(stream.DeleteRoute(s.ctx, attached.RouteID))
	theirs, err := stream.CreateRoute(s.ctx, Route{
		Name: "phone-" + s.e164, TrunkIDs: []string{attached.TrunkID}, CalledNumbers: []string{s.e164}, CallID: "{{uuid}}",
	})
	s.Require().NoError(err)

	_, err = s.lineFor(s.customer, s.e164)

	s.ErrorIs(err, errRoutedElsewhere)
	s.Equal([]string{theirs}, s.deployment.Rules())
	s.Equal(attached.RouteID, s.number().StreamRouteID, "the row is left as it was")
}

func (s *AppsSuite) TestANumberAnotherTrunkAtStreamListsIsNotGivenATrunkOrARule() {
	s.sealed()
	stream, attached, _ := s.onItsOwnTrunk()
	s.Require().NoError(stream.DeleteRoute(s.ctx, attached.RouteID))
	s.Require().NoError(stream.DeleteTrunk(s.ctx, attached.TrunkID))
	theirs, _, err := stream.CreateTrunk(s.ctx, Trunk{Name: "theirs", Numbers: []string{s.e164}})
	s.Require().NoError(err)

	_, err = s.lineFor(s.customer, s.e164)

	s.ErrorIs(err, errRoutedElsewhere)
	s.Equal([]string{theirs}, s.deployment.Trunks())
	s.Empty(s.deployment.Rules())
	s.Equal(attached.TrunkID, s.number().StreamTrunkID, "the row is left as it was")
}

func (s *AppsSuite) TestAttachingANumberWhoseOwnTrunkStreamLostPutsItOnASharedOne() {
	s.sealed()
	stream, attached, _ := s.onItsOwnTrunk()
	s.Require().NoError(s.service.Detach(s.ctx, s.customer, s.e164))
	s.Require().NoError(stream.DeleteRoute(s.ctx, attached.RouteID))
	s.Require().NoError(stream.DeleteTrunk(s.ctx, attached.TrunkID))

	again, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})

	s.Require().NoError(err)
	s.Equal([]string{again.TrunkID}, s.deployment.Trunks())
	s.NotEqual(attached.TrunkID, again.TrunkID)
	s.Equal([]string{s.e164}, s.deployment.TrunkNumbers(again.TrunkID))
	s.Equal([]string{again.RouteID}, s.deployment.Rules())
	s.Equal(again.Bridge, s.vendor.inbound.Bridge, "the vendor is told the new trunk")
	held := s.number()
	s.Equal(again.TrunkID, held.StreamTrunkID)
	s.Equal(again.RouteID, held.StreamRouteID)
}

// onALostSharedTrunk lines the suite's number on a shared trunk, then has Stream lose the
// trunk and its rule, and answers the app and the line.
func (s *AppsSuite) onALostSharedTrunk() (*Stream, TrunkLine) {
	stream, _, err := s.service.streamFor(s.ctx, s.customer)
	s.Require().NoError(err)
	first, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	s.Require().NoError(stream.DeleteRoute(s.ctx, first.RouteID))
	s.Require().NoError(stream.DeleteTrunk(s.ctx, first.TrunkID))
	return stream, first
}

func (s *AppsSuite) TestANumberJoiningASharedTrunkStreamLostGetsANewOneAndTheTrunksNumbersFollow() {
	s.sealed()
	_, first := s.onALostSharedTrunk()
	second := s.anotherNumber(s.customer, s.vendor.vendor)

	joined, err := s.lineFor(s.customer, second)

	s.Require().NoError(err)
	s.NotEqual(first.TrunkID, joined.TrunkID)
	s.Equal([]string{joined.TrunkID}, s.deployment.Trunks())
	s.Equal([]string{second}, s.deployment.TrunkNumbers(joined.TrunkID))

	again, err := s.lineFor(s.customer, s.e164)

	s.Require().NoError(err)
	s.Equal(joined.TrunkID, again.TrunkID, "a number left on the lost trunk joins the new one")
	s.Equal([]string{second, s.e164}, s.deployment.TrunkNumbers(joined.TrunkID))
	s.ElementsMatch([]string{joined.RouteID, again.RouteID}, s.deployment.Rules())
	s.Equal(again.RouteID, s.number().StreamRouteID)
}

func (s *AppsSuite) TestANumberJoiningALostTrunkWhileStreamRoutesItElsewhereIsRefused() {
	s.sealed()
	stream, _ := s.onALostSharedTrunk()
	second := s.anotherNumber(s.customer, s.vendor.vendor)
	theirs, _, err := stream.CreateTrunk(s.ctx, Trunk{Name: "theirs", Numbers: []string{second}})
	s.Require().NoError(err)

	_, err = s.lineFor(s.customer, second)

	s.ErrorIs(err, errRoutedElsewhere)
	s.Equal([]string{theirs}, s.deployment.Trunks())
	s.Empty(s.deployment.Rules())
}

func (s *AppsSuite) TestANumberJoiningALostTrunkWhileAnotherRuleAnswersItIsRefused() {
	s.sealed()
	stream, _ := s.onALostSharedTrunk()
	second := s.anotherNumber(s.customer, s.vendor.vendor)
	elsewhere, _, err := stream.CreateTrunk(s.ctx, Trunk{Name: "theirs", Numbers: []string{aNumber()}})
	s.Require().NoError(err)
	theirs, err := stream.CreateRoute(s.ctx, Route{
		Name: "theirs", TrunkIDs: []string{elsewhere}, CalledNumbers: []string{second}, CallID: "{{uuid}}",
	})
	s.Require().NoError(err)

	_, err = s.lineFor(s.customer, second)

	s.ErrorIs(err, errRoutedElsewhere)
	s.Equal([]string{elsewhere}, s.deployment.Trunks())
	s.Equal([]string{theirs}, s.deployment.Rules())
}

func (s *AppsSuite) TestARuleMadeAgainForALineThatThenFailsIsDeleted() {
	s.sealed()
	stream, attached, _ := s.onItsOwnTrunk()
	_, err := s.store.GlobalDB().NewUpdate().Table("stream_trunks").Set("bridge_uri = 'not a sip uri'").
		Where("id = ?", attached.TrunkID).Exec(s.ctx)
	s.Require().NoError(err)
	s.Require().NoError(stream.DeleteRoute(s.ctx, attached.RouteID))

	_, err = s.lineFor(s.customer, s.e164)

	s.Require().ErrorContains(err, "is not a sip uri")
	s.Empty(s.deployment.Rules())
	s.Equal(attached.RouteID, s.number().StreamRouteID)
}

// askedSince is each request the deployment's app was asked after the first since, as
// "<method> <path>".
func (s *AppsSuite) askedSince(since int) []string {
	var asked []string
	for _, request := range s.deployment.Requests(s.deployment.Key)[since:] {
		asked = append(asked, request.Method+" "+request.Path)
	}
	return asked
}

func (s *AppsSuite) TestACallFromANumberAnotherRuleAnswersPlacesOnATrunkOfItsOwn() {
	s.sealed()
	s.deployment.OneTrunkPerNumber()
	stream, attached, _ := s.onItsOwnTrunk()
	s.Require().NoError(stream.DeleteRoute(s.ctx, attached.RouteID))
	s.Require().NoError(stream.DeleteTrunk(s.ctx, attached.TrunkID))
	elsewhere, _, err := stream.CreateTrunk(s.ctx, Trunk{Name: "theirs", Numbers: []string{aNumber()}})
	s.Require().NoError(err)
	theirs, err := stream.CreateRoute(s.ctx, Route{
		Name: "theirs", TrunkIDs: []string{elsewhere}, CalledNumbers: []string{s.e164}, CallID: "{{uuid}}",
	})
	s.Require().NoError(err)

	_, err = s.callFrom(s.e164)

	s.Require().NoError(err)
	trunks := s.deployment.Trunks()
	s.Len(trunks, 2, "the call has a trunk of its own beside theirs")
	s.Contains(trunks, elsewhere)
	s.Contains(s.deployment.Rules(), theirs)
	s.Equal(attached.TrunkID, s.number().StreamTrunkID, "the row is left as it was")
}

func (s *AppsSuite) TestANotFoundForASharedTrunkStreamStillHasDropsNoneOfItsNumbers() {
	s.sealed()
	first, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	second := s.anotherNumber(s.customer, s.vendor.vendor)
	s.deployment.FailWith(http.MethodPut, "/sip/inbound_trunks/"+first.TrunkID, http.StatusNotFound)

	_, err = s.lineFor(s.customer, second)

	s.Require().True(notFound(err), "the update's own error is answered: %v", err)
	s.Equal([]string{first.TrunkID}, s.deployment.Trunks())
	s.Equal([]string{first.RouteID}, s.deployment.Rules())

	s.deployment.Heal(http.MethodPut, "/sip/inbound_trunks/"+first.TrunkID)
	joined, err := s.lineFor(s.customer, second)

	s.Require().NoError(err)
	s.Equal(first.TrunkID, joined.TrunkID, "the trunk's row is kept")
	s.Equal([]string{s.e164, second}, s.deployment.TrunkNumbers(first.TrunkID))
}

func (s *AppsSuite) TestAJoinWhoseTrunkUpdateFailsAsksStreamNothingMore() {
	s.sealed()
	first, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	second := s.anotherNumber(s.customer, s.vendor.vendor)
	s.deployment.Fail(http.MethodPut, "/sip/inbound_trunks/"+first.TrunkID)
	before := len(s.deployment.Requests(s.deployment.Key))

	_, err = s.lineFor(s.customer, second)

	s.Require().ErrorContains(err, "boom")
	s.Equal([]string{"GET /api/v2/video/sip/inbound_trunks", "PUT /api/v2/video/sip/inbound_trunks/" + first.TrunkID},
		s.askedSince(before), "only a trunk Stream says is gone is looked for")
	s.Equal([]string{first.TrunkID}, s.deployment.Trunks())
	s.Equal([]string{s.e164}, s.deployment.TrunkNumbers(first.TrunkID))
	s.Equal([]string{first.RouteID}, s.deployment.Rules())
}

func (s *AppsSuite) TestAnAdoptedNumberWhoseRuleUpdateFailsIsNotGivenANewRule() {
	s.sealed()
	_, attached, _ := s.onItsOwnTrunk()
	s.deployment.Fail(http.MethodPut, "/sip/inbound_routing_rules/"+attached.RouteID)
	before := len(s.deployment.Requests(s.deployment.Key))

	_, err := s.lineFor(s.customer, s.e164)

	s.Require().ErrorContains(err, "boom")
	s.NotErrorIs(err, errRoutedElsewhere)
	s.Equal([]string{"PUT /api/v2/video/sip/inbound_routing_rules/" + attached.RouteID}, s.askedSince(before))
	s.Equal([]string{attached.RouteID}, s.deployment.Rules())
	s.Equal([]string{attached.TrunkID}, s.deployment.Trunks())
	s.Equal(attached.RouteID, s.number().StreamRouteID)
}

func (s *AppsSuite) TestANumberJoiningAfterALostTrunkTakesAnotherTrunkWithRoom() {
	s.sealed()
	first, err := s.lineFor(s.customer, s.anotherNumber(s.customer, s.vendor.vendor))
	s.Require().NoError(err)
	_, err = s.lineFor(s.customer, s.anotherNumber(s.customer, s.vendor.vendor))
	s.Require().NoError(err)
	// The adopted trunk, with one number, is the roomiest of the group's two.
	stream, attached, _ := s.onItsOwnTrunk()
	_, err = s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	s.Require().NoError(stream.DeleteRoute(s.ctx, attached.RouteID))
	s.Require().NoError(stream.DeleteTrunk(s.ctx, attached.TrunkID))
	third := s.anotherNumber(s.customer, s.vendor.vendor)

	joined, err := s.lineFor(s.customer, third)

	s.Require().NoError(err)
	s.Equal(first.TrunkID, joined.TrunkID)
	s.Equal([]string{first.TrunkID}, s.deployment.Trunks(), "no trunk is made while one has room")
	s.Len(s.deployment.TrunkNumbers(first.TrunkID), 3)
}

func (s *AppsSuite) TestAnAdoptedNumberWhoseTrunkNoLongerListsItIsNotMovedOffIt() {
	s.sealed()
	stream, attached, _ := s.onItsOwnTrunk()
	s.Require().NoError(stream.DeleteRoute(s.ctx, attached.RouteID))
	s.Require().NoError(stream.UpdateTrunk(s.ctx, attached.TrunkID, Trunk{Name: "theirs", Numbers: []string{aNumber()}}))

	_, err := s.lineFor(s.customer, s.e164)

	s.ErrorIs(err, errTrunkChanged)
	s.Equal([]string{attached.TrunkID}, s.deployment.Trunks())
	s.Empty(s.deployment.Rules())
	held := s.number()
	s.Equal(attached.TrunkID, held.StreamTrunkID, "the row is left as it was")
	s.Equal(attached.RouteID, held.StreamRouteID)
}

func (s *AppsSuite) TestTwoNumbersJoiningAtOnceMakeOneTrunk() {
	s.sealed()
	second := s.anotherNumber(s.customer, s.vendor.vendor)
	var wait sync.WaitGroup
	for _, e164 := range []string{s.e164, second} {
		wait.Go(func() {
			_, err := s.lineFor(s.customer, e164)
			s.NoError(err)
		})
	}
	wait.Wait()

	trunks := s.deployment.Trunks()
	s.Require().Len(trunks, 1)
	s.ElementsMatch([]string{s.e164, second}, s.deployment.TrunkNumbers(trunks[0]))
}

func (s *AppsSuite) TestARefusedRuleLeavesTheNumberOffTheTrunk() {
	s.sealed()
	first, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	second := s.anotherNumber(s.customer, s.vendor.vendor)
	s.deployment.RateLimit("/sip/inbound_routing_rules")

	_, err = s.lineFor(s.customer, second)

	s.Require().Error(err)
	s.Equal([]string{s.e164}, s.deployment.TrunkNumbers(first.TrunkID))
	s.Equal([]string{first.RouteID}, s.deployment.Rules())
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, second)
	s.Require().NoError(err)
	s.Empty(held.StreamTrunkID)
}

func (s *AppsSuite) TestARefusedRuleOnANewTrunkLeavesNoTrunk() {
	s.sealed()
	s.deployment.RateLimit("/sip/inbound_routing_rules")

	_, err := s.lineFor(s.customer, s.e164)

	s.Require().Error(err)
	s.Empty(s.deployment.Trunks())
	s.Empty(s.number().StreamTrunkID)
}

func (s *AppsSuite) TestALineNeedsAKeyToSealTheTrunksPassword() {
	_, err := s.lineFor(s.customer, s.e164)

	s.ErrorIs(err, ErrNoSealer)
	s.Empty(s.deployment.Trunks(), "nothing is made in Stream")
}

func (s *AppsSuite) TestOneNumberLinedTwiceAtOnceIsOnItsTrunkOnce() {
	s.ownApp()
	s.sealed()
	var wait sync.WaitGroup
	for range 2 {
		wait.Go(func() {
			_, err := s.lineFor(s.customer, s.e164)
			s.NoError(err)
		})
	}
	wait.Wait()

	trunks := s.own.Trunks()
	s.Require().Len(trunks, 1)
	s.Equal([]string{s.e164}, s.own.TrunkNumbers(trunks[0]))
	s.Len(s.own.Rules(), 1)
}

// callFrom places a call from one of the suite customer's numbers.
func (s *AppsSuite) callFrom(e164 string) (Placed, error) {
	return s.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: e164, To: "+15550001111",
	})
}

func (s *AppsSuite) TestTwoCallsFromOneNumberAtOncePlaceBoth() {
	s.ownApp()
	s.sealed()
	s.own.OneTrunkPerNumber()

	_, first := s.callFrom(s.e164)
	_, second := s.callFrom(s.e164)

	s.Require().NoError(first)
	s.Require().NoError(second, "Stream lets a number be on one trunk only")
	s.Len(s.own.Trunks(), 1)
}

func (s *AppsSuite) TestACallFromAnAttachedNumberPlaces() {
	s.ownApp()
	s.sealed()
	s.own.OneTrunkPerNumber()
	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)

	_, err = s.callFrom(s.e164)

	s.Require().NoError(err)
	s.Equal([]string{attached.TrunkID}, s.own.Trunks())
}

func (s *AppsSuite) TestSixtyNumbersPlacingCallsStayUnderTheTrunkCap() {
	s.ownApp()
	s.sealed()
	s.own.CapTrunks(25)
	numbers := []string{s.e164}
	for range 59 {
		numbers = append(numbers, s.anotherNumber(s.customer, s.vendor.vendor))
	}

	for _, e164 := range numbers {
		_, err := s.callFrom(e164)
		s.Require().NoError(err)
	}

	s.Len(s.own.Trunks(), 2)
}

func (s *AppsSuite) TestACallsCustomIsOnItsStreamCall() {
	s.ownApp()
	s.sealed()
	custom := map[string]string{"lead": "42"}

	placed, err := s.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550001111", Custom: custom,
	})

	s.Require().NoError(err)
	made := s.own.Call(placed.CallType, placed.CallID)
	s.Require().NotNil(made, "the call the leg lands in is made before it is dialled")
	data, ok := made["data"].(map[string]any)
	s.Require().True(ok, "the call was created with data")
	s.Equal(map[string]any{"lead": "42"}, data["custom"])
}

// dialInto places a call from the suite's number into the session, carrying the custom.
func (s *AppsSuite) dialInto(sessionID, to string, custom map[string]string) error {
	_, err := s.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: to, SessionID: sessionID, Custom: custom,
	})
	return err
}

// heldWith puts the session's call in the customer's app with the custom given, as a leg
// arriving on a routing rule has Stream make it.
func (s *AppsSuite) heldWith(sessionID string, custom map[string]any) {
	creator := "a-leg"
	_, err := s.own.Client.Video().GetOrCreateCall(s.ctx, defaultCallType, sessionID, &getstream.GetOrCreateCallRequest{
		Data: &getstream.CallRequest{CreatedByID: &creator, Custom: custom},
	})
	s.Require().NoError(err)
}

// newRule is the one rule the app holds that it did not hold before.
func (s *AppsSuite) newRule(before []string) map[string]any {
	added := slices.DeleteFunc(s.own.Rules(), func(id string) bool { return slices.Contains(before, id) })
	s.Require().Len(added, 1)
	return s.own.Rule(added[0])
}

func (s *AppsSuite) TestADialIntoAHeldSessionWithOtherCustomIsRefusedBeforeItRings() {
	s.ownApp()
	s.sealed()
	session := uuid.NewString()
	s.Require().NoError(s.dialInto(session, "+15550001111", map[string]string{"k": "first"}))
	s.vendor.dialed = Outbound{}

	err := s.dialInto(session, "+15550002222", map[string]string{"k": "second"})

	s.ErrorIs(err, errCustomKept)
	s.Empty(s.vendor.dialed.To, "no leg rings")
	data, _ := s.own.Call(defaultCallType, session)["data"].(map[string]any)
	s.Equal(map[string]any{"k": "first"}, data["custom"])
}

func (s *AppsSuite) TestADialOnItsOwnTrunkIntoAHeldSessionWithOtherCustomMakesNoTrunk() {
	s.ownApp()
	session := uuid.NewString()
	s.heldWith(session, map[string]any{"k": "first"})
	trunks, rules := s.own.Trunks(), s.own.Rules()

	err := s.dialInto(session, "+15550002222", map[string]string{"k": "second"})

	s.ErrorIs(err, errCustomKept)
	s.Empty(s.vendor.dialed.To, "no leg rings")
	s.Equal(trunks, s.own.Trunks(), "no trunk is made for a leg that will not ring")
	s.Equal(rules, s.own.Rules())
}

func (s *AppsSuite) TestADialWhoseHeldCallCannotBeReadMakesNoTrunk() {
	// Without the call's answer the dial cannot tell whether its custom would be kept.
	s.ownApp()
	session := uuid.NewString()
	s.own.RateLimit("/video/call/" + defaultCallType + "/" + session)
	trunks := s.own.Trunks()

	err := s.dialInto(session, "+15550001111", map[string]string{"k": "v"})

	s.Require().Error(err)
	s.NotErrorIs(err, errCustomKept, "the client is told the read failed, not that its custom differs")
	s.Empty(s.vendor.dialed.To, "no leg rings")
	s.Equal(trunks, s.own.Trunks())
}

func (s *AppsSuite) TestADialIntoAHeldSessionPlacesOnlyWhenItsCustomIsTheCalls() {
	// The number is the router's own, which a leg's rule may have put on the call, so only
	// the caller's keys are compared. A call with none of them takes the custom sent.
	s.ownApp()
	s.sealed()
	sent := map[string]string{"k": "1"}
	number := "+15550109999"
	for name, edge := range map[string]struct {
		kept    map[string]any
		refused bool
		after   map[string]any
	}{
		"the same":                     {map[string]any{"k": "1"}, false, map[string]any{"k": "1"}},
		"the same and the number":      {map[string]any{"k": "1", NumberKey: number}, false, map[string]any{"k": "1", NumberKey: number}},
		"none":                         {nil, false, map[string]any{"k": "1"}},
		"only the number":              {map[string]any{NumberKey: number}, false, map[string]any{"k": "1", NumberKey: number}},
		"a number, not a text":         {map[string]any{"k": 1}, true, nil},
		"null":                         {map[string]any{"k": nil}, true, nil},
		"another value":                {map[string]any{"k": "2"}, true, nil},
		"another value and the number": {map[string]any{"k": "2", NumberKey: number}, true, nil},
		"another key":                  {map[string]any{"K": "1"}, true, nil},
		"a key more":                   {map[string]any{"k": "1", "extra": "x"}, true, nil},
		"a key fewer":                  {map[string]any{"other": "x"}, true, nil},
	} {
		s.Run(name, func() {
			session := uuid.NewString()
			s.heldWith(session, edge.kept)
			s.vendor.dialed = Outbound{}

			err := s.dialInto(session, "+15550001111", sent)

			data, _ := s.own.Call(defaultCallType, session)["data"].(map[string]any)
			if edge.refused {
				s.ErrorIs(err, errCustomKept)
				s.Empty(s.vendor.dialed.To)
				kept, err := json.Marshal(edge.kept)
				s.Require().NoError(err)
				now, err := json.Marshal(data["custom"])
				s.Require().NoError(err)
				s.JSONEq(string(kept), string(now), "a refused dial leaves the call's custom as it was")
				return
			}
			s.Require().NoError(err)
			s.Equal("+15550001111", s.vendor.dialed.To)
			s.Equal(edge.after, data["custom"])
		})
	}
}

func (s *AppsSuite) TestADialWithCustomIntoAnAgentsOwnCallPutsItsCustomOnIt() {
	// An agent makes its call with no custom and then dials into it with some, as the Python
	// SDK's Agent.outbound_call does.
	for _, line := range []struct {
		name   string
		shared bool
	}{{"shared line", true}, {"its own trunk", false}} {
		s.Run(line.name, func() {
			s.SetupTest()
			s.ownApp()
			if line.shared {
				s.sealed()
			}
			session := uuid.NewString()
			s.heldWith(session, nil)

			s.Require().NoError(s.dialInto(session, "+15550001111", map[string]string{"lead": "42"}))

			s.Equal("+15550001111", s.vendor.dialed.To)
			data, _ := s.own.Call(defaultCallType, session)["data"].(map[string]any)
			s.Equal(map[string]any{"lead": "42"}, data["custom"])
		})
	}
}

func (s *AppsSuite) TestADialWithCustomIsRefusedWhenItsCustomCannotBePutOnTheCall() {
	// The call is held with no custom, so the dial puts its custom there; when Stream
	// refuses that, the dial must not ring with the agent reading no custom.
	for _, line := range []struct {
		name   string
		shared bool
	}{{"shared line", true}, {"its own trunk", false}} {
		s.Run(line.name, func() {
			s.SetupTest()
			s.ownApp()
			if line.shared {
				s.sealed()
			}
			// A first dial elsewhere makes the shared line, so only a trunk of this dial counts.
			s.Require().NoError(s.dialInto(uuid.NewString(), "+15550009999", nil))
			s.vendor.dialed = Outbound{}
			session := uuid.NewString()
			s.heldWith(session, nil)
			trunks := len(s.own.Trunks())
			s.own.Fail(http.MethodPatch, "/video/call/"+defaultCallType+"/"+session)

			err := s.dialInto(session, "+15550001111", map[string]string{"lead": "42"})

			s.Error(err)
			s.Empty(s.vendor.dialed.To)
			s.Len(s.own.Trunks(), trunks)
		})
	}
}

func (s *AppsSuite) TestADialThatFailsAfterItsCustomIsOnTheCallLeavesItThere() {
	// Stream cannot take a custom key off a call, so the custom of a dial that then failed
	// stays: a retry with the same custom places, a corrected one is refused, as the place-call
	// doc says. A call that held the same custom before the dial is not changed by it.
	for _, line := range []struct {
		name   string
		shared bool
	}{{"shared line", true}, {"its own trunk", false}} {
		for _, held := range []struct {
			name   string
			custom map[string]any
		}{{"a call with none", nil}, {"a call holding the same", map[string]any{"k": "wrong"}}} {
			s.Run(line.name+", "+held.name, func() {
				s.SetupTest()
				s.ownApp()
				if line.shared {
					s.sealed()
				}
				session := uuid.NewString()
				s.heldWith(session, held.custom)
				s.vendor.err = errors.New("the vendor refused")

				err := s.dialInto(session, "+15550001111", map[string]string{"k": "wrong"})

				s.Require().Error(err)
				s.NotErrorIs(err, errCustomKept)
				data, _ := s.own.Call(defaultCallType, session)["data"].(map[string]any)
				s.Equal(map[string]any{"k": "wrong"}, data["custom"])

				s.vendor.err = nil
				s.vendor.dialed = Outbound{}
				s.ErrorIs(s.dialInto(session, "+15550002222", map[string]string{"k": "right"}), errCustomKept)
				s.Empty(s.vendor.dialed.To, "a corrected retry rings no leg")

				s.Require().NoError(s.dialInto(session, "+15550003333", map[string]string{"k": "wrong"}))
				s.Equal("+15550003333", s.vendor.dialed.To)
			})
		}
	}
}

func (s *AppsSuite) TestADialWithCustomIntoASessionInProgressPutsItsCustomOnTheCall() {
	// A session's call that is in progress is already there, whatever made it.
	s.ownApp()
	s.sealed()
	session := uuid.NewString()
	s.own.PutCall(defaultCallType, session, "agent-1")

	s.Require().NoError(s.dialInto(session, "+15550001111", map[string]string{"k": "v"}))

	s.Equal("+15550001111", s.vendor.dialed.To)
	data, _ := s.own.Call(defaultCallType, session)["data"].(map[string]any)
	s.Equal(map[string]any{"k": "v"}, data["custom"])
}

func (s *AppsSuite) TestADialIntoAHeldSessionWithTheSameCustomOrNonePlaces() {
	for _, line := range []struct {
		name   string
		shared bool
	}{{"shared line", true}, {"its own trunk", false}} {
		s.Run(line.name, func() {
			s.SetupTest()
			s.ownApp()
			if line.shared {
				s.sealed()
			}
			session := uuid.NewString()
			s.Require().NoError(s.dialInto(session, "+15550001111", map[string]string{"k": "v"}))

			s.Require().NoError(s.dialInto(session, "+15550002222", map[string]string{"k": "v"}))
			s.Equal("+15550002222", s.vendor.dialed.To)
			s.Require().NoError(s.dialInto(session, "+15550003333", nil))
			s.Equal("+15550003333", s.vendor.dialed.To)
		})
	}
}

func (s *AppsSuite) TestADialOnItsOwnTrunkCarriesItsCustomOnItsRule() {
	for name, session := range map[string]string{"a session chosen": "", "a session named": uuid.NewString()} {
		s.Run(name, func() {
			s.SetupTest()
			s.ownApp()
			rules := s.own.Rules()

			placed, err := s.service.Call(s.ctx, CallRequest{
				Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550001111",
				SessionID: session, Custom: map[string]string{"k": "v"},
			})

			s.Require().NoError(err)
			s.Equal("+15550001111", s.vendor.dialed.To)
			custom := s.newRule(rules)["call_configs"].(map[string]any)["custom_data"].(map[string]any)
			s.Equal(map[string]any{"k": "v"}, custom)
			if session == "" {
				s.Nil(s.own.Call(placed.CallType, placed.CallID), "the leg makes the call of a session nobody named")
			}
		})
	}
}

func (s *AppsSuite) TestTheVendorIsGivenTheSessionAsTheCallID() {
	s.sealed()

	placed, err := s.callFrom(s.e164)

	s.Require().NoError(err)
	s.Equal(placed.SessionID, s.vendor.dialed.CallID)
}

func (s *AppsSuite) TestACallOnAVendorWithoutTheHeaderGetsATrunkOfItsOwn() {
	plivo := s.numberOn("plivo")
	s.sealed()

	placed, err := s.callFrom(plivo)

	s.Require().NoError(err)
	s.Len(s.deployment.Trunks(), 1)
	s.Equal([]string{placed.CallID}, s.pinnedCalls(), "the rule names the call, as the leg cannot")
}

func (s *AppsSuite) TestWithoutAKeyACallStillPlacesOnATrunkOfItsOwn() {
	placed, err := s.callFrom(s.e164)

	s.Require().NoError(err)
	s.Len(s.deployment.Trunks(), 1)
	s.Equal([]string{placed.CallID}, s.pinnedCalls())
	s.Empty(s.vendor.dialed.CallID)
}

// pinnedCalls are the calls the deployment app's rules send their legs into.
func (s *AppsSuite) pinnedCalls() []string {
	var calls []string
	for _, id := range s.deployment.Rules() {
		calls = append(calls, s.deployment.Rule(id)["direct_routing_configs"].(map[string]any)["call_id"].(string))
	}
	return calls
}

func (s *AppsSuite) TestAFailedDialLeavesNoCallResource() {
	s.sealed()
	s.vendor.err = errors.New("the vendor refused")
	session := uuid.NewString()

	_, err := s.service.Call(s.ctx, CallRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550001111", SessionID: session,
	})

	s.Require().Error(err)
	resources, err := s.store.CallResources.Select(s.ctx, store.AppScope{App: 1, Unpinned: true}, defaultCallType, session)
	s.Require().NoError(err)
	s.Empty(resources)
}

func (s *AppsSuite) TestEndingASharedCallKeepsItsTrunk() {
	s.sealed()
	placed, err := s.callFrom(s.e164)
	s.Require().NoError(err)

	s.Require().NoError(s.service.ReleaseCall(s.ctx, store.AppScope{App: 1, Unpinned: true}, placed.CallType, placed.CallID))

	s.Len(s.deployment.Trunks(), 1, "the number's other calls use it")
	s.Len(s.deployment.Rules(), 1)
}

func (s *AppsSuite) TestATransferFromANumberWithALiveCallPlaces() {
	s.ownApp()
	s.sealed()
	s.own.OneTrunkPerNumber()
	placed, err := s.callFrom(s.e164)
	s.Require().NoError(err)

	_, err = s.service.Transfer(s.ctx, TransferRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550002222",
		CallID: placed.CallID, CallType: placed.CallType, StreamApp: placed.StreamApp,
	})

	s.Require().NoError(err)
	s.Len(s.own.Trunks(), 1)
	s.Equal(placed.CallID, s.vendor.dialed.CallID, "the human joins the call the agent is on")
}

func (s *AppsSuite) TestAFailedTransferFromTheCallsOwnNumberKeepsTheCallsRow() {
	s.sealed()
	placed, err := s.callFrom(s.e164)
	s.Require().NoError(err)
	s.vendor.err = errors.New("the vendor refused")

	_, err = s.service.Transfer(s.ctx, TransferRequest{
		Owner: routing.Owner{CustomerID: s.customer}, From: s.e164, To: "+15550002222",
		CallID: placed.CallID, CallType: placed.CallType, StreamApp: placed.StreamApp,
	})

	s.Require().Error(err)
	resources, err := s.store.CallResources.Select(s.ctx, store.AppScope{App: 1, Unpinned: true}, placed.CallType, placed.CallID)
	s.Require().NoError(err)
	s.Len(resources, 1, "the call's own leg is still live")
}

func (s *AppsSuite) TestAttachingANumberThatAlreadyPlacedCallsKeepsItsTrunk() {
	s.ownApp()
	s.sealed()
	s.own.OneTrunkPerNumber()
	_, err := s.callFrom(s.e164)
	s.Require().NoError(err)
	lined := s.number()

	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})

	s.Require().NoError(err)
	s.Equal(lined.StreamTrunkID, attached.TrunkID)
	s.Equal(lined.StreamRouteID, attached.RouteID)
	s.Equal([]string{attached.TrunkID}, s.own.Trunks())
	s.Equal([]string{attached.RouteID}, s.own.Rules())
	s.Equal(s.e164, s.vendor.inbound.E164, "the vendor is told to send calls to the line")
	s.NotNil(s.number().AttachedAt)
}

func (s *AppsSuite) TestReleasingOneOfTwoNumbersKeepsTheTrunkForTheOther() {
	s.ownApp()
	s.sealed()
	second := s.anotherNumber(s.customer, s.vendor.vendor)
	first, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	other, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: second})
	s.Require().NoError(err)
	s.Require().Equal(first.TrunkID, other.TrunkID)

	s.Require().NoError(s.service.Release(s.ctx, s.customer, s.e164))

	s.Equal([]string{first.TrunkID}, s.own.Trunks())
	s.Equal([]string{second}, s.own.TrunkNumbers(first.TrunkID))
	s.Equal([]string{other.RouteID}, s.own.Rules())
}

func (s *AppsSuite) TestReleasingTheLastNumberDeletesTheTrunk() {
	s.sealed()
	_, err := s.callFrom(s.e164)
	s.Require().NoError(err)

	s.Require().NoError(s.service.Release(s.ctx, s.customer, s.e164))

	s.Empty(s.deployment.Trunks())
	s.Empty(s.deployment.Rules())
	_, err = s.callFrom(s.anotherNumber(s.customer, s.vendor.vendor))
	s.Require().NoError(err, "the next number does not join the deleted trunk")
	s.Len(s.deployment.Trunks(), 1)
}

func (s *AppsSuite) TestATrunkStreamWouldNotDeleteIsKeptForTheReleaseToBeRetried() {
	s.sealed()
	_, err := s.callFrom(s.e164)
	s.Require().NoError(err)
	trunk := s.number().StreamTrunkID
	s.deployment.RateLimit("/sip/inbound_trunks/" + trunk)

	s.Require().Error(s.service.Release(s.ctx, s.customer, s.e164))

	s.Equal([]string{trunk}, s.deployment.Trunks())
	s.Equal(trunk, s.number().StreamTrunkID, "the trunk and its password are still known here")
	s.Empty(s.vendor.released, "the number is not given back while its trunk lives on")

	s.deployment.LiftRateLimit("/sip/inbound_trunks/" + trunk)
	s.Require().NoError(s.service.Release(s.ctx, s.customer, s.e164))

	s.Empty(s.deployment.Trunks())
	s.Equal([]string{s.e164}, s.vendor.released)
}

// A release takes the number off Stream as best it can: a rule Stream would not delete is
// logged, and the number is given back all the same.
func (s *AppsSuite) TestAReleaseStreamWillNotDeleteTheRuleOfStillGivesTheNumberBack() {
	s.sealed()
	_, err := s.callFrom(s.e164)
	s.Require().NoError(err)
	other := s.anotherNumber(s.customer, s.vendor.vendor)
	_, err = s.callFrom(other)
	s.Require().NoError(err)
	held := s.number()
	s.deployment.Fail(http.MethodDelete, "/sip/inbound_routing_rules/"+held.StreamRouteID)

	s.Require().NoError(s.service.Release(s.ctx, s.customer, s.e164))

	s.Equal([]string{s.e164}, s.vendor.released)
	_, err = s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, s.e164)
	s.ErrorIs(err, store.ErrNoNumber, "the number is released")
	s.Equal([]string{other}, s.deployment.TrunkNumbers(held.StreamTrunkID), "its trunk is its other number's")
}

// Stream keeps a trunk a rule still names, so the number's rule has to go before its last trunk does.
func (s *AppsSuite) TestAReleaseOfATrunksLastNumberIsTriedAgainWhenStreamWillNotDeleteItsRule() {
	s.sealed()
	_, err := s.callFrom(s.e164)
	s.Require().NoError(err)
	path := "/sip/inbound_routing_rules/" + s.number().StreamRouteID
	s.deployment.Fail(http.MethodDelete, path)

	s.Require().Error(s.service.Release(s.ctx, s.customer, s.e164))

	s.Empty(s.vendor.released)
	s.deployment.Heal(http.MethodDelete, path)
	s.Require().NoError(s.service.Release(s.ctx, s.customer, s.e164))
	s.Equal([]string{s.e164}, s.vendor.released)
	s.Empty(s.deployment.Trunks())
}

// goneAtStream takes a trunk away at Stream by hand, its rules first, as Stream asks.
func (s *AppsSuite) goneAtStream(trunkID string) {
	stream := NewStreamFromClient(s.deployment.Client)
	rules, err := stream.rulesOn(s.ctx, trunkID)
	s.Require().NoError(err)
	for rule := range rules {
		s.Require().NoError(stream.DeleteRoute(s.ctx, rule))
	}
	s.Require().NoError(stream.DeleteTrunk(s.ctx, trunkID))
}

func (s *AppsSuite) TestANumberWhoseTrunkIsAlreadyGoneAtStreamIsReleased() {
	s.sealed()
	_, err := s.callFrom(s.e164)
	s.Require().NoError(err)
	held := s.number()
	s.goneAtStream(held.StreamTrunkID)

	s.Require().NoError(s.service.Release(s.ctx, s.customer, s.e164))

	s.Equal([]string{s.e164}, s.vendor.released)
	group := store.TrunkGroup{CustomerID: s.customer, StreamAppPK: held.StreamAppPK, Vendor: held.Vendor}
	s.Require().NoError(s.store.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *store.TrunkGroupTx) error {
		trunks, err := tx.Trunks()
		s.Empty(trunks)
		return err
	}))
}

func (s *AppsSuite) TestAFreedPlaceOnAFullTrunkIsTakenBeforeASiblingIsOpened() {
	s.sealed()
	first, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)
	for range trunkCapacity - 1 {
		_, err := s.lineFor(s.customer, s.anotherNumber(s.customer, s.vendor.vendor))
		s.Require().NoError(err)
	}
	s.Require().NoError(s.service.Release(s.ctx, s.customer, s.e164))

	added, err := s.lineFor(s.customer, s.anotherNumber(s.customer, s.vendor.vendor))

	s.Require().NoError(err)
	s.Equal([]string{first.TrunkID}, s.deployment.Trunks())
	s.Equal(first.TrunkID, added.TrunkID)
	s.Len(s.deployment.TrunkNumbers(first.TrunkID), trunkCapacity)
}

func (s *AppsSuite) TestReattachingAfterRegistrationTakesTheNumberOffItsSharedTrunk() {
	s.sealed()
	second := s.anotherNumber(s.customer, s.vendor.vendor)
	first, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	other, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: second})
	s.Require().NoError(err)
	s.ownApp()

	moved, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})

	s.Require().NoError(err)
	s.Equal([]string{first.TrunkID}, s.deployment.Trunks())
	s.Equal([]string{second}, s.deployment.TrunkNumbers(first.TrunkID), "the other number keeps its trunk")
	s.Equal([]string{other.RouteID}, s.deployment.Rules())
	s.Equal([]string{moved.TrunkID}, s.own.Trunks())
	s.Equal([]string{s.e164}, s.own.TrunkNumbers(moved.TrunkID))
	s.Equal(int64(4242), s.number().StreamAppPK)
}

func (s *AppsSuite) TestReleasingANumberLinedSinceItWasReadTakesItOffItsTrunk() {
	// Release reads the number before it takes the lock: a call lining it in the
	// customer's new app meanwhile must not leave a released number on that trunk.
	stale := s.number()
	s.ownApp()
	s.sealed()
	second := s.anotherNumber(s.customer, s.vendor.vendor)
	other, err := s.lineFor(s.customer, second)
	s.Require().NoError(err)
	lined, err := s.lineFor(s.customer, s.e164)
	s.Require().NoError(err)

	s.Require().NoError(s.service.unline(s.ctx, stale, "released a number", time.Now().UTC()))

	s.Equal([]string{second}, s.own.TrunkNumbers(lined.TrunkID))
	s.Equal([]string{other.RouteID}, s.own.Rules())
	_, err = s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, s.e164)
	s.ErrorIs(err, store.ErrNoNumber, "the number is released")
}

func (s *AppsSuite) TestAttachingANumberWhoseTrunkPasswordWasNotKeptGivesItATrunkOfItsOwn() {
	first, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	_, err = s.store.GlobalDB().NewUpdate().Table("phone_numbers").Set("attached_at = NULL").
		Where("customer_id = ?", s.customer).Where("e164 = ?", s.e164).Exec(s.ctx)
	s.Require().NoError(err)
	s.sealed()

	again, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})

	s.Require().NoError(err)
	s.NotEqual(first.TrunkID, again.TrunkID)
	s.Equal([]string{again.TrunkID}, s.deployment.Trunks(), "the old trunk is deleted")
	s.Equal([]string{s.e164}, s.deployment.TrunkNumbers(again.TrunkID))
	s.Equal([]string{again.RouteID}, s.deployment.Rules())
	s.NotEmpty(s.trunk().BridgePasswordSealed, "its new trunk's password is kept")
}

func (s *AppsSuite) TestAttachingANumberOnASharedTrunkAgainChangesNothing() {
	s.sealed()
	first, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	s.vendor.inbound = Inbound{}
	made := s.deploymentWrites()
	s.Require().NotZero(made)

	again, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})

	s.Require().NoError(err)
	s.Equal(first.TrunkID, again.TrunkID)
	s.Equal(first.RouteID, again.RouteID)
	s.Equal(made, s.deploymentWrites(), "nothing is changed in Stream")
	s.Equal([]string{s.e164}, s.deployment.TrunkNumbers(first.TrunkID))
	s.Empty(s.vendor.inbound.E164, "the vendor is not told anything new")
}

func (s *AppsSuite) deploymentWrites() int {
	var writes int
	for _, request := range s.deployment.Requests(s.deployment.Key) {
		if request.Writes() {
			writes++
		}
	}
	return writes
}

// backdate moves a call's rows back in time, as a call that has been up for hours has.
func (s *AppsSuite) backdate(callID string, age time.Duration) {
	_, err := s.store.GlobalDB().NewUpdate().Table("call_resources").
		Set("created_at = ?", time.Now().UTC().Add(-age)).
		Where("call_id = ?", callID).Exec(s.ctx)
	s.Require().NoError(err)
}

func (s *AppsSuite) TestAPerCallTrunkWhoseCallNeverEndedIsSweptAfterSixHours() {
	placed, err := s.callFrom(s.e164)
	s.Require().NoError(err)
	s.Require().Len(s.deployment.Trunks(), 1)
	s.backdate(placed.CallID, 7*time.Hour)

	swept, err := s.service.SweepCallResources(s.ctx, time.Now().Add(-6*time.Hour))

	s.Require().NoError(err)
	s.Positive(swept)
	s.Empty(s.deployment.Trunks())
	s.Empty(s.deployment.Rules())
}

func (s *AppsSuite) TestAPerCallTrunkStreamWouldNotDeleteIsSweptAgainNextTime() {
	placed, err := s.callFrom(s.e164)
	s.Require().NoError(err)
	s.Require().Len(s.deployment.Trunks(), 1)
	trunk := s.deployment.Trunks()[0]
	s.backdate(placed.CallID, 7*time.Hour)
	s.deployment.RateLimit("/sip/inbound_trunks/" + trunk)

	_, err = s.service.SweepCallResources(s.ctx, time.Now().Add(-6*time.Hour))

	s.Require().NoError(err)
	s.Equal([]string{trunk}, s.deployment.Trunks())
	rows, err := s.store.CallResources.Select(s.ctx, store.AppScope{App: 1, Unpinned: true}, placed.CallType, placed.CallID)
	s.Require().NoError(err)
	s.NotEmpty(rows, "the row is what the next sweep finds the trunk by")

	s.deployment.LiftRateLimit("/sip/inbound_trunks/" + trunk)
	_, err = s.service.SweepCallResources(s.ctx, time.Now().Add(-6*time.Hour))

	s.Require().NoError(err)
	s.Empty(s.deployment.Trunks())
	rows, err = s.store.CallResources.Select(s.ctx, store.AppScope{App: 1, Unpinned: true}, placed.CallType, placed.CallID)
	s.Require().NoError(err)
	s.Empty(rows)
}

func (s *AppsSuite) TestAPerCallTrunkOfARecentCallIsNotSwept() {
	placed, err := s.callFrom(s.e164)
	s.Require().NoError(err)
	s.backdate(placed.CallID, time.Hour)

	_, err = s.service.SweepCallResources(s.ctx, time.Now().Add(-6*time.Hour))

	s.Require().NoError(err)
	s.Len(s.deployment.Trunks(), 1)
}

func (s *AppsSuite) TestASharedCallWhoseCallNeverEndedLosesItsRowButNotItsLine() {
	s.sealed()
	placed, err := s.callFrom(s.e164)
	s.Require().NoError(err)
	s.backdate(placed.CallID, 7*time.Hour)

	swept, err := s.service.SweepCallResources(s.ctx, time.Now().Add(-6*time.Hour))

	s.Require().NoError(err)
	s.Positive(swept)
	s.Len(s.deployment.Trunks(), 1, "the number's other calls use it")
	s.Len(s.deployment.Rules(), 1)
	rows, err := s.store.CallResources.Select(s.ctx, store.AppScope{App: 1, Unpinned: true}, placed.CallType, placed.CallID)
	s.Require().NoError(err)
	s.Empty(rows)
}

func (s *AppsSuite) TestMoreNumbersLiningAtOnceThanThePoolHasConnectionsAllLine() {
	s.sealed()
	s.store.GlobalDB().SetMaxOpenConns(3)
	s.T().Cleanup(func() {
		s.store.GlobalDB().SetMaxOpenConns(40)
		s.store.GlobalDB().SetMaxIdleConns(40)
	})
	numbers := []string{s.e164}
	for range 9 {
		numbers = append(numbers, s.anotherNumber(s.customer, s.vendor.vendor))
	}
	// A pool held by waiters deadlocks; the deadline turns that into a failure.
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
	stream, pin, err := s.service.streamFor(ctx, s.customer)
	s.Require().NoError(err)

	var wait sync.WaitGroup
	for _, e164 := range numbers {
		wait.Go(func() {
			held, err := s.store.PhoneNumbers.GetByE164(ctx, s.customer, e164)
			if !s.NoError(err) {
				return
			}
			_, err = s.service.line(ctx, stream, pin, held)
			s.NoError(err)
		})
	}
	wait.Wait()

	trunks := s.deployment.Trunks()
	s.Require().Len(trunks, 1)
	s.ElementsMatch(numbers, s.deployment.TrunkNumbers(trunks[0]))
}

func (s *AppsSuite) TestANumberMarkedAttachedWithNoTrunkIsAttachedAgain() {
	s.sealed()
	_, err := s.store.GlobalDB().NewUpdate().Table("phone_numbers").Set("attached_at = ?", time.Now().UTC()).
		Where("customer_id = ?", s.customer).Where("e164 = ?", s.e164).Exec(s.ctx)
	s.Require().NoError(err)

	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})

	s.Require().NoError(err)
	s.NotEmpty(attached.TrunkID)
	s.Equal([]string{s.e164}, s.deployment.TrunkNumbers(attached.TrunkID))
	s.Equal(attached.TrunkID, s.number().StreamTrunkID)
	s.Equal(s.e164, s.vendor.inbound.E164, "the vendor is told to send calls to the line")
}

func (s *AppsSuite) TestACallFromANumberAttachedInAnotherAppGoesOutOnATrunkOfItsOwn() {
	s.sealed()
	attached, err := s.service.Attach(s.ctx, Attachment{CustomerID: s.customer, E164: s.e164})
	s.Require().NoError(err)
	s.ownApp()
	s.own.OneTrunkPerNumber()

	placed, err := s.callFrom(s.e164)

	s.Require().NoError(err)
	s.Equal([]string{s.e164}, s.deployment.TrunkNumbers(attached.TrunkID), "its callers still reach it")
	held := s.number()
	s.Equal(attached.TrunkID, held.StreamTrunkID)
	s.Zero(held.StreamAppPK)
	rules := s.own.Rules()
	s.Require().Len(rules, 1)
	s.Equal(placed.CallID, s.own.Rule(rules[0])["direct_routing_configs"].(map[string]any)["call_id"],
		"the call's own rule is pinned to it")
}

func (s *AppsSuite) TestACallFromANumberLinedInAnotherAppMovesItsLine() {
	s.sealed()
	_, err := s.callFrom(s.e164)
	s.Require().NoError(err)
	s.ownApp()

	_, err = s.callFrom(s.e164)

	s.Require().NoError(err)
	s.Empty(s.deployment.Trunks(), "the old app's trunk no longer lists it")
	s.Empty(s.deployment.Rules())
	trunks := s.own.Trunks()
	s.Require().Len(trunks, 1)
	s.Equal([]string{s.e164}, s.own.TrunkNumbers(trunks[0]))
	s.Equal(int64(4242), s.number().StreamAppPK)
}

func (s *AppsSuite) TestNoNumberIsGivenOutTwice() {
	given := make(map[string]bool)
	for range 20_000 {
		number := aNumber()
		s.Require().Regexp(`^\+1512[2-9]\d{6}$`, number)
		s.Require().False(given[number], "%s was given out twice", number)
		given[number] = true
	}
}
