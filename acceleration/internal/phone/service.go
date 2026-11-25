package phone

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/activity"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/dlc"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/outcome"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/sipbridge"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
)

// Service is what the CLI and the HTTP API both talk to.
//
// A number is three things at once: a purchase at a vendor, a trunk at Stream and a row
// here. Doing them one at a time from two callers would eventually leave a number that is
// bought but unreachable, so the order lives in one place.
type Service struct {
	registry *Registry
	store    *store.Store
	groups   groupLocks
	stream   *Stream
	apps     Apps
	// Calls through a customer's own trunk are off unless both sealer and sipTrunks are
	// set: they need the key its password is sealed under and the provider that dials it.
	sealer    *auth.Sealer
	sipTrunks Provider
	recorder  *routing.Recorder
	gate      *dlc.Gate
	publicURL string
	// outcomes keeps how each placed call ended. Nil without a store.
	outcomes *outcome.Keeper
	// unanswered ends the session of a placed call nobody will answer. Nil leaves it to the
	// session's start timeout.
	unanswered func(ctx context.Context, customerID, sessionID string)
	// machine is told who picked up a placed call.
	machine func(ctx context.Context, customerID, sessionID string, machine Machine)
	clock   func() time.Time
	logger  *slog.Logger
	// heard is the calls whose participant events this process recorded lately (Heard), and
	// unhooked the apps it lately logged as not delivering them.
	heard, unhooked recently
	// unopened are the legs OpenLeg could not write, for the leg sweep (RunLegSweep) to.
	unopened unopenedLegs
	// connecting, when set, runs in Connect between reading the phone's kept leave and
	// connecting its leg, where a leave delivered at the same moment lands.
	connecting func()
	// agentJoinTimeout is how long an answered outbound leg waits before the agent check
	// (awaitAgent). Zero turns the check off. agentWait is the timer of that wait.
	agentJoinTimeout time.Duration
	agentWait        func(time.Duration) <-chan time.Time
	// closing is closed by Close, which stops the agent checks still waiting.
	closing   chan struct{}
	closeOnce sync.Once
}

// ServiceOptions configures a Service. Only the registry is required: without a store
// nothing is remembered, and without Stream a number can be bought but not attached.
type ServiceOptions struct {
	Registry *Registry
	Store    *store.Store
	Stream   *Stream
	// Apps answers which Stream app each customer's lines live in. When it is set, Stream
	// is not used.
	Apps Apps
	// Sealer seals the passwords of customers' own SIP trunks. Without it those trunks
	// are off, rather than their passwords stored in the clear.
	Sealer *auth.Sealer
	// SIPTrunks dials numbers on customers' own SIP trunks. It is kept out of the
	// registry, which is what every customer is offered to buy from.
	SIPTrunks Provider
	// Recorder files what telephony costs as request rows, beside what the models cost: each
	// month of a number, each call placed, and the minutes of every leg of a call.
	Recorder *routing.Recorder
	// Gate refuses a call to somebody who opted out, or past what the sandbox allows, and,
	// when it is charging, a call or a number the organization may not pay for.
	Gate *dlc.Gate
	// PublicURL is where this service is reachable from the internet, which the three
	// vendors that fetch a call plan on answer need in order to fetch it. Without it those
	// vendors say so rather than placing a call nothing will answer.
	PublicURL string
	// Now is the clock call legs are timed by. Nil is the wall clock.
	Now func() time.Time
	// AgentJoinTimeout is how long the person who answered a placed call waits for someone
	// else to be in the call. After it, a call with only the person in it is ended. Zero turns
	// the check off, as phone.agent_join_timeout 0 does.
	AgentJoinTimeout time.Duration
	Logger           *slog.Logger
}

// NewService returns a Service.
func NewService(options ServiceOptions) (*Service, error) {
	if options.Registry == nil {
		return nil, errors.New("phone: a vendor registry is required")
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}

	service := &Service{
		registry:  options.Registry,
		store:     options.Store,
		stream:    options.Stream,
		apps:      options.Apps,
		sealer:    options.Sealer,
		sipTrunks: options.SIPTrunks,
		recorder:  options.Recorder,
		gate:      options.Gate,
		publicURL: strings.TrimSuffix(options.PublicURL, "/"),
		clock:     options.Now,
		logger:    options.Logger,

		agentJoinTimeout: options.AgentJoinTimeout,
		agentWait:        time.After,
		closing:          make(chan struct{}),
	}
	if options.Store != nil {
		vendors := []string{SIPTrunkVendor}
		for _, vendor := range options.Registry.Vendors() {
			vendors = append(vendors, vendor.Vendor)
		}
		service.outcomes = outcome.NewKeeper(options.Store, service.now, vendors, options.Logger)
	}
	return service, nil
}

// Outcomes keeps how each placed call ended. Nil without a store.
func (s *Service) Outcomes() *outcome.Keeper { return s.outcomes }

// outcomeOf is the outcome a vendor's result is. It is the one place a vendor's word for a
// call meets the router's: an empty result is a report that is not final, and so no outcome.
func outcomeOf(result CallResult) (outcome.Outcome, error) {
	switch result {
	case "":
		return "", nil
	case CallCompleted:
		return outcome.Completed, nil
	case CallBusy:
		return outcome.Busy, nil
	case CallNoAnswer:
		return outcome.NoAnswer, nil
	case CallFailed:
		return outcome.Failed, nil
	case CallCanceled:
		return outcome.Canceled, nil
	}
	return "", stack.Wrap(fmt.Errorf("phone: no outcome for the call result %q", result))
}

// Apps says which Stream app a customer's phone lines live in. A trunk and its routing rule
// have to be in the same app as the call the agent joins, which is the customer's.
type Apps interface {
	// For is the app new lines for a customer are made in, with its pin.
	For(ctx context.Context, customer string) (*Stream, int64, error)
	// ForApp is the app a line already made is in, to finish it there.
	ForApp(ctx context.Context, customer string, app int64) (*Stream, error)
	// ForAppRemoving is the app a line already made is in, to remove it there. It reaches
	// wherever reading that app does: a line in the deployment's app that the customer may
	// no longer add to is still the router's own to take down, and one left there is billed.
	ForAppRemoving(ctx context.Context, customer string, app int64) (*Stream, error)
	// Placed is the pin of the app For makes new lines for a customer in, read without acting
	// for the customer, so one customer's request can ask where another's lines go.
	Placed(ctx context.Context, customer string) (int64, error)
}

// ErrNoStream is a service with no Stream app to make lines in: a deployment without Stream
// credentials.
var ErrNoStream = errors.New("phone: no stream credentials")

// Suspended returns ErrSuspended for an app Stream staff stopped from using the phone. A
// suspension that cannot be read refuses too: it is there to stop fraud that costs money.
func (s *Service) Suspended(ctx context.Context, customerID string) error {
	if s.store == nil {
		return nil
	}
	_, err := s.store.TelephonySuspensions.Get(ctx, customerID)
	if errors.Is(err, store.ErrNotSuspended) {
		return nil
	}
	if err != nil {
		return err
	}
	return stack.Wrap(ErrSuspended)
}

// Admit refuses a customer whose organization may not pay for telephony now, as the gate every
// call passes would, so work that would place calls can be refused before it starts.
func (s *Service) Admit(ctx context.Context, customerID string) error {
	return s.gate.Admit(ctx, customerID)
}

// EndCall ends a Stream call one of the customer's numbers brought somebody into, in the app
// the number's lines are in. Stream's SIP bridge has answered the caller by the time the call's
// session starts, so a call nobody may take is ended rather than left ringing.
func (s *Service) EndCall(ctx context.Context, customer string, app int64, callType, callID string) error {
	stream, err := s.streamForApp(ctx, customer, app)
	if err != nil {
		return err
	}
	return stream.EndCall(ctx, callType, callID)
}

// streamFor is the app new lines for a customer are made in, and its pin.
func (s *Service) streamFor(ctx context.Context, customer string) (*Stream, int64, error) {
	if s.apps != nil {
		return s.apps.For(ctx, customer)
	}
	if s.stream == nil {
		return nil, 0, ErrNoStream
	}
	return s.stream, 0, nil
}

// placed is the pin of the app new lines for a customer are made in, read without acting for it.
func (s *Service) placed(ctx context.Context, customer string) (int64, error) {
	if s.apps != nil {
		return s.apps.Placed(ctx, customer)
	}
	if s.stream == nil {
		return 0, ErrNoStream
	}
	return 0, nil
}

// settled reports whether placed answered where a customer's lines are made: a pin, or a
// refusal that holds until the customer's app or the deployment changes. Any other failure,
// such as a database read, is no answer.
func settled(err error) bool {
	return errors.Is(err, ErrNoStream) || streamapp.Settled(err)
}

// streamForApp is the app a line already made is in.
func (s *Service) streamForApp(ctx context.Context, customer string, app int64) (*Stream, error) {
	if s.apps != nil {
		return s.apps.ForApp(ctx, customer, app)
	}
	if s.stream == nil {
		return nil, ErrNoStream
	}
	return s.stream, nil
}

// streamToRemove is the app a line already made is in, to remove it there.
func (s *Service) streamToRemove(ctx context.Context, customer string, app int64) (*Stream, error) {
	if s.apps != nil {
		return s.apps.ForAppRemoving(ctx, customer, app)
	}
	if s.stream == nil {
		return nil, ErrNoStream
	}
	return s.stream, nil
}

// unwire removes a trunk and routing rule from the app they were made in. It is
// best-effort: a cleanup failure is logged rather than returned, so it cannot mask what
// the caller was doing.
func (s *Service) unwire(ctx context.Context, customer string, app int64, routeID, trunkID, why string) {
	if routeID == "" && trunkID == "" {
		return
	}
	stream, err := s.streamToRemove(ctx, customer, app)
	if err != nil {
		s.logger.Error("could not reach the Stream app a line was made in", "why", why,
			"trunk", trunkID, "route", routeID, "error", err)
		return
	}
	s.deleteLines(ctx, stream, routeID, trunkID, "", why)
}

// deleteLines deletes a routing rule and the trunk beside it from a Stream app, with what other
// rules hold the trunk: the number's, or any number's without one (deleteTrunkWithRules). It is
// best-effort: a failure is logged, never returned, so it cannot mask what is being undone.
func (s *Service) deleteLines(ctx context.Context, stream *Stream, routeID, trunkID, e164, why string) {
	if err := stream.DeleteRoute(ctx, routeID); err != nil {
		s.logger.Error("could not delete a routing rule", "why", why, "route", routeID, "error", err)
	}
	if err := stream.deleteTrunkWithRules(ctx, trunkID, e164); err != nil {
		s.logger.Error("could not delete a trunk", "why", why, "trunk", trunkID, "error", err)
	}
}

// Registry is the vendors this service knows about.
func (s *Service) Registry() *Registry { return s.registry }

// Search returns the numbers one vendor is offering.
func (s *Service) Search(ctx context.Context, vendor string, search Search) ([]Available, error) {
	if err := sold(search.Country); err != nil {
		return nil, err
	}
	provider, err := s.registry.Open(vendor)
	if err != nil {
		return nil, err
	}
	return provider.SearchNumbers(ctx, search)
}

// Skip is a vendor that was asked nothing, and why.
type Skip struct {
	Vendor string
	// Reason is in the same words an error would use, because to the caller it is one:
	// this vendor's inventory is missing from the answer.
	Reason string
	// Err is the error behind a vendor that failed, for a caller that must not repeat its
	// words. It is nil for a vendor that was not asked.
	Err error
}

// Offers is what the vendors between them have for sale, and which of them did not answer.
//
// The skipped list is part of the result rather than a log line: a search for a Colorado
// number that reached two of eight vendors found what two vendors had, and a caller deciding
// whether to buy needs to know which.
type Offers struct {
	Numbers []Available
	Skipped []Skip
}

// SearchAll asks every usable vendor at once and merges what they offer.
//
// A vendor whose API cannot express one of the filters is skipped rather than asked, because
// dropping the filter would answer a search for Colorado with numbers from Ohio. Capabilities
// are different: every vendor reports what a number carries even when it cannot filter on it,
// so those are checked here on the results.
func (s *Service) SearchAll(ctx context.Context, search Search) (Offers, error) {
	if err := sold(search.Country); err != nil {
		return Offers{}, err
	}
	usable := s.registry.Available()
	if len(usable) == 0 {
		return Offers{}, stack.Wrap(NotConfigured(errors.New("phone: no vendor has the credentials to be searched")))
	}

	type answer struct {
		offered []Available
		skip    *Skip
	}
	answers := make([]answer, len(usable))

	var group sync.WaitGroup
	for index, name := range usable {
		provider, err := s.registry.Open(name)
		if err != nil {
			answers[index] = answer{skip: &Skip{Vendor: name, Reason: err.Error()}}
			continue
		}
		if missing := search.Unsupported(provider); len(missing) > 0 {
			answers[index] = answer{skip: &Skip{
				Vendor: name,
				Reason: "cannot search by " + join(missing),
			}}
			continue
		}

		group.Add(1)
		go func() {
			defer group.Done()
			offered, err := provider.SearchNumbers(ctx, search)
			if err != nil {
				answers[index] = answer{skip: &Skip{Vendor: name, Reason: err.Error(), Err: err}}
				return
			}
			answers[index] = answer{offered: offered}
		}()
	}
	group.Wait()

	var offers Offers
	for _, each := range answers {
		if each.skip != nil {
			offers.Skipped = append(offers.Skipped, *each.skip)
			continue
		}
		for _, offer := range each.offered {
			if search.Matches(offer) {
				offers.Numbers = append(offers.Numbers, offer)
			}
		}
	}

	// Cheapest first is the order a buyer wants; the number breaks the tie so the same
	// search twice gives the same answer, and a vendor that quotes no price is not
	// thereby made to look free.
	slices.SortFunc(offers.Numbers, func(a, b Available) int {
		if a.MonthlyCostMicros != b.MonthlyCostMicros {
			return cmp.Compare(quoted(a.MonthlyCostMicros), quoted(b.MonthlyCostMicros))
		}
		return cmp.Compare(a.E164, b.E164)
	})
	return offers, nil
}

// quoted sorts an unquoted price last rather than first, since zero here means the vendor
// did not say what it costs.
func quoted(micros int64) int64 {
	if micros == 0 {
		return math.MaxInt64
	}
	return micros
}

// join renders filters for an error message.
func join(filters []Filter) string {
	rendered := make([]string, 0, len(filters))
	for _, filter := range filters {
		rendered = append(rendered, string(filter))
	}
	return strings.Join(rendered, " or ")
}

// Purchase is a number to buy on a customer's behalf.
type Purchase struct {
	Vendor string
	E164   string
	// Country is the inventory the number was offered from, an ISO 3166-1 alpha-2 code.
	// Only some vendors need it, and the search that found the number reported it.
	Country string
	Owner   routing.Owner
}

// ErrCallHook says the app's call hook could not be pointed at this router.
var ErrCallHook = errors.New("phone: call hook")

// pointCallHook makes the app deliver its call events to this router, when the router knows
// where it is reachable from.
func (s *Service) pointCallHook(ctx context.Context, stream *Stream, app int64) error {
	if s.publicURL == "" {
		return nil
	}
	if _, err := stream.EnsureCallHook(ctx, s.publicURL, app); err != nil {
		return stack.Wrap(fmt.Errorf("%w: %w", ErrCallHook, err))
	}
	return nil
}

// unverifiedTier is the tier stream-accelerate names an organization whose
// core_organization.agents_verified is not true (appstore.go). Tiers are otherwise opaque here;
// an empty one, as a router not behind that proxy sees, is not refused.
const unverifiedTier = "not-verified"

// Buy buys a number and records that the customer now holds it.
//
// The vendor is the side that costs money, so it goes before the number's row: a row without a
// number would be a lie. An order kept before the vendor is asked makes a number whose row is
// not written one the sweep records (SweepOrders), rather than one paid for and held by nobody.
func (s *Service) Buy(ctx context.Context, purchase Purchase) (store.PhoneNumber, error) {
	if purchase.Owner.CustomerID == "" {
		return store.PhoneNumber{}, stack.Wrap(errors.New("phone: a number must belong to a customer"))
	}
	// Refused before anything is changed or paid for, since a number in another form would not be
	// recorded.
	if !store.IsE164(purchase.E164) {
		return store.PhoneNumber{}, stack.Wrap(fmt.Errorf("phone: %w: %q", store.ErrNotE164, purchase.E164))
	}
	// The number's own country, not only the one the request names, since +1 is also Jamaica,
	// Puerto Rico and the Bahamas.
	if purchase.Country != "" {
		if err := sold(purchase.Country); err != nil {
			return store.PhoneNumber{}, err
		}
	}
	if err := sold(Country(purchase.E164)); err != nil {
		return store.PhoneNumber{}, err
	}
	if err := s.Suspended(ctx, purchase.Owner.CustomerID); err != nil {
		return store.PhoneNumber{}, err
	}
	if purchase.Owner.OrganizationTier == unverifiedTier {
		return store.PhoneNumber{}, stack.Wrap(ErrVerificationRequired)
	}
	// A number is charged every month from the day it is bought, so an organization that may
	// not spend money does not buy one.
	if err := s.gate.Admit(ctx, purchase.Owner.CustomerID); err != nil {
		return store.PhoneNumber{}, err
	}
	// An app Stream blocked could not use a number, so it does not get to pay for one.
	stream, app, err := s.streamFor(ctx, purchase.Owner.CustomerID)
	if errors.Is(err, streamapp.ErrStreamAppDisconnected) {
		return store.PhoneNumber{}, stack.Wrap(err)
	}
	// An app whose calls could not reach the router would hold a number nobody can call.
	if err == nil {
		if err := s.pointCallHook(ctx, stream, app); err != nil {
			return store.PhoneNumber{}, err
		}
	}
	provider, err := s.registry.Open(purchase.Vendor)
	if err != nil {
		return store.PhoneNumber{}, err
	}

	// Some vendors say what a number costs when it is offered and not when it is bought, so
	// the offer is read first, while the number is still for sale.
	quoted := s.quote(ctx, provider, purchase)
	// The first month is paid out of credit booked now, so a number is never bought on credit
	// that does not cover it.
	booked, err := s.bookFirstMonth(ctx, purchase, quoted)
	if err != nil {
		return store.PhoneNumber{}, err
	}
	// To the microsecond Postgres keeps, so a month billed now and one billed from the stored
	// row name the same purchase.
	purchasedAt := s.now().UTC().Truncate(time.Microsecond)
	order := store.PhoneNumberOrder{
		CustomerID: purchase.Owner.CustomerID, Vendor: purchase.Vendor, E164: purchase.E164, Action: store.OrderBuy,
		Country: cmp.Or(purchase.Country, Country(purchase.E164)), MonthlyCostMicros: quoted, Tags: purchase.Owner.Tags,
		CreatedAt: purchasedAt, NextAttemptAt: purchasedAt.Add(orderSettles),
	}
	if s.store != nil {
		if err := s.store.PhoneNumberOrders.Insert(ctx, &order); err != nil {
			booked.Release()
			return store.PhoneNumber{}, err
		}
	}
	// A failed purchase is a row of its own; a bought number is billed by its months, the
	// first of them below.
	buying := routing.Meter{
		Recorder: s.recorder,
		Config:   routing.ProviderConfig{Provider: purchase.Vendor, Model: ModelNumber},
		Owner:    purchase.Owner,
		Code:     vendorFailed,
		Omit:     func(pricing.Quantities) bool { return true },
	}
	bought, err := routing.Call(ctx, buying, func(ctx context.Context) (Number, error) {
		return provider.BuyNumber(ctx, Order{E164: purchase.E164, Country: purchase.Country})
	})
	if err != nil {
		booked.Release()
		s.forgetRefusedOrder(ctx, order, err)
		return store.PhoneNumber{}, err
	}
	if bought.MonthlyCostMicros == 0 {
		bought.MonthlyCostMicros = quoted
	}

	held := store.PhoneNumber{
		E164:              bought.E164,
		Vendor:            bought.Vendor,
		Country:           bought.Country,
		Capabilities:      names(bought.Capabilities),
		MonthlyCostMicros: bought.MonthlyCostMicros,
		CustomerID:        purchase.Owner.CustomerID,
		Tags:              purchase.Owner.Tags,
		VendorID:          bought.VendorID,
		PurchasedAt:       purchasedAt,
	}
	// The month it was bought in is charged at once, whether or not the row below is kept.
	s.billMonth(held, purchase.Owner, held.PurchasedAt)
	booked.Charged()
	if s.store == nil {
		return held, nil
	}
	recorded, err := s.store.PhoneNumberOrders.Bought(ctx, order.ID, &held)
	if err == nil && !recorded {
		// A sweep took the order first, and recorded the number or found the vendor without it.
		err = errors.New("its order was settled by the sweep")
	}
	if err != nil {
		// The number is bought either way, so saying so is more useful than failing and
		// leaving the caller thinking it was not. Its order is kept for the sweep.
		// Support finds the number at the vendor by its order, and the log holds no number.
		return held, stack.Wrap(fmt.Errorf("phone: a number bought from %s as %q was not recorded: %w",
			held.Vendor, held.VendorID, err))
	}
	return held, nil
}

// Release gives a number back and stops the monthly charge.
func (s *Service) Release(ctx context.Context, customerID, e164 string) error {
	held, err := s.number(ctx, customerID, e164, "releasing")
	if err != nil {
		return err
	}
	// Callers stop first: a vendor that refuses fails the release before anything changes,
	// so it can be tried again.
	if err := s.detach(ctx, held); err != nil {
		return err
	}
	// Off its trunk first: a trunk Stream would not delete fails the release before the
	// number is given back, so the release can be tried again.
	if err := s.unline(ctx, held, "released a number", time.Time{}); err != nil {
		return err
	}
	seller, err := s.seller(held.Vendor)
	if err != nil {
		return err
	}
	releasedAt := time.Now().UTC()
	var order store.PhoneNumberOrder
	// A number on the customer's own trunk was never bought here, so there is nobody to
	// give it back to.
	if seller != nil {
		// Kept before the vendor is asked, so a number given back whose row is not released
		// below is released by the sweep (SweepOrders), rather than billed every month.
		order = store.PhoneNumberOrder{CustomerID: held.CustomerID, Vendor: held.Vendor, E164: e164, Action: store.OrderRelease,
			CreatedAt: releasedAt, NextAttemptAt: s.now().Add(orderSettles)}
		if err := s.store.PhoneNumberOrders.Insert(ctx, &order); err != nil {
			return err
		}
		if err := seller.ReleaseNumber(ctx, e164); err != nil {
			s.forgetRefusedOrder(ctx, order, err)
			if age := s.now().Sub(held.PurchasedAt); age < NumberSettles && errors.As(err, new(*VendorError)) {
				return stack.Wrap(fmt.Errorf("%w: %s was bought %s ago: %w", ErrNumberSettling, e164, age.Round(time.Second), err))
			}
			return err
		}
	}
	// The trunk and rule the number was attached with would otherwise outlive it, billed
	// and pointing a number nobody holds at a call.
	if err := s.unline(ctx, held, "released a number", releasedAt); err != nil {
		return err
	}
	s.forgetOrder(ctx, order)
	return nil
}

// forgetRefusedOrder forgets an order the vendor answered err to, when err shows the vendor did
// nothing: a 4xx, which it did not carry out, or a refusal before it was asked, by a vendor not
// implemented or missing a setting, or of an order it cannot take as asked. Any other failure,
// a timeout, a dropped connection or a 5xx, may come after the vendor bought or released the
// number, so the order is left to the sweep, which asks the vendor what it holds.
func (s *Service) forgetRefusedOrder(ctx context.Context, order store.PhoneNumberOrder, err error) {
	refused, ok := errors.AsType[*VendorError](err)
	if ok && refused.Status >= 400 && refused.Status < 500 ||
		errors.Is(err, ErrNotImplemented) || errors.Is(err, ErrNotConfigured) || errors.Is(err, ErrInvalid) {
		s.forgetOrder(ctx, order)
	}
}

// forgetOrder forgets an order whose outcome is recorded, or that the vendor refused. One that
// cannot be forgotten is left to the sweep, which asks the vendor again.
func (s *Service) forgetOrder(ctx context.Context, order store.PhoneNumberOrder) {
	if s.store == nil || order.ID == 0 {
		return
	}
	if err := s.store.PhoneNumberOrders.Delete(context.WithoutCancel(ctx), order.ID); err != nil {
		s.logger.Warn("could not forget a number's order; the sweep settles it", "order", order.ID, "error", err)
	}
}

// orderSettles is how old an order is before the sweep settles it: far longer than a purchase
// or a release takes, so one still running is never settled under it. orderRetry is how long a
// sweep holds the orders it claimed, and so when one it could not settle is tried again, by any
// router. orderSweepBatch bounds how many one sweep claims.
const (
	orderSettles    = 10 * time.Minute
	orderRetry      = time.Hour
	orderSweepBatch = 100
)

// SweepOrders settles the purchases and releases of numbers whose outcome was not recorded,
// by asking each vendor whether it holds the number: one bought and held gets its row, and its
// first month if the purchase did not bill it; one given back and no longer held is released,
// as of when it was. Any other order changed nothing and is forgotten. An order whose vendor
// cannot say is logged for an operator and kept, to be tried again after orderRetry behind the
// orders due before then. Every router sweeps, and each order is claimed by one of them at a
// time. It reports how many it settled.
func (s *Service) SweepOrders(ctx context.Context) (int, error) {
	if s.store == nil {
		return 0, nil
	}
	orders, err := s.store.PhoneNumberOrders.Claim(ctx, orderRetry, orderSweepBatch)
	if err != nil {
		return 0, err
	}
	settled := 0
	for _, order := range orders {
		if err := s.settleOrder(ctx, order); err != nil {
			s.logger.Error("could not settle a number's purchase or release", "order", order.ID, "action", order.Action,
				"vendor", order.Vendor, "customer", order.CustomerID, "error", err)
			continue
		}
		settled++
	}
	return settled, nil
}

// settleOrder settles one order by what its vendor holds, and forgets it.
func (s *Service) settleOrder(ctx context.Context, order store.PhoneNumberOrder) error {
	provider, err := s.registry.Open(order.Vendor)
	if err != nil {
		return err
	}
	finder, ok := provider.(NumberFinder)
	if !ok {
		return stack.Wrap(fmt.Errorf("phone: %s cannot say whether it holds a number, so the order is settled by hand", order.Vendor))
	}
	vendorID, held, err := finder.HoldsNumber(ctx, order.E164)
	if err != nil {
		return err
	}
	switch {
	case order.Action == store.OrderBuy && held:
		return s.recordBought(ctx, order, vendorID)
	case order.Action == store.OrderRelease && !held:
		number, err := s.store.PhoneNumbers.GetByE164(ctx, order.CustomerID, order.E164)
		if err != nil && !errors.Is(err, store.ErrNoNumber) {
			return err
		}
		// Only the row the order gave back: the number may have been bought again since, or put
		// on the customer's own trunk.
		if err == nil && number.Vendor == order.Vendor && !number.PurchasedAt.After(order.CreatedAt) {
			if err := s.unline(ctx, number, "released a number", order.CreatedAt); err != nil {
				return err
			}
			s.logger.Warn("released a number given back whose release was not recorded", "order", order.ID,
				"vendor", order.Vendor, "customer", order.CustomerID)
		}
	}
	return s.store.PhoneNumberOrders.Delete(ctx, order.ID)
}

// recordBought writes the row of a number an order bought, from the order, and bills its first
// month unless the purchase did. A row that already holds the number, another purchase's, leaves
// nothing to record.
func (s *Service) recordBought(ctx context.Context, order store.PhoneNumberOrder, vendorID string) error {
	number := store.PhoneNumber{
		E164: order.E164, Vendor: order.Vendor, Country: order.Country, MonthlyCostMicros: order.MonthlyCostMicros,
		CustomerID: order.CustomerID, Tags: order.Tags, VendorID: vendorID, PurchasedAt: order.CreatedAt,
	}
	month := numberMonth(number, number.PurchasedAt).UsageID
	unbilled, err := s.store.UsageToBill(ctx, []uuid.UUID{month})
	if err != nil {
		return err
	}
	bought, err := s.store.PhoneNumberOrders.Bought(ctx, order.ID, &number)
	if errors.Is(err, store.ErrNumberRecorded) {
		return s.store.PhoneNumberOrders.Delete(ctx, order.ID)
	}
	if err != nil || !bought {
		return err
	}
	s.logger.Warn("recorded a number bought whose row was not written", "order", order.ID, "vendor", order.Vendor,
		"vendor_id", vendorID, "customer", order.CustomerID)
	if unbilled[month] {
		s.billMonth(number, routing.Owner{
			CustomerID: number.CustomerID, Attribution: routing.Attribution{APIKey: OriginNumberRental}, Tags: number.Tags,
		}, number.PurchasedAt)
	}
	return nil
}

// Detach stops a number's callers reaching the agent. The number keeps its trunk and rule,
// so its calls still go out on them and attaching it again picks them up.
func (s *Service) Detach(ctx context.Context, customerID, e164 string) error {
	held, err := s.number(ctx, customerID, e164, "detaching")
	if err != nil {
		return err
	}
	return s.detach(ctx, held)
}

// number is a number the customer holds, looked up for doing something to it.
func (s *Service) number(ctx context.Context, customerID, e164, doing string) (store.PhoneNumber, error) {
	if s.store == nil {
		return store.PhoneNumber{}, stack.Wrap(fmt.Errorf("phone: %s a number needs a database to know who holds it", doing))
	}
	return s.store.PhoneNumbers.GetByE164(ctx, customerID, e164)
}

// detach stops the vendor sending a held number's callers, then forgets it was attached.
func (s *Service) detach(ctx context.Context, held store.PhoneNumber) error {
	seller, err := s.seller(held.Vendor)
	if err != nil {
		return err
	}
	// The vendor first: one that refuses still sends callers, so the number stays attached
	// for them and the detach can be tried again. A number on the customer's own trunk was
	// never pointed anywhere by a vendor here.
	if stopper, ok := seller.(InboundStopper); ok {
		if err := stopper.StopInbound(ctx, held.E164); err != nil {
			return err
		}
	}
	group := store.TrunkGroup{CustomerID: held.CustomerID, StreamAppPK: held.StreamAppPK, Vendor: held.Vendor}
	return s.inTrunkGroup(ctx, group, func(ctx context.Context, tx *store.TrunkGroupTx) error {
		number, err := tx.Number(held.E164)
		if err != nil {
			return err
		}
		if number.AttachedAt == nil {
			return nil
		}
		return tx.Detach(held.E164)
	})
}

// seller is the vendor that sold a number, or none for one on the customer's own trunk,
// which was never bought here.
func (s *Service) seller(vendor string) (Provider, error) {
	if vendor == SIPTrunkVendor {
		return nil, nil
	}
	return s.registry.Open(vendor)
}

// unline takes a number off its trunk and deletes its rule, in the app they are in, and
// forgets them on the number. With a releasedAt it also gives the number back, as of then,
// under the same lock, so a line made for it meanwhile cannot put it back on a trunk. A Stream
// failure is logged, not returned, so the number is still taken off, save a trunk Stream would
// not delete (see offTrunk).
func (s *Service) unline(ctx context.Context, held store.PhoneNumber, why string, releasedAt time.Time) error {
	app := held.StreamAppPK
	// A call or attach lining the number in another app while this waited for its lock
	// moves it there, and a release follows it.
	for range 3 {
		var moved bool
		group := store.TrunkGroup{CustomerID: held.CustomerID, StreamAppPK: app, Vendor: held.Vendor}
		// Found before the lock: reaching the app may read the database, which under the lock
		// would take a second pooled connection.
		stream, reached := s.streamToRemove(ctx, held.CustomerID, app)
		err := s.inTrunkGroup(ctx, group, func(ctx context.Context, tx *store.TrunkGroupTx) error {
			number, err := tx.Number(held.E164)
			if err != nil {
				return err
			}
			if number.StreamTrunkID != "" && number.StreamAppPK != app {
				moved, app = true, number.StreamAppPK
				return nil
			}
			if number.StreamTrunkID != "" {
				if err := s.offTrunk(ctx, tx, stream, reached, number, why, false); err != nil {
					return err
				}
			}
			if !releasedAt.IsZero() {
				return tx.ReleaseNumber(number.E164, releasedAt)
			}
			return nil
		})
		if err != nil || !moved || releasedAt.IsZero() {
			return err
		}
	}
	return stack.Wrap(fmt.Errorf("phone: %s kept moving between apps while it was released", held.E164))
}

// offTrunk takes a number off its trunk and deletes its rule. The trunk is deleted with its
// last number; one the number had to itself (StreamTrunk.Own), or one without a row, is
// deleted with it as best it can be. A shared trunk Stream would not delete keeps its row,
// which holds its password and is what a retry deletes it by. With must, anything else Stream
// does not do fails it too, and the rows stay as they were, for a caller that needs the number
// gone from Stream: forgotten here and left there, nothing would take it down.
func (s *Service) offTrunk(ctx context.Context, tx *store.TrunkGroupTx, stream *Stream, reached error, number store.PhoneNumber, why string, must bool) error {
	if reached != nil {
		stream = nil
		s.logger.Error("could not reach the Stream app a line was made in", "why", why,
			"trunk", number.StreamTrunkID, "route", number.StreamRouteID, "error", reached)
	}
	trunks, err := tx.Trunks()
	if err != nil {
		return err
	}
	i := slices.IndexFunc(trunks, func(t store.StreamTrunk) bool { return t.ID == number.StreamTrunkID })
	if i < 0 || trunks[i].Own {
		switch {
		case stream != nil && must:
			if err := stream.DeleteRoute(ctx, number.StreamRouteID); err != nil {
				return err
			}
			// Without a row other numbers may be on the trunk yet, and keep their rules.
			if err := stream.deleteTrunkWithRules(ctx, number.StreamTrunkID, number.E164); err != nil {
				return err
			}
		case stream != nil:
			s.deleteLines(ctx, stream, number.StreamRouteID, number.StreamTrunkID, number.E164, why)
		}
		if i >= 0 {
			if err := tx.DeleteTrunk(number.StreamTrunkID); err != nil {
				return err
			}
		}
		return tx.ClearLine(number.E164)
	}
	if stream != nil {
		if err := stream.DeleteRoute(ctx, number.StreamRouteID); err != nil {
			if must {
				return err
			}
			s.logger.Error("could not delete a routing rule", "why", why, "route", number.StreamRouteID, "error", err)
		}
	}
	trunk := trunks[i]
	trunk.Numbers = slices.DeleteFunc(slices.Clone(trunk.Numbers), func(n string) bool { return n == number.E164 })
	if len(trunk.Numbers) == 0 {
		if stream != nil {
			if err := stream.deleteTrunkWithRules(ctx, trunk.ID, ""); err != nil {
				return err
			}
		}
		if err := tx.DeleteTrunk(trunk.ID); err != nil {
			return err
		}
		return tx.ClearLine(number.E164)
	}
	allowedIPs, err := s.allowlistFor(number.Vendor)
	if err != nil {
		return err
	}
	// Stored as it should be either way: the trunk's next update resends its numbers,
	// which mends one Stream failed to take off.
	if stream != nil {
		err := stream.UpdateTrunk(ctx, trunk.ID, Trunk{
			Name: sharedTrunkName(number.Vendor, number.CustomerID), Numbers: trunk.Numbers, AllowedIPs: allowedIPs,
		})
		// A trunk Stream no longer has lists the number nowhere.
		if err != nil && must && !notFound(err) {
			return err
		}
		if err != nil {
			s.logger.Error("could not take a number off a trunk", "why", why, "trunk", trunk.ID, "error", err)
		}
	}
	if err := tx.SaveTrunk(trunk); err != nil {
		return err
	}
	return tx.ClearLine(number.E164)
}

// Attachment points a number at the agent: every caller lands in a call of their own,
// agent:<uuid>, and the session answering them is opened under that uuid.
type Attachment struct {
	CustomerID string
	E164       string
	// AllowedIPs are the vendor's signalling addresses. Empty accepts calls from anywhere
	// that has the trunk password.
	AllowedIPs []string
}

// Attached is what a number was connected to.
type Attached struct {
	TrunkID string
	RouteID string
	Bridge  Bridge
}

// NumberKey is the custom field an inbound call carries the number it rang under, which is
// how the call that arrives is traced back to the number: its name is the session's.
const NumberKey = "phone_number"

// ErrNoSealer says this deployment has no keyring to seal or open a number's trunk password.
var ErrNoSealer = errors.New("phone: no key to seal a number's trunk password: set ROUTER_DEV_KEK (local), or ROUTER_AUTH_KEK_V1_WRAPPED with ROUTER_AUTH_KMS_KEY (hosted)")

// ErrNoSealerToCall says this deployment has no keyring to seal the trunk password a vendor
// fetches on answer, so a call on that vendor cannot be placed.
var ErrNoSealerToCall = errors.New("phone: no key to seal the trunk password the vendor fetches on answer, so the call cannot be placed: set ROUTER_DEV_KEK (local), or ROUTER_AUTH_KEK_V1_WRAPPED with ROUTER_AUTH_KMS_KEY (hosted)")

// Attach puts a number on the trunk it shares with the customer's other numbers of its
// vendor, with a rule of its own, and tells the vendor to send calls there. This is what
// turns a bought number into one that reaches an agent. Without a key to seal the trunk's
// password, the number gets a trunk of its own. Attaching a number already attached in this
// app changes nothing and answers what it has.
func (s *Service) Attach(ctx context.Context, attachment Attachment) (Attached, error) {
	if s.store == nil {
		return Attached{}, stack.Wrap(errors.New("phone: attaching a number needs a database to know who holds it"))
	}

	held, err := s.store.PhoneNumbers.GetByE164(ctx, attachment.CustomerID, attachment.E164)
	if err != nil {
		return Attached{}, stack.Wrap(err)
	}
	if held.Vendor == SIPTrunkVendor {
		return Attached{}, stack.Wrap(Invalid(errors.New("phone: inbound calls to a number on the customer's own sip trunk are not supported")))
	}
	stream, pin, err := s.streamFor(ctx, attachment.CustomerID)
	if errors.Is(err, ErrNoStream) {
		return Attached{}, stack.Wrap(marked{errors.New("phone: attaching a number needs stream credentials"), ErrNoStream})
	}
	if err != nil {
		return Attached{}, err
	}
	provider, err := s.registry.Open(held.Vendor)
	if err != nil {
		return Attached{}, stack.Wrap(err)
	}
	declared, _ := s.registry.Lookup(held.Vendor)
	// Refused before anything is made: attached, the number would look fine and never answer.
	if declared.AsksForTrunk && s.sealer == nil {
		return Attached{}, stack.Wrap(fmt.Errorf("phone: %s numbers are answered with their stored trunk: %w",
			held.Vendor, ErrNoSealer))
	}
	attachedHere := held.AttachedAt != nil && held.StreamTrunkID != "" && held.StreamAppPK == pin
	// Refused before anything is made, the app's call hook included, as a number pointed at a
	// webhook that refuses its calls would look fine and never answer. A number attached
	// already is not pointed anywhere again, so it is not refused.
	if hooked, ok := provider.(CallHooked); ok && !attachedHere {
		if err := hooked.CallHookReady(s.publicURL); err != nil {
			return Attached{}, err
		}
	}
	if err := s.pointCallHook(ctx, stream, pin); err != nil {
		if !attachedHere {
			return Attached{}, err
		}
		// A running line is not taken down over a repair that failed: its program is asking
		// again on every start.
		s.logger.Error("could not point the call hook of a Stream app", "customer", attachment.CustomerID, "stream_app", pin, "error", err)
	}

	// The same request is sent on every run of a program waiting for calls, so a number
	// already attached in this app is answered with what it has.
	if attachedHere {
		attached := Attached{TrunkID: held.StreamTrunkID, RouteID: held.StreamRouteID}
		trunk, err := s.store.StreamTrunks.Get(ctx, held.CustomerID, held.StreamTrunkID)
		if errors.Is(err, store.ErrNoStreamTrunk) {
			return attached, nil
		}
		if err != nil {
			return Attached{}, err
		}
		attached.Bridge, err = Bridge{URI: trunk.BridgeURI}.WithNumber(held.E164)
		if err != nil {
			return Attached{}, err
		}
		return attached, nil
	}
	if err := s.clearLinesFor(ctx, held, pin); err != nil {
		return Attached{}, err
	}
	// A number lined in another app is taken off there first: the customer's app is where
	// its calls go now.
	if held.StreamTrunkID != "" && held.StreamAppPK != pin {
		if err := s.unline(ctx, held, "re-attached a number in another app", time.Time{}); err != nil {
			return Attached{}, err
		}
		held.StreamTrunkID, held.StreamRouteID = "", ""
	}

	if s.sealer != nil {
		line, err := s.line(ctx, stream, pin, held)
		if err == nil {
			attached := Attached{TrunkID: line.TrunkID, RouteID: line.RouteID, Bridge: line.Bridge}
			// The line stays when this fails: the number's calls go out on it.
			if err := s.finishAttach(ctx, provider, attachment, pin, held, attached, nil); err != nil {
				return Attached{}, err
			}
			return attached, nil
		}
		if !errors.Is(err, store.ErrNoStoredBridge) {
			return Attached{}, stack.Wrap(err)
		}
		// Attached before its trunk's password was kept, so its trunk cannot be shared: it is
		// replaced by one of its own, which keeps it.
		if err := s.unline(ctx, held, "re-attached a number", time.Time{}); err != nil {
			return Attached{}, err
		}
	} else if held.StreamTrunkID != "" {
		// Still on a trunk of its own (detached, or lined for calls out), and Stream refuses a
		// second trunk naming it.
		if err := s.unline(ctx, held, "re-attached a number", time.Time{}); err != nil {
			return Attached{}, err
		}
	}

	allowedIPs, err := trunkAllowlist(declared)
	if err != nil {
		return Attached{}, stack.Wrap(err)
	}

	trunkID, bridge, err := s.createTrunk(ctx, stream, pin, Trunk{
		Name:       "phone-" + attachment.E164,
		Numbers:    []string{attachment.E164},
		AllowedIPs: allowedIPs,
	})
	if err != nil {
		return Attached{}, stack.Wrap(err)
	}

	var routeID string
	committed := false
	defer func() {
		if committed {
			return
		}
		// Roll back what this attach created but did not finish wiring up, so a failed
		// attach does not leave a billable Stream trunk behind. Best-effort: a cleanup
		// error is logged, never returned, so it cannot mask the real failure.
		s.deleteLines(ctx, stream, routeID, trunkID, attachment.E164, "rolling back a failed attach")
	}()

	// Every caller is a conversation of their own, so each lands in a call named for the
	// session that answers them. The name is only known once Stream renders it, so the
	// number is put on the call, where the hook announcing it finds it.
	routeID, err = stream.CreateRoute(ctx, Route{
		Name:          "phone-" + attachment.E164,
		TrunkIDs:      []string{trunkID},
		CalledNumbers: []string{attachment.E164},
		CallID:        lineCallTemplate,
		CallType:      defaultCallType,
		Custom:        map[string]string{NumberKey: attachment.E164},
	})
	if err != nil {
		return Attached{}, stack.Wrap(err)
	}

	bridge, err = bridge.WithNumber(attachment.E164)
	if err != nil {
		return Attached{}, stack.Wrap(err)
	}
	attached := Attached{TrunkID: trunkID, RouteID: routeID, Bridge: bridge}
	// A vendor that asks per call where to send a caller needs the password then. Without a
	// key it is not kept, rather than kept in the clear.
	own := store.StreamTrunk{ID: trunkID, BridgeURI: bridge.URI, BridgeUsername: bridge.Username, Own: true}
	if s.sealer != nil {
		own.BridgePasswordSealed, err = s.sealer.SealWithAAD(bridge.Password, bridgeAAD(attachment.CustomerID, attachment.E164))
		if err != nil {
			return Attached{}, stack.Wrap(err)
		}
		own.BridgePasswordKEKVersion = s.sealer.CurrentVersion()
	}
	if err := s.finishAttach(ctx, provider, attachment, pin, held, attached, &own); err != nil {
		return Attached{}, err
	}
	committed = true
	return attached, nil
}

// finishAttach tells the vendor to send the number's calls to its trunk, and records that
// they go there, saving own, the trunk made for the number alone, with it. The agent the
// number had stays: taking it off its old trunk cleared it, and attaching again never takes
// away an agent someone set.
func (s *Service) finishAttach(ctx context.Context, provider Provider, attachment Attachment, pin int64, held store.PhoneNumber, attached Attached, own *store.StreamTrunk) error {
	record := store.NumberAttachment{
		TrunkID: attached.TrunkID, RouteID: attached.RouteID, StreamAppPK: pin, CallType: defaultCallType,
		ConfigID: held.ConfigID,
	}

	err := provider.ConfigureInbound(ctx, Inbound{E164: attachment.E164, Bridge: attached.Bridge, PublicURL: s.publicURL})
	if err != nil {
		return stack.Wrap(err)
	}
	// The vendor has already been told to send calls to the trunk. There is no primitive to
	// un-configure a provider's inbound routing, so a failure here leaves the vendor
	// pointing at a trunk that, when it is the number's own, is about to be rolled back.
	if own == nil {
		return s.store.PhoneNumbers.Attach(ctx, attachment.CustomerID, attachment.E164, record)
	}
	group := store.TrunkGroup{CustomerID: attachment.CustomerID, StreamAppPK: pin, Vendor: held.Vendor}
	return s.inTrunkGroup(ctx, group, func(_ context.Context, tx *store.TrunkGroupTx) error {
		if err := tx.SaveTrunk(*own); err != nil {
			return err
		}
		return tx.Attach(attachment.E164, record)
	})
}

// InboundBridge is the Stream trunk calls to an attached number of the vendor go to, with
// its password opened.
func (s *Service) InboundBridge(ctx context.Context, vendor, e164 string) (Bridge, error) {
	if s.store == nil {
		return Bridge{}, stack.Wrap(errors.New("phone: finding where a number's calls go needs a database"))
	}
	if s.sealer == nil {
		return Bridge{}, stack.Wrap(ErrNoSealer)
	}
	stored, err := s.store.PhoneNumbers.AttachedBridge(ctx, vendor, e164)
	if err != nil {
		return Bridge{}, err
	}
	// The vendor's hook asking is the number's app's, however it is answered below.
	activity.NoteCustomer(ctx, stored.CustomerID, activity.AreaPhoneSMS)
	if err := s.Suspended(ctx, stored.CustomerID); err != nil {
		return Bridge{}, err
	}
	// The vendor bills the minutes of a call it answers as surely as one placed, and an agent
	// on it is billed work too, so a call to an organization that may not pay is not answered.
	if err := s.gate.Admit(ctx, stored.CustomerID); err != nil {
		return Bridge{}, err
	}
	aad := trunkAAD(stored.CustomerID, stored.TrunkID)
	if stored.Own {
		aad = bridgeAAD(stored.CustomerID, e164)
	}
	password, err := s.sealer.OpenWithAADVersion(stored.PasswordSealed, aad, stored.KEKVersion)
	if err != nil {
		return Bridge{}, stack.Wrap(fmt.Errorf("phone: open the trunk password of %s: %w", e164, err))
	}
	return Bridge{URI: stored.URI, Username: stored.Username, Password: password}.WithNumber(e164)
}

// CallRequest is a call to place from one of the customer's numbers.
type CallRequest struct {
	Owner routing.Owner
	// From is one of the customer's own numbers, which is what the person sees.
	From string
	// To is who to call.
	To string
	// SessionID is the session that holds the call: the answered leg is routed into its
	// call, agent:<session id>. Empty chooses one, because two calls from the same number
	// are two conversations and must not land in the same place.
	SessionID string
	// RingTimeout is how long to ring before giving up. Zero leaves the vendor's default.
	RingTimeout time.Duration
	// InitialDigits are pressed once the person answers, for reaching an extension behind
	// a menu.
	InitialDigits string
	// Headers are carried to the person's leg as custom SIP headers.
	Headers map[string]string
	// Custom is put on the Stream call, where the agent in it can read it. It is set at
	// Stream rather than at the vendor, so every vendor can carry it.
	Custom map[string]string
	// DetectMachine asks the vendor to tell an answering machine from a person, when it
	// can. The answer reaches the hook set with OnMachine.
	DetectMachine bool
}

// errCustomKept is a call into a session whose call is already there with other custom data:
// the call keeps the custom it was created with, so the agent in it would not read this one.
var errCustomKept = Invalid(errors.New("phone: the session's call already carries other custom data; leave custom out or use a new session"))

// Placed is a call that is on its way, and where to meet it.
type Placed struct {
	// VendorCallID identifies the ringing leg at the vendor, for hanging up or pressing
	// digits on it.
	VendorCallID string
	// Status is the vendor's own word for where the call is, e.g. "queued" or "ringing".
	Status string
	// Vendor is who is placing it.
	Vendor string
	// SessionID is the session to open on the call, with voice started: the answered leg
	// is routed into agent:<session id>, and an agent that is not in it hears nothing when
	// the person picks up.
	SessionID string
	// CallID and CallType are that call.
	CallID   string
	CallType string
	// StreamApp is the app that call is in, which the agent has to join it in.
	StreamApp int64
}

// Call places an outbound call and bridges it into a Stream call.
//
// Stream's SIP is inbound only, so the vendor originates the call and connects it to a
// trunk, rather than Stream dialling out. The trunk alone is not enough: without a routing
// rule the answered leg arrives with nothing pointing it at a call, so this creates both and
// pins the rule to the call the agent is waiting in, the way Transfer does. A vendor that
// names the call on its leg goes out on the number's shared line instead, since Stream lets a
// number be on one trunk only.
func (s *Service) Call(ctx context.Context, request CallRequest) (Placed, error) {
	sent := false
	placed, err := s.call(ctx, request, &sent)
	if err != nil && !sent && ctx.Err() != nil {
		return Placed{}, fmt.Errorf("%w: %w", ErrNotPlaced, err)
	}
	if err != nil && !sent && !lasting(err) {
		return Placed{}, notDialed{err}
	}
	return placed, err
}

// lasting reports whether a call failed before it was sent for a reason that placing it again
// would meet again: the request itself, a number or SIP trunk the app does not hold, or one
// another app holds, its Stream app, or the deployment's own setup (credentials, keys, a public
// URL, SIP trunks). Such a failure is not marked ErrNotDialed, so a campaign does not try it for
// ever.
func lasting(err error) bool {
	return errors.Is(err, ErrInvalid) || errors.Is(err, store.ErrNoNumber) || errors.Is(err, store.ErrNoSIPTrunk) ||
		errors.Is(err, store.ErrNumberHeldElsewhere) ||
		errors.Is(err, ErrNoStream) || errors.Is(err, ErrNoSealerToCall) ||
		errors.Is(err, ErrNotConfigured) || errors.Is(err, ErrSIPTrunksDisabled) ||
		errors.Is(err, errLasting) || (err != nil && streamapp.Settled(err))
}

// errLasting marks a failure that has no kind of its own and that a second try meets again: a
// vendor that cannot be built from its configuration, or a password the router's keys cannot
// open or seal.
var errLasting = errors.New("phone: a lasting failure")

type lastingError struct{ err error }

func (e lastingError) Error() string        { return e.err.Error() }
func (e lastingError) Unwrap() error        { return e.err }
func (e lastingError) Is(target error) bool { return target == errLasting }

// call is Call. It sets sent once the vendor is asked to dial: from then on the phone may ring
// whatever the call returns.
func (s *Service) call(ctx context.Context, request CallRequest, sent *bool) (Placed, error) {
	if err := s.Suspended(ctx, request.Owner.CustomerID); err != nil {
		return Placed{}, err
	}
	if request.To != "" {
		if err := Dialable(request.To); err != nil {
			return Placed{}, err
		}
	}
	room, err := s.gate.AllowCall(ctx, request.Owner.CustomerID, request.To)
	if err != nil {
		return Placed{}, stack.Wrap(err)
	}
	stream, pin, err := s.streamFor(ctx, request.Owner.CustomerID)
	if errors.Is(err, ErrNoStream) {
		return Placed{}, stack.Wrap(marked{errors.New("phone: placing a call needs stream credentials"), ErrNoStream})
	}
	if err != nil {
		return Placed{}, err
	}
	if s.store == nil {
		return Placed{}, stack.Wrap(errors.New("phone: placing a call needs a database to know who holds the number"))
	}
	if request.To == "" {
		return Placed{}, stack.Wrap(Invalid(errors.New("phone: a call needs someone to call")))
	}
	if request.From == "" {
		return Placed{}, stack.Wrap(Invalid(errors.New("phone: a call needs one of your numbers to call from")))
	}

	held, err := s.store.PhoneNumbers.GetByE164(ctx, request.Owner.CustomerID, request.From)
	if err != nil {
		return Placed{}, stack.Wrap(err)
	}
	provider, trunk, err := s.dialVendor(ctx, held, pin)
	if err != nil {
		return Placed{}, stack.Wrap(err)
	}

	outbound := Outbound{
		From:          request.From,
		To:            request.To,
		RingTimeout:   request.RingTimeout,
		InitialDigits: request.InitialDigits,
		Headers:       request.Headers,
		Trunk:         trunk,
	}
	// A term the vendor cannot express is refused rather than dropped: a call placed
	// without the ring timeout that was asked for is not the call that was asked for.
	if missing := outbound.Unsupported(provider); len(missing) > 0 {
		return Placed{}, stack.Wrap(Invalid(fmt.Errorf("phone: %s cannot place a call with %s",
			held.Vendor, joinFeatures(missing))))
	}

	allowedIPs, err := s.allowlistFor(held.Vendor)
	if err != nil {
		return Placed{}, stack.Wrap(err)
	}
	// A vendor that fetches its plan on answer and sends the trunk's password gets it from a
	// parked bridge, which keeps it sealed: refused before anything is made at Stream.
	if _, hosted := provider.(AnswerRenderer); hosted && s.sealer == nil && sendsPassword(s.registry, held.Vendor) {
		return Placed{}, stack.Wrap(fmt.Errorf("phone: %s: %w", held.Vendor, ErrNoSealerToCall))
	}

	sessionID := request.SessionID
	if sessionID == "" {
		sessionID = uuid.NewString()
	} else if held, ok := conversation.SessionID(sessionID); ok {
		sessionID = held
	} else {
		return Placed{}, stack.Wrap(Invalid(fmt.Errorf("phone: the session id %q is not one a session can have", sessionID)))
	}
	callID, callType := sessionID, defaultCallType

	// The call is kept, and counted against the calls its organization may have at once, before
	// anything is made at Stream for it: a call the count refuses costs nothing there.
	// Only a vendor the router can be reached by can post how the call ended. A call through
	// a customer's own trunk is told by the router's own bridge.
	_, reader := provider.(StatusReader)
	reports := reader && s.publicURL != ""
	token, err := s.outcomes.PlacedWithin(ctx, outcome.Call{
		Key: outcome.Key{
			CustomerID: request.Owner.CustomerID, StreamAppPK: pin, CallType: callType, CallID: callID,
			Local: request.From, Remote: request.To,
		},
		Vendor:  held.Vendor,
		Reports: reports || trunk != nil,
	}, room.OrganizationID, room.AtOnce)
	if errors.Is(err, store.ErrCallsAtOnce) {
		return Placed{}, room.Refusal(err)
	}
	if err != nil {
		return Placed{}, notPlaced(ctx, err)
	}
	// Until the dial, whose failures forget it themselves, a call that goes no further is
	// forgotten here: it never rang.
	forget := true
	defer func() {
		if forget {
			s.forgetOutcome(ctx, token, callID)
		}
	}()
	// Its leg counts against the organization it was placed under once it is dialled too.
	if room.OrganizationID != "" {
		request.Owner.OrganizationID = room.OrganizationID
	}

	if err := s.clearLinesFor(ctx, held, pin); err != nil {
		return Placed{}, err
	}
	line, shared, err := s.sharedLine(ctx, stream, pin, provider, held)
	if err != nil {
		return Placed{}, stack.Wrap(err)
	}
	// The number's rule carries only the number, so on its line a call with custom of its own
	// is created with it here, before the leg arrives; otherwise the leg creates the call. A
	// session the caller named may hold its call already, which a get-or-create only loads, so
	// that call is asked for here too, before anything is made for the leg.
	if len(request.Custom) > 0 && (shared || request.SessionID != "") {
		kept, err := stream.CreateCall(ctx, callType, callID, request.Custom)
		if err != nil {
			return Placed{}, err
		}
		// The number is the router's, put there by a leg's rule, and no caller can send it.
		delete(kept, NumberKey)
		switch {
		case maps.EqualFunc(kept, request.Custom, func(stored any, sent string) bool { return stored == sent }):
		case len(kept) == 0:
			// A call made without custom, as an agent makes its own call, takes this one. A dial
			// that fails after this keeps it on the call: Stream's update-call sets a key to null
			// but cannot remove it, so there is no putting it back.
			if err := stream.PutCustom(ctx, callType, callID, request.Custom); err != nil {
				return Placed{}, err
			}
		default:
			return Placed{}, stack.Wrap(errCustomKept)
		}
	}
	var trunkID, routeID string
	committed := false
	if shared {
		trunkID, routeID = line.TrunkID, line.RouteID
		outbound.Bridge, outbound.CallID = line.Bridge, callID
	} else {
		var bridge Bridge
		trunkID, bridge, err = s.createTrunk(ctx, stream, pin, Trunk{
			Name:       "call-" + callID,
			Numbers:    []string{request.From},
			AllowedIPs: allowedIPs,
		})
		if err != nil {
			return Placed{}, stack.Wrap(err)
		}

		defer func() {
			if committed {
				return
			}
			// Roll back what this call created but never placed, so a call that did not
			// start does not leave a billable Stream trunk behind. Best-effort: a cleanup
			// error is logged, never returned, so it cannot mask the real failure.
			s.deleteLines(ctx, stream, routeID, trunkID, request.From, "rolling back a call that did not place")
		}()

		routeID, err = stream.CreateRoute(ctx, Route{
			Name:          "call-" + callID,
			TrunkIDs:      []string{trunkID},
			CalledNumbers: []string{request.From},
			CallID:        callID,
			CallType:      callType,
			Custom:        request.Custom,
		})
		if err != nil {
			return Placed{}, stack.Wrap(err)
		}

		outbound.Bridge, err = bridge.WithNumber(request.From)
		if err != nil {
			return Placed{}, stack.Wrap(err)
		}
	}
	if err := outbound.Validate(); err != nil {
		return Placed{}, stack.Wrap(err)
	}
	// Three vendors will not take the plan on this request and fetch one on answer, so
	// they get somewhere to fetch it from instead.
	if _, hosted := provider.(AnswerRenderer); hosted {
		outbound.AnswerURL, err = s.park(ctx, held.Vendor, request.Owner.CustomerID, callID, outbound)
		if err != nil {
			return Placed{}, stack.Wrap(err)
		}
	}

	if reports {
		outbound.StatusURL = s.publicURL + "/v1/phone/status/" + token
	}
	if _, detects := provider.(MachineDetector); detects && request.DetectMachine && s.publicURL != "" {
		outbound.MachineURL = s.publicURL + "/v1/phone/machine/" + token
	}

	if trunk != nil {
		customerID := request.Owner.CustomerID
		outbound.Connected = func(media sipbridge.MediaAddrs) {
			s.recordBridgeMedia(context.WithoutCancel(ctx), customerID, sessionID, media)
		}
		// Call refuses to run without a store, so report always has its keeper.
		outbound.Ended = s.trunkCallEnded(ctx, token, callID)
		// A per-call Stream trunk's password is never stored, and Stream challenges the
		// re-INVITE a takeover sends, so only a call on a shared line can move.
		if shared {
			outbound.Resume = &TrunkCallRef{
				CustomerID: customerID, SIPTrunkID: held.SIPTrunkID, StreamTrunkID: line.TrunkID,
				From: request.From, CallID: callID, Token: token,
			}
		}
	}

	resource := &store.CallResource{
		TrunkID:     trunkID,
		RouteID:     routeID,
		CallType:    callType,
		CallID:      callID,
		CustomerID:  request.Owner.CustomerID,
		StreamAppPK: pin,
		Shared:      shared,
	}
	forget = false
	// A shared line outlives the call, so its row costs nothing to undo when the dial fails,
	// and the call's app (store.CallResources.Pin) is known before its leg arrives.
	if shared {
		if err := s.store.CallResources.Insert(ctx, resource); err != nil {
			s.forgetOutcome(ctx, token, callID)
			return Placed{}, stack.Wrap(err)
		}
	}

	dialed, err := routing.Call(ctx, s.dialMeter(held.Vendor, request.Owner), func(ctx context.Context) (Dialed, error) {
		*sent = true
		return provider.Dial(ctx, outbound)
	})
	if err != nil {
		if shared {
			s.forgetCallResource(ctx, resource)
		}
		// The vendor was asked: dialMeter reserves nothing, so routing.Call fails only in the dial.
		// A request that went away mid-dial is no refusal, though: the phone may be ringing.
		if ctx.Err() == nil {
			s.forgetRefused(ctx, token, held.Vendor, callID)
		} else {
			s.forgetOutcome(ctx, token, callID)
		}
		return Placed{}, stack.Wrap(err)
	}

	committed = true
	if err := s.outcomes.Dialed(context.WithoutCancel(ctx), token, dialed.VendorCallID); err != nil {
		s.logger.Error("could not keep a placed call's vendor id", "call", callID,
			"vendor_call_id", dialed.VendorCallID, "error", err)
	}
	// The vendor bills the call from when the person answers, which is when their phone joins
	// the Stream call: the leg rings until then, and one never answered is not billed.
	s.OpenLeg(ctx, request.Owner, store.CallLeg{
		StreamAppPK: pin, CallType: callType, CallID: callID, Vendor: held.Vendor, VendorCallID: dialed.VendorCallID,
		Direction: store.Outbound, LocalNumber: request.From, RemoteNumber: request.To, Token: token,
	}, false)
	sites := placedSites{streamSIPHost: BridgeHost(outbound.Bridge.URI)}
	if trunk != nil {
		sites.customerTrunkHost = trunk.Host
	}
	s.logger.Info("placed a phone call", append([]any{"session_id", sessionID,
		"vendor_call_id", dialed.VendorCallID, "vendor", held.Vendor}, sites.logged()...)...)
	if !shared {
		if err := s.store.CallResources.Insert(ctx, resource); err != nil {
			// The call is placed and connecting; deleting its trunk now would drop a live
			// call. A logged, rare leak beats that. session_ended cannot clean a trunk it
			// has no row for, which is the cost of this failing.
			s.logger.Error("could not record a call's trunk for cleanup; it may leak",
				"trunk", trunkID, "call", callID, "error", err)
		}
	}
	s.recordPlaced(context.WithoutCancel(ctx), request.Owner.CustomerID, sessionID, dialed, held.Vendor, sites)
	return Placed{
		VendorCallID: dialed.VendorCallID,
		Status:       dialed.Status,
		Vendor:       held.Vendor,
		SessionID:    sessionID,
		CallID:       callID,
		CallType:     callType,
		StreamApp:    pin,
	}, nil
}

// forgetOutcome drops the row kept for a call that never rang, which has no outcome. It runs
// on a request that may be gone, and logs a failure rather than mask the one it follows.
func (s *Service) forgetOutcome(ctx context.Context, token, callID string) {
	if err := s.outcomes.Failed(context.WithoutCancel(ctx), token); err != nil {
		s.logger.Error("could not forget a call that did not place", "call", callID, "error", err)
	}
}

// forgetRefused is forgetOutcome for a call the vendor was asked to dial and refused, which is
// counted as refused.
func (s *Service) forgetRefused(ctx context.Context, token, vendor, callID string) {
	if err := s.outcomes.Refused(context.WithoutCancel(ctx), token, vendor); err != nil {
		s.logger.Error("could not forget a call that did not place", "call", callID, "error", err)
	}
}

// placedSites is where a placed call was sent: the Stream SIP host, and the host of the
// customer's own trunk when the call went out through one. Never the trunk's username or
// password.
type placedSites struct {
	streamSIPHost, customerTrunkHost string
}

func (p placedSites) details() map[string]any {
	details := map[string]any{"stream_sip_host": p.streamSIPHost}
	if p.customerTrunkHost != "" {
		details["customer_trunk_host"] = p.customerTrunkHost
	}
	return details
}

func (p placedSites) logged() []any {
	logged := []any{"stream_sip_host", p.streamSIPHost}
	if p.customerTrunkHost != "" {
		logged = append(logged, "customer_trunk_host", p.customerTrunkHost)
	}
	return logged
}

// recordPlaced keeps where a placed call was sent, found later by its session id. The call
// is already ringing, so a failure is logged and goes no further, and a slow store cannot
// hold the reply for longer than agentLogTimeout.
func (s *Service) recordPlaced(
	ctx context.Context,
	customerID, sessionID string,
	placed Dialed,
	vendor string,
	sites placedSites,
) {
	details := sites.details()
	details["vendor"] = vendor
	details["vendor_call_id"] = placed.VendorCallID
	ctx, cancel := context.WithTimeout(ctx, agentLogTimeout)
	defer cancel()
	err := s.store.AgentLogs.Insert(ctx, &store.AgentLog{
		CustomerID: customerID,
		SessionID:  sessionID,
		Source:     "system",
		Severity:   "info",
		EventType:  "call_placed",
		Message:    "Call placed",
		Details:    details,
	})
	if err != nil {
		s.logger.Warn("could not record a placed call", "session_id", sessionID, "error", err)
	}
}

// trunkCallEnded is what a call through a customer's own trunk tells when it ends. It never
// fails the caller: the call is over either way.
func (s *Service) trunkCallEnded(ctx context.Context, token, callID string) func(CallStatus) {
	return func(status CallStatus) {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), trunkReportTimeout)
		defer cancel()
		if _, err := s.report(ctx, token, status); err != nil {
			s.logger.Error("could not keep how a call through a customer's sip trunk ended",
				"call", callID, "result", status.Result, "error", err)
		}
	}
}

// trunkReportTimeout bounds keeping how a call through a customer's own trunk ended. The
// bridge waits for it, and the node waits for the bridge when it stops.
const trunkReportTimeout = 5 * time.Second

// agentLogTimeout bounds writing a call_placed or bridge_media row. The node waits for the
// bridge_media row when it stops.
const agentLogTimeout = 5 * time.Second

// recordBridgeMedia keeps where each side of a call through a customer's own SIP trunk
// sends its audio from, as the router's bridge saw in their SDP. It is the only place the
// Stream side's real address shows: the SIP host's DNS answer depends on who asks.
func (s *Service) recordBridgeMedia(ctx context.Context, customerID, sessionID string, media sipbridge.MediaAddrs) {
	if media.Stream == "" && media.Customer == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, agentLogTimeout)
	defer cancel()
	details := map[string]any{}
	if media.Stream != "" {
		details["stream_media_addr"] = media.Stream
	}
	if media.Customer != "" {
		details["customer_media_addr"] = media.Customer
	}
	err := s.store.AgentLogs.Insert(ctx, &store.AgentLog{
		CustomerID: customerID,
		SessionID:  sessionID,
		Source:     "system",
		Severity:   "info",
		EventType:  "bridge_media",
		Message:    "Bridge media",
		Details:    details,
	})
	if err != nil {
		s.logger.Warn("could not record a bridged call's media addresses", "session_id", sessionID, "error", err)
	}
}

// bridgeLifetime is how long a parked bridge is claimable for. It has to outlast the longest
// a vendor will ring, and nothing beyond that: a bridge nobody claimed is a call nobody
// answered.
const bridgeLifetime = 10 * time.Minute

// park saves what the vendor should be told on answer, and returns where it fetches it from.
func (s *Service) park(
	ctx context.Context,
	vendor, customerID, callID string,
	outbound Outbound,
) (string, error) {
	if s.publicURL == "" {
		return "", stack.Wrap(NotConfigured(fmt.Errorf(
			"phone: %s fetches its call plan when the person answers, so it needs a "+
				"public url to fetch it from, and none is configured", vendor)))
	}

	token := s.store.MintToken()
	bridge := &store.CallBridge{
		Token:         token,
		CustomerID:    customerID,
		Vendor:        vendor,
		TrunkURI:      outbound.Bridge.URI,
		TrunkUsername: outbound.Bridge.Username,
		InitialDigits: outbound.InitialDigits,
		CallID:        callID,
		ExpiresAt:     time.Now().UTC().Add(bridgeLifetime),
	}
	// Only a vendor that sends the password is given one, so only its bridge keeps one.
	if outbound.Bridge.Password != "" {
		if s.sealer == nil {
			return "", stack.Wrap(ErrNoSealerToCall)
		}
		sealed, err := s.sealer.SealWithAAD(outbound.Bridge.Password, callBridgeAAD(customerID, token))
		if err != nil {
			return "", stack.Wrap(lastingError{fmt.Errorf("phone: seal a parked bridge's trunk password: %w", err)})
		}
		bridge.TrunkPasswordSealed, bridge.TrunkKEKVersion = sealed, s.sealer.CurrentVersion()
	}
	if err := s.store.CallBridges.Insert(ctx, bridge); err != nil {
		return "", err
	}
	return s.publicURL + "/v1/phone/answer/" + token, nil
}

// Answer renders what a vendor should do now that the person it called has picked up.
//
// The token is the whole of the request's authentication: the vendor fetching this has no
// customer to name. Claiming spends the token, so a plan is served once and a token that
// leaks afterwards is a token for nothing.
func (s *Service) Answer(ctx context.Context, token string) (Plan, error) {
	if s.store == nil {
		return Plan{}, errors.New("phone: answering a call needs a database to know what to answer")
	}

	bridge, err := s.store.CallBridges.Claim(ctx, token)
	if err != nil {
		return Plan{}, err
	}
	activity.NoteCustomer(ctx, bridge.CustomerID, activity.AreaPhoneSMS)
	provider, err := s.registry.Open(bridge.Vendor)
	if err != nil {
		return Plan{}, err
	}
	renderer, ok := provider.(AnswerRenderer)
	if !ok {
		return Plan{}, fmt.Errorf("phone: %s does not fetch a call plan, so it has none to serve",
			bridge.Vendor)
	}

	var password string
	if len(bridge.TrunkPasswordSealed) > 0 {
		if s.sealer == nil {
			return Plan{}, stack.Wrap(ErrNoSealer)
		}
		password, err = s.sealer.OpenWithAADVersion(bridge.TrunkPasswordSealed, callBridgeAAD(bridge.CustomerID, token), bridge.TrunkKEKVersion)
		if err != nil {
			return Plan{}, stack.Wrap(fmt.Errorf("phone: open a parked bridge's trunk password: %w", err))
		}
	}

	return renderer.Answer(Bridge{
		URI:      bridge.TrunkURI,
		Username: bridge.TrunkUsername,
		Password: password,
	}, bridge.InitialDigits)
}

// sendsPassword reports whether a vendor presents the trunk's password when it bridges.
func sendsPassword(registry *Registry, vendor string) bool {
	declared, _ := registry.Lookup(vendor)
	return declared.Authenticates() == TrunkPassword
}

// callBridgeAAD binds a parked bridge's sealed trunk password to its customer and its token,
// which only the fetch presents.
func callBridgeAAD(customerID, token string) []byte {
	return []byte("call_bridges/" + customerID + "/" + token)
}

// ErrNotPlaced is a call the vendor was never asked to dial, its context ended or its placing
// failed (ErrPlacingFailed): nobody was rung.
var ErrNotPlaced = errors.New("phone: the call was not placed")

// notPlaced is err, the failure of placing a call, as ErrNotPlaced: nothing was asked of the
// vendor, so nobody was rung. It is ErrPlacingFailed too unless ctx ended, as a shutdown ends
// it, which is no failure of the database's to wait out.
func notPlaced(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return stack.Wrap(fmt.Errorf("%w: %w", ErrNotPlaced, err))
	}
	return stack.Wrap(fmt.Errorf("%w: %w: %w", ErrNotPlaced, ErrPlacingFailed, err))
}

// ErrPlacingFailed is a call whose placing failed other than by its organization's calls at
// once, the count or its lock not answering in time: it is ErrNotPlaced too, and worth trying
// again only once the database answers.
var ErrPlacingFailed = errors.New("phone: the call could not be counted against its organization's calls")

// ErrNotDialed is a call that failed before the vendor was asked to dial it, for a reason that
// may not be met again, such as a database blip: nobody was rung. It marks the failure, whose
// own text and kind are kept.
var ErrNotDialed = errors.New("phone: the call failed before it was dialed")

type notDialed struct{ err error }

func (e notDialed) Error() string        { return e.err.Error() }
func (e notDialed) Unwrap() error        { return e.err }
func (e notDialed) Is(target error) bool { return target == ErrNotDialed }

// ErrUnknownStatus is a status post for no call waiting for one: its token is unknown or swept,
// or its vendor reports no status.
var ErrUnknownStatus = errors.New("phone: that call is not waiting for its status")

// ErrUnreadableStatus is a status post its vendor's provider could not read.
var ErrUnreadableStatus = errors.New("phone: that status could not be read")

// ReportStatus takes what a vendor posted to the status URL of the call token was made for.
func (s *Service) ReportStatus(ctx context.Context, token, contentType string, body []byte) error {
	call, err := s.callOfToken(ctx, token)
	if err != nil {
		return err
	}
	provider, err := s.registry.Open(call.Vendor)
	if err != nil {
		return err
	}
	reader, ok := provider.(StatusReader)
	if !ok {
		return stack.Wrap(fmt.Errorf("%w: %s reports no status", ErrUnknownStatus, call.Vendor))
	}
	status, err := reader.ReadStatus(contentType, body)
	if err != nil {
		return stack.Wrap(fmt.Errorf("%w: %w", ErrUnreadableStatus, err))
	}
	_, err = s.report(ctx, token, status)
	return err
}

// callOfToken is the call a vendor's post to one of its URLs is for.
func (s *Service) callOfToken(ctx context.Context, token string) (outcome.Call, error) {
	if s.outcomes == nil {
		return outcome.Call{}, stack.Wrap(ErrUnknownStatus)
	}
	// The endpoint is public: a token not shaped like the ones made in Place never reaches the
	// database.
	_, random := store.SplitToken(token)
	if id, err := uuid.Parse(random); err != nil || id.String() != random {
		return outcome.Call{}, stack.Wrap(ErrUnknownStatus)
	}
	call, err := s.outcomes.ByToken(ctx, token)
	if errors.Is(err, outcome.ErrUnknownCall) {
		return outcome.Call{}, stack.Wrap(ErrUnknownStatus)
	}
	if err == nil {
		activity.NoteCustomer(ctx, call.CustomerID, activity.AreaPhoneSMS)
	}
	return call, err
}

// Machine takes what a vendor posted to the machine URL of the call token was made for, and
// tells the hook set with OnMachine.
func (s *Service) Machine(ctx context.Context, token, contentType string, body []byte) error {
	call, err := s.callOfToken(ctx, token)
	if err != nil {
		return err
	}
	provider, err := s.registry.Open(call.Vendor)
	if err != nil {
		return err
	}
	detector, ok := provider.(MachineDetector)
	if !ok {
		return stack.Wrap(fmt.Errorf("%w: %s detects no answering machines", ErrUnknownStatus, call.Vendor))
	}
	report, err := detector.ReadMachine(contentType, body)
	if err != nil {
		return stack.Wrap(fmt.Errorf("%w: %w", ErrUnreadableStatus, err))
	}
	s.logger.Info("a vendor reported who answered", "vendor", call.Vendor, "answered_by", report.Word,
		"machine", report.Machine, "customer", call.CustomerID, "session", call.CallID)
	if s.machine != nil {
		s.machine(ctx, call.CustomerID, call.CallID, report.Machine)
	}
	return nil
}

// OnMachine sets what is told who picked up a placed call. Called before the service is used.
// hook is called on the vendor's request, so it must not hold up the answer.
func (s *Service) OnMachine(hook func(ctx context.Context, customerID, sessionID string, machine Machine)) {
	s.machine = hook
}

// report hands a vendor's status for the call token was made for to the keeper, which decides
// what it means.
func (s *Service) report(ctx context.Context, token string, status CallStatus) (outcome.Call, error) {
	reported, err := outcomeOf(status.Result)
	if err != nil {
		return outcome.Call{}, err
	}
	call, err := s.outcomes.Reported(ctx, token, outcome.Report{
		Outcome: reported, Word: status.Word, SIPCode: status.SIPCode, Ringing: status.Ringing,
		PostDialDelay: status.PostDialDelay,
	})
	if errors.Is(err, outcome.ErrUnknownCall) {
		return outcome.Call{}, stack.Wrap(ErrUnknownStatus)
	}
	if err != nil {
		return outcome.Call{}, err
	}
	// A repeated post asks again, which is harmless: ending a session that has ended does
	// nothing.
	if call.NobodyAnswered() && s.unanswered != nil {
		s.unanswered(ctx, call.CustomerID, call.CallID)
	}
	return call, nil
}

// OnUnanswered sets what ends the session of a placed call once its vendor says nobody will
// answer, rather than the agent waiting out its start timeout. Called before the service is
// used. end is called on the vendor's status request, so it must not hold up the answer.
func (s *Service) OnUnanswered(end func(ctx context.Context, customerID, sessionID string)) {
	s.unanswered = end
}

// SweepBridges removes the bridges left behind by calls nobody answered.
func (s *Service) SweepBridges(ctx context.Context) (int64, error) {
	if s.store == nil {
		return 0, nil
	}
	return s.store.CallBridges.Sweep(ctx)
}

// statusTokenLifetime is how long a placed call's status URL takes posts: past the longest a
// call may last, with an hour for a vendor's late post.
const statusTokenLifetime = MaxCallLength + time.Hour

// SweepStatusTokens retires the status URLs of calls placed longer than statusTokenLifetime
// ago, keeping their outcomes. It reports how many it retired.
func (s *Service) SweepStatusTokens(ctx context.Context) (int64, error) {
	if s.outcomes == nil {
		return 0, nil
	}
	return s.outcomes.Sweep(ctx, s.now().Add(-statusTokenLifetime))
}

// ReleaseCall deletes the per-call Stream trunks and routes an ended call left behind in one
// app, each in the app it was made in. A call's id is only unique within an app, so the
// event that ended it releases only what that app holds; unpinned also releases what was
// made before apps had identities. It is safe to call for any call: one with no per-call
// resources (an inbound or video call) releases nothing. Deletes are best-effort so a
// cleanup failure cannot wedge the event that triggered it.
func (s *Service) ReleaseCall(ctx context.Context, scope store.AppScope, callType, callID string) error {
	if s.store == nil || (s.stream == nil && s.apps == nil) {
		return nil
	}
	resources, err := s.store.CallResources.Release(ctx, scope, callType, callID)
	if err != nil {
		return fmt.Errorf("phone: release call resources: %w", err)
	}
	for _, resource := range resources {
		if resource.Shared {
			continue
		}
		s.unwire(ctx, resource.CustomerID, resource.StreamAppPK, resource.RouteID, resource.TrunkID, "a call ended")
	}
	return nil
}

// SweepCallResources forgets the calls older than before whose end hook never arrived, and
// unwires their per-call trunks. It returns how many calls' rows it forgot. A shared line
// belongs to its number, not the call, so it is left at Stream.
//
// A per-call row is forgotten only once Stream deleted its trunk, so a trunk Stream kept is
// tried again on the next sweep. Two routers sweeping at once may both delete one trunk,
// and the one that loses only logs it.
func (s *Service) SweepCallResources(ctx context.Context, before time.Time) (int, error) {
	if s.store == nil || (s.stream == nil && s.apps == nil) {
		return 0, nil
	}
	stale, err := s.store.CallResources.SelectStale(ctx, before)
	if err != nil {
		return 0, fmt.Errorf("phone: sweep call resources: %w", err)
	}
	var forgot int
	for _, resource := range stale {
		if !resource.Shared {
			stream, err := s.streamToRemove(ctx, resource.CustomerID, resource.StreamAppPK)
			if err == nil {
				if err := stream.DeleteRoute(ctx, resource.RouteID); err != nil {
					s.logger.Error("could not delete a routing rule", "why", "a call never ended", "route", resource.RouteID, "error", err)
				}
				err = stream.deleteTrunkWithRules(ctx, resource.TrunkID, "")
			}
			if err != nil {
				s.logger.Error("could not delete the trunk of a call that never ended",
					"trunk", resource.TrunkID, "route", resource.RouteID, "error", err)
				continue
			}
		}
		if err := s.store.CallResources.Forget(ctx, resource); err != nil {
			return forgot, fmt.Errorf("phone: sweep call resources: %w", err)
		}
		forgot++
	}
	return forgot, nil
}

// trunkAllowlist is the addresses a vendor's trunk should accept calls from.
//
// A vendor that can present the trunk's password needs no allowlist. One that cannot needs
// one, and refusing to place the call without it is the point: Stream reads an empty
// allowlist as "accept everything" rather than "password only", so a trunk with neither is
// a way into a customer's calls for anyone who learns its uri.
func trunkAllowlist(vendor Vendor) ([]string, error) {
	if vendor.Authenticates() == TrunkPassword {
		return nil, nil
	}
	if len(vendor.Signalling) == 0 {
		return nil, stack.Wrap(NotConfigured(fmt.Errorf(
			"phone: %s cannot send a password to a trunk, so the trunk has to know its "+
				"signalling addresses, and none are declared for it",
			vendor.Vendor)))
	}
	return vendor.Signalling, nil
}

// dialVendor is who places a call from a held number, lined in the app pin names, and the
// customer's own trunk the call goes through when the number is on one.
func (s *Service) dialVendor(ctx context.Context, held store.PhoneNumber, pin int64) (Provider, *SIPTrunk, error) {
	provider, err := s.provider(held.Vendor)
	if err != nil || held.Vendor != SIPTrunkVendor {
		return provider, nil, err
	}
	if err := s.sipTrunksReady(); err != nil {
		return nil, nil, err
	}
	// A vendor sold the number to another customer, which a trunk's word does not outweigh. Stream
	// lists a number on one trunk per app, so lined for this call in the app the buyer's lines are
	// made in, it would leave them unable to attach it or call from it there. A buyer whose app is
	// known to be unreadable (disconnected, no identity) is taken to share this one; a read that
	// failed is returned as it is, so the call is tried again rather than refused for good.
	buyers, err := s.store.PhoneNumbers.BoughtElsewhere(ctx, held.CustomerID, held.E164)
	if err != nil {
		return nil, nil, err
	}
	for _, buyer := range buyers {
		theirs, err := s.placed(ctx, buyer)
		if !settled(err) {
			return nil, nil, err
		}
		if err != nil || s.store.SameStreamApp(theirs, pin) {
			return nil, nil, stack.Wrap(store.ErrNumberHeldElsewhere)
		}
	}
	trunk, err := s.openSIPTrunk(ctx, held.CustomerID, held.SIPTrunkID)
	if err != nil {
		return nil, nil, err
	}
	return provider, trunk, nil
}

// allowlistFor is the addresses a per-call Stream trunk should accept the vendor's leg
// from. The bridge to a customer's own trunk presents the Stream trunk's password, so it
// needs none.
func (s *Service) allowlistFor(vendor string) ([]string, error) {
	if vendor == SIPTrunkVendor {
		return nil, nil
	}
	declared, _ := s.registry.Lookup(vendor)
	return trunkAllowlist(declared)
}

// sharedLine is the line a call from the number goes out on, or false when the call makes a
// trunk and rule of its own: the vendor cannot name the call on its leg, there is no key to
// seal a shared trunk's password with, the number was attached before its password was
// kept, it is attached in another app, or Stream routes it through a rule or trunk its line
// did not make. A number lined in another app only to place calls moves its line here.
func (s *Service) sharedLine(ctx context.Context, stream *Stream, pin int64, provider Provider, held store.PhoneNumber) (TrunkLine, bool, error) {
	if StreamLegOf(provider) != StreamLegShared {
		return TrunkLine{}, false, nil
	}
	// An attached number stays where its callers are sent. Stream lets a number be on one
	// trunk per app, so the call can still have one of its own here.
	if held.StreamTrunkID != "" && held.StreamAppPK != pin && held.AttachedAt == nil {
		if err := s.unline(ctx, held, "lined a number in another app", time.Time{}); err != nil {
			return TrunkLine{}, false, err
		}
	}
	line, err := s.line(ctx, stream, pin, held)
	if errors.Is(err, ErrNoSealer) || errors.Is(err, store.ErrNoStoredBridge) ||
		errors.Is(err, errLinedElsewhere) || errors.Is(err, errRoutedElsewhere) {
		return TrunkLine{}, false, nil
	}
	if err != nil {
		return TrunkLine{}, false, err
	}
	return line, true, nil
}

// clearLinesFor takes a number a vendor sold the customer off the lines other customers' own
// trunks hold it on, in the app pin names, before it is lined or attached there. Stream lists a
// number on one trunk per app, so a line made for a trunk the number was added to before it was
// bought would refuse the buyer's for good: its customer, which may no longer call from it there
// (dialVendor), would never take it down. A number on the customer's own trunk takes nobody's.
func (s *Service) clearLinesFor(ctx context.Context, held store.PhoneNumber, pin int64) error {
	if held.Vendor == SIPTrunkVendor {
		return nil
	}
	lines, err := s.store.PhoneNumbers.TrunkLinesElsewhere(ctx, held.CustomerID, held.E164, pin)
	if err != nil {
		return err
	}
	for _, line := range lines {
		if err := s.evict(ctx, line); err != nil {
			return err
		}
	}
	return nil
}

// createTrunk makes a trunk for one number at Stream, past a forgotten line of the number's.
func (s *Service) createTrunk(ctx context.Context, stream *Stream, pin int64, trunk Trunk) (string, Bridge, error) {
	var id string
	var bridge Bridge
	err := s.pastForgottenLines(ctx, stream, pin, trunk.Numbers[0], func() (err error) {
		id, bridge, err = stream.CreateTrunk(ctx, trunk)
		return err
	})
	return id, bridge, err
}

// pastForgottenLines runs put, which puts a number on a trunk, and runs it once more when Stream
// refused it and a forgotten line of the number's was taken down. Stream is asked what it lists only
// then: a number it takes has no such line in its way, so asking first would let a list that
// fails or hangs stop a call there is nothing to clear for. A list that fails leaves the refusal
// the answer; a line Stream will not take down is an error of its own.
func (s *Service) pastForgottenLines(ctx context.Context, stream *Stream, pin int64, e164 string, put func() error) error {
	refusal := put()
	if !refused(refusal) {
		return refusal
	}
	listing, err := s.sipTrunksListing(ctx, stream, pin, e164)
	if err != nil {
		s.logger.Error("could not look for a number's forgotten line", "stream_app", pin, "error", err)
		return refusal
	}
	var cleared bool
	for _, trunk := range listing {
		taken, err := s.clearForgotten(ctx, stream, trunk, e164)
		if err != nil {
			return err
		}
		cleared = cleared || taken
	}
	if !cleared {
		return refusal
	}
	return put()
}

// sipTrunksListing are the trunks made in the app for numbers on customers' own trunks that Stream
// lists the number on. A data move deletes a number's row, gives it back or makes it another
// number's without reaching Stream, so only Stream still knows a line it left.
func (s *Service) sipTrunksListing(ctx context.Context, stream *Stream, pin int64, e164 string) ([]store.StreamTrunk, error) {
	trunks, err := s.store.StreamTrunks.OfSIPTrunks(ctx, pin)
	if err != nil || len(trunks) == 0 {
		return nil, err
	}
	listing, err := stream.listing(ctx, e164)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(trunks, func(t store.StreamTrunk) bool { return !slices.Contains(listing, t.ID) }), nil
}

// clearForgotten deletes a number's rules on a trunk and leaves the trunk with the numbers its
// rows say, or deletes it, with the rules of any number on it alone, when they are none. It answers
// false for a number still held on the trunk, which it leaves as it is. A failure leaves the trunk's
// row, to be found again by the next attach or call Stream refuses the number.
func (s *Service) clearForgotten(ctx context.Context, stream *Stream, trunk store.StreamTrunk, e164 string) (bool, error) {
	var cleared bool
	group := store.TrunkGroup{CustomerID: trunk.CustomerID, StreamAppPK: trunk.StreamAppPK, Vendor: trunk.Vendor}
	err := s.inTrunkGroup(ctx, group, func(ctx context.Context, tx *store.TrunkGroupTx) error {
		trunks, err := tx.Trunks()
		if err != nil {
			return err
		}
		i := slices.IndexFunc(trunks, func(t store.StreamTrunk) bool { return t.ID == trunk.ID })
		if i < 0 || slices.Contains(trunks[i].Numbers, e164) {
			return nil
		}
		held := trunks[i].Numbers
		rules, err := stream.rulesOn(ctx, trunk.ID)
		if err != nil {
			return err
		}
		for id, number := range rules {
			if number != e164 && len(held) > 0 {
				continue
			}
			if err := stream.DeleteRoute(ctx, id); err != nil {
				return err
			}
		}
		cleared = true
		if len(held) == 0 {
			if err := stream.DeleteTrunk(ctx, trunk.ID); err != nil {
				return err
			}
			return tx.DeleteTrunk(trunk.ID)
		}
		return stream.UpdateTrunk(ctx, trunk.ID, Trunk{Name: sharedTrunkName(trunk.Vendor, trunk.CustomerID), Numbers: held})
	})
	return cleared, err
}

// evict takes another customer's line for a number down at Stream, and only then forgets it. A
// Stream that cannot be reached, or that refuses, leaves it as it was, to be found and taken down
// by the next attach or call that needs it gone.
func (s *Service) evict(ctx context.Context, line store.PhoneNumber) error {
	// Found before the lock: reaching the app may read the database, which under the lock
	// would take a second pooled connection.
	stream, err := s.streamToRemove(ctx, line.CustomerID, line.StreamAppPK)
	if err != nil {
		return err
	}
	group := store.TrunkGroup{CustomerID: line.CustomerID, StreamAppPK: line.StreamAppPK, Vendor: line.Vendor}
	return s.inTrunkGroup(ctx, group, func(ctx context.Context, tx *store.TrunkGroupTx) error {
		number, err := tx.Number(line.E164)
		// Given back by its customer since it was found.
		if errors.Is(err, store.ErrNoNumber) {
			return nil
		}
		if err != nil {
			return err
		}
		// Taken off, or lined in another app, since it was found.
		if number.StreamTrunkID == "" || number.StreamAppPK != line.StreamAppPK {
			return nil
		}
		return s.offTrunk(ctx, tx, stream, nil, number, "another customer bought the number", true)
	})
}

// forgetCallResource deletes the row of a leg on a shared line that was never placed.
// Best-effort: a failure is logged, never returned, so it cannot mask the dial's own.
func (s *Service) forgetCallResource(ctx context.Context, resource *store.CallResource) {
	if err := s.store.CallResources.Delete(ctx, resource.ID); err != nil {
		s.logger.Error("could not delete the row of a leg that did not place",
			"trunk", resource.TrunkID, "call", resource.CallID, "error", err)
	}
}

// provider is the provider of a vendor name, which for sip_trunk is not in the registry.
func (s *Service) provider(vendor string) (Provider, error) {
	if vendor != SIPTrunkVendor {
		return s.registry.Open(vendor)
	}
	if s.sipTrunks == nil {
		return nil, stack.Wrap(ErrSIPTrunksDisabled)
	}
	return s.sipTrunks, nil
}

// joinFeatures renders call features for an error message.
func joinFeatures(features []CallFeature) string {
	rendered := make([]string, 0, len(features))
	for _, feature := range features {
		rendered = append(rendered, string(feature))
	}
	return strings.Join(rendered, " or ")
}

// TransferRequest hands a live call to a human.
type TransferRequest struct {
	Owner routing.Owner
	// From is the customer's number the human is called from, which is what they see.
	From string
	// To is the human being brought onto the call.
	To string
	// CallID is the Stream call to bring them into, which is the one the agent and the
	// caller are already on.
	CallID string
	// CallType is the Stream call type. Empty means "default".
	CallType string
	// StreamApp is the app that call is in, the session's pin: zero for the deployment's.
	StreamApp int64
}

// Transfer brings a human onto a call that is already happening.
//
// Stream's SIP is inbound only, so a transfer is not a handover of the caller's leg: it is
// a second leg, originated at the vendor and routed into the same Stream call, after which
// three parties are on it and the agent leaves. The caller is never moved, which is why they
// hear nothing of it happening and why nothing is lost if the human does not answer.
//
// The routing rule is pinned to the live call rather than to the number's own template, so
// the human joins this conversation rather than whichever one their number would have
// landed in. It is created per transfer because a trunk's password is only readable when
// the trunk is made, and keeping SIP credentials to reuse later is a worse trade than
// making a second trunk. A vendor that names the call on its leg uses the number's shared
// line instead.
func (s *Service) Transfer(ctx context.Context, request TransferRequest) (Dialed, error) {
	if err := s.Suspended(ctx, request.Owner.CustomerID); err != nil {
		return Dialed{}, err
	}
	if request.To != "" {
		if err := Dialable(request.To); err != nil {
			return Dialed{}, err
		}
	}
	// A transfer dials someone as surely as a call does, so it answers to the same gate; the
	// leg it transfers is already up, and only the human's counts as a new call.
	into := dlc.Call{Type: cmp.Or(request.CallType, defaultCallType), ID: request.CallID}
	if err := s.gate.AllowTransfer(ctx, request.Owner.CustomerID, request.To, into); err != nil {
		return Dialed{}, stack.Wrap(err)
	}
	// The human is routed into the call the agent is on, so the trunk is made in the app
	// that call is in.
	stream, err := s.streamForApp(ctx, request.Owner.CustomerID, request.StreamApp)
	if errors.Is(err, ErrNoStream) {
		return Dialed{}, stack.Wrap(marked{errors.New("phone: transferring a call needs stream credentials"), ErrNoStream})
	}
	if err != nil {
		return Dialed{}, err
	}
	if s.store == nil {
		return Dialed{}, stack.Wrap(errors.New("phone: transferring a call needs a database to know who holds the number"))
	}
	if request.CallID == "" {
		return Dialed{}, stack.Wrap(Invalid(errors.New("phone: a transfer needs the call to transfer into")))
	}
	if request.To == "" {
		return Dialed{}, stack.Wrap(Invalid(errors.New("phone: a transfer needs someone to transfer to")))
	}
	if request.From == "" {
		return Dialed{}, stack.Wrap(Invalid(errors.New("phone: a transfer needs one of your numbers to dial from")))
	}

	held, err := s.store.PhoneNumbers.GetByE164(ctx, request.Owner.CustomerID, request.From)
	if err != nil {
		return Dialed{}, stack.Wrap(err)
	}
	provider, trunk, err := s.dialVendor(ctx, held, request.StreamApp)
	if err != nil {
		return Dialed{}, stack.Wrap(err)
	}

	allowedIPs, err := s.allowlistFor(held.Vendor)
	if err != nil {
		return Dialed{}, stack.Wrap(err)
	}

	// Resolve the call type the same way CreateRoute does, so the row recorded below
	// carries the concrete type that session_ended's call_cid will name. A transfer joins
	// an existing call whose type was fixed at creation; storing the raw empty type would
	// not match "default" at cleanup and would leak the transfer's trunk.
	callType := request.CallType
	if callType == "" {
		callType = defaultCallType
	}

	if err := s.clearLinesFor(ctx, held, request.StreamApp); err != nil {
		return Dialed{}, err
	}
	var line TrunkLine
	var shared bool
	// The number's shared rule routes into defaultCallType only, so a call of another type
	// takes a rule of its own pinned to it.
	if callType == defaultCallType {
		line, shared, err = s.sharedLine(ctx, stream, request.StreamApp, provider, held)
		if err != nil {
			return Dialed{}, stack.Wrap(err)
		}
	}
	var trunkID, routeID string
	var bridge Bridge
	committed := false
	if shared {
		trunkID, routeID, bridge = line.TrunkID, line.RouteID, line.Bridge
	} else {
		trunkID, bridge, err = s.createTrunk(ctx, stream, request.StreamApp, Trunk{
			Name:       "transfer-" + request.CallID,
			Numbers:    []string{request.From},
			AllowedIPs: allowedIPs,
		})
		if err != nil {
			return Dialed{}, stack.Wrap(err)
		}

		defer func() {
			if committed {
				return
			}
			// Roll back what this transfer created but never placed, so a transfer that did
			// not start does not leave a billable Stream trunk behind. Best-effort: a cleanup
			// error is logged, never returned, so it cannot mask the real failure.
			s.deleteLines(ctx, stream, routeID, trunkID, request.From, "rolling back a transfer that did not place")
		}()

		routeID, err = stream.CreateRoute(ctx, Route{
			Name:          "transfer-" + request.CallID,
			TrunkIDs:      []string{trunkID},
			CalledNumbers: []string{request.From},
			CallID:        request.CallID,
			CallType:      callType,
		})
		if err != nil {
			return Dialed{}, stack.Wrap(err)
		}

		bridge, err = bridge.WithNumber(request.From)
		if err != nil {
			return Dialed{}, stack.Wrap(err)
		}
	}

	resource := &store.CallResource{
		TrunkID:     trunkID,
		RouteID:     routeID,
		CallType:    callType,
		CallID:      request.CallID,
		CustomerID:  request.Owner.CustomerID,
		StreamAppPK: request.StreamApp,
		Shared:      shared,
	}
	if shared {
		if err := s.store.CallResources.Insert(ctx, resource); err != nil {
			return Dialed{}, stack.Wrap(err)
		}
	}

	outbound := Outbound{From: request.From, To: request.To, Bridge: bridge, Trunk: trunk}
	if shared {
		outbound.CallID = request.CallID
	}
	placed, err := routing.Call(ctx, s.dialMeter(held.Vendor, request.Owner), func(ctx context.Context) (Dialed, error) {
		return provider.Dial(ctx, outbound)
	})
	if err != nil {
		if shared {
			s.forgetCallResource(ctx, resource)
		}
		return Dialed{}, stack.Wrap(err)
	}

	committed = true
	// The human's leg is a leg of the call it joins, billed to whoever transferred from when
	// they answer.
	s.OpenLeg(ctx, request.Owner, store.CallLeg{
		StreamAppPK: request.StreamApp, CallType: callType, CallID: request.CallID, Vendor: held.Vendor,
		VendorCallID: placed.VendorCallID,
		Direction:    store.Outbound, LocalNumber: request.From, RemoteNumber: request.To,
	}, false)
	if !shared {
		if err := s.store.CallResources.Insert(ctx, resource); err != nil {
			// The transfer leg is placed and connecting; deleting its trunk now would drop
			// a live call. A logged, rare leak beats that. session_ended cannot clean a
			// trunk it has no row for, which is the cost of this failing.
			s.logger.Error("could not record a transfer's trunk for cleanup; it may leak",
				"trunk", trunkID, "call", request.CallID, "error", err)
		}
	}
	return placed, nil
}

// SendDigits presses digits on a call this service placed, which is how an agent answers a
// menu it has reached.
//
// The call is named by the id its vendor gave when it was dialled. An inbound call has no
// such id here, because it arrived at Stream rather than being made from this service, so
// only calls placed from here can be pressed at. The vendor accounts are shared, so a call
// customerID did not place is ErrCallNotFound, as one nobody placed is.
func (s *Service) SendDigits(ctx context.Context, customerID, vendor, vendorCallID, digits string) error {
	if vendorCallID == "" {
		return stack.Wrap(Invalid(errors.New("phone: pressing digits needs the call to press them on")))
	}
	if err := ValidateDigits(digits); err != nil {
		return err
	}
	if s.store == nil {
		return stack.Wrap(errors.New("phone: pressing digits needs a database"))
	}
	placed, err := s.store.CallLegs.Placed(ctx, customerID, vendor, vendorCallID)
	if err != nil {
		return err
	}
	if !placed {
		return stack.Wrap(ErrCallNotFound)
	}
	provider, err := s.provider(vendor)
	if err != nil {
		return err
	}
	return provider.SendDigits(ctx, vendorCallID, digits)
}

// Numbers returns what a customer holds.
func (s *Service) Numbers(ctx context.Context, customerID string, includeReleased bool) ([]store.PhoneNumber, error) {
	if s.store == nil {
		return nil, stack.Wrap(errors.New("phone: listing numbers needs a database"))
	}
	return s.store.PhoneNumbers.Select(ctx, customerID, store.PhoneNumberFilter{IncludeReleased: includeReleased})
}

// names renders capabilities for storage, since Postgres holds them as plain text.
func names(capabilities []Capability) []string {
	rendered := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		rendered = append(rendered, string(capability))
	}
	return rendered
}

// HangUpUnanswered hangs up the call placed into the customer's Stream call callID when nobody
// answered it, and keeps its outcome canceled. A call answered, ended or decided, and a Stream
// call nothing was placed into, are left as they are.
func (s *Service) HangUpUnanswered(ctx context.Context, customerID, callID string) error {
	if s.outcomes == nil {
		return nil
	}
	call, err := s.outcomes.Read(ctx, customerID, callID)
	if errors.Is(err, outcome.ErrUnknownCall) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("phone: read the call to hang up: %w", err)
	}
	if call.AnsweredAt != nil || call.Outcome != "" || call.VendorCallID == "" {
		return nil
	}
	provider, err := s.provider(call.Vendor)
	if err != nil {
		return err
	}
	hanger, ok := provider.(Hanger)
	if !ok {
		return stack.Wrap(fmt.Errorf("%w: %s cannot hang up a call it placed", ErrNotImplemented, call.Vendor))
	}
	if err := hanger.HangUp(ctx, call.VendorCallID); err != nil {
		return err
	}
	if _, err := s.outcomes.Canceled(ctx, call.Key); err != nil {
		return fmt.Errorf("phone: keep a hung-up call canceled: %w", err)
	}
	return nil
}
