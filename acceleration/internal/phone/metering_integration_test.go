//go:build integration

package phone

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/suite"
	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation/chattest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/dlc"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing/pricingtest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv/redisready"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
	"github.com/GetStream/Vision-Agents/acceleration/prices"
)

// MeteringSuite is what the phone service bills, written to real Postgres through a real
// recorder priced from the vendor list: a placed call and a transfer as free dial rows and as
// legs billed by the minute from when their phone joined, a leg nothing ended, and each month
// of a number. The vendor is a stub named telnyx, and the service's clock is the suite's.
type MeteringSuite struct {
	suite.Suite
	ctx      context.Context
	store    *store.Store
	redis    rueidis.Client
	recorder *routing.Recorder
	prices   *pricing.List
	service  *Service
	vendor   *stub
	clock    *meteringClock
	logs     *meteringLogs
	// deployment is the Stream app the suite's calls are in.
	deployment *chattest.Server
	customer   string
	e164       string
	// registry is where the service opens s.vendor.
	registry *Registry
	// failing fails the queries a test asks it to.
	failing *failingQueries
}

func TestMeteringSuite(t *testing.T) { suite.Run(t, new(MeteringSuite)) }

func (s *MeteringSuite) SetupSuite() {
	dsn, address := os.Getenv("ROUTER_POSTGRES_DSN"), os.Getenv("ROUTER_REDIS_ADDR")
	if dsn == "" || address == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN and ROUTER_REDIS_ADDR must be set")
	}
	s.ctx = context.Background()
	opened, err := store.Open(testenv.Database(dsn, "phone_metering"))
	s.Require().NoError(err)
	s.Require().NoError(opened.Migrate(s.ctx))
	s.store = opened
	s.T().Cleanup(func() { s.Require().NoError(opened.Close()) })
	s.failing = &failingQueries{}
	opened.ShardDB().AddQueryHook(s.failing)
	s.redis, err = redisready.NewClient(rueidis.ClientOption{InitAddress: []string{address}, DisableCache: true})
	s.Require().NoError(err)
	s.T().Cleanup(s.redis.Close)
}

func (s *MeteringSuite) SetupTest() {
	s.failing.fail("", 0)
	s.withSandbox(dlc.Sandbox{})
}

// withSandbox builds the service over a fresh customer, a gate holding them to sandbox, and a
// recorder written out before the test reads.
func (s *MeteringSuite) withSandbox(sandbox dlc.Sandbox) {
	list, err := pricing.NewList("vendor", pricing.AllAtCost(prices.Vendor().Entries), slog.New(slog.DiscardHandler))
	s.Require().NoError(err)
	s.prices = list
	s.recorder = routing.NewRecorder(routing.Phone, s.store, nil, list, slog.New(slog.DiscardHandler))
	s.T().Cleanup(s.recorder.Close)
	s.clock = &meteringClock{at: time.Now().UTC().Truncate(time.Second)}
	s.customer = "customer-" + uuid.NewString()
	s.e164 = aNumber()
	s.vendor = &stub{vendor: "telnyx", monthly: 1_000_000}
	config, err := DefaultConfig()
	s.Require().NoError(err)
	registry := NewRegistry(config)
	registry.Register(s.vendor.vendor, func() (Provider, error) { return s.vendor, nil })
	s.registry = registry
	declared, _ := config.Lookup(s.vendor.vendor)
	for _, name := range declared.Credentials {
		s.T().Setenv(name, "set")
	}
	deployment := chattest.NewServer(s.T())
	s.deployment = deployment
	s.logs = &meteringLogs{}
	s.service, err = NewService(ServiceOptions{
		Registry: registry, Store: s.store, Recorder: s.recorder, Now: s.clock.now,
		Apps:   &testApps{deployment: deployment, own: map[string]*chattest.Server{}, pins: map[string]int64{}, blocked: map[string]bool{}},
		Gate:   dlc.NewGate(s.store, s.redis, sandbox, nil),
		Logger: slog.New(slog.NewTextHandler(s.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	s.Require().NoError(err)
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: s.e164, Vendor: s.vendor.vendor, Country: "US", CustomerID: s.customer, PurchasedAt: s.clock.now(),
	}))
}

func (s *MeteringSuite) owner() routing.Owner {
	return routing.Owner{CustomerID: s.customer, Attribution: routing.Attribution{APIKey: "key-a", UserID: "u1"}}
}

var everyApp = store.AppScope{Unpinned: true}

// place places a call from the suite's number.
func (s *MeteringSuite) place() Placed {
	placed, err := s.service.Call(s.ctx, CallRequest{Owner: s.owner(), From: s.e164, To: "+15125550123"})
	s.Require().NoError(err)
	return placed
}

// answered is the phone of a call joining it, as the participant event says.
func (s *MeteringSuite) answered(callType, callID, participant string) {
	s.Require().NoError(s.service.Connect(s.ctx, everyApp, callType, callID, "session-1", participant, "", time.Time{}))
}

// joined is a phone of number joining a call, as the participant event says.
func (s *MeteringSuite) joined(callType, callID, participant, number string) {
	s.Require().NoError(s.service.Connect(s.ctx, everyApp, callType, callID, "session-1", participant, number, time.Time{}))
}

// left is a phone leaving a call, as the participant event says.
func (s *MeteringSuite) left(callType, callID, participant string) {
	s.Require().NoError(s.service.Left(s.ctx, everyApp, callType, callID, participant, time.Time{}))
}

// ended is the call ending, as session_ended says.
func (s *MeteringSuite) ended(callType, callID string) {
	s.Require().NoError(s.service.EndLegs(s.ctx, everyApp, callType, callID, time.Time{}))
}

// arrived opens the inbound leg of a call to the suite's number, as a session_started that
// lists no caller does.
func (s *MeteringSuite) arrived(callID string) {
	s.service.OpenLeg(s.ctx, routing.Owner{CustomerID: s.customer, Attribution: routing.Attribution{APIKey: routing.OriginPhoneInbound}},
		store.CallLeg{CallType: defaultCallType, CallID: callID, StreamSessionID: "session-1", Vendor: s.vendor.vendor,
			Direction: store.Inbound, LocalNumber: s.e164, Country: "US"}, true)
}

// transfer rings human into the call.
func (s *MeteringSuite) transfer(callID, human string) {
	_, err := s.service.Transfer(s.ctx, TransferRequest{Owner: s.owner(), From: s.e164, To: human, CallID: callID})
	s.Require().NoError(err)
}

func (s *MeteringSuite) TestTheCustomerWhoTransferredCanPressDigitsOnTheTransferLeg() {
	s.transfer("call-"+uuid.NewString(), "+15125550124")

	s.Require().NoError(s.service.SendDigits(s.ctx, s.customer, s.vendor.vendor, s.vendor.called, "12"))
	s.Equal([]string{s.vendor.called + ":12"}, s.vendor.pressed)
}

func (s *MeteringSuite) TestAnotherCustomerCannotPressDigitsOnATransferLeg() {
	s.transfer("call-"+uuid.NewString(), "+15125550124")

	err := s.service.SendDigits(s.ctx, "customer-"+uuid.NewString(), s.vendor.vendor, s.vendor.called, "12")
	s.ErrorIs(err, ErrCallNotFound)
	s.Empty(s.vendor.pressed)
}

// legsOf is the legs of the call, open or ended, by direction.
func (s *MeteringSuite) legsOf(callID string) map[string]store.CallLeg {
	var legs []store.CallLeg
	s.Require().NoError(s.store.ShardDB().NewSelect().Model(&legs).
		Where("customer_id = ?", s.customer).Where("call_id = ?", callID).Scan(s.ctx))
	byDirection := make(map[string]store.CallLeg, len(legs))
	for _, leg := range legs {
		byDirection[leg.Direction] = leg
	}
	s.Require().Len(byDirection, len(legs), "one leg each way")
	return byDirection
}

func (s *MeteringSuite) TestATransfersPhoneConnectsItsOwnLegNotAnInboundOneWithoutItsCaller() {
	callID := "call-" + uuid.NewString()
	s.arrived(callID)
	s.clock.advance(time.Minute)
	s.transfer(callID, "+15125550124")
	s.clock.advance(10 * time.Second)
	s.joined(defaultCallType, callID, "human-1", "+15125550124")
	s.clock.advance(2 * time.Minute)

	s.left(defaultCallType, callID, "human-1")

	legs := s.legsOf(callID)
	s.Equal("human-1", legs[store.Outbound].ParticipantSession, "the human's number is the transfer's")
	s.NotNil(legs[store.Outbound].EndedAt, "the human hung up")
	s.Empty(legs[store.Inbound].ParticipantSession)
	s.Nil(legs[store.Inbound].EndedAt, "the caller is still on the line")
	s.Equal(pricing.Quantities{pricing.CallSeconds: 120}, s.row(ModelCallMinutes).Meters)

	s.clock.advance(time.Minute)
	s.ended(defaultCallType, callID)
	minutes := s.rowsWhen(ModelCallMinutes, 2)
	s.Equal(pricing.Quantities{pricing.CallSeconds: 250}, minutes[0].Meters, "the caller's every second, from when it arrived")
	s.Equal("inbound", minutes[0].Attributes[AttributeDirection])
}

// A leave delivered before its join ends the leg the join connects, rather than leave it
// open, and billed, until the call ends.
func (s *MeteringSuite) TestALeaveBeforeItsJoinEndsTheLegItConnects() {
	callID := "call-" + uuid.NewString()
	s.arrived(callID)
	s.transfer(callID, "+15125550124")

	s.left(defaultCallType, callID, "human-1")
	s.Len(s.openLegs(), 2, "no leg is the phone's yet")
	s.clock.advance(time.Second)
	s.joined(defaultCallType, callID, "human-1", "+15125550124")

	legs := s.legsOf(callID)
	s.Equal("human-1", legs[store.Outbound].ParticipantSession)
	s.NotNil(legs[store.Outbound].EndedAt, "the phone had already left")
	s.Zero(s.row(ModelCallMinutes).Meters[pricing.CallSeconds])
	s.clock.advance(time.Hour)
	s.ended(defaultCallType, callID)
	minutes := s.rowsWhen(ModelCallMinutes, 2)
	s.Zero(minutes[1].Meters[pricing.CallSeconds], "the transfer, which the call ending did not bill again")
	s.Equal(pricing.Quantities{pricing.CallSeconds: 3601}, minutes[0].Meters, "the caller")
	time.Sleep(300 * time.Millisecond)
	s.Len(s.rows(ModelCallMinutes), 2)
}

// A leave and a join both delivered before the leg's row is written end the leg as it opens,
// billing nothing: a leg starts no earlier than it was dialled.
func (s *MeteringSuite) TestAJoinAndLeaveBeforeTheirLegEndItAsItOpens() {
	callID := "call-" + uuid.NewString()
	s.joined(defaultCallType, callID, "human-1", "+15125550124")
	s.clock.advance(40 * time.Second)
	s.left(defaultCallType, callID, "human-1")
	s.clock.advance(time.Second)

	s.transfer(callID, "+15125550124")

	s.Empty(s.openLegs())
	s.Zero(s.row(ModelCallMinutes).Meters[pricing.CallSeconds])
	s.Zero(s.kept(callID))
}

// A placed call's phone can join before the vendor's answer writes its leg: the leg takes
// the join, and starts from when it was dialled.
func (s *MeteringSuite) TestAJoinBeforeTheOutboundLegIsWrittenStartsItFromTheJoin() {
	callID := "call-" + uuid.NewString()
	s.joined(defaultCallType, callID, "human-1", "+15125550124")
	s.Empty(s.openLegs())
	s.clock.advance(5 * time.Second)

	s.transfer(callID, "+15125550124")
	s.clock.advance(time.Minute)
	s.joined(defaultCallType, callID, "human-1", "+15125550124")
	s.ended(defaultCallType, callID)

	s.Equal(pricing.Quantities{pricing.CallSeconds: 60}, s.row(ModelCallMinutes).Meters, "from when it was dialled, after the join")
	s.Zero(s.kept(callID), "the join was taken")
}

// A vendor may present the number a call was placed from as the caller of the call it bridges
// into Stream: that is the leg's phone too.
func (s *MeteringSuite) TestAPhoneJoiningAsTheNumberTheCallWasPlacedFromConnectsTheLeg() {
	callID := "call-" + uuid.NewString()
	s.arrived(callID)
	s.transfer(callID, "+15125550124")
	s.joined(defaultCallType, callID, "human-1", s.e164)

	legs := s.legsOf(callID)
	s.Equal("human-1", legs[store.Outbound].ParticipantSession)
	s.Empty(legs[store.Inbound].ParticipantSession)
}

// The caller's join delivered before the session_started that opens the inbound leg is taken
// by that leg, so the caller leaving ends it. Hook numbers may lack the plus.
func (s *MeteringSuite) TestACallersJoinBeforeSessionStartedIsTakenByTheInboundLeg() {
	callID := "call-" + uuid.NewString()
	s.joined(defaultCallType, callID, "caller-1", "15125550199")
	s.Empty(s.openLegs())
	s.clock.advance(2 * time.Second)
	s.arrived(callID)
	s.transfer(callID, "+15125550124")
	s.clock.advance(30 * time.Second)

	s.joined(defaultCallType, callID, "human-1", "15125550124")
	s.clock.advance(time.Minute)
	s.left(defaultCallType, callID, "human-1")
	s.left(defaultCallType, callID, "caller-1")

	legs := s.legsOf(callID)
	s.Equal("caller-1", legs[store.Inbound].ParticipantSession)
	s.NotNil(legs[store.Inbound].EndedAt, "the caller hung up")
	minutes := s.rowsWhen(ModelCallMinutes, 2)
	s.Equal(pricing.Quantities{pricing.CallSeconds: 60}, minutes[1].Meters, "the transfer")
	s.Equal(pricing.Quantities{pricing.CallSeconds: 90}, minutes[0].Meters, "the caller, from the call arriving to their leave")
	s.Zero(s.kept(callID))
}

// A leave kept between Connect reading the phone's leaves and connecting its leg ends the leg
// at it, rather than being forgotten with the leg left open, and billed, until the call ends.
func (s *MeteringSuite) TestALeaveRacingItsJoinEndsTheLeg() {
	callID := "call-" + uuid.NewString()
	s.transfer(callID, "+15125550124")
	s.clock.advance(10 * time.Second)
	s.service.connecting = func() {
		s.service.connecting = nil
		s.left(defaultCallType, callID, "human-1")
	}

	s.joined(defaultCallType, callID, "human-1", "+15125550124")

	s.Empty(s.openLegs(), "the leg ended at the leave")
	s.Zero(s.kept(callID), "the leave was applied, then forgotten")
	s.clock.advance(time.Hour)
	s.ended(defaultCallType, callID)
	s.row(ModelCallMinutes)
	time.Sleep(300 * time.Millisecond)
	s.Len(s.rows(ModelCallMinutes), 1)
	s.Zero(s.rows(ModelCallMinutes)[0].Meters[pricing.CallSeconds], "not the hour until the call ended")
}

// An event time from before the leg was dialled starts it when it was dialled, and one further
// back than an event is ever late is taken as now: neither bills hours nobody talked.
func (s *MeteringSuite) TestAnEventTimeFromLongAgoOrBeforeTheDialIsClamped() {
	callID := "call-" + uuid.NewString()
	s.transfer(callID, "+15125550124")
	dialed := s.clock.now()
	s.clock.advance(time.Minute)
	s.Require().NoError(s.service.Connect(s.ctx, everyApp, defaultCallType, callID, "session-1", "human-1", "+15125550124",
		dialed.Add(-10*time.Minute)))
	s.Equal(dialed, s.legsOf(callID)[store.Outbound].StartedAt.UTC(), "no earlier than the dial")

	late := "call-" + uuid.NewString()
	s.transfer(late, "+15125550125")
	s.clock.advance(3 * time.Hour)
	s.Require().NoError(s.service.Connect(s.ctx, everyApp, defaultCallType, late, "session-1", "human-2", "+15125550125",
		s.clock.now().Add(-150*time.Minute)))
	s.Equal(s.clock.now(), s.legsOf(late)[store.Outbound].StartedAt.UTC(), "an event two and a half hours old happened now")
}

// A join refused once and delivered again after the call ended revives the leg the call's end
// left unanswered: billed once, from the join to the end.
func (s *MeteringSuite) TestAJoinDeliveredAgainAfterTheCallEndedRevivesItsLegAndBillsItOnce() {
	callID := "call-" + uuid.NewString()
	s.transfer(callID, "+15125550124")
	joined := s.clock.now().Add(30 * time.Second)
	s.clock.advance(2 * time.Minute)
	s.ended(defaultCallType, callID)
	s.Equal(store.LegUnanswered, s.legsOf(callID)[store.Outbound].EndReason)
	s.clock.advance(time.Minute)

	for range 2 {
		s.Require().NoError(s.service.Connect(s.ctx, everyApp, defaultCallType, callID, "session-1", "human-1", "+15125550124", joined))
	}
	s.ended(defaultCallType, callID)

	leg := s.legsOf(callID)[store.Outbound]
	s.Equal(store.LegEnded, leg.EndReason)
	s.Equal("human-1", leg.ParticipantSession)
	s.Equal(pricing.Quantities{pricing.CallSeconds: 90}, s.row(ModelCallMinutes).Meters)
	time.Sleep(300 * time.Millisecond)
	s.Len(s.rows(ModelCallMinutes), 1)
	s.Zero(s.kept(callID), "nothing kept for a leg that will never open")
}

// Two transfers placed from the same number pair with their phones by the number each rang;
// the number they were placed from only matches when no leg's remote number does.
func (s *MeteringSuite) TestTwoLegsPlacedFromOneNumberPairByTheNumberEachRang() {
	callID := "call-" + uuid.NewString()
	s.transfer(callID, "+15125550124")
	s.clock.advance(time.Second)
	s.transfer(callID, "+15125550125")

	s.joined(defaultCallType, callID, "human-2", "+15125550125")
	s.joined(defaultCallType, callID, "human-1", s.e164)

	legs := s.openLegs()
	s.Require().Len(legs, 2)
	for _, leg := range legs {
		switch leg.RemoteNumber {
		case "+15125550125":
			s.Equal("human-2", leg.ParticipantSession, "the number it rang, though both were placed from one")
		default:
			s.Equal("human-1", leg.ParticipantSession)
		}
	}
}

// A phone's kept join is not taken by a leg another phone already has, nor does it move that
// leg's start.
func (s *MeteringSuite) TestAKeptJoinDoesNotMoveTheStartOfALegAnotherPhoneHas() {
	callID := "call-" + uuid.NewString()
	s.transfer(callID, "+15125550124")
	s.clock.advance(10 * time.Second)
	s.joined(defaultCallType, callID, "human-1", "+15125550124")
	started := s.legsOf(callID)[store.Outbound].StartedAt.UTC()
	joined := s.clock.now().Add(-time.Minute)
	s.Require().NoError(s.store.CallParticipants.Record(s.ctx, everyApp, store.CallParticipant{
		CallType: defaultCallType, CallID: callID, ParticipantSession: "human-2", Number: "+15125550124",
		JoinedAt: &joined,
	}, s.clock.now()))
	s.clock.advance(time.Second)

	s.transfer(callID, "+15125550124")

	legs := s.openLegs()
	s.Require().Len(legs, 2)
	for _, leg := range legs {
		if leg.ParticipantSession == "human-1" {
			s.Equal(started, leg.StartedAt.UTC())
		} else {
			s.Equal("human-2", leg.ParticipantSession, "the second transfer takes the kept join")
		}
	}
}

func (s *MeteringSuite) TestTheSweepDeletesJoinsKeptForADay() {
	callID := "call-" + uuid.NewString()
	s.joined(defaultCallType, callID, "human-1", "+15125550124")
	s.Equal(1, s.kept(callID))

	s.clock.advance(23 * time.Hour)
	_, err := s.service.SweepLegs(s.ctx)
	s.Require().NoError(err)
	s.Equal(1, s.kept(callID))
	s.clock.advance(2 * time.Hour)
	_, err = s.service.SweepLegs(s.ctx)
	s.Require().NoError(err)
	s.Zero(s.kept(callID))
}

func (s *MeteringSuite) TestAStoreThatCannotBeReachedFailsTheJoinAndTheLeave() {
	unreachable, err := store.Open("postgres://postgres:postgres@127.0.0.1:1/none?sslmode=disable")
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = unreachable.Close() })
	service, err := NewService(ServiceOptions{Registry: s.service.registry, Store: unreachable, Logger: slog.New(slog.DiscardHandler)})
	s.Require().NoError(err)

	s.Error(service.Connect(s.ctx, everyApp, defaultCallType, "call-1", "session-1", "human-1", "+15125550124", time.Time{}))
	s.Error(service.Left(s.ctx, everyApp, defaultCallType, "call-1", "human-1", time.Time{}))
}

// Legs are timed by when Stream made each event, not when it was delivered: a join delivered
// late, a leave delivered before it, and a session_ended delivered a minute after the call.
func (s *MeteringSuite) TestLegsAreTimedByWhenTheirEventsWereMade() {
	callID := "call-" + uuid.NewString()
	made := s.clock.now()
	s.service.OpenLeg(s.ctx, routing.Owner{CustomerID: s.customer, Attribution: routing.Attribution{APIKey: routing.OriginPhoneInbound}},
		store.CallLeg{CallType: defaultCallType, CallID: callID, StreamSessionID: "session-1", Vendor: s.vendor.vendor,
			Direction: store.Inbound, LocalNumber: s.e164, Country: "US", DialedAt: made}, true)
	s.transfer(callID, "+15125550124")
	s.clock.advance(5 * time.Minute)

	s.Require().NoError(s.service.Left(s.ctx, everyApp, defaultCallType, callID, "human-1", made.Add(100*time.Second)))
	s.Require().NoError(s.service.Connect(s.ctx, everyApp, defaultCallType, callID, "session-1", "human-1", "+15125550124",
		made.Add(40*time.Second)))
	s.Require().NoError(s.service.EndLegs(s.ctx, everyApp, defaultCallType, callID, made.Add(4*time.Minute)))

	minutes := s.rowsWhen(ModelCallMinutes, 2)
	s.Equal(pricing.Quantities{pricing.CallSeconds: 240}, minutes[0].Meters, "the caller, from the session_started made at the start")
	s.Equal(pricing.Quantities{pricing.CallSeconds: 60}, minutes[1].Meters, "the human, from their join to their leave")

	later := "call-" + uuid.NewString()
	s.Require().NoError(s.service.Connect(s.ctx, everyApp, defaultCallType, later, "session-1", "human-2", "",
		s.clock.now().Add(time.Hour)))
	var kept store.CallParticipant
	s.Require().NoError(s.store.ShardDB().NewSelect().Model(&kept).Where("call_id = ?", later).Scan(s.ctx))
	s.Equal(s.clock.now(), kept.JoinedAt.UTC(), "a time still to come on the service's clock is taken as now")
}

const unhooked = "call hook does not deliver participant events; re-run cmd/phone hooks"

// The agent's own join is a participant event on every placed call: one that ends unanswered
// with none delivered is an app whose call hook was not pointed again, logged once an hour.
func (s *MeteringSuite) TestAPlacedCallEndingWithNoParticipantEventIsLoggedOnceAnHourPerApp() {
	placed := s.place()
	s.ended(placed.CallType, placed.CallID)
	s.Equal(1, s.logs.count("ERROR", unhooked))

	again := s.place()
	s.ended(again.CallType, again.CallID)
	s.Equal(1, s.logs.count("ERROR", unhooked), "once an hour per app")

	s.clock.advance(61 * time.Minute)
	third := s.place()
	s.ended(third.CallType, third.CallID)
	s.Equal(2, s.logs.count("ERROR", unhooked))
}

func (s *MeteringSuite) TestAnUnansweredPlacedCallWhoseAgentJoinWasDeliveredIsNotLogged() {
	placed := s.place()
	s.Require().NoError(s.service.Heard(s.ctx, everyApp, placed.CallType, placed.CallID))
	s.Require().NoError(s.service.Heard(s.ctx, everyApp, placed.CallType, placed.CallID))

	s.ended(placed.CallType, placed.CallID)

	s.Zero(s.logs.count("ERROR", unhooked))
	answered := s.place()
	s.answered(answered.CallType, answered.CallID, "phone-1")
	s.ended(answered.CallType, answered.CallID)
	s.Zero(s.logs.count("ERROR", unhooked), "an answered call is not checked")
}

// A participant event of a video call, which rings no phone, writes nothing: only a call with an
// outbound leg ringing is checked for a hook that does not deliver them.
func (s *MeteringSuite) TestAParticipantEventOfACallWithNoLegRingingRecordsNothing() {
	video := "call-" + uuid.NewString()
	for range 3 {
		s.Require().NoError(s.service.Heard(s.ctx, everyApp, defaultCallType, video))
	}
	s.Zero(s.kept(video))

	placed := s.place()
	s.clock.advance(2 * heardRecheck)
	s.Require().NoError(s.service.Heard(s.ctx, everyApp, placed.CallType, placed.CallID))
	s.Equal(1, s.kept(placed.CallID))
}

// The sweep deletes every row kept for a day, in as many batches as it takes.
func (s *MeteringSuite) TestTheSweepDeletesEveryRowKeptForADayInBatches() {
	callID := "call-" + uuid.NewString()
	old := s.clock.now().Add(-25 * time.Hour)
	rows := make([]store.CallParticipant, legSweepBatch+1)
	for i := range rows {
		rows[i] = store.CallParticipant{Unpinned: true, CallType: defaultCallType, CallID: callID,
			ParticipantSession: "phone-" + strconv.Itoa(i), JoinedAt: &old, RecordedAt: old}
	}
	_, err := s.store.ShardDB().NewInsert().Model(&rows).Exec(s.ctx)
	s.Require().NoError(err)

	_, err = s.service.SweepLegs(s.ctx)
	s.Require().NoError(err)

	s.Zero(s.kept(callID))
}

// outboundMinutes is what seconds of an outbound US call cost at the vendor list's rates, read
// from vendor.json so a price change there leaves the tests passing.
func (s *MeteringSuite) outboundMinutes(seconds int64) int64 {
	item := pricing.Item{Modality: string(routing.Phone), Provider: "telnyx", Model: ModelCallMinutes, Region: pricing.DefaultRegion,
		Attributes: map[string]string{"country": "US", "direction": "outbound"}}
	return pricingtest.Micros(s.T(), s.prices, item, pricing.Quantities{pricing.CallSeconds: seconds})
}

// kept is how many joins and leaves of the call are kept for a leg yet to open.
func (s *MeteringSuite) kept(callID string) int {
	count, err := s.store.ShardDB().NewSelect().Model((*store.CallParticipant)(nil)).
		Where("call_id = ?", callID).Count(s.ctx)
	s.Require().NoError(err)
	return count
}

func (s *MeteringSuite) TestAPlacedCallIsBilledForItsMinutesFromWhenItWasAnswered() {
	placed := s.place()
	s.clock.advance(20 * time.Second)
	s.answered(placed.CallType, placed.CallID, "phone-1")
	s.clock.advance(61 * time.Second)

	s.ended(placed.CallType, placed.CallID)

	dial := s.row(ModelDial)
	s.True(dial.Success)
	s.False(dial.Billable, "placing a call is free: the vendor bills its minutes")
	minutes := s.row(ModelCallMinutes)
	s.Equal(pricing.Quantities{pricing.CallSeconds: 61}, minutes.Meters, "the ringing is not billed")
	s.Equal(s.outboundMinutes(61), minutes.CostMicros, "two started minutes")
	s.Equal("key-a", minutes.APIKey, "the minutes keep the key that placed the call")
	s.Equal("u1", minutes.UserID)
	s.Equal(map[string]string{"country": "US", "direction": "outbound", "measured_by": "router"}, minutes.Attributes)
}

func (s *MeteringSuite) TestACallNobodyAnsweredBillsNothing() {
	placed := s.place()
	s.clock.advance(time.Minute)

	s.ended(placed.CallType, placed.CallID)

	s.row(ModelDial)
	s.Never(func() bool { return len(s.rows(ModelCallMinutes)) > 0 }, time.Second, 50*time.Millisecond)
	s.Empty(s.openLegs())
}

func (s *MeteringSuite) TestALegEndsWhenItsPhoneLeaves() {
	placed := s.place()
	s.answered(placed.CallType, placed.CallID, "phone-1")
	s.answered(placed.CallType, placed.CallID, "phone-1")
	s.clock.advance(30 * time.Second)

	s.left(placed.CallType, placed.CallID, "phone-1")
	s.clock.advance(time.Minute)
	s.ended(placed.CallType, placed.CallID)

	minutes := s.row(ModelCallMinutes)
	s.Equal(pricing.Quantities{pricing.CallSeconds: 30}, minutes.Meters, "billed until the phone hung up, not the call")
	time.Sleep(300 * time.Millisecond)
	s.Len(s.rows(ModelCallMinutes), 1, "the join delivered twice starts one leg")
}

func (s *MeteringSuite) TestAFailedDialIsARowThatIsNotBilledAndOpensNoLeg() {
	s.vendor.err = errors.New("the vendor refused")

	_, err := s.service.Call(s.ctx, CallRequest{Owner: s.owner(), From: s.e164, To: "+15125550123"})

	s.Require().Error(err)
	dial := s.row(ModelDial)
	s.False(dial.Success)
	s.Equal("vendor_failed", dial.ErrorCode)
	s.Empty(s.openLegs())
}

func (s *MeteringSuite) TestATransferIsALegOfTheCallItJoins() {
	callID := "call-" + uuid.NewString()
	_, err := s.service.Transfer(s.ctx, TransferRequest{Owner: s.owner(), From: s.e164, To: "+15125550124", CallID: callID})
	s.Require().NoError(err)
	s.answered(defaultCallType, callID, "human-1")
	s.clock.advance(3 * time.Minute)

	s.ended(defaultCallType, callID)

	minutes := s.row(ModelCallMinutes)
	s.Equal(pricing.Quantities{pricing.CallSeconds: 180}, minutes.Meters)
	s.Equal(s.outboundMinutes(180), minutes.CostMicros)
}

func (s *MeteringSuite) TestALegLeftOpenIsClosedAtFourHoursAndNotBilled() {
	placed := s.place()
	s.answered(placed.CallType, placed.CallID, "phone-1")
	s.clock.advance(MaxCallLength + time.Minute)

	_, err := s.service.SweepLegs(s.ctx)
	s.Require().NoError(err)
	s.Require().NoError(s.service.EndLegs(s.ctx, everyApp, placed.CallType, placed.CallID, time.Time{}),
		"the session_ended that comes late ends nothing")

	minutes := s.row(ModelCallMinutes)
	s.Equal(pricing.Quantities{pricing.CallSeconds: int64(MaxCallLength / time.Second)}, minutes.Meters)
	s.False(minutes.Billable, "nothing says how long it really lasted")
	s.Zero(minutes.CostMicros)
	s.Equal(strconv.FormatInt(s.outboundMinutes(int64(MaxCallLength/time.Second)), 10), minutes.Attributes[routing.AttributeFreeCostMicros],
		"priced, at 240 minutes")
	s.Equal(store.LegSwept, minutes.Attributes[AttributeEndedBy])
	time.Sleep(300 * time.Millisecond)
	s.Len(s.rows(ModelCallMinutes), 1)
}

func (s *MeteringSuite) TestALegThatRangForFourHoursIsEndedUnansweredAndBillsNothing() {
	s.place()
	s.clock.advance(MaxCallLength + time.Minute)

	_, err := s.service.SweepLegs(s.ctx)
	s.Require().NoError(err)

	s.Empty(s.openLegs())
	s.Never(func() bool { return len(s.rows(ModelCallMinutes)) > 0 }, time.Second, 50*time.Millisecond)
}

func (s *MeteringSuite) TestRecordingAnEndedLegAgainBillsNothing() {
	placed := s.place()
	s.answered(placed.CallType, placed.CallID, "phone-1")
	s.clock.advance(time.Minute)
	ended, err := s.store.CallLegs.EndCall(s.ctx, everyApp, placed.CallType, placed.CallID, s.clock.now())
	s.Require().NoError(err)
	s.Require().Len(ended, 1)

	s.service.settleLeg(s.ctx, ended[0], true)
	s.service.settleLeg(s.ctx, ended[0], false)

	s.row(ModelCallMinutes)
	time.Sleep(300 * time.Millisecond)
	s.Len(s.rows(ModelCallMinutes), 1, "the leg's usage id names it")
}

// A process that dies between ending a leg and writing its row leaves the leg ended and
// unbilled: the sweep finds it and writes its row, once.
func (s *MeteringSuite) TestALegWhoseRowWasLostIsRecordedAgainByTheSweep() {
	placed := s.place()
	s.answered(placed.CallType, placed.CallID, "phone-1")
	s.clock.advance(90 * time.Second)
	_, err := s.store.CallLegs.EndCall(s.ctx, everyApp, placed.CallType, placed.CallID, s.clock.now())
	s.Require().NoError(err)
	s.clock.advance(10 * time.Minute)

	_, err = s.service.SweepLegs(s.ctx)
	s.Require().NoError(err)
	minutes := s.row(ModelCallMinutes)
	s.Equal(pricing.Quantities{pricing.CallSeconds: 90}, minutes.Meters)
	s.True(minutes.Billable)

	_, err = s.service.SweepLegs(s.ctx)
	s.Require().NoError(err)
	time.Sleep(300 * time.Millisecond)
	s.Len(s.rows(ModelCallMinutes), 1)
}

// A leg whose row a dropped connection fails to write is written again at once, and its call
// billed.
func (s *MeteringSuite) TestALegWhoseFirstWriteFailsIsBilled() {
	callID := "call-" + uuid.NewString()
	s.failing.fail(`INSERT INTO "call_legs"`, 1)
	s.arrived(callID)
	s.clock.advance(2 * time.Minute)

	s.ended(defaultCallType, callID)

	minutes := s.row(ModelCallMinutes)
	s.Equal(pricing.Quantities{pricing.CallSeconds: 120}, minutes.Meters)
	s.True(minutes.Billable)
}

// A leg whose row cannot be written while its call is up is written by the leg sweep, which
// bills it from the join and the leave its phone had meanwhile.
func (s *MeteringSuite) TestALegThatCouldNotBeWrittenIsWrittenAndBilledByTheSweep() {
	callID := "call-" + uuid.NewString()
	s.failing.fail(`INSERT INTO "call_legs"`, openAttempts)
	s.arrived(callID)
	s.Empty(s.openLegs())
	s.joined(defaultCallType, callID, "caller-1", "15125550199")
	s.clock.advance(90 * time.Second)
	s.left(defaultCallType, callID, "caller-1")
	s.ended(defaultCallType, callID)

	_, err := s.service.SweepLegs(s.ctx)
	s.Require().NoError(err)

	minutes := s.row(ModelCallMinutes)
	s.Equal(pricing.Quantities{pricing.CallSeconds: 90}, minutes.Meters)
	s.True(minutes.Billable)
	s.Zero(s.kept(callID), "the join and the leave went to the leg")
	_, err = s.service.SweepLegs(s.ctx)
	s.Require().NoError(err)
	s.Never(func() bool { return len(s.rows(ModelCallMinutes)) > 1 }, time.Second, 50*time.Millisecond,
		"the next sweep writes it no more")
}

// A leg written by the sweep after its call ended, its phone's leave never heard, ends when the
// call did: it is billed to then, and no longer counts against the organization's calls.
func (s *MeteringSuite) TestALegWrittenAfterItsCallEndedEndsWithTheCall() {
	callID := "call-" + uuid.NewString()
	s.failing.fail(`INSERT INTO "call_legs"`, openAttempts)
	s.arrived(callID)
	s.clock.advance(90 * time.Second)
	s.ended(defaultCallType, callID)
	s.clock.advance(10 * time.Minute)

	_, err := s.service.SweepLegs(s.ctx)

	s.Require().NoError(err)
	s.Empty(s.openLegs())
	minutes := s.row(ModelCallMinutes)
	s.Equal(pricing.Quantities{pricing.CallSeconds: 90}, minutes.Meters)
	s.True(minutes.Billable)
}

// The legs the sweep still holds when the router stops get a last write, once the database is
// back, and are billed as the sweep would have.
func (s *MeteringSuite) TestALegKeptWhenTheRouterStopsIsWrittenOnceMore() {
	callID := "call-" + uuid.NewString()
	s.failing.fail(`INSERT INTO "call_legs"`, openAttempts)
	s.arrived(callID)
	s.Require().Empty(s.openLegs())

	s.service.StopLegSweep(s.ctx)

	s.Len(s.openLegs(), 1)
	s.Zero(s.logs.count("ERROR", "could not record a call leg"))
}

// A leg that cannot be written once the sweep has stopped is logged at ERROR, not kept for a
// sweep that will never write it.
func (s *MeteringSuite) TestALegThatCannotBeWrittenAfterTheSweepStoppedIsLogged() {
	s.service.StopLegSweep(s.ctx)
	callID := "call-" + uuid.NewString()
	s.failing.fail(`INSERT INTO "call_legs"`, openAttempts)

	s.arrived(callID)

	s.Equal(1, s.logs.count("ERROR", "could not record a call leg; its minutes are not billed"))
	_, err := s.service.SweepLegs(s.ctx)
	s.Require().NoError(err)
	s.Empty(s.openLegs(), "nothing was kept to write")
}

func (s *MeteringSuite) TestEveryLegOfAnEndedCallCountsTowardTheSandboxsMinutes() {
	s.withSandbox(dlc.Sandbox{Enabled: true})
	s.Require().NoError(s.service.gate.SetRecipients(s.ctx, s.customer, []string{"+15125550123", "+15125550124"}))
	placed := s.place()
	s.answered(placed.CallType, placed.CallID, "phone-1")
	s.clock.advance(4 * time.Minute)
	_, err := s.service.Transfer(s.ctx, TransferRequest{Owner: s.owner(), From: s.e164, To: "+15125550124", CallID: placed.CallID})
	s.Require().NoError(err)
	s.answered(placed.CallType, placed.CallID, "human-1")
	s.clock.advance(6 * time.Minute)

	s.ended(placed.CallType, placed.CallID)

	usage, err := s.service.gate.Usage(s.ctx, s.customer)
	s.Require().NoError(err)
	s.Equal(int64(16*60), usage.AudioSeconds, "both legs, not only the one that began first")
	for _, minutes := range s.rowsWhen(ModelCallMinutes, 2) {
		s.False(minutes.Billable, "the sandbox gives its minutes away")
		s.Equal("true", minutes.Attributes[AttributeDLCSandbox])
		s.NotEmpty(minutes.Attributes[routing.AttributeFreeCostMicros])
	}
}

func (s *MeteringSuite) TestALegTheSweepEndedDoesNotCountTowardTheSandboxsMinutes() {
	s.withSandbox(dlc.Sandbox{Enabled: true})
	s.Require().NoError(s.service.gate.SetRecipients(s.ctx, s.customer, []string{"+15125550123"}))
	placed := s.place()
	s.answered(placed.CallType, placed.CallID, "phone-1")
	s.clock.advance(MaxCallLength + time.Minute)

	_, err := s.service.SweepLegs(s.ctx)
	s.Require().NoError(err)

	s.Equal(store.LegSwept, s.row(ModelCallMinutes).Attributes[AttributeEndedBy])
	usage, err := s.service.gate.Usage(s.ctx, s.customer)
	s.Require().NoError(err)
	s.Zero(usage.AudioSeconds, "nothing says how long it really lasted")
}

func (s *MeteringSuite) TestALegCarriesItsAppsOrganization() {
	organization := "org-" + uuid.NewString()
	_, err := s.store.GlobalDB().NewInsert().Model(&store.AppOrganization{
		AppID: s.customer, OrganizationID: organization, SeenAt: time.Now().UTC(),
	}).Exec(s.ctx)
	s.Require().NoError(err)

	s.place()

	legs := s.openLegs()
	s.Require().Len(legs, 1)
	s.Equal(organization, legs[0].OrganizationID, "an organization's open calls are counted from its legs")
}

func (s *MeteringSuite) TestABoughtNumberIsBilledItsFirstMonth() {
	bought, err := s.service.Buy(s.ctx, Purchase{Vendor: s.vendor.vendor, E164: aNumber(), Owner: s.owner()})
	s.Require().NoError(err)

	month := s.row(ModelNumber)
	s.Equal(int64(1_080_000), month.CostMicros, "the price the vendor quoted, resold at the factor")
	s.Equal(int64(1_000_000), month.BaseCostMicros, "the price the vendor quoted")
	s.True(month.Billable)
	s.Equal("key-a", month.APIKey, "the month it was bought in is the buyer's")
	s.Equal(bought.PurchasedAt, month.StartedAt.UTC())
}

// A number bought whose row could not be written is recorded by the sweep, from the vendor's
// account, as of when it was bought, and its first month is billed once.
func (s *MeteringSuite) TestANumberBoughtWithoutItsRowIsRecordedByTheSweep() {
	e164 := aNumber()
	s.vendor.offers = []Available{{E164: e164, MonthlyCostMicros: 1_000_000}}
	s.failing.fail(`INSERT INTO "phone_numbers"`, 1)
	_, err := s.service.Buy(s.ctx, Purchase{Vendor: s.vendor.vendor, E164: e164, Owner: s.owner()})
	s.Require().ErrorContains(err, "was not recorded")
	_, err = s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, e164)
	s.Require().ErrorIs(err, store.ErrNoNumber)
	month := s.row(ModelNumber)
	s.ordersSettle()

	_, err = s.service.SweepOrders(s.ctx)

	s.Require().NoError(err)
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, e164)
	s.Require().NoError(err)
	s.Equal(month.StartedAt.UTC(), held.PurchasedAt.UTC(), "bought when the purchase billed its first month")
	s.Equal(int64(1_000_000), held.MonthlyCostMicros)
	s.Never(func() bool { return len(s.rows(ModelNumber)) > 1 }, time.Second, 50*time.Millisecond,
		"the first month is billed once")
	s.Zero(s.orders(), "nothing is left to settle")
}

// A number given back at the vendor whose release could not be written is released by the
// sweep, as of when it was given back, rather than billed every month.
func (s *MeteringSuite) TestANumberGivenBackWithoutItsReleaseIsReleasedByTheSweep() {
	s.failing.fail(`SET released_at = `, 1)
	s.Require().Error(s.service.Release(s.ctx, s.customer, s.e164))
	returned := time.Now()
	_, err := s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, s.e164)
	s.Require().NoError(err, "the vendor has it back, and the row still holds it")
	s.ordersSettle()

	_, err = s.service.SweepOrders(s.ctx)

	s.Require().NoError(err)
	_, err = s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, s.e164)
	s.ErrorIs(err, store.ErrNoNumber)
	s.Zero(s.orders())
	numbers, err := s.store.PhoneNumbers.Select(s.ctx, s.customer, store.PhoneNumberFilter{IncludeReleased: true})
	s.Require().NoError(err)
	s.Require().Len(numbers, 1)
	s.Require().NotNil(numbers[0].ReleasedAt)
	s.False(numbers[0].ReleasedAt.After(returned), "released as of when the vendor took it back, not when the sweep ran")
}

// A number the vendor bought, whose answer was lost to a timeout, is recorded by the sweep and
// its first month billed.
func (s *MeteringSuite) TestANumberBoughtWhoseAnswerWasLostIsRecordedByTheSweep() {
	e164 := aNumber()
	s.vendor.offers = []Available{{E164: e164, MonthlyCostMicros: 1_000_000}}
	s.vendor.doneErr = &VendorError{Vendor: s.vendor.vendor, Path: "/v2/number_orders", Message: "no answer", Cause: context.DeadlineExceeded}
	_, err := s.service.Buy(s.ctx, Purchase{Vendor: s.vendor.vendor, E164: e164, Owner: s.owner()})
	s.Require().Error(err)
	s.ordersSettle()

	_, err = s.service.SweepOrders(s.ctx)

	s.Require().NoError(err)
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, e164)
	s.Require().NoError(err, "the vendor holds it, so the customer does")
	s.Equal(int64(1_000_000), held.MonthlyCostMicros)
	var months []store.Request
	for _, row := range s.rowsWhen(ModelNumber, 2) {
		if row.Success {
			months = append(months, row)
		}
	}
	s.Require().Len(months, 1, "its first month, beside the purchase's failure")
	s.Equal(held.PurchasedAt.UTC(), months[0].StartedAt.UTC())
	s.Zero(s.orders())
}

// A number the vendor took back, whose answer was a 5xx, is released by the sweep rather than
// billed every month.
func (s *MeteringSuite) TestANumberGivenBackWhoseAnswerWasAFailureIsReleasedByTheSweep() {
	s.vendor.holds = map[string]bool{s.e164: true}
	s.vendor.doneErr = &VendorError{Vendor: s.vendor.vendor, Path: "/v2/phone_numbers", Status: http.StatusBadGateway}
	s.Require().Error(s.service.Release(s.ctx, s.customer, s.e164))
	s.ordersSettle()

	_, err := s.service.SweepOrders(s.ctx)

	s.Require().NoError(err)
	_, err = s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, s.e164)
	s.ErrorIs(err, store.ErrNoNumber)
	s.Zero(s.orders())
}

// A release that failed at a vendor still holding the number leaves the customer holding it:
// the sweep releases only what the vendor no longer has.
func (s *MeteringSuite) TestANumberTheVendorStillHoldsAfterAFailedReleaseStaysHeld() {
	s.vendor.holds = map[string]bool{s.e164: true}
	s.vendor.releaseErr = &VendorError{Vendor: s.vendor.vendor, Path: "/v2/phone_numbers", Status: http.StatusServiceUnavailable}
	s.Require().Error(s.service.Release(s.ctx, s.customer, s.e164))
	s.Require().Equal(1, s.orders())
	s.ordersSettle()

	_, err := s.service.SweepOrders(s.ctx)

	s.Require().NoError(err)
	held, err := s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, s.e164)
	s.Require().NoError(err)
	s.Nil(held.ReleasedAt)
	s.Zero(s.orders())
}

// An order at a vendor that cannot say what it holds records nothing and is kept for an
// operator, and the sweeps that follow do not ask about it again until it is due once more.
func (s *MeteringSuite) TestAnOrderAVendorCannotSettleIsKeptAndNotAskedAboutEverySweep() {
	e164 := aNumber()
	s.vendor.offers = []Available{{E164: e164, MonthlyCostMicros: 1_000_000}}
	s.vendor.doneErr = &VendorError{Vendor: s.vendor.vendor, Path: "/v2/number_orders", Status: http.StatusGatewayTimeout}
	_, err := s.service.Buy(s.ctx, Purchase{Vendor: s.vendor.vendor, E164: e164, Owner: s.owner()})
	s.Require().Error(err)
	// The vendor as one that has no NumberFinder.
	s.registry.Register(s.vendor.vendor, func() (Provider, error) { return struct{ Provider }{s.vendor}, nil })
	s.ordersSettle()

	_, err = s.service.SweepOrders(s.ctx)
	s.Require().NoError(err)
	_, err = s.service.SweepOrders(s.ctx)
	s.Require().NoError(err)

	_, err = s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, e164)
	s.ErrorIs(err, store.ErrNoNumber, "nothing is recorded that the vendor did not confirm")
	s.Equal(1, s.orders(), "kept for an operator")
	// Other tests' orders are in the same tables, so only this customer's lines are counted.
	s.Equal(1, s.logs.count("ERROR", "customer="+s.customer), "asked about once, not every sweep")
	s.Contains(s.logs.String(), "could not settle a number's purchase or release")
}

// A purchase the vendor refused, whose order could not be forgotten, leaves no number.
func (s *MeteringSuite) TestAnOrderTheVendorDoesNotHoldIsForgottenWithoutANumber() {
	e164 := aNumber()
	s.failing.fail(`DELETE FROM "phone_number_orders"`, 1)
	s.vendor.err = &VendorError{Vendor: s.vendor.vendor, Path: "/v2/number_orders", Status: http.StatusUnprocessableEntity}
	_, err := s.service.Buy(s.ctx, Purchase{Vendor: s.vendor.vendor, E164: e164, Owner: s.owner()})
	s.Require().Error(err)
	s.vendor.err = nil
	s.Require().Equal(1, s.orders())
	s.ordersSettle()

	_, err = s.service.SweepOrders(s.ctx)

	s.Require().NoError(err)
	_, err = s.store.PhoneNumbers.GetByE164(s.ctx, s.customer, e164)
	s.ErrorIs(err, store.ErrNoNumber)
	s.Zero(s.orders())
}

// A purchase from a vendor not implemented, which no vendor was asked about and none can settle,
// leaves no order for the sweep to log every hour.
func (s *MeteringSuite) TestAPurchaseFromAVendorNotImplementedLeavesNoOrder() {
	_, err := s.service.Buy(s.ctx, Purchase{Vendor: "bics", E164: aNumber(), Owner: s.owner()})
	s.Require().ErrorIs(err, ErrNotImplemented)
	s.ordersSettle()

	_, err = s.service.SweepOrders(s.ctx)

	s.Require().NoError(err)
	s.Zero(s.orders())
	s.Zero(s.logs.count("ERROR", "customer="+s.customer))
}

// A purchase refused before the vendor was asked, for a setting the deployment lacks, leaves
// no order.
func (s *MeteringSuite) TestAPurchaseFromAVendorMissingASettingLeavesNoOrder() {
	s.vendor.err = NotConfigured(errors.New("telnyx: a setting is required to order a number"))

	_, err := s.service.Buy(s.ctx, Purchase{Vendor: s.vendor.vendor, E164: aNumber(), Owner: s.owner()})

	s.Require().ErrorIs(err, ErrNotConfigured)
	s.Zero(s.orders())
}

// A purchase refused before the vendor was asked, as one it cannot take, leaves no order.
func (s *MeteringSuite) TestAPurchaseTheVendorCannotTakeAsAskedLeavesNoOrder() {
	s.vendor.err = Invalid(errors.New("telnyx: buying a number needs the country it is sold in"))

	_, err := s.service.Buy(s.ctx, Purchase{Vendor: s.vendor.vendor, E164: aNumber(), Owner: s.owner()})

	s.Require().ErrorIs(err, ErrInvalid)
	s.Zero(s.orders())
}

// ordersSettle lets this customer's orders get old enough to settle: on the suite's clock, which
// stamps them, and on Postgres's, which a sweep's claim judges them by.
func (s *MeteringSuite) ordersSettle() {
	settled := orderSettles + time.Minute
	s.clock.advance(settled)
	_, err := s.store.GlobalDB().NewUpdate().Model((*store.PhoneNumberOrder)(nil)).
		Set("next_attempt_at = next_attempt_at - make_interval(secs => ?)", settled.Seconds()).
		Where("customer_id = ?", s.customer).Exec(s.ctx)
	s.Require().NoError(err)
}

// orders counts the customer's orders still to settle.
func (s *MeteringSuite) orders() int {
	count, err := s.store.GlobalDB().NewSelect().Model((*store.PhoneNumberOrder)(nil)).
		Where("customer_id = ?", s.customer).Count(s.ctx)
	s.Require().NoError(err)
	return count
}

func (s *MeteringSuite) TestANumberWhosePriceIsOnlyInItsOfferIsBilledTheOffer() {
	e164 := aNumber()
	s.vendor.monthly = 0
	s.vendor.offers = []Available{{E164: "+15125550000", MonthlyCostMicros: 9}, {E164: e164, MonthlyCostMicros: 1_500_000}}

	bought, err := s.service.Buy(s.ctx, Purchase{Vendor: s.vendor.vendor, E164: e164, Owner: s.owner()})
	s.Require().NoError(err)

	s.Equal(int64(1_500_000), bought.MonthlyCostMicros, "the number's own offer")
	s.Equal(int64(1_620_000), s.row(ModelNumber).CostMicros)
}

func (s *MeteringSuite) TestANumberWithNoPriceIsBillableWorkWithNoPrice() {
	s.vendor.monthly = 0

	_, err := s.service.Buy(s.ctx, Purchase{Vendor: s.vendor.vendor, E164: aNumber(), Owner: s.owner()})
	s.Require().NoError(err)

	month := s.row(ModelNumber)
	s.True(month.Billable, "billable, so the alarm for work with no price goes off")
	s.Zero(month.CostMicros)
}

func (s *MeteringSuite) TestANumberIsBilledEachMonthAfterTheOneItWasBoughtIn() {
	// The suite's own number, which has no price, is not the one under test.
	s.Require().NoError(s.store.PhoneNumbers.Release(s.ctx, s.customer, s.e164, s.clock.now()))
	bought, err := s.service.Buy(s.ctx, Purchase{Vendor: s.vendor.vendor, E164: aNumber(), Owner: s.owner()})
	s.Require().NoError(err)
	s.row(ModelNumber)
	rentals := Rentals{Store: s.store, Recorder: s.recorder, Logger: slog.New(slog.DiscardHandler)}
	from := bought.PurchasedAt.Add(-time.Hour)

	_, err = rentals.Bill(s.ctx, from, bought.PurchasedAt.Add(time.Hour))
	s.Require().NoError(err)
	time.Sleep(300 * time.Millisecond)
	s.Len(s.rows(ModelNumber), 1, "the month it was bought in is the purchase's, billed once")

	next := monthAfter(bought.PurchasedAt, 1)
	_, err = rentals.Bill(s.ctx, from, next.Add(time.Hour))
	s.Require().NoError(err)
	s.rowsWhen(ModelNumber, 2)
	_, err = rentals.Bill(s.ctx, from, next.Add(2*time.Hour))
	s.Require().NoError(err)
	time.Sleep(300 * time.Millisecond)
	months := s.rows(ModelNumber)
	s.Len(months, 2, "a rerun bills nothing twice")
	s.Equal(OriginNumberRental, months[1].APIKey)
	s.Equal(next, months[1].StartedAt.UTC())

	s.Require().NoError(s.store.PhoneNumbers.Release(s.ctx, s.customer, bought.E164, next.Add(2*time.Hour)))
	_, err = rentals.Bill(s.ctx, from, monthAfter(bought.PurchasedAt, 2).Add(time.Hour))
	s.Require().NoError(err)
	time.Sleep(300 * time.Millisecond)
	s.Len(s.rows(ModelNumber), 2, "a number released is not billed again")
}

func (s *MeteringSuite) TestAnImportedRequestNamingANumbersComingMonthDoesNotStopItBeingBilled() {
	s.Require().NoError(s.store.PhoneNumbers.Release(s.ctx, s.customer, s.e164, s.clock.now()))
	bought, err := s.service.Buy(s.ctx, Purchase{Vendor: s.vendor.vendor, E164: aNumber(), Owner: s.owner()})
	s.Require().NoError(err)
	s.row(ModelNumber)
	next := monthAfter(bought.PurchasedAt, 1)
	month := routing.UsageID("number", bought.Vendor, bought.E164,
		strconv.FormatInt(bought.PurchasedAt.UTC().UnixMicro(), 10), next.Format("2006-01"))

	// A request row as an export carries it, sent with the usage id of the number's next month.
	other := "customer-" + uuid.NewString()
	s.Require().NoError(s.store.Requests.Insert(s.ctx, &store.Request{UsageID: uuid.Must(uuid.NewV7()), CustomerID: other,
		Modality: string(routing.Phone), Provider: s.vendor.vendor, Model: ModelNumber, StartedAt: next, Success: true}))
	var row map[string]any
	_, err = s.store.ExportCustomer(s.ctx, other, func(table string, encoded json.RawMessage) error {
		if table == "requests" {
			return json.Unmarshal(encoded, &row)
		}
		return nil
	})
	s.Require().NoError(err)
	s.Require().NotNil(row)
	row["usage_id"] = month.String()
	encoded, err := json.Marshal(row)
	s.Require().NoError(err)
	s.Require().NoError(s.store.ImportRow(s.ctx, s.customer, "requests", encoded))
	s.Require().NoError(s.store.ImportRow(s.ctx, s.customer, "requests", encoded))

	// Rows written to ship to a usage service that is not there, so they stay to be shipped.
	client, err := usage.NewClient(usage.ClientOptions{URL: "http://127.0.0.1:1", Token: "token"})
	s.Require().NoError(err)
	writer := routing.NewWriter(routing.WriterOptions{Store: s.store, Usage: client, Logger: slog.New(slog.DiscardHandler)})
	s.T().Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		writer.Close(ctx)
	})
	recorder := routing.NewRecorderOn(routing.Phone, writer, s.prices, slog.New(slog.DiscardHandler))
	_, err = Rentals{Store: s.store, Recorder: recorder, Logger: slog.New(slog.DiscardHandler)}.
		Bill(s.ctx, bought.PurchasedAt.Add(-time.Hour), next.Add(time.Hour))
	s.Require().NoError(err)

	s.Require().Eventually(func() bool {
		var billed []store.Request
		s.Require().NoError(s.store.ShardDB().NewSelect().Model(&billed).
			Where("customer_id = ?", s.customer).Where("usage_id = ?", month).Scan(s.ctx))
		return len(billed) == 1 && billed[0].Billable && billed[0].ToShip && billed[0].APIKey == OriginNumberRental
	}, 5*time.Second, 25*time.Millisecond, "the number's next month is billed")
	s.Len(s.rows(ModelNumber), 3, "the month it was bought in, the next one, and the imported row once")
}

func (s *MeteringSuite) TestAMonthThatBeganBeforeFromIsNeverBilled() {
	now := time.Now().UTC().Truncate(time.Second)
	e164 := aNumber()
	s.Require().NoError(s.store.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: e164, Vendor: s.vendor.vendor, Country: "US", CustomerID: s.customer,
		MonthlyCostMicros: 1_000_000, PurchasedAt: now.AddDate(0, -1, -10),
	}))
	rentals := Rentals{Store: s.store, Recorder: s.recorder, Logger: slog.New(slog.DiscardHandler)}

	_, err := rentals.Bill(s.ctx, now.Add(-24*time.Hour), now)
	s.Require().NoError(err)

	s.Never(func() bool { return len(s.rows(ModelNumber)) > 0 }, time.Second, 50*time.Millisecond,
		"the month that began ten days ago began before the job was switched on")
}

func (s *MeteringSuite) TestAMovedCustomersBoughtNumberIsNotBilledAgainHere() {
	s.Require().NoError(s.store.PhoneNumbers.Release(s.ctx, s.customer, s.e164, s.clock.now()))
	now := time.Now().UTC().Truncate(time.Second)
	// The month that began three days ago is the source's to bill, which held the number then.
	row, err := json.Marshal(map[string]any{
		"id": rand.Int64N(1 << 50), "e164": aNumber(), "vendor": s.vendor.vendor, "country": "US",
		"capabilities": []string{"voice"}, "monthly_cost_micros": 1_000_000, "customer_id": s.customer,
		"tags": map[string]string{}, "purchased_at": now.AddDate(0, -1, -3),
	})
	s.Require().NoError(err)
	s.Require().NoError(s.store.ImportRow(s.ctx, s.customer, "phone_numbers", row))
	rentals := Rentals{Store: s.store, Recorder: s.recorder, Logger: slog.New(slog.DiscardHandler)}

	_, err = rentals.Bill(s.ctx, now.AddDate(0, 0, -30), time.Now().UTC())
	s.Require().NoError(err)

	s.Never(func() bool { return len(s.rows(ModelNumber)) > 0 }, time.Second, 50*time.Millisecond,
		"the destination bills no month of a number it does not hold")
}

// row waits for the customer's one row of a model and reads it.
func (s *MeteringSuite) row(model string) store.Request {
	return s.rowsWhen(model, 1)[0]
}

// rowsWhen waits until the customer has want rows of a model, the recorder writing them off the
// caller, and reads them oldest first.
func (s *MeteringSuite) rowsWhen(model string, want int) []store.Request {
	var rows []store.Request
	s.Require().Eventually(func() bool {
		rows = s.rows(model)
		return len(rows) >= want
	}, 5*time.Second, 25*time.Millisecond, "no %d %s rows", want, model)
	return rows
}

func (s *MeteringSuite) rows(model string) []store.Request {
	var rows []store.Request
	s.Require().NoError(s.store.ShardDB().NewSelect().Model(&rows).
		Where("customer_id = ?", s.customer).Where("modality = ?", routing.Phone).Where("model = ?", model).
		Order("started_at").Scan(s.ctx))
	return rows
}

func (s *MeteringSuite) openLegs() []store.CallLeg {
	var legs []store.CallLeg
	s.Require().NoError(s.store.ShardDB().NewSelect().Model(&legs).
		Where("customer_id = ?", s.customer).Where("ended_at IS NULL").Scan(s.ctx))
	return legs
}

// meteringLogs is what the service logged.
type meteringLogs struct {
	mu      sync.Mutex
	written strings.Builder
}

func (l *meteringLogs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.written.Write(p)
}

// String is every line written.
func (l *meteringLogs) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.written.String()
}

// count is how many lines at level say message.
func (l *meteringLogs) count(level, message string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	lines := 0
	for line := range strings.Lines(l.written.String()) {
		if strings.Contains(line, "level="+level) && strings.Contains(line, message) {
			lines++
		}
	}
	return lines
}

// failingQueries fails the next queries holding a text, as a database that does not answer in
// time does.
type failingQueries struct {
	mu    sync.Mutex
	query string
	left  int
}

// fail fails the next times queries holding query.
func (f *failingQueries) fail(query string, times int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.query, f.left = query, times
}

func (f *failingQueries) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	f.mu.Lock()
	failing := f.left > 0 && strings.Contains(event.Query, f.query)
	if failing {
		f.left--
	}
	f.mu.Unlock()
	if !failing {
		return ctx
	}
	expired, cancel := context.WithDeadline(ctx, time.Time{})
	cancel()
	return expired
}

func (*failingQueries) AfterQuery(context.Context, *bun.QueryEvent) {}

// meteringClock is the service's clock, moved by the test.
type meteringClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *meteringClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *meteringClock) advance(by time.Duration) { c.set(c.now().Add(by)) }

func (c *meteringClock) set(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = at
}
