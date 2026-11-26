//go:build integration

package node

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidishook"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv/redisready"
)

// DirectorySuite covers what a node says in Redis about what it holds.
type DirectorySuite struct {
	suite.Suite
	ctx        context.Context
	client     rueidis.Client
	roundTrips *roundTrips
	prefix     string
}

func TestDirectorySuite(t *testing.T) {
	suite.Run(t, new(DirectorySuite))
}

func (s *DirectorySuite) SetupTest() {
	address := os.Getenv("ROUTER_REDIS_ADDR")
	if address == "" {
		s.T().Skip("ROUTER_REDIS_ADDR not set")
	}
	s.Require().NoError(redisready.Ready(address))
	client, err := redisready.NewClient(rueidis.ClientOption{InitAddress: []string{address}, DisableCache: true})
	s.Require().NoError(err)
	s.T().Cleanup(client.Close)
	s.ctx = context.Background()
	s.roundTrips = &roundTrips{}
	s.client = rueidishook.WithHook(client, s.roundTrips)
	s.prefix = "directory-test:" + time.Now().Format(time.RFC3339Nano)
}

// A renewal says again everything the node holds in one round trip, however many sessions,
// customers and offers that is, and what lapsed is back.
func (s *DirectorySuite) TestARenewalIsOneRoundTrip() {
	directory, err := NewDirectory(DirectoryOptions{Redis: s.client, Address: "node-a:1", Shard: "eu", Prefix: s.prefix})
	s.Require().NoError(err)
	defer directory.Close()
	for _, session := range []string{"s1", "s2", "s3"} {
		directory.Hold(s.ctx, "acme", 7, session, "agent-"+session, "messaging:"+session)
	}
	s.callOf(directory, "acme")
	directory.Wait(s.ctx, "acme")
	directory.Wait(s.ctx, "globex")
	directory.Offer(s.ctx, "acme", "agent-s1", []byte(`{"tools":[]}`))
	directory.Offer(s.ctx, "globex", "agent-s2", []byte(`{"tools":[]}`))
	keys, err := s.client.Do(s.ctx, s.client.B().Keys().Pattern(s.prefix+":*").Build()).AsStrSlice()
	s.Require().NoError(err)
	// Each session: itself and its conversation, each under its customer and under its id alone,
	// and its agent by app and customer, by customer, and by its id alone. And the call of one.
	s.Require().Len(keys, 3*7+1+2+2+2)
	s.Require().NoError(s.client.Do(s.ctx, s.client.B().Del().Key(keys...).Build()).Error())

	before := s.roundTrips.sent.Load()
	directory.renewAll(s.ctx)
	s.Equal(int64(1), s.roundTrips.sent.Load()-before)

	for _, key := range keys {
		ttl, err := s.client.Do(s.ctx, s.client.B().Pttl().Key(key).Build()).AsInt64()
		s.Require().NoError(err)
		s.Greater(ttl, int64(0), "%s is held again, for a while", key)
	}
	peer, err := NewDirectory(DirectoryOptions{Redis: s.client, Address: "node-b:1", Prefix: s.prefix})
	s.Require().NoError(err)
	peer.Close()
	held, err := peer.NodeByConversation(s.ctx, "acme", "messaging:s3")
	s.Require().NoError(err)
	s.Equal("node-a:1", held)
	writing, err := peer.NodesByAgent(s.ctx, "agent-s2", everyApp)
	s.Require().NoError(err)
	s.Equal(map[string]string{"acme": "node-a:1"}, writing)
	waiting, err := peer.Waiting(s.ctx, "globex")
	s.Require().NoError(err)
	s.Equal([]string{"node-a:1"}, waiting)
	offers, err := peer.Offers(s.ctx, "acme", "agent-s1")
	s.Require().NoError(err)
	s.JSONEq(`{"tools":[]}`, string(offers["node-a:1"]))
}

// Two customers may each run a session of one id, on different nodes: each is found by its own
// customer, whichever node said so last.
func (s *DirectorySuite) TestTwoCustomersSessionsOfOneIdAreFoundApart() {
	first := s.directoryAt("node-a:1")
	second := s.directoryAt("node-b:1")
	first.Hold(s.ctx, "acme", 0, "order-42", "", "")
	second.Hold(s.ctx, "globex", 0, "order-42", "", "")

	peer := s.directoryAt("node-c:1")
	s.Equal("node-a:1", s.nodeOf(peer, "acme", "order-42"))
	s.Equal("node-b:1", s.nodeOf(peer, "globex", "order-42"))
}

// A router of the release before holds a session under its id alone, and is still found.
func (s *DirectorySuite) TestASessionHeldUnderItsIdAloneIsFound() {
	s.Require().NoError(s.client.Do(s.ctx, s.client.B().Set().Key(s.prefix+":session:order-42").Value("node-a:1").Build()).Error())

	s.Equal("node-a:1", s.nodeOf(s.directoryAt("node-c:1"), "acme", "order-42"))
}

// A router of the release before reads a session's node by its id alone.
func (s *DirectorySuite) TestASessionHeldHereIsFoundByItsIdAlone() {
	s.directoryAt("node-a:1").Hold(s.ctx, "acme", 0, "order-42", "", "")

	held, err := s.client.Do(s.ctx, s.client.B().Get().Key(s.prefix+":session:order-42").Build()).ToString()
	s.Require().NoError(err)
	s.Equal("node-a:1", held)
}

// Two customers may each run a session writing as one agent: each customer's is found, whichever
// node said so last.
func (s *DirectorySuite) TestTwoCustomersWritingAsOneAgentAreFoundApart() {
	s.directoryAt("node-a:1").Hold(s.ctx, "acme", 7, "s1", "support", "")
	s.directoryAt("node-b:1").Hold(s.ctx, "globex", 7, "s2", "support", "")

	peer := s.directoryAt("node-c:1")
	s.Equal("node-a:1", s.writingAs(peer, "acme", 7, "support"))
	s.Equal("node-b:1", s.writingAs(peer, "globex", 7, "support"))
	writing, err := peer.NodesByAgent(s.ctx, "support", everyApp)
	s.Require().NoError(err)
	s.Equal(map[string]string{"acme": "node-a:1", "globex": "node-b:1"}, writing)
}

// A customer may run a session writing as one agent in each of two Stream apps: each app's is
// found, and none in a third.
func (s *DirectorySuite) TestOneCustomerWritingAsOneAgentInTwoAppsIsFoundApartByApp() {
	s.directoryAt("node-a:1").Hold(s.ctx, "acme", 7, "s1", "support", "")
	s.directoryAt("node-b:1").Hold(s.ctx, "acme", 8, "s2", "support", "")

	peer := s.directoryAt("node-c:1")
	s.Equal("node-a:1", s.writingAs(peer, "acme", 7, "support"))
	s.Equal("node-b:1", s.writingAs(peer, "acme", 8, "support"))
	s.Empty(s.writingAs(peer, "acme", 0, "support"))
}

// The nodes writing as an agent are those of the sessions the caller admits by customer and app,
// as a message in one app is answered only by a session there.
func (s *DirectorySuite) TestTheNodesWritingAsAnAgentAreThoseOfTheSessionsAdmitted() {
	s.directoryAt("node-a:1").Hold(s.ctx, "acme", 7, "s1", "support", "")
	s.directoryAt("node-b:1").Hold(s.ctx, "globex", 8, "s2", "support", "")
	s.directoryAt("node-c:1").Hold(s.ctx, "initech", 0, "s3", "support", "")
	peer := s.directoryAt("node-d:1")

	inTheDeploymentsApp, err := peer.NodesByAgent(s.ctx, "support", func(_ string, app int64) bool { return app == 0 || app == 7 })
	s.Require().NoError(err)
	globexes, err := peer.NodesByAgent(s.ctx, "support", func(customer string, _ int64) bool { return customer == "globex" })
	s.Require().NoError(err)

	s.Equal(map[string]string{"acme": "node-a:1", "initech": "node-c:1"}, inTheDeploymentsApp)
	s.Equal(map[string]string{"globex": "node-b:1"}, globexes)
}

// The nodes writing as an agent are the others: a node's own claim names a session it runs, which
// it finds among its own, or one that ended there and has not lapsed yet.
func (s *DirectorySuite) TestTheNodesWritingAsAnAgentAreNeverTheOneAsking() {
	here := s.directoryAt("node-a:1")
	here.Hold(s.ctx, "acme", 7, "s1", "support", "")
	s.directoryAt("node-b:1").Hold(s.ctx, "globex", 7, "s2", "support", "")

	writing, err := here.NodesByAgent(s.ctx, "support", everyApp)

	s.Require().NoError(err)
	s.Equal(map[string]string{"globex": "node-b:1"}, writing)
}

// A customer id may hold a colon, and an app id holds none: the customer is all that follows the
// first.
func (s *DirectorySuite) TestACustomerWritingAsAnAgentIsFoundByItsWholeIdThoughItHoldsAColon() {
	s.directoryAt("node-a:1").Hold(s.ctx, "a:7", 7, "s1", "support", "")

	peer := s.directoryAt("node-c:1")
	writing, err := peer.NodesByAgent(s.ctx, "support", func(customer string, app int64) bool { return customer == "a:7" && app == 7 })
	s.Require().NoError(err)
	s.Equal(map[string]string{"a:7": "node-a:1"}, writing)
	s.Equal("node-a:1", s.writingAs(peer, "a:7", 7, "support"))
}

// A claim on an agent that says no app, as a router of the release before writes it, is read
// nowhere: an app it does not say may be one the message was not written in.
func (s *DirectorySuite) TestAClaimOnAnAgentByItsCustomerAloneIsFoundNowhere() {
	live := strconv.FormatInt(time.Now().Add(time.Minute).UnixMilli(), 10)
	s.Require().NoError(s.client.Do(s.ctx, s.client.B().Hset().Key(s.prefix+":writing-as:support").
		FieldValue().FieldValue("acme", live+" node-a:1").Build()).Error())

	peer := s.directoryAt("node-c:1")
	s.Empty(s.writingAs(peer, "acme", 0, "support"))
	writing, err := peer.NodesByAgent(s.ctx, "support", everyApp)
	s.Require().NoError(err)
	s.Empty(writing)
}

// A renewal says a claim on an agent in its session's app again, not in the deployment's.
func (s *DirectorySuite) TestARenewedClaimOnAnAgentIsInItsSessionsApp() {
	directory := s.directoryAt("node-a:1")
	directory.Hold(s.ctx, "acme", 7, "s1", "support", "")
	s.Require().NoError(s.client.Do(s.ctx, s.client.B().Del().Key(s.prefix+":writing-as-in:support").Build()).Error())

	directory.renewAll(s.ctx)

	s.Equal("node-a:1", s.writingAs(s.directoryAt("node-c:1"), "acme", 7, "support"))
}

// A router of the release before reads a claim on an agent by its customer alone.
func (s *DirectorySuite) TestASessionWritingAsAnAgentIsFoundByItsCustomerAlone() {
	s.directoryAt("node-a:1").Hold(s.ctx, "acme", 7, "s1", "support", "")

	entry, err := s.client.Do(s.ctx, s.client.B().Hget().Key(s.prefix+":writing-as:support").Field("acme").Build()).ToString()
	s.Require().NoError(err)
	address, live := liveClaim(entry, time.Now().UnixMilli())
	s.True(live)
	s.Equal("node-a:1", address)
}

// The same for two customers answering in conversations of one cid.
func (s *DirectorySuite) TestTwoCustomersAnsweringInOneConversationAreFoundApart() {
	s.directoryAt("node-a:1").Hold(s.ctx, "acme", 0, "s1", "", "messaging:general")
	s.directoryAt("node-b:1").Hold(s.ctx, "globex", 0, "s2", "", "messaging:general")

	peer := s.directoryAt("node-c:1")
	s.Equal("node-a:1", s.answeringIn(peer, "acme", "messaging:general"))
	s.Equal("node-b:1", s.answeringIn(peer, "globex", "messaging:general"))
}

// A customer id may hold a colon, as a cid does, so customer "a:b" in "c:d" and customer "a" in
// "b:c:d" are two conversations.
func (s *DirectorySuite) TestACustomerAndCidAreNotTakenForAnother() {
	s.directoryAt("node-a:1").Hold(s.ctx, "a:b", 0, "s1", "", "c:d")
	s.directoryAt("node-b:1").Hold(s.ctx, "a", 0, "s2", "", "b:c:d")

	peer := s.directoryAt("node-c:1")
	s.Equal("node-a:1", s.answeringIn(peer, "a:b", "c:d"))
	s.Equal("node-b:1", s.answeringIn(peer, "a", "b:c:d"))
}

// A node that died stops renewing its customer's claim on an agent while another customer's
// renewals keep the agent's entry alive: that claim lapses all the same.
func (s *DirectorySuite) TestAClaimOnAnAgentThatLapsedIsFoundNowhere() {
	lapsed := strconv.FormatInt(time.Now().Add(-time.Second).UnixMilli(), 10)
	s.Require().NoError(s.client.Do(s.ctx, s.client.B().Hset().Key(s.prefix+":writing-as-in:support").
		FieldValue().FieldValue("7:acme", lapsed+" node-a:1").Build()).Error())
	s.directoryAt("node-b:1").Hold(s.ctx, "globex", 7, "s2", "support", "")

	peer := s.directoryAt("node-c:1")
	s.Empty(s.writingAs(peer, "acme", 7, "support"))
	writing, err := peer.NodesByAgent(s.ctx, "support", everyApp)
	s.Require().NoError(err)
	s.Equal(map[string]string{"globex": "node-b:1"}, writing)
}

// A router of the release before holds a conversation under its cid alone, and an approval in it
// is still carried there.
func (s *DirectorySuite) TestAConversationHeldUnderItsCidAloneIsFound() {
	s.Require().NoError(s.client.Do(s.ctx, s.client.B().Set().Key(s.prefix+":channel:messaging:general").Value("node-a:1").Build()).Error())

	s.Equal("node-a:1", s.answeringIn(s.directoryAt("node-c:1"), "acme", "messaging:general"))
}

// A claim on an agent lapses thirty seconds after it is made unless its node renews it, though
// other customers' renewals keep the agent's entry alive.
func (s *DirectorySuite) TestAClaimOnAnAgentLapsesThirtySecondsAfterItIsMade() {
	before := time.Now()
	s.directoryAt("node-a:1").Hold(s.ctx, "acme", 7, "s1", "support", "")
	after := time.Now()

	entry, err := s.client.Do(s.ctx, s.client.B().Hget().Key(s.prefix+":writing-as-in:support").Field("7:acme").Build()).ToString()
	s.Require().NoError(err)
	until, address, _ := strings.Cut(entry, " ")
	lapses, err := strconv.ParseInt(until, 10, 64)
	s.Require().NoError(err)
	s.Equal("node-a:1", address)
	s.GreaterOrEqual(lapses, before.Add(30*time.Second).UnixMilli())
	s.LessOrEqual(lapses, after.Add(30*time.Second).UnixMilli())
}

// A router of the release before reads a session's agent and conversation by their ids alone.
func (s *DirectorySuite) TestASessionHeldHereIsFoundByItsAgentAndConversationAlone() {
	s.directoryAt("node-a:1").Hold(s.ctx, "acme", 7, "s1", "support", "messaging:general")

	for _, key := range []string{s.prefix + ":agent:support", s.prefix + ":channel:messaging:general"} {
		held, err := s.client.Do(s.ctx, s.client.B().Get().Key(key).Build()).ToString()
		s.Require().NoError(err, key)
		s.Equal("node-a:1", held, key)
	}
}

func (s *DirectorySuite) TestAReleasedSessionIsFoundNowhere() {
	directory := s.directoryAt("node-a:1")
	directory.Hold(s.ctx, "acme", 0, "order-42", "", "")

	directory.Release(s.ctx, "acme", "order-42")

	s.Empty(s.nodeOf(s.directoryAt("node-c:1"), "acme", "order-42"))
	exists, err := s.client.Do(s.ctx, s.client.B().Exists().Key(s.prefix+":session:order-42").Build()).AsInt64()
	s.Require().NoError(err)
	s.Zero(exists, "a router of the release before would still forward to it")
}

func (s *DirectorySuite) holder(app int64, id string) string {
	customer, err := s.directoryAt("node-x:1").CallHolder(s.ctx, app, id)
	s.Require().NoError(err)
	return customer
}

// callOf has a directory say a customer's session is on the call order-42 of app 7.
func (s *DirectorySuite) callOf(directory *Directory, customer string) func() {
	release, err := directory.HoldCall(s.ctx, customer, 7, "order-42", nil)
	s.Require().NoError(err)
	return func() { release(s.ctx) }
}

// lapsed puts what is said of the call, as a node that stopped renewing leaves it, in the past.
func (s *DirectorySuite) lapsed() {
	key := s.prefix + ":call:7:order-42"
	fields, err := s.client.Do(s.ctx, s.client.B().Hgetall().Key(key).Build()).AsStrMap()
	s.Require().NoError(err)
	for field, said := range fields {
		_, customer, _ := strings.Cut(said, " ")
		s.Require().NoError(s.client.Do(s.ctx, s.client.B().Hset().Key(key).FieldValue().FieldValue(field, "1 "+customer).Build()).Error())
	}
}

func (s *DirectorySuite) TestASessionsCallIsFoundByItsAppUntilItIsReleased() {
	directory := s.directoryAt("node-a:1")
	release := s.callOf(directory, "a customer:1")

	s.Equal("a customer:1", s.holder(7, "order-42"))
	s.Empty(s.holder(0, "order-42"), "another app's call of the name")
	s.Empty(s.holder(7, "order-43"))
	ttl, err := s.client.Do(s.ctx, s.client.B().Pttl().Key(s.prefix+":call:7:order-42").Build()).AsInt64()
	s.Require().NoError(err)
	s.InDelta(30000, ttl, 5000, "it lapses with the node")
	s.lapsed()
	s.Empty(s.holder(7, "order-42"))
	directory.renewAll(s.ctx)
	s.Equal("a customer:1", s.holder(7, "order-42"))

	release()
	s.Empty(s.holder(7, "order-42"))
	directory.renewAll(s.ctx)
	s.Empty(s.holder(7, "order-42"))
}

func (s *DirectorySuite) TestAHoldLapsesThirtySecondsAfterItIsSaid() {
	s.callOf(s.directoryAt("node-a:1"), "acme")

	fields, err := s.client.Do(s.ctx, s.client.B().Hgetall().Key(s.prefix+":call:7:order-42").Build()).AsStrMap()
	s.Require().NoError(err)
	s.Require().Len(fields, 1)
	for _, said := range fields {
		until, _, _ := strings.Cut(said, " ")
		at, err := strconv.ParseInt(until, 10, 64)
		s.Require().NoError(err)
		s.InDelta(time.Now().Add(30*time.Second).UnixMilli(), at, 5000)
	}
}

func (s *DirectorySuite) TestACallAnotherCustomerHoldsStaysItsOwn() {
	first, second := s.directoryAt("node-a:1"), s.directoryAt("node-b:1")
	release := s.callOf(first, "acme")

	refused, err := second.HoldCall(s.ctx, "acm", 7, "order-42", nil)
	s.ErrorIs(err, ErrCallHeld)
	refused(s.ctx)
	second.renewAll(s.ctx)
	s.Equal("acme", s.holder(7, "order-42"))
	// A hold refused is not said once the call is free.
	s.lapsed()
	second.renewAll(s.ctx)
	s.Empty(s.holder(7, "order-42"))

	// One that lapsed and was taken is not renewed over, nor taken back with it.
	s.callOf(second, "globex")
	first.renewAll(s.ctx)
	s.Equal("globex", s.holder(7, "order-42"))
	release()
	s.Equal("globex", s.holder(7, "order-42"))
}

// A renewal another customer's hold refuses is said, and no other is.
func (s *DirectorySuite) TestARenewalAnotherCustomersHoldRefusesIsLogged() {
	var logged bytes.Buffer
	directory, err := NewDirectory(DirectoryOptions{
		Redis: s.client, Address: "node-a:1", Prefix: s.prefix,
		Logger: slog.New(slog.NewTextHandler(&logged, nil)),
	})
	s.Require().NoError(err)
	directory.Close()
	s.callOf(directory, "acme")
	_, err = directory.HoldCall(s.ctx, "acme", 7, "order-43", nil)
	s.Require().NoError(err)
	directory.renewAll(s.ctx)
	s.Empty(logged.String())
	s.lapsed()
	s.callOf(s.directoryAt("node-b:1"), "globex")

	directory.renewAll(s.ctx)

	s.Equal(1, strings.Count(logged.String(), "its hold is not renewed"))
	s.Contains(logged.String(), "level=WARN")
	s.Contains(logged.String(), "app_id=acme")
	s.Contains(logged.String(), s.prefix+":call:7:order-42")
}

// When a renewal is refused because another customer holds the call, onLost is called once,
// releasing inside onLost does not deadlock or delete the new holder's key, and other calls
// on the node stay held.
func (s *DirectorySuite) TestARenewalAnotherCustomersHoldRefusesCallsOnLostOnceAndLeavesTheirs() {
	var logged bytes.Buffer
	first, err := NewDirectory(DirectoryOptions{
		Redis: s.client, Address: "node-a:1", Prefix: s.prefix,
		Logger: slog.New(slog.NewTextHandler(&logged, nil)),
	})
	s.Require().NoError(err)
	first.Close()

	var lostCount, deferredLostCount, keptCount atomic.Int32
	var release func(context.Context)
	release, err = first.HoldCall(s.ctx, "acme", 7, "order-42", func() {
		lostCount.Add(1)
		release(s.ctx)
	})
	s.Require().NoError(err)
	// A second lost hold whose onLost does not call release synchronously (as an async
	// session close does): renewAll itself must drop it so a later renewal does not re-fire.
	_, err = first.HoldCall(s.ctx, "acme", 7, "order-42b", func() {
		deferredLostCount.Add(1)
	})
	s.Require().NoError(err)
	_, err = first.HoldCall(s.ctx, "acme", 7, "order-43", func() {
		keptCount.Add(1)
	})
	s.Require().NoError(err)

	// Clear Redis for order-42 and order-42b and let globex take both.
	s.Require().NoError(s.client.Do(s.ctx, s.client.B().Del().Key(s.prefix+":call:7:order-42", s.prefix+":call:7:order-42b").Build()).Error())
	s.callOf(s.directoryAt("node-b:1"), "globex")
	_, err = s.directoryAt("node-b:1").HoldCall(s.ctx, "globex", 7, "order-42b", nil)
	s.Require().NoError(err)

	first.renewAll(s.ctx)
	first.renewAll(s.ctx)

	s.Equal(int32(1), lostCount.Load(), "onLost must fire once when renewal returns 0")
	s.Equal(int32(1), deferredLostCount.Load(), "onLost must fire once even when release has not run yet")
	s.Equal(int32(0), keptCount.Load(), "an unaffected call must not lose its hold")
	s.Equal(2, strings.Count(logged.String(), "its hold is not renewed"))
	s.Equal("globex", s.holder(7, "order-42"), "releasing the lost hold must leave the new holder intact")
	s.Equal("globex", s.holder(7, "order-42b"))
	s.Equal("acme", s.holder(7, "order-43"))
}

// A transient Redis error during renewal does not fire onLost or drop the hold.
func (s *DirectorySuite) TestARenewalThatCannotReachRedisKeepsTheHold() {
	first := s.directoryAt("node-a:1")
	var lost atomic.Int32
	_, err := first.HoldCall(s.ctx, "acme", 7, "order-42", func() { lost.Add(1) })
	s.Require().NoError(err)

	cancelled, cancel := context.WithCancel(s.ctx)
	cancel()
	first.renewAll(cancelled)
	s.Equal(int32(0), lost.Load())

	// Once Redis answers again, the hold is still renewed.
	s.lapsed()
	first.renewAll(s.ctx)
	s.Equal(int32(0), lost.Load())
	s.Equal("acme", s.holder(7, "order-42"))

	// A closed Redis client (connection error) also does not fire onLost.
	downClient, err := redisready.NewClient(rueidis.ClientOption{InitAddress: []string{os.Getenv("ROUTER_REDIS_ADDR")}, DisableCache: true})
	s.Require().NoError(err)
	downDir, err := NewDirectory(DirectoryOptions{
		Redis: downClient, Address: "node-down:1", Prefix: s.prefix, Logger: slog.New(slog.DiscardHandler),
	})
	s.Require().NoError(err)
	downDir.Close()
	_, err = downDir.HoldCall(s.ctx, "acme", 7, "order-44", func() { lost.Add(1) })
	s.Require().NoError(err)
	downClient.Close()
	downDir.renewAll(s.ctx)
	s.Equal(int32(0), lost.Load())

	// A hung Redis that accepts TCP but never replies times out without firing onLost.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	defer listener.Close()
	var hung atomic.Bool
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			if hung.Load() {
				s.T().Cleanup(func() { _ = conn.Close() })
				continue
			}
			upstream, err := net.Dial("tcp", os.Getenv("ROUTER_REDIS_ADDR"))
			if err != nil {
				_ = conn.Close()
				return
			}
			s.T().Cleanup(func() { _ = conn.Close(); _ = upstream.Close() })
			go func() { _, _ = io.Copy(upstream, conn) }()
			go func() {
				answer := make([]byte, 64<<10)
				for {
					n, err := upstream.Read(answer)
					if err != nil {
						return
					}
					if hung.Load() {
						return
					}
					if _, err := conn.Write(answer[:n]); err != nil {
						return
					}
				}
			}()
		}
	}()
	hungClient, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{listener.Addr().String()}, DisableCache: true, ForceSingleClient: true, PipelineMultiplex: -1,
	})
	s.Require().NoError(err)
	s.T().Cleanup(hungClient.Close)
	hungDir, err := NewDirectory(DirectoryOptions{
		Redis: hungClient, Address: "node-hung:1", Prefix: s.prefix, Logger: slog.New(slog.DiscardHandler),
	})
	s.Require().NoError(err)
	hungDir.Close()
	_, err = hungDir.HoldCall(s.ctx, "acme", 7, "order-45", func() { lost.Add(1) })
	s.Require().NoError(err)
	hung.Store(true)
	timeoutCtx, stopTimeout := context.WithTimeout(s.ctx, 200*time.Millisecond)
	defer stopTimeout()
	hungDir.renewAll(timeoutCtx)
	s.Equal(int32(0), lost.Load())
}

// A node that stopped renewing leaves its sessions' fields behind: the next hold drops them.
func (s *DirectorySuite) TestAHoldDropsWhatLapsed() {
	s.callOf(s.directoryAt("node-a:1"), "acme")
	s.callOf(s.directoryAt("node-b:1"), "acme")
	s.lapsed()

	s.callOf(s.directoryAt("node-c:1"), "globex")

	fields, err := s.client.Do(s.ctx, s.client.B().Hkeys().Key(s.prefix+":call:7:order-42").Build()).AsStrSlice()
	s.Require().NoError(err)
	s.Equal([]string{"node-c:1#1"}, fields)
}

// A Redis that takes most of the time there is to answer is waited for.
func (s *DirectorySuite) TestACallIsHeldAndFoundThoughRedisIsSlow() {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			upstream, err := net.Dial("tcp", os.Getenv("ROUTER_REDIS_ADDR"))
			if err != nil {
				_ = conn.Close()
				return
			}
			go func() { _, _ = io.Copy(upstream, conn); _ = upstream.Close() }()
			go func() {
				defer conn.Close()
				answer := make([]byte, 64<<10)
				for {
					n, err := upstream.Read(answer)
					if err != nil {
						return
					}
					time.Sleep(700 * time.Millisecond)
					if _, err := conn.Write(answer[:n]); err != nil {
						return
					}
				}
			}()
		}
	}()
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{listener.Addr().String()}, DisableCache: true, ForceSingleClient: true, PipelineMultiplex: -1,
	})
	s.Require().NoError(err)
	s.T().Cleanup(client.Close)
	directory, err := NewDirectory(DirectoryOptions{Redis: client, Address: "node-a:1", Prefix: s.prefix})
	s.Require().NoError(err)
	directory.Close()

	_, err = directory.HoldCall(s.ctx, "acme", 7, "order-42", nil)
	s.Require().NoError(err)
	customer, err := directory.CallHolder(s.ctx, 7, "order-42")
	s.Require().NoError(err)
	s.Equal("acme", customer)
}

// A session Redis could not be told of is logged.
func (s *DirectorySuite) TestASessionThatCannotBeSaidIsLogged() {
	var logged bytes.Buffer
	client, err := redisready.NewClient(rueidis.ClientOption{InitAddress: []string{os.Getenv("ROUTER_REDIS_ADDR")}, DisableCache: true})
	s.Require().NoError(err)
	directory, err := NewDirectory(DirectoryOptions{
		Redis: client, Address: "node-a:1", Prefix: s.prefix, Logger: slog.New(slog.NewTextHandler(&logged, nil)),
	})
	s.Require().NoError(err)
	directory.Close()
	client.Close()

	directory.Hold(s.ctx, "acme", 7, "order-42", "", "")

	s.Equal(1, strings.Count(logged.String(), "could not say this node is running a session"))
}

// A client redialling a Redis that does not answer keeps every caller on the connection waiting
// for the dial of another: one with less time is answered in its own all the same.
func (s *DirectorySuite) TestACallerIsAnsweredInTimeBesideAnothersRedial() {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	defer listener.Close()
	dialled := make(chan struct{}, 8)
	var hung atomic.Bool
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			if hung.Load() {
				dialled <- struct{}{}
				defer conn.Close()
				continue
			}
			upstream, err := net.Dial("tcp", os.Getenv("ROUTER_REDIS_ADDR"))
			if err != nil {
				_ = conn.Close()
				return
			}
			go func() { _, _ = io.Copy(upstream, conn); _ = upstream.Close() }()
			go func() { _, _ = io.Copy(conn, upstream) }()
			s.T().Cleanup(func() { _ = conn.Close() })
			go func() {
				for !hung.Load() {
					time.Sleep(5 * time.Millisecond)
				}
				_ = conn.Close()
			}()
		}
	}()
	client, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{listener.Addr().String()}, DisableCache: true, ForceSingleClient: true})
	s.Require().NoError(err)
	s.T().Cleanup(client.Close)
	directory, err := NewDirectory(DirectoryOptions{Redis: client, Address: "node-a:1", Prefix: s.prefix})
	s.Require().NoError(err)
	directory.Close()
	hung.Store(true)
	redialling, stop := context.WithTimeout(s.ctx, 20*time.Second)
	defer stop()
	// The client has several connections, and a key is always asked of the same one: these are
	// enough keys for every connection to be redialled.
	for i := range 32 {
		go func() {
			for redialling.Err() == nil {
				_, _ = directory.Node(redialling, "acme", fmt.Sprintf("order-%d", i))
			}
		}()
	}
	select {
	case <-dialled:
	case <-time.After(10 * time.Second):
		s.FailNow("nobody redialled")
	}
	for more := true; more; {
		select {
		case <-dialled:
		case <-time.After(300 * time.Millisecond):
			more = false
		}
	}

	for what, call := range map[string]func(){
		"asking":  func() { _, _ = directory.CallHolder(s.ctx, 7, "order-42") },
		"holding": func() { _, _ = directory.HoldCall(s.ctx, "acme", 7, "order-42", nil) },
		"letting go": func() {
			directory.releaseCall(s.ctx, heldCall{"acme", directory.callKey(7, "order-42"), "field"})
		},
		"saying": func() { directory.Hold(s.ctx, "acme", 7, "order-42", "", "") },
	} {
		began := time.Now()
		call()
		s.Less(time.Since(began), 1500*time.Millisecond, what)
	}
}

// A customer's sessions on one call, here and on another node, each hold it until the last is gone.
func (s *DirectorySuite) TestACallIsHeldUntilTheLastSessionOnItIsGone() {
	first, second := s.directoryAt("node-a:1"), s.directoryAt("node-b:1")
	here, again, there := s.callOf(first, "acme"), s.callOf(first, "acme"), s.callOf(second, "acme")
	refused := func() {
		_, err := s.directoryAt("node-c:1").HoldCall(s.ctx, "globex", 7, "order-42", nil)
		s.ErrorIs(err, ErrCallHeld)
	}

	refused()
	there()
	refused()
	here()
	refused()
	s.Equal("acme", s.holder(7, "order-42"))
	again()
	s.Empty(s.holder(7, "order-42"))
}

// Two nodes' first holds of one customer's call are each their own.
func (s *DirectorySuite) TestACallTwoNodesHoldFirstIsHeldUntilBothAreGone() {
	here, there := s.callOf(s.directoryAt("node-a:1"), "acme"), s.callOf(s.directoryAt("node-b:1"), "acme")

	there()
	s.Equal("acme", s.holder(7, "order-42"))
	_, err := s.directoryAt("node-c:1").HoldCall(s.ctx, "globex", 7, "order-42", nil)
	s.ErrorIs(err, ErrCallHeld)
	here()
	s.Empty(s.holder(7, "order-42"))
}

// A hold Redis did not take is said with the next renewal, until it is released.
func (s *DirectorySuite) TestAHoldThatWasNotSaidIsSaidWithTheNextRenewal() {
	directory := s.directoryAt("node-a:1")
	cancelled, cancel := context.WithCancel(s.ctx)
	cancel()
	release, err := directory.HoldCall(cancelled, "acme", 7, "order-42", nil)
	s.Require().Error(err)
	s.Require().NotErrorIs(err, ErrCallHeld)
	s.Require().Empty(s.holder(7, "order-42"))

	directory.renewAll(s.ctx)
	s.Equal("acme", s.holder(7, "order-42"))
	release(s.ctx)
	s.Empty(s.holder(7, "order-42"))
}

// Each claim that cannot be taken back is said, so a Redis that refuses one of the two leaves the
// other's failure in the log too.
func (s *DirectorySuite) TestEachClaimThatCannotBeTakenBackIsLogged() {
	var logged bytes.Buffer
	directory, err := NewDirectory(DirectoryOptions{
		Redis: s.client, Address: "node-a:1", Prefix: s.prefix,
		Logger: slog.New(slog.NewTextHandler(&logged, nil)),
	})
	s.Require().NoError(err)
	directory.Close()
	directory.Hold(s.ctx, "acme", 0, "order-42", "", "")
	gone, cancel := context.WithCancel(s.ctx)
	cancel()

	directory.Release(gone, "acme", "order-42")

	s.Equal(2, strings.Count(logged.String(), "could not take back this node's claim on a session"))
}

// A DEL naming two keys is refused on a Redis Cluster when they are in different hash slots, and a
// session's two keys are. This holds with no cluster: each key is deleted by a command of its own.
func (s *DirectorySuite) TestAReleasedSessionsKeysAreDeletedOneCommandEach() {
	sent := &sentCommands{}
	directory, err := NewDirectory(DirectoryOptions{Redis: rueidishook.WithHook(s.client, sent), Address: "node-a:1", Prefix: s.prefix})
	s.Require().NoError(err)
	directory.Close()
	directory.Hold(s.ctx, "acme", 0, "order-42", "", "")
	sent.reset()

	directory.Release(s.ctx, "acme", "order-42")

	var deleted []string
	for _, command := range sent.all() {
		if strings.EqualFold(command[0], "DEL") {
			s.Len(command, 2, "a DEL names one key: %v", command)
			deleted = append(deleted, command[1:]...)
		}
	}
	s.ElementsMatch([]string{s.prefix + ":session:order-42", directory.sessionKey(heldSession{"acme", "order-42"})}, deleted)
}

// clusterAddressVar names a node of a Redis Cluster with more than one shard, which CI does not
// run. Three `redis-server --cluster-enabled yes` joined by `redis-cli --cluster create` make one.
const clusterAddressVar = "ROUTER_TEST_REDIS_CLUSTER_ADDR"

// On a cluster, a command naming keys of different hash slots panics in the client before it is
// sent, and a session's key under its customer and under its id alone are in different slots.
// Closing a session must drop both.
func (s *DirectorySuite) TestAReleasedSessionIsFoundNowhereOnACluster() {
	address := os.Getenv(clusterAddressVar)
	if address == "" {
		s.T().Skipf("%s not set", clusterAddressVar)
	}
	cluster, err := redisready.NewClient(rueidis.ClientOption{InitAddress: []string{address}, DisableCache: true})
	s.Require().NoError(err)
	s.T().Cleanup(cluster.Close)
	directory, err := NewDirectory(DirectoryOptions{Redis: cluster, Address: "node-a:1", Prefix: s.prefix})
	s.Require().NoError(err)
	directory.Close()
	keys := []string{directory.sessionKey(heldSession{"acme", "order-42"}), s.prefix + ":session:order-42"}
	slots := map[int64]bool{}
	for _, key := range keys {
		slot, err := cluster.Do(s.ctx, cluster.B().ClusterKeyslot().Key(key).Build()).AsInt64()
		s.Require().NoError(err)
		slots[slot] = true
	}
	s.Require().Len(slots, 2, "the two keys of a session sit in different slots")
	directory.Hold(s.ctx, "acme", 0, "order-42", "", "")
	for _, key := range keys {
		exists, err := cluster.Do(s.ctx, cluster.B().Exists().Key(key).Build()).AsInt64()
		s.Require().NoError(err)
		s.Require().EqualValues(1, exists, "%s is held", key)
	}

	directory.Release(s.ctx, "acme", "order-42")

	for _, key := range keys {
		exists, err := cluster.Do(s.ctx, cluster.B().Exists().Key(key).Build()).AsInt64()
		s.Require().NoError(err)
		s.Zero(exists, "%s is still held", key)
	}
}

// The shard's index names the nodes saying they are in it now: not a node of another shard,
// nor one whose claim lapsed because it stopped renewing.
func (s *DirectorySuite) TestANodeInAShardIsALiveNodeOfThatShard() {
	for _, node := range []struct{ address, shard string }{{"eu-a:1", "eu"}, {"us-a:1", "us"}} {
		directory, err := NewDirectory(DirectoryOptions{Redis: s.client, Address: node.address, Shard: node.shard, Prefix: s.prefix})
		s.Require().NoError(err)
		defer directory.Close()
	}
	lapsed := float64(time.Now().Add(-time.Second).UnixMilli())
	s.Require().NoError(s.client.Do(s.ctx, s.client.B().Zadd().Key(s.prefix+":inshard:eu").ScoreMember().
		ScoreMember(lapsed, "eu-gone:1").Build()).Error())
	asking, err := NewDirectory(DirectoryOptions{Redis: s.client, Address: "us-b:1", Shard: "us", Prefix: s.prefix})
	s.Require().NoError(err)
	asking.Close()

	for range 20 {
		found, err := asking.NodeInShard(s.ctx, "eu")
		s.Require().NoError(err)
		s.Equal("eu-a:1", found)
	}
	none, err := asking.NodeInShard(s.ctx, "asia")
	s.Require().NoError(err)
	s.Empty(none, "a shard no node is in has a node")
}

func (s *DirectorySuite) TestANodeThatClosedIsNoNodeOfItsShard() {
	leaving, err := NewDirectory(DirectoryOptions{Redis: s.client, Address: "eu-a:1", Shard: "eu", Prefix: s.prefix})
	s.Require().NoError(err)
	found, err := leaving.NodeInShard(s.ctx, "eu")
	s.Require().NoError(err)
	s.Require().Equal("eu-a:1", found)

	leaving.Close()

	found, err = leaving.NodeInShard(s.ctx, "eu")
	s.Require().NoError(err)
	s.Empty(found, "a node that closed is still handed its shard's work")
}

// A customer kept in shard eu is carried only to the workers of eu's nodes: not to a node of
// another shard, nor to one that never said which shard it is in. Another customer's reach all.
func (s *DirectorySuite) TestWorkersOutsideTheShardsACustomerIsKeptInAreNotNamed() {
	for _, node := range []struct{ address, shard string }{{"eu-a:1", "eu"}, {"us-a:1", "us"}, {"unsaid:1", ""}} {
		directory, err := NewDirectory(DirectoryOptions{Redis: s.client, Address: node.address, Shard: node.shard, Prefix: s.prefix})
		s.Require().NoError(err)
		defer directory.Close()
		for _, customer := range []string{"kept", "free"} {
			defer directory.Wait(s.ctx, customer)()
			directory.Offer(s.ctx, customer, "support", []byte(`{}`))
		}
	}
	asking, err := NewDirectory(DirectoryOptions{Redis: s.client, Address: "eu-b:1", Shard: "eu", Prefix: s.prefix})
	s.Require().NoError(err)
	defer asking.Close()
	asking.KeepWithin(keptInEU)

	waiting, err := asking.Waiting(s.ctx, "kept")
	s.Require().NoError(err)
	s.Equal([]string{"eu-a:1"}, waiting)
	offers, err := asking.Offers(s.ctx, "kept", "support")
	s.Require().NoError(err)
	s.Equal([]string{"eu-a:1"}, slices.Collect(maps.Keys(offers)))

	waiting, err = asking.Waiting(s.ctx, "free")
	s.Require().NoError(err)
	s.ElementsMatch([]string{"eu-a:1", "us-a:1", "unsaid:1"}, waiting)
	offers, err = asking.Offers(s.ctx, "free", "support")
	s.Require().NoError(err)
	s.Len(offers, 3)
}

func (s *DirectorySuite) TestAWorkerHereTakesTheWorkOfACustomerKeptInThisShardAlone() {
	eu, err := NewDirectory(DirectoryOptions{Redis: s.client, Address: "eu-a:1", Shard: "eu", Prefix: s.prefix})
	s.Require().NoError(err)
	defer eu.Close()
	us, err := NewDirectory(DirectoryOptions{Redis: s.client, Address: "us-a:1", Shard: "us", Prefix: s.prefix})
	s.Require().NoError(err)
	defer us.Close()
	eu.KeepWithin(keptInEU)
	us.KeepWithin(keptInEU)

	for directory, takes := range map[*Directory]bool{eu: true, us: false} {
		here, err := directory.TakesHere(s.ctx, "kept")
		s.Require().NoError(err)
		s.Equal(takes, here, directory.Shard())
		here, err = directory.TakesHere(s.ctx, "free")
		s.Require().NoError(err)
		s.True(here, "a customer kept nowhere in particular is not taken in %s", directory.Shard())
	}
}

// keptInEU keeps customer kept's work in shard eu and nobody else's anywhere.
func keptInEU(_ context.Context, customer string) ([]string, error) {
	if customer == "kept" {
		return []string{"eu"}, nil
	}
	return nil, nil
}

// directoryAt is a node's directory at address, renewing nothing for the test's length.
func (s *DirectorySuite) directoryAt(address string) *Directory {
	directory, err := NewDirectory(DirectoryOptions{Redis: s.client, Address: address, Prefix: s.prefix})
	s.Require().NoError(err)
	directory.Close()
	return directory
}

// nodeOf is where directory says a customer's session runs.
func (s *DirectorySuite) nodeOf(directory *Directory, customer, session string) string {
	address, err := directory.Node(s.ctx, customer, session)
	s.Require().NoError(err)
	return address
}

// writingAs is where directory says a customer's session in app writing as an agent runs.
func (s *DirectorySuite) writingAs(directory *Directory, customer string, app int64, agent string) string {
	address, err := directory.NodeByAgent(s.ctx, customer, app, agent)
	s.Require().NoError(err)
	return address
}

// everyApp admits every customer's sessions in every app.
func everyApp(string, int64) bool { return true }

// answeringIn is where directory says a customer's session answering in a conversation runs.
func (s *DirectorySuite) answeringIn(directory *Directory, customer, cid string) string {
	address, err := directory.NodeByConversation(s.ctx, customer, cid)
	s.Require().NoError(err)
	return address
}

// roundTrips counts what is sent to Redis: a command, or a pipeline of them, is one.
type roundTrips struct{ sent atomic.Int64 }

func (r *roundTrips) Do(client rueidis.Client, ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	r.sent.Add(1)
	return client.Do(ctx, cmd)
}

func (r *roundTrips) DoMulti(client rueidis.Client, ctx context.Context, multi ...rueidis.Completed) []rueidis.RedisResult {
	r.sent.Add(1)
	return client.DoMulti(ctx, multi...)
}

func (r *roundTrips) DoCache(client rueidis.Client, ctx context.Context, cmd rueidis.Cacheable, ttl time.Duration) rueidis.RedisResult {
	r.sent.Add(1)
	return client.DoCache(ctx, cmd, ttl)
}

func (r *roundTrips) DoMultiCache(client rueidis.Client, ctx context.Context, multi ...rueidis.CacheableTTL) []rueidis.RedisResult {
	r.sent.Add(1)
	return client.DoMultiCache(ctx, multi...)
}

func (r *roundTrips) Receive(client rueidis.Client, ctx context.Context, subscribe rueidis.Completed, fn func(msg rueidis.PubSubMessage)) error {
	r.sent.Add(1)
	return client.Receive(ctx, subscribe, fn)
}

func (r *roundTrips) DoStream(client rueidis.Client, ctx context.Context, cmd rueidis.Completed) rueidis.RedisResultStream {
	r.sent.Add(1)
	return client.DoStream(ctx, cmd)
}

func (r *roundTrips) DoMultiStream(client rueidis.Client, ctx context.Context, multi ...rueidis.Completed) rueidis.MultiRedisResultStream {
	r.sent.Add(1)
	return client.DoMultiStream(ctx, multi...)
}

// sentCommands keeps every command sent to Redis, in order.
type sentCommands struct {
	mu       sync.Mutex
	commands [][]string
}

func (c *sentCommands) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commands = nil
}

func (c *sentCommands) all() [][]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]string(nil), c.commands...)
}

func (c *sentCommands) keep(commands ...rueidis.Completed) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, command := range commands {
		// The client reuses a command's slice once it is sent.
		c.commands = append(c.commands, slices.Clone(command.Commands()))
	}
}

func (c *sentCommands) Do(client rueidis.Client, ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	c.keep(cmd)
	return client.Do(ctx, cmd)
}

func (c *sentCommands) DoMulti(client rueidis.Client, ctx context.Context, multi ...rueidis.Completed) []rueidis.RedisResult {
	c.keep(multi...)
	return client.DoMulti(ctx, multi...)
}

func (c *sentCommands) DoCache(client rueidis.Client, ctx context.Context, cmd rueidis.Cacheable, ttl time.Duration) rueidis.RedisResult {
	return client.DoCache(ctx, cmd, ttl)
}

func (c *sentCommands) DoMultiCache(client rueidis.Client, ctx context.Context, multi ...rueidis.CacheableTTL) []rueidis.RedisResult {
	return client.DoMultiCache(ctx, multi...)
}

func (c *sentCommands) Receive(client rueidis.Client, ctx context.Context, subscribe rueidis.Completed, fn func(msg rueidis.PubSubMessage)) error {
	return client.Receive(ctx, subscribe, fn)
}

func (c *sentCommands) DoStream(client rueidis.Client, ctx context.Context, cmd rueidis.Completed) rueidis.RedisResultStream {
	return client.DoStream(ctx, cmd)
}

func (c *sentCommands) DoMultiStream(client rueidis.Client, ctx context.Context, multi ...rueidis.Completed) rueidis.MultiRedisResultStream {
	return client.DoMultiStream(ctx, multi...)
}
