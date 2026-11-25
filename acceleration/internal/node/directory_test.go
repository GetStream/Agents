//go:build integration

package node

import (
	"context"
	"os"
	"strconv"
	"strings"
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
	directory.Close()
	for _, session := range []string{"s1", "s2", "s3"} {
		directory.Hold(s.ctx, "acme", session, "agent-"+session, "messaging:"+session)
	}
	s.callOf(directory, "acme")
	directory.Wait(s.ctx, "acme")
	directory.Wait(s.ctx, "globex")
	directory.Offer(s.ctx, "acme", "agent-s1", []byte(`{"tools":[]}`))
	directory.Offer(s.ctx, "globex", "agent-s2", []byte(`{"tools":[]}`))
	keys, err := s.client.Do(s.ctx, s.client.B().Keys().Pattern(s.prefix+":*").Build()).AsStrSlice()
	s.Require().NoError(err)
	// Each session: itself, its agent and its conversation, each under its customer and under
	// its id alone. And the call of one.
	s.Require().Len(keys, 3*6+1+2+2+1)
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
	writing, err := peer.NodesByAgent(s.ctx, "agent-s2")
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
	first.Hold(s.ctx, "acme", "order-42", "", "")
	second.Hold(s.ctx, "globex", "order-42", "", "")

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
	s.directoryAt("node-a:1").Hold(s.ctx, "acme", "order-42", "", "")

	held, err := s.client.Do(s.ctx, s.client.B().Get().Key(s.prefix+":session:order-42").Build()).ToString()
	s.Require().NoError(err)
	s.Equal("node-a:1", held)
}

// Two customers may each run a session writing as one agent: each customer's is found, whichever
// node said so last.
func (s *DirectorySuite) TestTwoCustomersWritingAsOneAgentAreFoundApart() {
	s.directoryAt("node-a:1").Hold(s.ctx, "acme", "s1", "support", "")
	s.directoryAt("node-b:1").Hold(s.ctx, "globex", "s2", "support", "")

	peer := s.directoryAt("node-c:1")
	s.Equal("node-a:1", s.writingAs(peer, "acme", "support"))
	s.Equal("node-b:1", s.writingAs(peer, "globex", "support"))
	writing, err := peer.NodesByAgent(s.ctx, "support")
	s.Require().NoError(err)
	s.Equal(map[string]string{"acme": "node-a:1", "globex": "node-b:1"}, writing)
}

// The same for two customers answering in conversations of one cid.
func (s *DirectorySuite) TestTwoCustomersAnsweringInOneConversationAreFoundApart() {
	s.directoryAt("node-a:1").Hold(s.ctx, "acme", "s1", "", "messaging:general")
	s.directoryAt("node-b:1").Hold(s.ctx, "globex", "s2", "", "messaging:general")

	peer := s.directoryAt("node-c:1")
	s.Equal("node-a:1", s.answeringIn(peer, "acme", "messaging:general"))
	s.Equal("node-b:1", s.answeringIn(peer, "globex", "messaging:general"))
}

// A customer id may hold a colon, as a cid does, so customer "a:b" in "c:d" and customer "a" in
// "b:c:d" are two conversations.
func (s *DirectorySuite) TestACustomerAndCidAreNotTakenForAnother() {
	s.directoryAt("node-a:1").Hold(s.ctx, "a:b", "s1", "", "c:d")
	s.directoryAt("node-b:1").Hold(s.ctx, "a", "s2", "", "b:c:d")

	peer := s.directoryAt("node-c:1")
	s.Equal("node-a:1", s.answeringIn(peer, "a:b", "c:d"))
	s.Equal("node-b:1", s.answeringIn(peer, "a", "b:c:d"))
}

// A node that died stops renewing its customer's claim on an agent while another customer's
// renewals keep the agent's entry alive: that claim lapses all the same.
func (s *DirectorySuite) TestAClaimOnAnAgentThatLapsedIsFoundNowhere() {
	lapsed := strconv.FormatInt(time.Now().Add(-time.Second).UnixMilli(), 10)
	s.Require().NoError(s.client.Do(s.ctx, s.client.B().Hset().Key(s.prefix+":writing-as:support").
		FieldValue().FieldValue("acme", lapsed+" node-a:1").Build()).Error())
	s.directoryAt("node-b:1").Hold(s.ctx, "globex", "s2", "support", "")

	peer := s.directoryAt("node-c:1")
	s.Empty(s.writingAs(peer, "acme", "support"))
	writing, err := peer.NodesByAgent(s.ctx, "support")
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
	s.directoryAt("node-a:1").Hold(s.ctx, "acme", "s1", "support", "")
	after := time.Now()

	entry, err := s.client.Do(s.ctx, s.client.B().Hget().Key(s.prefix+":writing-as:support").Field("acme").Build()).ToString()
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
	s.directoryAt("node-a:1").Hold(s.ctx, "acme", "s1", "support", "messaging:general")

	for _, key := range []string{s.prefix + ":agent:support", s.prefix + ":channel:messaging:general"} {
		held, err := s.client.Do(s.ctx, s.client.B().Get().Key(key).Build()).ToString()
		s.Require().NoError(err, key)
		s.Equal("node-a:1", held, key)
	}
}

func (s *DirectorySuite) TestAReleasedSessionIsFoundNowhere() {
	directory := s.directoryAt("node-a:1")
	directory.Hold(s.ctx, "acme", "order-42", "", "")

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
	release, err := directory.HoldCall(s.ctx, customer, 7, "order-42")
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

	refused, err := second.HoldCall(s.ctx, "acm", 7, "order-42")
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

// A customer's sessions on one call, here and on another node, each hold it until the last is gone.
func (s *DirectorySuite) TestACallIsHeldUntilTheLastSessionOnItIsGone() {
	first, second := s.directoryAt("node-a:1"), s.directoryAt("node-b:1")
	here, again, there := s.callOf(first, "acme"), s.callOf(first, "acme"), s.callOf(second, "acme")
	refused := func() {
		_, err := s.directoryAt("node-c:1").HoldCall(s.ctx, "globex", 7, "order-42")
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

// A hold Redis did not take is said with the next renewal, until it is released.
func (s *DirectorySuite) TestAHoldThatWasNotSaidIsSaidWithTheNextRenewal() {
	directory := s.directoryAt("node-a:1")
	cancelled, cancel := context.WithCancel(s.ctx)
	cancel()
	release, err := directory.HoldCall(cancelled, "acme", 7, "order-42")
	s.Require().Error(err)
	s.Require().NotErrorIs(err, ErrCallHeld)
	s.Require().Empty(s.holder(7, "order-42"))

	directory.renewAll(s.ctx)
	s.Equal("acme", s.holder(7, "order-42"))
	release(s.ctx)
	s.Empty(s.holder(7, "order-42"))
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

// writingAs is where directory says a customer's session writing as an agent runs.
func (s *DirectorySuite) writingAs(directory *Directory, customer, agent string) string {
	address, err := directory.NodeByAgent(s.ctx, customer, agent)
	s.Require().NoError(err)
	return address
}

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
