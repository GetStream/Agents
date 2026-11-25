//go:build integration

package node

import (
	"bytes"
	"context"
	"log/slog"
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
	directory.Close()
	for _, session := range []string{"s1", "s2", "s3"} {
		directory.Hold(s.ctx, "acme", 7, session, "agent-"+session, "messaging:"+session)
	}
	directory.Wait(s.ctx, "acme")
	directory.Wait(s.ctx, "globex")
	directory.Offer(s.ctx, "acme", "agent-s1", []byte(`{"tools":[]}`))
	directory.Offer(s.ctx, "globex", "agent-s2", []byte(`{"tools":[]}`))
	keys, err := s.client.Do(s.ctx, s.client.B().Keys().Pattern(s.prefix+":*").Build()).AsStrSlice()
	s.Require().NoError(err)
	// Each session: itself and its conversation, each under its customer and under its id alone,
	// and its agent by app and customer, by customer, and by its id alone.
	s.Require().Len(keys, 3*7+2+2+1)
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
