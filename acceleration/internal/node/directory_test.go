//go:build integration

package node

import (
	"context"
	"os"
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
		directory.Hold(s.ctx, "acme", session, 0, "agent-"+session, "messaging:"+session)
	}
	directory.Wait(s.ctx, "acme")
	directory.Wait(s.ctx, "globex")
	directory.Offer(s.ctx, "acme", "agent-s1", []byte(`{"tools":[]}`))
	directory.Offer(s.ctx, "globex", "agent-s2", []byte(`{"tools":[]}`))
	keys, err := s.client.Do(s.ctx, s.client.B().Keys().Pattern(s.prefix+":*").Build()).AsStrSlice()
	s.Require().NoError(err)
	s.Require().Len(keys, 3*5+2+2+1)
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
	first.Hold(s.ctx, "acme", "order-42", 0, "", "")
	second.Hold(s.ctx, "globex", "order-42", 0, "", "")

	peer := s.directoryAt("node-c:1")
	s.Equal("node-a:1", s.nodeOf(peer, "acme", "order-42"))
	s.Equal("node-b:1", s.nodeOf(peer, "globex", "order-42"))
}

// A session held under its id alone, as a router of two releases before held it, says nothing
// of whose it is, and another customer's of the id may run anywhere: it is not found.
func (s *DirectorySuite) TestASessionHeldUnderItsIdAloneIsNotFound() {
	s.Require().NoError(s.client.Do(s.ctx, s.client.B().Set().Key(s.prefix+":session:order-42").Value("node-a:1").Build()).Error())

	s.Empty(s.nodeOf(s.directoryAt("node-c:1"), "acme", "order-42"))
}

// A session is held under its customer and id alone: a router of the release before reads the
// key of the id when its customer's is not there, and would carry another customer's request
// for the id to this node.
func (s *DirectorySuite) TestASessionIsNotHeldUnderItsIdAlone() {
	s.directoryAt("node-a:1").Hold(s.ctx, "acme", "order-42", 0, "", "")

	exists, err := s.client.Do(s.ctx, s.client.B().Exists().Key(s.prefix+":session:order-42").Build()).AsInt64()
	s.Require().NoError(err)
	s.Zero(exists)
	s.Equal("node-a:1", s.nodeOf(s.directoryAt("node-c:1"), "acme", "order-42"))
}

// Two customers' sessions of one id in Stream apps of their own each write as the agent of the
// id and answer in the conversation of the id, on nodes of their own: each is found by its app,
// or by its customer, whichever node said so last.
func (s *DirectorySuite) TestTwoCustomersAgentsAndConversationsOfOneNameAreFoundApart() {
	s.directoryAt("node-a:1").Hold(s.ctx, "acme", "order-42", 111, "order-42", "agent:order-42")
	s.directoryAt("node-b:1").Hold(s.ctx, "globex", "order-42", 222, "order-42", "agent:order-42")

	peer := s.directoryAt("node-c:1")
	for app, address := range map[int64]string{111: "node-a:1", 222: "node-b:1"} {
		held, err := peer.NodeByAgent(s.ctx, app, "order-42")
		s.Require().NoError(err)
		s.Equal(address, held, "the agent in app %d", app)
	}
	for customer, address := range map[string]string{"acme": "node-a:1", "globex": "node-b:1"} {
		held, err := peer.NodeByConversation(s.ctx, customer, "agent:order-42")
		s.Require().NoError(err)
		s.Equal(address, held, "%s's conversation", customer)
	}
}

// A router of the release before holds an agent and a conversation by their names alone, and
// reads them so: they are held so as well, and what it held is found.
func (s *DirectorySuite) TestAnAgentAndAConversationAreHeldByTheirNamesAloneForTheReleaseBefore() {
	s.directoryAt("node-a:1").Hold(s.ctx, "acme", "order-42", 111, "order-42", "agent:order-42")
	for _, key := range []string{":agent:order-42", ":channel:agent:order-42"} {
		held, err := s.client.Do(s.ctx, s.client.B().Get().Key(s.prefix+key).Build()).ToString()
		s.Require().NoError(err)
		s.Equal("node-a:1", held, key)
	}

	for key, address := range map[string]string{":agent:support": "node-b:1", ":channel:agent:support": "node-b:1"} {
		s.Require().NoError(s.client.Do(s.ctx, s.client.B().Set().Key(s.prefix+key).Value(address).Build()).Error())
	}
	peer := s.directoryAt("node-c:1")
	held, err := peer.NodeByAgent(s.ctx, 111, "support")
	s.Require().NoError(err)
	s.Equal("node-b:1", held)
	held, err = peer.NodeByConversation(s.ctx, "acme", "agent:support")
	s.Require().NoError(err)
	s.Equal("node-b:1", held)
}

func (s *DirectorySuite) TestAReleasedSessionIsFoundNowhere() {
	directory := s.directoryAt("node-a:1")
	directory.Hold(s.ctx, "acme", "order-42", 0, "", "")

	directory.Release(s.ctx, "acme", "order-42")

	s.Empty(s.nodeOf(s.directoryAt("node-c:1"), "acme", "order-42"))
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
