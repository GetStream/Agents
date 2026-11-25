// Package node reaches the other nodes of a deployment.
//
// A session lives in one process's memory, so an operation on it -- saying something,
// interrupting, reading what models it settled on -- only works on the node running it. A
// load balancer has no reason to prefer that node, so the request lands wherever it
// lands, and the relay beside this package only covers the events socket.
//
// The answer here is to carry the request itself to the node that can answer it: the
// Directory says which node that is, and the Forwarder hands it over by gRPC. The node
// receiving it runs its own handler, so it authenticates and authorizes the caller
// itself; a node is not trusted to have checked.
package node

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/rueidis"
)

// holdFor is how long a node's claim on a session lasts unrenewed. A node that crashes
// leaves its claim behind for at most this long, and during it a request for one of that
// node's sessions is forwarded to something that is not there.
const holdFor = 30 * time.Second

// renewEvery renews well inside that, so a Redis that drops a write has several more
// chances before a session this node is running starts to look unheld.
const renewEvery = 10 * time.Second

// DirectoryOptions configures a Directory.
type DirectoryOptions struct {
	// Redis is required, and is the same client the rest of the deployment uses.
	Redis rueidis.Client
	// Address is required: it is the host and port this node's peers reach it at.
	Address string
	// Shard is the shard this node is in, empty for a deployment of one. A node in another
	// shard is not on this one's relay, so a socket for a session it runs is carried there.
	Shard string
	// Prefix names the keys, so two deployments sharing one Redis do not forward each
	// other's requests.
	Prefix string
	Logger *slog.Logger
}

// Directory is who is running what, as far as Redis knows.
//
// A node writes its claims here and renews them until it lets go, so the entries are a
// statement about now rather than a record: a node that stops saying it runs a session
// stops being asked about it, whether it let go or died.
type Directory struct {
	redis    rueidis.Client
	address  string
	shard    string
	sessions string
	// agents and channels are a session's agent and conversation by their ids alone: a router
	// of the release before reads channels, when a customer's own key is not there, as this one
	// does, and only an older one reads agents.
	agents   string
	channels string
	// writingAs is a claim on an agent by customer alone, which a router of the release before
	// reads, and writingAsIn the same by the Stream app as well, which is what is read here.
	writingAs   string
	writingAsIn string
	answeringIn string
	workers     string
	hosted      string
	shards      string
	inShard     string
	handouts    string
	logger      *slog.Logger
	cancel      context.CancelFunc

	mu sync.Mutex
	// held maps a session this node is running to the Stream app it acts in, the agent it
	// writes as and the conversation it answers in, the last two empty for a session without one.
	held map[heldSession]claim
	// waiting counts the dispatch workers connected here, by customer.
	waiting map[string]int
	// offered is what the workers here host, by customer and agent.
	offered map[offerKey][]byte
}

type offerKey struct{ customerID, agentID string }

// heldSession is a session by its customer and id: two customers may each have a session of
// one id.
type heldSession struct{ customerID, sessionID string }

// claim is what a session this node holds is found by besides its own id, and the Stream app it
// acts in.
type claim struct {
	app                     int64
	agentID, conversationID string
}

// offerEntry is one node's offer as Redis holds it. A hash cannot expire its fields one at
// a time, so each says when it lapses.
type offerEntry struct {
	Until int64           `json:"until"`
	Offer json.RawMessage `json:"offer"`
}

// NewDirectory returns a Directory and starts renewing what it is given to hold.
func NewDirectory(options DirectoryOptions) (*Directory, error) {
	if options.Redis == nil {
		return nil, errors.New("node: a redis client is required")
	}
	if options.Address == "" {
		return nil, errors.New("node: an address this node's peers can reach it at is required")
	}
	prefix := options.Prefix
	if prefix == "" {
		prefix = "node"
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())
	directory := &Directory{
		redis:       options.Redis,
		address:     options.Address,
		shard:       options.Shard,
		sessions:    prefix + ":session:",
		agents:      prefix + ":agent:",
		channels:    prefix + ":channel:",
		writingAs:   prefix + ":writing-as:",
		writingAsIn: prefix + ":writing-as-in:",
		answeringIn: prefix + ":answering-in:",
		workers:     prefix + ":workers:",
		hosted:      prefix + ":hosted:",
		shards:      prefix + ":shard:",
		inShard:     prefix + ":inshard:",
		handouts:    prefix + ":handout:",
		logger:      logger,
		cancel:      cancel,
		held:        map[heldSession]claim{},
		waiting:     map[string]int{},
		offered:     map[offerKey][]byte{},
	}
	directory.sayShard(ctx)
	go directory.renew(ctx)

	return directory, nil
}

// Address is where this node's peers reach it.
func (d *Directory) Address() string { return d.address }

// Shard is the shard this node is in, empty for a deployment of one.
func (d *Directory) Shard() string { return d.shard }

// NodeInShard is where a node of shard can be reached, picked at random among those saying
// they are in it, and empty when none is.
func (d *Directory) NodeInShard(ctx context.Context, shard string) (string, error) {
	now := strconv.FormatInt(time.Now().UnixMilli(), 10)
	addresses, err := d.redis.Do(ctx, d.redis.B().Zrange().Key(d.inShard+shard).
		Min("("+now).Max("+inf").Byscore().Build()).AsStrSlice()
	if err != nil || len(addresses) == 0 {
		return "", err
	}
	return addresses[rand.IntN(len(addresses))], nil
}

// InAnotherShard reports whether the node at an address is in a shard other than this one's.
// A node that has not said is taken to be in this one, which is every node of a deployment of
// one shard.
func (d *Directory) InAnotherShard(ctx context.Context, address string) (bool, error) {
	if d.shard == "" {
		return false, nil
	}
	shard, err := d.lookup(ctx, d.shards+address)
	return shard != "" && shard != d.shard, err
}

// sayShard says which shard this node is in, for holdFor.
func (d *Directory) sayShard(ctx context.Context) {
	for _, result := range d.redis.DoMulti(ctx, d.shardCommands()...) {
		if err := result.Error(); err != nil {
			d.logger.Warn("could not say which shard this node is in", "shard", d.shard, "error", err)
			return
		}
	}
}

// shardCommands say which shard this node is in, by its address and among the shard's nodes,
// none for a node that has not been told. The shard's nodes are a sorted set scored by when
// each one's claim lapses, as a customer's workers are (advertiseCommands).
func (d *Directory) shardCommands() []rueidis.Completed {
	if d.shard == "" {
		return nil
	}
	key := d.inShard + d.shard
	now := time.Now()
	return []rueidis.Completed{
		d.redis.B().Set().Key(d.shards + d.address).Value(d.shard).Ex(holdFor).Build(),
		d.redis.B().Zadd().Key(key).ScoreMember().
			ScoreMember(float64(now.Add(holdFor).UnixMilli()), d.address).Build(),
		d.redis.B().Zremrangebyscore().Key(key).
			Min("-inf").Max(strconv.FormatInt(now.UnixMilli(), 10)).Build(),
		d.redis.B().Pexpire().Key(key).Milliseconds(holdFor.Milliseconds()).Build(),
	}
}

// Hold says this node is running a customer's session in a Stream app, and goes on saying so
// until Release.
//
// The agent id is held beside it because an arriving message names a channel and no
// session at all, so the node that can answer it has to be findable by the agent writing
// there, and by the app, which is the only thing a hook says is whose. The conversation id (the
// channel's cid) is held too, because a person's answer to a call held for their approval names
// the conversation and no session either: the Router's own session in a channel of a type an
// agent config answers in runs on whichever node took the hook.
func (d *Directory) Hold(ctx context.Context, customerID string, app int64, sessionID, agentID, conversationID string) {
	key, held := heldSession{customerID, sessionID}, claim{app, agentID, conversationID}
	d.mu.Lock()
	d.held[key] = held
	d.mu.Unlock()

	d.write(ctx, key, held)
}

// Release says this node is not running a customer's session any more.
//
// Only the session's own claim is dropped. The agent's and the conversation's are left to
// expire, because a second session for the same agent or conversation may have started on
// another node and overwritten them, and deleting that would leave it unreachable rather
// than merely stale.
func (d *Directory) Release(ctx context.Context, customerID, sessionID string) {
	key := heldSession{customerID, sessionID}
	d.mu.Lock()
	_, held := d.held[key]
	delete(d.held, key)
	d.mu.Unlock()
	if !held {
		return
	}

	// One DEL a key: on a Redis Cluster the two keys are in different hash slots, and one DEL over
	// both is refused before it is sent.
	deletes := []rueidis.Completed{
		d.redis.B().Del().Key(d.sessionKey(key)).Build(),
		d.redis.B().Del().Key(d.sessions + sessionID).Build(),
	}
	for _, result := range d.redis.DoMulti(ctx, deletes...) {
		if err := result.Error(); err != nil {
			d.logger.Warn("could not take back this node's claim on a session",
				"app_id", customerID, "session", sessionID, "error", err)
		}
	}
}

// Node is where the node running a customer's session can be reached, and empty when no node
// says it is running one.
//
// A router of the release before says only the id (node:session:<id>), so that key is read
// when the customer's is not there. Ids are still unique across customers in every row that
// release writes, and the node it names answers only a session of the caller's own.
func (d *Directory) Node(ctx context.Context, customerID, sessionID string) (string, error) {
	return d.lookup(ctx, d.sessionKey(heldSession{customerID, sessionID}), d.sessions+sessionID)
}

// NodeByAgent is the same for a customer's session in a Stream app writing as an agent, which is
// what an arriving message has to be answered by. An agent id is any customer's to choose, so
// each customer's is held apart, and one customer starting a session as another's agent does not
// move where the other's messages go. Each app's is held apart too, as a message is answered only
// in the app it was written in.
func (d *Directory) NodeByAgent(ctx context.Context, customerID string, app int64, agentID string) (string, error) {
	entry, err := d.redis.Do(ctx, d.redis.B().Hget().Key(d.writingAsIn+agentID).Field(writerField(customerID, app)).Build()).ToString()
	if rueidis.IsRedisNil(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if address, live := liveClaim(entry, time.Now().UnixMilli()); live {
		return address, nil
	}
	return "", nil
}

// NodesByAgent is where the other nodes running the sessions that write as an agent can be
// reached, by customer, among the sessions admits takes by their customer and Stream app, for a
// message that names no customer. A customer with sessions in two apps it admits is one entry,
// either node. This node is never among them: what runs here is what its own sessions say, and a
// claim of a session that ended here lasts until it lapses.
func (d *Directory) NodesByAgent(ctx context.Context, agentID string, admits func(customer string, app int64) bool) (map[string]string, error) {
	entries, err := d.redis.Do(ctx, d.redis.B().Hgetall().Key(d.writingAsIn+agentID).Build()).AsStrMap()
	if err != nil {
		return nil, err
	}
	now := time.Now().UnixMilli()
	var nodes map[string]string
	for field, entry := range entries {
		customerID, app, ok := parseWriterField(field)
		if !ok || !admits(customerID, app) {
			continue
		}
		address, live := liveClaim(entry, now)
		if !live || address == d.address {
			continue
		}
		if nodes == nil {
			nodes = map[string]string{}
		}
		nodes[customerID] = address
	}
	return nodes, nil
}

// NodeByConversation is the same for a customer's session answering in a conversation, by the
// channel's cid, which is what an answer to a call held for a person's approval names.
//
// A router of the release before says only the cid (node:channel:<cid>), so that key is read when
// the customer's is not there, as Node reads a session's. The node it names answers an approval
// only for a session of the caller's own, and a customer whose session holds the cid on a node of
// this release is found by its own key first.
func (d *Directory) NodeByConversation(ctx context.Context, customerID, conversationID string) (string, error) {
	return d.lookup(ctx, d.answeringKey(customerID, conversationID), d.channels+conversationID)
}

// Wait says this node has a dispatch worker connected for a customer, and goes on saying
// so until the function it returns is called.
//
// A call or a message arrives at whichever node the load balancer picked, and the worker
// that has to answer it may be connected to any other: this is how that node is found.
func (d *Directory) Wait(ctx context.Context, customerID string) func() {
	d.mu.Lock()
	d.waiting[customerID]++
	d.mu.Unlock()
	d.advertise(ctx, customerID)

	return func() {
		d.mu.Lock()
		d.waiting[customerID]--
		last := d.waiting[customerID] == 0
		if last {
			delete(d.waiting, customerID)
		}
		d.mu.Unlock()
		if !last {
			return
		}

		// The worker has gone, whether or not the caller's request is still running.
		ctx := context.WithoutCancel(ctx)
		command := d.redis.B().Zrem().Key(d.workers + customerID).Member(d.address).Build()
		if err := d.redis.Do(ctx, command).Error(); err != nil {
			d.logger.Warn("could not take back this node's claim on a customer's workers",
				"app_id", customerID, "error", err)
		}
	}
}

// Waiting is where the other nodes with a dispatch worker connected for a customer can be
// reached. This node is never among them.
func (d *Directory) Waiting(ctx context.Context, customerID string) ([]string, error) {
	now := strconv.FormatInt(time.Now().UnixMilli(), 10)
	addresses, err := d.redis.Do(ctx, d.redis.B().Zrange().Key(d.workers+customerID).
		Min("("+now).Max("+inf").Byscore().Build()).AsStrSlice()
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(addresses, func(address string) bool { return address == d.address }), nil
}

// Offer says what the workers here host for sessions under an agent, a JSON document the
// directory carries without reading, and goes on saying so until it is offered again. A nil
// offer withdraws it.
func (d *Directory) Offer(ctx context.Context, customerID, agentID string, offer []byte) {
	key := offerKey{customerID, agentID}
	d.mu.Lock()
	if offer == nil {
		delete(d.offered, key)
	} else {
		d.offered[key] = offer
	}
	d.mu.Unlock()

	if offer != nil {
		d.publishOffer(ctx, key, offer)
		return
	}
	command := d.redis.B().Hdel().Key(d.offerKey(key)).Field(d.address).Build()
	if err := d.redis.Do(context.WithoutCancel(ctx), command).Error(); err != nil {
		d.logger.Warn("could not withdraw the tools this node's workers hosted",
			"app_id", customerID, "agent", agentID, "error", err)
	}
}

// Offers is what the other nodes' workers host for sessions under an agent, by where those
// nodes can be reached.
func (d *Directory) Offers(ctx context.Context, customerID, agentID string) (map[string][]byte, error) {
	entries, err := d.redis.Do(ctx, d.redis.B().Hgetall().Key(d.offerKey(offerKey{customerID, agentID})).Build()).AsStrMap()
	if err != nil {
		return nil, err
	}
	now := time.Now().UnixMilli()
	offers := map[string][]byte{}
	for address, raw := range entries {
		var entry offerEntry
		if address == d.address || json.Unmarshal([]byte(raw), &entry) != nil || entry.Until <= now {
			continue
		}
		offers[address] = entry.Offer
	}
	return offers, nil
}

// HandOut says a message written in a customer's channel cid was handed to one of its workers,
// as entry, a JSON document the directory carries without reading, for as long as lasts. The
// worker's session may be opened on any node, which reads it there (HandedOut).
func (d *Directory) HandOut(ctx context.Context, customerID, cid string, entry []byte, lasts time.Duration) {
	command := d.redis.B().Set().Key(d.handouts + customerID + ":" + cid).Value(string(entry)).Px(lasts).Build()
	if err := d.redis.Do(ctx, command).Error(); err != nil {
		d.logger.Warn("could not say a channel's message was handed to a worker",
			"app_id", customerID, "channel", cid, "error", err)
	}
}

// HandedOut is what HandOut said of a customer's channel, on any node, and nil once it lapsed
// or when nothing was.
func (d *Directory) HandedOut(ctx context.Context, customerID, cid string) ([]byte, error) {
	entry, err := d.redis.Do(ctx, d.redis.B().Get().Key(d.handouts+customerID+":"+cid).Build()).AsBytes()
	if rueidis.IsRedisNil(err) {
		return nil, nil
	}
	return entry, err
}

// Close stops renewing. What this node held expires rather than being given up, because
// a process on its way out has a shutdown grace period to finish the calls it is carrying
// and is still the only node that can answer for them. New work for its shard is another
// matter: the node leaves the shard's nodes at once (NodeInShard).
func (d *Directory) Close() {
	d.cancel()
	if d.shard == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := d.redis.Do(ctx, d.redis.B().Zrem().Key(d.inShard+d.shard).Member(d.address).Build()).Error(); err != nil {
		d.logger.Warn("could not say this node left its shard", "shard", d.shard, "error", err)
	}
}

// lookup is the first of keys that is set, in one round trip, and empty when none is.
func (d *Directory) lookup(ctx context.Context, keys ...string) (string, error) {
	gets := make([]rueidis.Completed, 0, len(keys))
	for _, key := range keys {
		gets = append(gets, d.redis.B().Get().Key(key).Build())
	}
	for _, result := range d.redis.DoMulti(ctx, gets...) {
		address, err := result.ToString()
		if rueidis.IsRedisNil(err) {
			continue
		}
		return address, err
	}
	return "", nil
}

// sessionKey is where a customer's session is held: node:session:<customer>:<id>. A session id
// holds no colon, so no customer and id make the key of another.
func (d *Directory) sessionKey(key heldSession) string {
	return d.sessions + key.customerID + ":" + key.sessionID
}

// writerField is the field of a customer's claim on an agent in a Stream app: <app>:<customer>. An
// app id holds no colon, so the first one ends it.
func writerField(customerID string, app int64) string {
	return strconv.FormatInt(app, 10) + ":" + customerID
}

// parseWriterField is the customer and app of a writerField, false for a field that is not one.
func parseWriterField(field string) (string, int64, bool) {
	prefix, customerID, found := strings.Cut(field, ":")
	app, err := strconv.ParseInt(prefix, 10, 64)
	return customerID, app, found && err == nil
}

// liveClaim is the address a customer's claim on an agent names, as writeCommands writes it, and
// false once it lapsed: when, then the address.
func liveClaim(entry string, now int64) (string, bool) {
	until, address, _ := strings.Cut(entry, " ")
	lapses, err := strconv.ParseInt(until, 10, 64)
	return address, err == nil && lapses > now
}

// answeringKey is where a customer's session answering in a conversation is held:
// node:answering-in:<customer>:<cid>. A cid holds a colon and a customer id may, so the customer
// is escaped, and no customer and cid make the key of another.
func (d *Directory) answeringKey(customerID, conversationID string) string {
	return d.answeringIn + url.QueryEscape(customerID) + ":" + conversationID
}

// write says this node holds a session, for holdFor.
func (d *Directory) write(ctx context.Context, key heldSession, held claim) {
	for _, result := range d.redis.DoMulti(ctx, d.writeCommands(key, held)...) {
		if err := result.Error(); err != nil {
			d.logger.Warn("could not say this node is running a session",
				"app_id", key.customerID, "session", key.sessionID, "error", err)
			return
		}
	}
}

// writeCommands hold the session, its agent and its conversation under its customer, and under
// their ids alone as well, which is all an older router reads.
//
// The agent is one hash for every customer and Stream app writing as it, a field each, because a
// message in the deployment's app may name no customer, and the node answering it is then picked
// from all of theirs in that app. A hash cannot expire its fields one at a time, so each says when
// it lapses, before the address. The hash by customer alone is the one a router of the release
// before reads, and the agent by its id alone a router older than that.
func (d *Directory) writeCommands(key heldSession, held claim) []rueidis.Completed {
	commands := []rueidis.Completed{
		d.redis.B().Set().Key(d.sessionKey(key)).Value(d.address).Ex(holdFor).Build(),
		d.redis.B().Set().Key(d.sessions + key.sessionID).Value(d.address).Ex(holdFor).Build(),
	}
	if held.agentID != "" {
		entry := strconv.FormatInt(time.Now().Add(holdFor).UnixMilli(), 10) + " " + d.address
		writers, byCustomer := d.writingAsIn+held.agentID, d.writingAs+held.agentID
		commands = append(commands,
			d.redis.B().Hset().Key(writers).FieldValue().FieldValue(writerField(key.customerID, held.app), entry).Build(),
			d.redis.B().Pexpire().Key(writers).Milliseconds(holdFor.Milliseconds()).Build(),
			d.redis.B().Hset().Key(byCustomer).FieldValue().FieldValue(key.customerID, entry).Build(),
			d.redis.B().Pexpire().Key(byCustomer).Milliseconds(holdFor.Milliseconds()).Build(),
			d.redis.B().Set().Key(d.agents+held.agentID).Value(d.address).Ex(holdFor).Build())
	}
	if held.conversationID != "" {
		commands = append(commands,
			d.redis.B().Set().Key(d.answeringKey(key.customerID, held.conversationID)).Value(d.address).Ex(holdFor).Build(),
			d.redis.B().Set().Key(d.channels+held.conversationID).Value(d.address).Ex(holdFor).Build())
	}
	return commands
}

// advertise says this node has workers for a customer, for holdFor.
//
// One sorted set per customer, scored by when each node's claim lapses, because a set
// cannot expire its members one at a time: a node that died is read past by its score and
// swept out by the next node to renew.
func (d *Directory) advertise(ctx context.Context, customerID string) {
	for _, result := range d.redis.DoMulti(ctx, d.advertiseCommands(customerID)...) {
		if err := result.Error(); err != nil {
			d.logger.Warn("could not say this node has workers for a customer",
				"app_id", customerID, "error", err)
			return
		}
	}
}

func (d *Directory) advertiseCommands(customerID string) []rueidis.Completed {
	key := d.workers + customerID
	now := time.Now()
	return []rueidis.Completed{
		d.redis.B().Zadd().Key(key).ScoreMember().
			ScoreMember(float64(now.Add(holdFor).UnixMilli()), d.address).Build(),
		d.redis.B().Zremrangebyscore().Key(key).
			Min("-inf").Max(strconv.FormatInt(now.UnixMilli(), 10)).Build(),
		d.redis.B().Pexpire().Key(key).Milliseconds(holdFor.Milliseconds()).Build(),
	}
}

// publishOffer says what the workers here host for an agent, for holdFor.
func (d *Directory) publishOffer(ctx context.Context, key offerKey, offer []byte) {
	commands, err := d.offerCommands(key, offer)
	if err != nil {
		d.logger.Warn("could not say what this node's workers host", "agent", key.agentID, "error", err)
		return
	}
	for _, result := range d.redis.DoMulti(ctx, commands...) {
		if err := result.Error(); err != nil {
			d.logger.Warn("could not say what this node's workers host",
				"app_id", key.customerID, "agent", key.agentID, "error", err)
			return
		}
	}
}

func (d *Directory) offerCommands(key offerKey, offer []byte) ([]rueidis.Completed, error) {
	entry, err := json.Marshal(offerEntry{Until: time.Now().Add(holdFor).UnixMilli(), Offer: offer})
	if err != nil {
		return nil, err
	}
	hash := d.offerKey(key)
	return []rueidis.Completed{
		d.redis.B().Hset().Key(hash).FieldValue().FieldValue(d.address, string(entry)).Build(),
		d.redis.B().Pexpire().Key(hash).Milliseconds(holdFor.Milliseconds()).Build(),
	}, nil
}

func (d *Directory) offerKey(key offerKey) string {
	return d.hosted + key.customerID + ":" + key.agentID
}

// requestsFor is the channel the node at an address takes work for its workers on.
func (d *Directory) requestsFor(address string) string {
	return d.hosted + "requests:" + address
}

// renew keeps every claim this node holds alive until the process stops.
func (d *Directory) renew(ctx context.Context) {
	ticker := time.NewTicker(renewEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		d.renewAll(ctx)
	}
}

// renewAll says again everything this node holds, in one round trip however much that is.
func (d *Directory) renewAll(ctx context.Context) {
	d.mu.Lock()
	held := maps.Clone(d.held)
	waiting := maps.Clone(d.waiting)
	offered := maps.Clone(d.offered)
	d.mu.Unlock()

	commands := d.shardCommands()
	for key, claimed := range held {
		commands = append(commands, d.writeCommands(key, claimed)...)
	}
	for customerID := range waiting {
		commands = append(commands, d.advertiseCommands(customerID)...)
	}
	for key, offer := range offered {
		offers, err := d.offerCommands(key, offer)
		if err != nil {
			d.logger.Warn("could not say what this node's workers host", "agent", key.agentID, "error", err)
			continue
		}
		commands = append(commands, offers...)
	}
	if len(commands) == 0 {
		return
	}
	for _, result := range d.redis.DoMulti(ctx, commands...) {
		if err := result.Error(); err != nil {
			d.logger.Warn("could not renew what this node holds", "error", err)
			return
		}
	}
}
