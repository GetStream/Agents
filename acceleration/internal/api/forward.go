package api

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/http"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/dispatch"
	"github.com/GetStream/Vision-Agents/acceleration/internal/node"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// forwardTimeout bounds one request carried to the node that can answer it. Generous,
// because what is carried is an ordinary API call and the node answering it applies
// whatever timeout that call already had; short enough that a node which has stopped
// answering does not hold a caller indefinitely.
const forwardTimeout = 30 * time.Second

// shardHeader names the shard (the region) that answered a list read from one shard alone,
// such as the agent logs.
const shardHeader = "X-Shard"

// unreachableNode is what a caller is told when the session exists but the node running
// it does not answer, which is a different thing from a session that is not there.
const unreachableNode = "the node running this session could not be reached"

// onOwningNode carries a request about a session to the node running it, and an answer to a
// call held for a person's approval in a conversation (answerToolApproval) to the node running
// the session that holds it.
//
// A session lives in one process's memory and a load balancer has no reason to prefer
// that process, so without this a request lands on a node that cannot answer it and is
// told the session does not exist.
//
// Inside withCustomer, so a session is found by the caller's customer and its id: two
// customers may each have a session of one id. The node that answers authenticates the caller
// again, as it does every request it serves, and is the only one to log it or count it against
// a quota.
func (s *Server) onOwningNode(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.inPolicyShard(w, r) {
			return
		}
		address, elsewhere, failure := s.owningNode(r)
		if failure != nil {
			var refused APIError
			if !errors.As(failure, &refused) {
				s.logger.Error("could not find out which shard a session lives in", "error", failure)
				refused = internalError()
			}
			writeError(w, refused)
			return
		}
		if !elsewhere {
			next.ServeHTTP(w, r)
			return
		}
		// A stream, like a socket, cannot be carried over a call that answers once.
		if r.Header.Get("Upgrade") != "" || r.URL.Path == "/v1/agents/logs/stream" {
			s.proxySocket(w, r, address)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), forwardTimeout)
		defer cancel()
		if err := s.forwarder.Forward(ctx, address, w, r); err != nil {
			s.logger.Error("could not reach the node running a session",
				"node", address, "path", loggedPath(s.mux, r), "error", err)
			writeError(w, unavailable(unreachableNode))
		}
	})
}

// owningNode is where to send a request this node cannot answer, and false when this node
// is the one to answer it. It fails when the session lives in a shard no node of which answers:
// its rows are there and nowhere else.
func (s *Server) owningNode(r *http.Request) (string, bool, error) {
	if s.directory == nil || s.forwarder == nil {
		return "", false, nil
	}
	// A request a peer handed over is answered here. Two nodes each believing the other
	// holds the session would otherwise pass it back and forth.
	if r.Header.Get(node.ForwardedHeader) != "" {
		return "", false, nil
	}
	address, ok, failure := s.nodeFor(r)
	if !ok || failure != nil {
		return "", false, failure
	}
	// Nothing says it is running and it lives in this shard: the session ended, or never
	// existed, and what reads the row is as able to answer here as anywhere in the shard.
	if address == "" || address == s.directory.Address() {
		return "", false, nil
	}
	// A socket cannot be carried over a call that answers once, and within a shard does not
	// need to be: the relay reaches a session's events wherever the socket lands. The relay
	// is the shard's own, so a socket for a session another shard runs is proxied there.
	if r.Header.Get("Upgrade") != "" {
		other, err := s.directory.InAnotherShard(r.Context(), address)
		if err != nil {
			s.logger.Warn("could not find out which shard a node is in", "node", address, "error", err)
			return "", false, nil
		}
		return address, other, nil
	}
	return address, true, nil
}

// nodeFor is the node the directory says runs the session a request is about, or a node of
// the shard it lives in when nothing runs it, false when the request is about none, names no
// customer, or this node runs it. A request that names no customer is answered here, where it
// is refused.
func (s *Server) nodeFor(r *http.Request) (string, bool, error) {
	customer, ok := CustomerFrom(r.Context())
	if !ok {
		return "", false, nil
	}
	if cid, ok := approvalInPath(r.URL.Path); ok {
		if s.sessions != nil {
			// Another customer's session in a conversation of the same cid is no reason to answer here.
			if _, running := s.sessions.ByConversationWhere(cid, func(owner string, _ int64) bool { return owner == customer }); running {
				return "", false, nil
			}
		}
		address, err := s.directory.NodeByConversation(r.Context(), customer, cid)
		if err != nil {
			s.logger.Warn("could not find out which node is answering in a conversation",
				"customer", customer, "conversation", cid, "error", err)
			return "", false, nil
		}
		return address, true, nil
	}
	if call, ok := callInPath(r.URL.Path); ok {
		return s.callNode(r.Context(), customer, call)
	}
	if shardRowsInPath(r.URL.Path) {
		return s.regionNode(r, customer)
	}
	id, ok := sessionInPath(r.URL.Path)
	if !ok {
		return "", false, nil
	}
	if s.sessions != nil && s.sessions.Running(customer, id) {
		return "", false, nil
	}

	address, err := s.directory.Node(r.Context(), customer, id)
	if err != nil {
		s.logger.Warn("could not find out which node is running a session",
			"customer", customer, "session", id, "error", err)
		return "", false, nil
	}
	if address == "" {
		return s.homeNode(r.Context(), customer, id)
	}
	return address, true, nil
}

// errHomeShardDown is a session whose shard has no node answering. Its rows are there alone,
// so no other shard can read it, carry it on or answer in it.
var errHomeShardDown = APIError{Type: ErrorTypeUnavailable, Code: codeShardUnavailable,
	Message: "the region this session lives in could not be reached"}

// errCallShardDown is a call whose shard has no node answering: its rows are there alone.
var errCallShardDown = APIError{Type: ErrorTypeUnavailable, Code: codeShardUnavailable,
	Message: "the region this call lives in could not be reached"}

// homeNode is a node of the shard a session nothing runs lives in, when that is another shard:
// it is read and carried on there, where its rows are.
func (s *Server) homeNode(ctx context.Context, customer, id string) (string, bool, error) {
	if s.store == nil {
		return "", false, nil
	}
	home, err := s.store.SessionHomes.Elsewhere(ctx, customer, id)
	if err != nil || home == "" {
		return "", false, err
	}
	address, err := s.directory.NodeInShard(ctx, home)
	if err != nil {
		return "", false, err
	}
	if address == "" {
		return "", false, errHomeShardDown
	}
	return address, true, nil
}

// answeringShard is the shard a read of rows one shard keeps is answered from, for shardHeader,
// empty on a deployment of one.
func (s *Server) answeringShard() string {
	if s.store == nil || !s.store.Indexed() {
		return ""
	}
	return s.store.ShardName()
}

// errUnknownRegion is a read of one shard's rows asking for a region no shard is named.
var errUnknownRegion = invalidRequest("region names no region of this deployment")

// errRegionDown is a read of one shard's rows asking for a region no node of which answers.
var errRegionDown = APIError{Type: ErrorTypeUnavailable, Code: codeShardUnavailable,
	Message: "the region asked for could not be reached"}

// regionNode is a node of the shard a read of one shard's rows is for, when that is another
// shard: the home of the session the logs are filtered to, or else the region asked for.
func (s *Server) regionNode(r *http.Request, customer string) (string, bool, error) {
	query := r.URL.Query()
	if id := query.Get("session_id"); id != "" && strings.HasPrefix(r.URL.Path, "/v1/agents/logs") {
		return s.homeNode(r.Context(), customer, id)
	}
	region := query.Get("region")
	if region == "" || region == s.directory.Shard() {
		return "", false, nil
	}
	if !slices.Contains(s.shardNames, region) {
		return "", false, errUnknownRegion
	}
	address, err := s.directory.NodeInShard(r.Context(), region)
	if err != nil {
		return "", false, err
	}
	if address == "" {
		return "", false, errRegionDown
	}
	return address, true, nil
}

// callNode is a node of the shard a call lives in, when that is another shard: its row, and the
// requests and turns it made, are there alone.
func (s *Server) callNode(ctx context.Context, customer, id string) (string, bool, error) {
	if s.store == nil {
		return "", false, nil
	}
	home, err := s.store.Calls.Elsewhere(ctx, customer, id)
	if err != nil || home == "" {
		return "", false, err
	}
	address, err := s.directory.NodeInShard(ctx, home)
	if err != nil {
		return "", false, err
	}
	if address == "" {
		return "", false, errCallShardDown
	}
	return address, true, nil
}

// proxySocket carries a socket to the node running its session, upgrade and all, for as long
// as it stays open. The node at the other end answers it as one that landed there.
func (s *Server) proxySocket(w http.ResponseWriter, r *http.Request, address string) {
	if err := carrySocket(w, r, address); err != nil {
		s.logger.Error("could not reach the node running a session",
			"node", address, "path", loggedPath(s.mux, r), "error", err)
		writeError(w, unavailable(unreachableNode))
	}
}

// carrySocket is proxySocket answering nothing on w when the node at address could not be
// reached, which it returns.
func carrySocket(w http.ResponseWriter, r *http.Request, address string) error {
	var failed error
	proxy := &httputil.ReverseProxy{
		Rewrite: func(out *httputil.ProxyRequest) {
			out.SetURL(&url.URL{Scheme: "http", Host: address})
			out.Out.Host = r.Host
			out.Out.Header.Set(node.ForwardedHeader, "1")
		},
		ErrorHandler: func(_ http.ResponseWriter, _ *http.Request, err error) { failed = err },
	}
	proxy.ServeHTTP(w, r)
	return failed
}

// sessionInPath returns the session a path is about, if it is about one. The collection path
// /v1/agents/sessions/query is about none: looking it up as an id would cost every list a trip
// to the shared Redis and database that finds nobody.
func sessionInPath(path string) (string, bool) {
	rest, ok := strings.CutPrefix(path, "/v1/agents/sessions/")
	if !ok {
		return "", false
	}
	id, _, _ := strings.Cut(rest, "/")

	return id, id != "" && id != "query"
}

// callInPath returns the call a path is about, if it is about one (/v1/agents/calls/{id}...),
// and not for the collection under /v1/agents/calls/reviews.
func callInPath(path string) (string, bool) {
	rest, ok := strings.CutPrefix(path, "/v1/agents/calls/")
	if !ok {
		return "", false
	}
	id, _, _ := strings.Cut(rest, "/")

	return id, id != "" && id != "reviews"
}

// shardRowsInPath reports whether a path reads rows each shard keeps of its own and ships
// nowhere: the agent logs and a connection's invocations.
func shardRowsInPath(path string) bool {
	if path == "/v1/agents/logs" || strings.HasPrefix(path, "/v1/agents/logs/") {
		return true
	}
	rest, ok := strings.CutPrefix(path, "/v1/agents/connections/")
	if !ok {
		return false
	}
	id, ok := strings.CutSuffix(rest, "/invocations")

	return ok && id != "" && !strings.Contains(id, "/")
}

// approvalInPath returns the conversation an answer to a held call is about, if the path is
// one (POST /v1/agents/conversations/{cid}/tool_approvals).
func approvalInPath(path string) (string, bool) {
	rest, ok := strings.CutPrefix(path, "/v1/agents/conversations/")
	if !ok {
		return "", false
	}
	cid, ok := strings.CutSuffix(rest, "/tool_approvals")

	return cid, ok && cid != "" && !strings.Contains(cid, "/")
}

// forwardedMessage hands an arriving message to the node running the conversation on its
// channel, reporting whether this node is not the one to answer it.
//
// Without it an agent answering on another node is invisible from here and the message
// goes to a worker instead, which is a second agent writing into a conversation the first
// is already answering in. So the directory decides, not the handover: a node that holds
// the conversation and cannot be reached is still the only one that may answer. taken is
// false when that node could not be reached or would not take the message. A channel sessions
// of more than one customer write in, and that nothing says is one customer's, is answered by
// none of them, as liveSessionOn answers one here: elsewhere and taken.
func (s *Server) forwardedMessage(ctx context.Context, origin hookOrigin, event messageEvent) (elsewhere, taken bool) {
	if s.directory == nil || s.forwarder == nil || origin.handOver == nil {
		return false, false
	}

	address, err := s.answeringNode(ctx, origin, event)
	if errors.Is(err, store.ErrAmbiguousHook) {
		s.logger.Warn("not answering a message in a channel more than one customer's sessions run on",
			"channel", event.ChannelID, "stream_app", origin.app, "error", err)
		return true, true
	}
	if err != nil {
		s.logger.Warn("could not find out which node is answering in a channel",
			"channel", event.ChannelID, "error", err)
		return false, false
	}
	if address == "" || address == s.directory.Address() {
		return false, false
	}

	ctx, cancel := context.WithTimeout(ctx, forwardTimeout)
	defer cancel()
	answer, err := origin.handOver(ctx, address)
	switch {
	case err != nil:
		s.logger.Error("could not reach the node answering in a channel",
			"node", address, "channel", event.ChannelID, "error", err)
	case answer.GetStatus() != http.StatusOK:
		s.logger.Warn("the node answering in a channel would not take a message",
			"node", address, "channel", event.ChannelID, "status", answer.GetStatus())
	default:
		return true, true
	}

	return true, false
}

// answeringNode is the node running the session that answers a message, among the sessions in
// the hook's app (origin.owns), by customer as liveSessionOn picks one here: the hook's
// customer's, and in the deployment's app the customer's the channel names or else the store
// does. While neither does, it is the only customer's writing as the agent, and
// store.ErrAmbiguousHook when there are more.
func (s *Server) answeringNode(ctx context.Context, origin hookOrigin, event messageEvent) (string, error) {
	if !origin.deployment {
		return s.directory.NodeByAgent(ctx, origin.customer, origin.app, event.ChannelID)
	}
	nodes, err := s.directory.NodesByAgent(ctx, event.ChannelID, origin.owns)
	if err != nil || len(nodes) == 0 {
		return "", err
	}
	if owner, stamped := event.ChannelCustom[conversation.CustomerField]; stamped {
		customer, _ := owner.(string)
		return nodes[customer], nil
	}
	owner, known, err := s.storedOwnerOf(ctx, origin, event)
	if err != nil {
		return "", err
	}
	if known {
		return nodes[owner], nil
	}
	if len(nodes) > 1 {
		return "", store.ErrAmbiguousHook
	}
	for _, address := range nodes {
		return address, nil
	}
	return "", nil
}

// forwardedToWorkers hands a hook to another node with a worker waiting for the customer,
// reporting whether this node is not the one to answer it.
//
// A worker waits on whichever node its socket landed on, and Stream's hook lands wherever
// the load balancer sends it, so without this a call rings out on a node with nobody
// connected while a worker sits idle on the next one. A worker connected here that takes the
// hook's kind of work is always asked first, which costs nothing and is most of the time; one
// that takes other work, such as a process only hosting tools, is passed over.
//
// Handed over with handOver, as forwardedMessage hands a message over; nil hands nothing over.
// taken is false when that node could not be reached or would not take the hook.
func (s *Server) forwardedToWorkers(ctx context.Context, handOver func(context.Context, string) (*node.ForwardResponse, error), customerID string, kind dispatch.Kind) (elsewhere, taken bool) {
	if s.directory == nil || s.forwarder == nil || handOver == nil {
		return false, false
	}
	if s.dispatch.Takes(customerID, kind) {
		return false, false
	}

	addresses, err := s.directory.Waiting(ctx, customerID)
	if err != nil {
		s.logger.Warn("could not find out which nodes have a customer's workers",
			"customer", customerID, "error", err)
		return false, false
	}
	if len(addresses) == 0 {
		return false, false
	}
	address := addresses[rand.IntN(len(addresses))]

	ctx, cancel := context.WithTimeout(ctx, forwardTimeout)
	defer cancel()
	answer, err := handOver(ctx, address)
	switch {
	case err != nil:
		s.logger.Error("could not reach the node with a customer's workers",
			"node", address, "customer", customerID, "error", err)
	case answer.GetStatus() != http.StatusOK:
		s.logger.Warn("the node with a customer's workers would not take a hook",
			"node", address, "customer", customerID, "status", answer.GetStatus())
	default:
		return true, true
	}

	return true, false
}
