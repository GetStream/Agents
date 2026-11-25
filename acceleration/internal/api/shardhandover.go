package api

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GetStream/Vision-Agents/acceleration/internal/node"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// The kinds of work handed to another shard, the work label of shardFallbacks.
const (
	shardWorkCall    = "call"
	shardWorkMessage = "message"
	// shardWorkRequest is a request that starts new work, such as a session (newWorkInPath).
	shardWorkRequest = "request"
)

// shardFallbacks counts by the shards ShardRouting names, so its series are bounded by config.
var shardFallbacks = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "router",
	Name:      "shard_handover_fallbacks_total",
	Help: "Work meant for another shard that was done on the router it landed on, because no node of that " +
		"shard was up or reachable, by the shard meant and the work: call, message or request (api handedToShard, inPolicyShard)",
}, []string{"shard", "work"})

// errResidencyDown is work kept in a geography no node of whose shards took it. It is done
// nowhere else, so no shard outside the geography holds its data.
var errResidencyDown = APIError{Type: ErrorTypeUnavailable, Code: codeShardUnavailable,
	Message: "the region this agent's work is kept in could not be reached"}

// errNoResidencyShard is work kept in a geography this deployment runs no shard in.
var errNoResidencyShard = APIError{Type: ErrorTypeUnavailable, Code: codeShardUnavailable,
	Message: "this deployment runs no region this agent's work may be kept in"}

// errPolicyUnread is work whose routing policy could not be read: it may be kept in a geography,
// so it is not done anywhere until the policy is known.
var errPolicyUnread = unavailable("could not read which regions this agent's work may be done in")

// errSocketElsewhere is a socket session whose agent's work is kept in a geography this shard is
// outside. The socket is open here by then, and cannot be carried to another shard.
var errSocketElsewhere = APIError{Type: ErrorTypeUnavailable, Code: codeShardUnavailable,
	Message: "this agent's work is kept in another region: open the socket in that region"}

// newWorkBodyLimit is huma's default MaxBodyBytes, which the operations newWorkInPath names keep:
// a larger body is refused by the operation itself.
const newWorkBodyLimit = 1 << 20

// placement is where a routing policy has new work done: the shards to try, in order, none for
// this node's; and strict, for work none of them takes that is failed rather than done here.
type placement struct {
	shards []string
	strict bool
}

// placed is where new work for customerID under configID is done (shardmap.Map.Place), given
// closest, the shard today's rules pick for it, empty for this node's. The policy is read only
// when shards are routed, so a deployment of one reads none. A config nobody holds any more
// follows its app's policy. refused is work that is done nowhere, answered with failure.
func (s *Server) placed(ctx context.Context, customerID, configID, closest string) (place placement, failure APIError, refused bool) {
	if s.store == nil || !s.routesByShard(ctx) {
		return placement{}, APIError{}, false
	}
	policy, err := s.store.Apps.RoutingPolicy(ctx, customerID, configID)
	if errors.Is(err, store.ErrNoAgentConfig) {
		policy, err = s.store.Apps.RoutingPolicy(ctx, customerID, "")
	}
	if err != nil {
		s.logger.Error("could not read the routing policy of new work", "customer", customerID, "config", configID, "error", err)
		return placement{}, errPolicyUnread, true
	}
	here := s.directory.Shard()
	shard, err := s.shards.Place(policy, cmp.Or(closest, here))
	if err != nil {
		s.logger.Error("new work is kept in a geography no shard of this deployment is in",
			"customer", customerID, "config", configID, "policy", policy, "error", err)
		return placement{}, errNoResidencyShard, true
	}
	if shard == here {
		return placement{}, APIError{}, false
	}
	shards := []string{shard}
	if policy.Strict() {
		for _, other := range s.shards.Allowed(policy) {
			if other != shard {
				shards = append(shards, other)
			}
		}
	}
	return placement{shards: shards, strict: policy.Strict()}, APIError{}, false
}

// newWorkInPath reports whether a request starts work that nothing holds yet: a session, over
// HTTP or a socket, a run or a placed call.
func newWorkInPath(r *http.Request) bool {
	switch r.Method + " " + r.URL.Path {
	case "POST /v1/agents/sessions", "GET /v1/agents/socket", "POST /v1/agents/runs", "POST /v1/phone/calls":
		return true
	}
	return false
}

// inPolicyShard carries a request that starts new work (newWorkInPath) to a node of the shard
// its routing policy places it in, reporting whether it answered w. Work landing in its shard,
// and work of a policy that is not strict whose shard has no node up or reachable, is served
// here; strict work no shard of its geography takes is answered 503.
//
// A socket is placed by its app's policy: its agent is named in its first frame, after the
// upgrade (refuseSocketElsewhere).
func (s *Server) inPolicyShard(w http.ResponseWriter, r *http.Request) bool {
	if !newWorkInPath(r) || s.store == nil || !s.routesByShard(r.Context()) {
		return false
	}
	customer, ok := CustomerFrom(r.Context())
	if !ok {
		return false
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, newWorkBodyLimit+1))
	r.Body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(body), r.Body), r.Body}
	if err != nil || len(body) > newWorkBodyLimit {
		return false
	}
	configID, failure, refused := s.namedConfig(r.Context(), customer, body)
	if refused {
		writeError(w, failure)
		return true
	}
	place, failure, refused := s.placed(r.Context(), customer, configID, "")
	if refused {
		writeError(w, failure)
		return true
	}
	return len(place.shards) > 0 && s.carriedToPlace(w, r, place, body)
}

// carriedToPlace is inPolicyShard for work placed in another shard than this node's.
func (s *Server) carriedToPlace(w http.ResponseWriter, r *http.Request, place placement, body []byte) bool {
	// A request a peer carried here is not carried on: two routers disagreeing on where it
	// belongs would pass it back and forth. Anyone can send the header, so strict work is
	// refused rather than done outside its geography.
	if r.Header.Get(node.ForwardedHeader) != "" {
		if place.strict {
			writeError(w, errResidencyDown)
		}
		return place.strict
	}
	defer func() { r.Body = io.NopCloser(bytes.NewReader(body)) }()
	for _, shard := range place.shards {
		if shard == s.directory.Shard() {
			return false
		}
		address, err := s.directory.NodeInShard(r.Context(), shard)
		if err == nil && address != "" {
			carry := s.carryTo
			if r.Header.Get("Upgrade") != "" {
				carry = s.carrySocketTo
			}
			if err = carry(w, r, shard, address, body); err == nil {
				return true
			}
		}
		if !place.strict {
			s.doneHere(shard, shardWorkRequest, address, err)
			return false
		}
		s.logger.Warn("no node of a shard new work is kept in could take it", "shard", shard, "node", address, "error", err)
	}
	s.logger.Error("no shard of the geography new work is kept in could take it; refusing it",
		"shards", place.shards, "path", loggedPath(s.mux, r))
	writeError(w, errResidencyDown)
	return true
}

// carryTo is carriedToPlace for one node of shard. The error is the node never getting the
// request; any other failure is answered on w.
func (s *Server) carryTo(w http.ResponseWriter, r *http.Request, shard, address string, body []byte) error {
	r.Body = io.NopCloser(bytes.NewReader(body))
	ctx, cancel := context.WithTimeout(r.Context(), carriedFor(r, body))
	defer cancel()
	err := s.forwarder.Forward(node.ToShard(ctx, shard), address, w, r)
	// Unavailable is a node that never got the request. After any other failure it may have
	// started the work, and done here too it would be done twice.
	if status.Code(err) == codes.Unavailable {
		return err
	}
	if err != nil {
		s.logger.Error("could not carry new work to a node of the shard it is placed in",
			"shard", shard, "node", address, "path", loggedPath(s.mux, r), "error", err)
		writeError(w, unavailable(unreachableNode))
	}
	return nil
}

// carriedFor is how long new work carried to another shard may take to be answered. The node
// doing it is cancelled with the carry, so a run is given its own timeout (timeout_ms) and the
// grace its node takes to close it, rather than forwardTimeout alone.
func carriedFor(r *http.Request, body []byte) time.Duration {
	if r.URL.Path != "/v1/agents/runs" {
		return forwardTimeout
	}
	timeout := defaultRunTimeout
	var run struct {
		TimeoutMs *int `json:"timeout_ms"`
	}
	if json.Unmarshal(body, &run) == nil && run.TimeoutMs != nil {
		timeout = time.Duration(*run.TimeoutMs) * time.Millisecond
	}
	return max(timeout+runLeaseGrace, forwardTimeout)
}

// carrySocketTo is carryTo for a socket, which has no body. The error is the node never getting
// the upgrade, its address not dialled; any other failure is answered on w.
func (s *Server) carrySocketTo(w http.ResponseWriter, r *http.Request, shard, address string, _ []byte) error {
	err := carrySocket(w, r, address)
	if dial, ok := errors.AsType[*net.OpError](err); ok && dial.Op == "dial" {
		return err
	}
	if err != nil {
		s.logger.Error("could not carry a socket to a node of the shard it is placed in",
			"shard", shard, "node", address, "path", loggedPath(s.mux, r), "error", err)
		writeError(w, unavailable(unreachableNode))
	}
	return nil
}

// namedConfig is the id of the agent config a request starting new work names in its body,
// by config_id or by agent, or that answers the number a placed call is made from, as an
// inbound call to it is answered. Empty for none, or a config or number that is not the
// customer's, which the operation refuses itself.
func (s *Server) namedConfig(ctx context.Context, customerID string, body []byte) (string, APIError, bool) {
	var named struct {
		ConfigID string `json:"config_id"`
		Agent    string `json:"agent"`
		From     string `json:"from"`
	}
	if json.Unmarshal(body, &named) != nil || named.ConfigID != "" {
		return named.ConfigID, APIError{}, false
	}
	if named.Agent == "" && named.From != "" {
		number, err := s.store.PhoneNumbers.GetByE164(ctx, customerID, named.From)
		if isNumberNotHeld(err) {
			return "", APIError{}, false
		}
		if err != nil {
			s.logger.Error("could not read the number a placed call is made from", "customer", customerID, "error", err)
			return "", errPolicyUnread, true
		}
		return number.ConfigID, APIError{}, false
	}
	if named.Agent == "" {
		return "", APIError{}, false
	}
	config, err := s.store.AgentConfigs.GetByName(ctx, customerID, named.Agent)
	if errors.Is(err, store.ErrNoAgentConfig) {
		return "", APIError{}, false
	}
	if err != nil {
		s.logger.Error("could not read the agent config new work names", "customer", customerID, "agent", named.Agent, "error", err)
		return "", errPolicyUnread, true
	}
	return config.ID, APIError{}, false
}

// refuseSocketElsewhere is the refusal of a socket session whose agent keeps its work in a
// geography this shard is outside, which its app's policy did not place it in at the upgrade
// (inPolicyShard). Work of any other policy is done where the socket is.
func (s *Server) refuseSocketElsewhere(ctx context.Context, customerID string, config *store.AgentConfig) (APIError, bool) {
	if config == nil || config.RoutingPolicy == "" {
		return APIError{}, false
	}
	place, failure, refused := s.placed(ctx, customerID, config.ID, "")
	if refused {
		return failure, true
	}
	if place.strict && !slices.Contains(place.shards, s.directory.Shard()) {
		return errSocketElsewhere, true
	}
	return APIError{}, false
}

// routesByShard reports whether this node sends work to other shards: ShardRouting maps
// something, this node knows its own shard and can reach its peers, and the request is not one
// a peer handed over. Two routers disagreeing on where a request belongs would otherwise pass
// it back and forth.
func (s *Server) routesByShard(ctx context.Context) bool {
	return s.shards.Enabled() && s.reachesShards(ctx)
}

// reachesShards reports whether this node hands work to the shard it lives in: it knows its own
// shard and can reach its peers, and the request is not one a peer handed over.
func (s *Server) reachesShards(ctx context.Context) bool {
	return s.directory != nil && s.forwarder != nil && s.directory.Shard() != "" && !node.Forwarded(ctx)
}

// inTokenShard hands a vendor's call back about a placed call to a node of the shard that placed
// it, which the call's token names (store.SplitToken): the call's rows are there alone. A token
// that names no shard was minted before tokens named one, and is answered here.
func (s *Server) inTokenShard(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		shard, _ := store.SplitToken(r.PathValue("token"))
		if shard == "" || !s.reachesShards(r.Context()) || shard == s.directory.Shard() {
			next(w, r)
			return
		}
		address, err := s.directory.NodeInShard(r.Context(), shard)
		if err != nil {
			s.logger.Error("could not find a node of the shard a call lives in", "shard", shard, "error", err)
			writeError(w, internalError())
			return
		}
		if address == "" {
			writeError(w, errCallShardDown)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), forwardTimeout)
		defer cancel()
		if err := s.forwarder.Forward(ctx, address, w, r); err != nil {
			s.logger.Error("could not reach a node of the shard a call lives in", "shard", shard, "node", address, "error", err)
			writeError(w, errCallShardDown)
		}
	}
}

// handedHome hands a call event to a node of the shard the call lives in (store.CallHomes),
// and answers w with what that node answered, reporting whether it did. An event no node of
// that shard can take is answered 503 for Stream to deliver again: the call's legs and lines
// are there alone.
func (s *Server) handedHome(w http.ResponseWriter, r *http.Request, origin hookOrigin, event callEvent, payload []byte) bool {
	callType, callID, split := strings.Cut(event.CallCid, ":")
	if !split || s.store == nil || !s.reachesShards(r.Context()) {
		return false
	}
	home, err := s.store.CallHomes.Elsewhere(r.Context(), origin.scope(), callType, callID)
	if err != nil {
		s.logger.Warn("could not read the shard a call lives in", "call", event.CallCid, "error", err)
		writeError(w, unavailable("could not tell where this call lives"))
		return true
	}
	if home == "" {
		return false
	}
	address, err := s.directory.NodeInShard(r.Context(), home)
	if err != nil || address == "" {
		s.logger.Error("no node of the shard a call lives in could take its event", "call", event.CallCid,
			"shard", home, "error", err)
		writeError(w, errCallShardDown)
		return true
	}
	ctx, cancel := context.WithTimeout(r.Context(), forwardTimeout)
	defer cancel()
	answer, err := s.forwarder.Ask(node.ToShard(ctx, home), address, r, payload)
	if err != nil {
		s.logger.Error("could not hand a call event to the shard the call lives in", "call", event.CallCid,
			"shard", home, "node", address, "error", err)
		writeError(w, errCallShardDown)
		return true
	}
	for _, header := range answer.GetHeaders() {
		for _, value := range header.GetValues() {
			w.Header().Add(header.GetName(), value)
		}
	}
	w.WriteHeader(int(answer.GetStatus()))
	_, _ = w.Write(answer.GetBody()) // the hook's sender has gone if this fails
	return true
}

// handedToShard hands what arrived to a node of the shard place puts the work in when this node
// is in another, reporting whether this node is not the one to do it and whether that node took
// it. Handed over with handOver, as forwardedToWorkers hands a hook over; nil hands nothing
// over. The node it reaches answers it as one that landed there, and may still carry it on
// once, to a worker or the node running its conversation (node.HandedToShard).
//
// With no node of the shard up, or none reachable, work that is not strict is done here rather
// than dropped, and counted in shardFallbacks for an alert. Strict work tries the next shard of
// its geography, and is down, done nowhere, when none takes it (errResidencyDown).
func (s *Server) handedToShard(ctx context.Context, handOver func(context.Context, string) (*node.ForwardResponse, error), place placement, work string) (elsewhere, taken, down bool) {
	if handOver == nil || len(place.shards) == 0 || !s.routesByShard(ctx) {
		return false, false, false
	}
	for _, shard := range place.shards {
		if shard == s.directory.Shard() {
			return false, false, false
		}
		address, err := s.directory.NodeInShard(ctx, shard)
		if err == nil && address != "" {
			if taken, err = s.handTo(ctx, handOver, shard, address, work); err == nil {
				return true, taken, false
			}
		}
		if !place.strict {
			s.doneHere(shard, work, address, err)
			return false, false, false
		}
		s.logger.Warn("no node of a shard work is kept in could take it", "shard", shard, "work", work, "node", address, "error", err)
	}
	s.logger.Error("no shard of the geography work is kept in could take it; not doing it here",
		"shards", place.shards, "work", work)
	return true, false, true
}

// handTo is handedToShard for one node of shard, reporting whether it took the work. The error
// is the node never getting it.
func (s *Server) handTo(ctx context.Context, handOver func(context.Context, string) (*node.ForwardResponse, error), shard, address, work string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, forwardTimeout)
	defer cancel()
	answer, err := handOver(node.ToShard(ctx, shard), address)
	switch {
	// Unavailable is a node that never got the request. Any other failure may come after the
	// node acted on it, such as its reply lost on the way back, and done here too it would be
	// done twice.
	case status.Code(err) == codes.Unavailable:
		return false, err
	case err != nil:
		s.logger.Error("could not hand work to a node of the shard it is meant for",
			"shard", shard, "work", work, "node", address, "error", err)
	case answer.GetStatus() != http.StatusOK:
		s.logger.Warn("a node of the shard work is meant for would not take it",
			"shard", shard, "work", work, "node", address, "status", answer.GetStatus())
	default:
		return true, nil
	}
	return false, nil
}

// doneHere counts and logs work meant for shard that this node does instead.
func (s *Server) doneHere(shard, work, address string, err error) {
	shardFallbacks.WithLabelValues(shard, work).Inc()
	s.logger.Error("no node of the shard work is meant for could take it; doing it here",
		"shard", shard, "work", work, "node", address, "error", err)
}

// userShard is the shard a user's work under an agent of region is done in
// (shardmap.Map.ForUser), empty for this node. A user whose shard cannot be read is served
// where the hook landed: a database failure does not hold up their message.
func (s *Server) userShard(ctx context.Context, customerID, region, userID string) string {
	if s.store == nil || !s.routesByShard(ctx) {
		return ""
	}
	last, err := s.store.Users.Shard(ctx, customerID, userID)
	if err != nil {
		s.logger.Warn("could not read the shard that last served a user", "customer", customerID, "user", userID, "error", err)
	}
	return s.shards.ForUser(region, last)
}

// configShard is userShard for the agent config configID answers with, read only when shards
// are routed. A config nobody holds any more, or none, is an agent of the default region.
func (s *Server) configShard(ctx context.Context, customerID, configID, userID string) string {
	if s.store == nil || !s.routesByShard(ctx) {
		return ""
	}
	var region string
	if configID != "" {
		config, err := s.store.AgentConfigs.Get(ctx, customerID, configID)
		if err != nil && !errors.Is(err, store.ErrNoAgentConfig) {
			s.logger.Warn("could not read the region of the agent answering a message", "customer", customerID, "config", configID, "error", err)
		}
		region = config.Region
	}
	return s.userShard(ctx, customerID, region, userID)
}
