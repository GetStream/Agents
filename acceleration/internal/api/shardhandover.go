package api

import (
	"context"
	"errors"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GetStream/Vision-Agents/acceleration/internal/node"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// The kinds of work a hook hands to another shard, the work label of shardFallbacks.
const (
	shardWorkCall    = "call"
	shardWorkMessage = "message"
)

// shardFallbacks counts by the shards ShardRouting names, so its series are bounded by config.
var shardFallbacks = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "router",
	Name:      "shard_handover_fallbacks_total",
	Help: "Hooks meant for another shard that were done on the router they landed on, because no node of that " +
		"shard was up or reachable, by the shard meant and the work: call or message (api handedToShard)",
}, []string{"shard", "work"})

// routesByShard reports whether this node sends work to other shards: ShardRouting maps
// something, this node knows its own shard and can reach its peers, and the request is not one
// a peer handed over. Two routers disagreeing on where a request belongs would otherwise pass
// it back and forth.
func (s *Server) routesByShard(ctx context.Context) bool {
	return s.shards.Enabled() && s.directory != nil && s.forwarder != nil && s.directory.Shard() != "" &&
		!node.Forwarded(ctx)
}

// handedToShard hands what arrived to a node of shard when the work is meant there and this
// node is in another, reporting whether this node is not the one to do it and whether that
// node took it. Handed over with handOver, as forwardedToWorkers hands a hook over; nil hands
// nothing over. The node it reaches answers it as one that landed there, and may still carry
// it on once, to a worker or the node running its conversation (node.HandedToShard).
//
// With no node of shard up, or none reachable, the work is done here rather than dropped, and
// counted in shardFallbacks for an alert.
func (s *Server) handedToShard(ctx context.Context, handOver func(context.Context, string) (*node.ForwardResponse, error), shard, work string) (elsewhere, taken bool) {
	if handOver == nil || shard == "" || !s.routesByShard(ctx) || shard == s.directory.Shard() {
		return false, false
	}
	address, err := s.directory.NodeInShard(ctx, shard)
	if err != nil || address == "" {
		s.doneHere(shard, work, address, err)
		return false, false
	}

	ctx, cancel := context.WithTimeout(ctx, s.forwardTimeout)
	defer cancel()
	answer, err := handOver(node.ToShard(ctx, shard), address)
	switch {
	// Unavailable is a node that never got the request. Any other failure may come after the
	// node acted on it, such as its reply lost on the way back, and done here too it would be
	// done twice.
	case status.Code(err) == codes.Unavailable:
		s.doneHere(shard, work, address, err)
		return false, false
	case err != nil:
		s.logger.Error("could not hand work to a node of the shard it is meant for",
			"shard", shard, "work", work, "node", address, "error", err)
	case answer.GetStatus() != http.StatusOK:
		s.logger.Warn("a node of the shard work is meant for would not take it",
			"shard", shard, "work", work, "node", address, "status", answer.GetStatus())
	default:
		return true, true
	}
	return true, false
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
