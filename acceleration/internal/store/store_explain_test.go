//go:build integration

package store

import (
	"context"
	"strings"
)

// explained runs read on a store of the suite's databases and answers the plan of each
// statement it sent to the shard, over calls seeded across customers, sessions, agents and
// campaigns, so a plan that reads every call shows up as a sequential scan.
func (s *StoreSuite) explained(read func(*Store)) []string {
	_, err := s.store.shard.ExecContext(s.ctx, `INSERT INTO agent_sessions (id, customer_id, state, created_at)
		SELECT 'seeded-session-' || n, 'customer-' || (n % 40), 'closed', ?::timestamptz + n * interval '1 second'
		FROM generate_series(1, 1000) AS n`, s.base)
	s.Require().NoError(err)
	_, err = s.store.shard.ExecContext(s.ctx, `INSERT INTO calls (id, session_id, customer_id, agent_id, campaign_id, call_id, started_at)
		SELECT 'seeded-call-' || n, 'seeded-session-' || (n % 1000 + 1), 'customer-' || ((n % 1000 + 1) % 40),
			'agent-' || (n % 200), CASE WHEN n % 10 = 0 THEN 'campaign-' || (n % 30) END,
			CASE WHEN n % 3 = 0 THEN 'stream-' || n ELSE '' END, ?::timestamptz + n * interval '1 second'
		FROM generate_series(1, 5000) AS n`, s.base)
	s.Require().NoError(err)
	_, err = s.store.shard.ExecContext(s.ctx, "ANALYZE agent_sessions, calls")
	s.Require().NoError(err)

	watched, err := OpenShards(Options{DSN: s.dsn, ShardDSN: s.shardDSN})
	s.Require().NoError(err)
	defer watched.Close()
	sent := &queries{}
	watched.shard.AddQueryHook(sent)
	read(watched)
	s.Require().NotEmpty(sent.sent)

	plans := make([]string, 0, len(sent.sent))
	for _, query := range sent.sent {
		var plan []string
		s.Require().NoError(s.store.shard.NewRaw("EXPLAIN "+query).Scan(context.Background(), &plan))
		plans = append(plans, strings.Join(plan, "\n"))
	}
	return plans
}

// usesIndex checks every plan reads calls by index, and by index at least once.
func (s *StoreSuite) usesIndex(plans []string, index string) {
	for _, plan := range plans {
		s.NotContains(plan, "Seq Scan on calls", plan)
		s.Contains(plan, index, plan)
	}
}

func (s *StoreSuite) TestTheLatestCallOfEachSessionIsReadOffTheSessionIndex() {
	s.usesIndex(s.explained(func(store *Store) {
		_, err := store.Calls.Latest(s.ctx, "customer-7", []string{"seeded-session-7", "seeded-session-47"})
		s.Require().NoError(err)
	}), "calls_session_idx")
}

func (s *StoreSuite) TestTheLastVoicedCallIsReadOffTheSessionIndex() {
	s.usesIndex(s.explained(func(store *Store) {
		_, err := store.Calls.LastVoiced(s.ctx, "customer-7", "seeded-session-7")
		s.Require().NoError(err)
	}), "calls_session_idx")
}

func (s *StoreSuite) TestASessionsCallsAreListedOffTheSessionIndex() {
	s.usesIndex(s.explained(func(store *Store) {
		_, err := store.Calls.Select(s.ctx, "customer-7", CallFilter{SessionID: "seeded-session-7"})
		s.Require().NoError(err)
	}), "calls_session_idx")
}

func (s *StoreSuite) TestAnAgentsCallsAreListedOffTheAgentIndex() {
	s.usesIndex(s.explained(func(store *Store) {
		_, err := store.Calls.Select(s.ctx, "customer-7", CallFilter{AgentID: "agent-6"})
		s.Require().NoError(err)
	}), "calls_customer_agent_idx")
}

func (s *StoreSuite) TestACampaignsCallsAreListedOffTheCampaignIndex() {
	s.usesIndex(s.explained(func(store *Store) {
		_, err := store.Calls.Select(s.ctx, "customer-10", CallFilter{CampaignID: "campaign-10"})
		s.Require().NoError(err)
	}), "calls_campaign_idx")
}
