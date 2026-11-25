//go:build integration

package store

import (
	"encoding/json"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

func (s *ShardedStoreSuite) TestACallAnotherShardRanIsListedOnceShippedButFoundThereAlone() {
	s.Require().NoError(s.store.Calls.Start(s.ctx, &Call{ID: "here", CustomerID: "app", StartedAt: s.base}))
	s.Require().NoError(s.other.Calls.Start(s.ctx, &Call{ID: "there", CustomerID: "app", StartedAt: s.base.Add(time.Minute)}))
	unshipped, err := s.store.Calls.Select(s.ctx, "app", CallFilter{})
	s.Require().NoError(err)
	s.Require().Len(unshipped, 1, "the other shard is not read")
	s.Equal("here", unshipped[0].ID)
	s.Require().NoError(s.other.ShipIndex(s.ctx, s.base))

	calls, err := s.store.Calls.Select(s.ctx, "app", CallFilter{})
	s.Require().NoError(err)
	s.Require().Len(calls, 2)
	s.Equal("there", calls[0].ID, "newest first, whichever shard ran it")
	s.Equal("here", calls[1].ID)

	_, err = s.store.Calls.Get(s.ctx, "app", "there")
	s.ErrorIs(err, ErrNoCall, "its row is in the shard that ran it alone")
	found, err := s.other.Calls.Get(s.ctx, "app", "there")
	s.Require().NoError(err)
	s.Equal("there", found.ID)
}

// Defect: the owner-kind backfill decides on this shard's sessions alone, and stamps a
// conversation with the one kind it sees here.
func (s *ShardedStoreSuite) TestTheKindsAUserHeldAConversationAsAreEveryShardsShipped() {
	held := func(store *Store, id, kind string) {
		s.Require().NoError(store.Sessions.Upsert(s.ctx, &AgentSession{ID: id, CustomerID: "app", ConversationID: "agent:c",
			UserID: "sam", CallerKind: kind, CreatedAt: s.base}))
	}
	held(s.store, "here", UserKindAuthenticated)
	held(s.other, "there", UserKindGuest)
	s.Require().NoError(s.other.ShipIndex(s.ctx, s.base))

	kinds, err := s.store.Sessions.CallerKinds(s.ctx, "app", "agent:c", "sam")
	s.Require().NoError(err)
	s.ElementsMatch([]string{UserKindAuthenticated, UserKindGuest}, kinds)
	guest, err := s.store.Sessions.HeldAsGuest(s.ctx, "app", "sam")
	s.Require().NoError(err)
	s.True(guest, "held as a guest in the other shard")
}

func (s *ShardedStoreSuite) TestSpendIsWhatEveryShardShipped() {
	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{Modality: "llm", CustomerID: "app", Provider: "openai", Model: "gpt", StartedAt: s.base, CostMicros: 300}))
	s.Require().NoError(insertRequest(s.ctx, s.other, &Request{Modality: "llm", CustomerID: "app", Provider: "openai", Model: "gpt", StartedAt: s.base, CostMicros: 200}))
	s.Require().NoError(s.store.ShipStats(s.ctx, s.base.Add(time.Minute)))

	meters, err := s.store.Spend.Meters(s.ctx, []string{"app"}, s.base.Add(-time.Hour))
	s.Require().NoError(err)
	s.Require().Len(meters, 1)
	s.EqualValues(300, meters[0].CostMicros, "the other shard's request is not read before it ships")

	s.Require().NoError(s.other.ShipStats(s.ctx, s.base.Add(time.Minute)))
	meters, err = s.store.Spend.Meters(s.ctx, []string{"app"}, s.base.Add(-time.Hour))
	s.Require().NoError(err)
	s.Require().Len(meters, 1)
	s.EqualValues(500, meters[0].CostMicros)
}

func (s *ShardedStoreSuite) TestSpendALimitReadsIsThisShardsRequestsShippedOrNot() {
	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{Modality: "llm", CustomerID: "app", Provider: "openai", Model: "gpt", StartedAt: s.base, CostMicros: 300, Billable: true}))
	s.Require().NoError(insertRequest(s.ctx, s.other, &Request{Modality: "llm", CustomerID: "app", Provider: "openai", Model: "gpt", StartedAt: s.base, CostMicros: 200, Billable: true}))
	s.Require().NoError(s.other.ShipStats(s.ctx, s.base.Add(time.Minute)))
	// A window starting inside the hour: the hourly sums would count the whole hour or none of it.
	since := s.base.Add(-time.Minute)

	spent, err := s.store.Spend.Since(s.ctx, "app", since)
	s.Require().NoError(err)
	s.EqualValues(300, spent, "this shard's unshipped request, and not the other shard's")
	subject, err := s.store.Spend.Subject(s.ctx, usage.SubjectRef{Subject: usage.SubjectApp, AppID: "app"}, since, false)
	s.Require().NoError(err)
	s.EqualValues(300, subject)
}

func (s *ShardedStoreSuite) TestARequestsLogIsInTheShardThatWroteIt() {
	s.sessionCall(s.other, Call{ID: "call", SessionID: "session", CustomerID: "app", AgentID: "agent", StartedAt: s.base}, "cfg")
	s.Require().NoError(insertRequest(s.ctx, s.other, &Request{Modality: "tts", CustomerID: "app", AgentID: "agent", Provider: "cartesia", Model: "sonic", StartedAt: s.base.Add(time.Second), Success: true}))

	logs, err := s.other.AgentLogs.Select(s.ctx, "app", AgentLogFilter{Limit: 10})
	s.Require().NoError(err)
	s.Require().Len(logs, 1, "one row, in the shard, rather than one in the shared database as well")
	here, err := s.store.AgentLogs.Select(s.ctx, "app", AgentLogFilter{Limit: 10})
	s.Require().NoError(err)
	s.Empty(here, "another shard's log is read there")
	s.Equal("tts request completed", logs[0].Message)
	s.Equal("cfg", logs[0].ConfigID)
	s.Equal("session", logs[0].SessionID, "the session of the call, not the call")
	s.Equal(1, s.logsIn(s.other.shard, "app"))
	s.Zero(s.logsIn(s.store.global, "app"))
}

// requestLogged is the log row a request the agent made writes, in a shard. Each test names its
// own customer, since nothing clears the log between tests.
func (s *ShardedStoreSuite) requestLogged(customer string, success bool, errorCode string) AgentLog {
	s.sessionCall(s.other, Call{ID: "call", SessionID: "session-" + customer, CustomerID: customer, AgentID: "agent", StartedAt: s.base}, "cfg")
	s.Require().NoError(insertRequest(s.ctx, s.other, &Request{Modality: "llm", CustomerID: customer, AgentID: "agent", Provider: "openai", Model: "gpt",
		StartedAt: s.base.Add(time.Second), InputTokens: 10, OutputTokens: 4, Success: success, ErrorCode: errorCode}))
	logs, err := s.other.AgentLogs.Select(s.ctx, customer, AgentLogFilter{Limit: 10})
	s.Require().NoError(err)
	s.Require().Len(logs, 1)
	logged, err := s.other.AgentLogs.Get(s.ctx, customer, logs[0].ID)
	s.Require().NoError(err)
	return logged
}

func (s *ShardedStoreSuite) TestAFailedRequestIsLoggedAsAnError() {
	logged := s.requestLogged("failed-log", false, "provider_error")

	s.Equal("llm request failed", logged.Message)
	s.Equal("error", logged.Severity)
	s.Equal("provider_error", logged.Details["error_code"])
}

func (s *ShardedStoreSuite) TestARequestItsCallerCancelledIsLoggedAsNoError() {
	logged := s.requestLogged("cancelled-log", false, ErrorCancelled)

	s.Equal("llm request cancelled", logged.Message)
	s.Equal("info", logged.Severity, "a superseded flow decision or a barge-in is routine")
	s.Equal(ErrorCancelled, logged.Details["error_code"])
	s.EqualValues(4, logged.Details["output_tokens"], "what it generated is still there")
}

func (s *ShardedStoreSuite) TestSessionsOpenInTwoShardsAtOnceAreOnePeak() {
	s.opened("here", "app", s.base, nil)
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "app", "here", s.base, s.base.Add(10*time.Minute), ""))
	there := &AgentSession{ID: "there", CustomerID: "app", AgentName: "docs", UserID: "jlahey", CallerKind: "user", CreatedAt: s.base.Add(5 * time.Minute)}
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, there))
	s.Require().NoError(s.other.Sessions.Close(s.ctx, "app", "there", s.base.Add(5*time.Minute), s.base.Add(20*time.Minute), ""))
	s.Require().NoError(s.store.ShipStats(s.ctx, s.base.Add(time.Hour)))
	s.Require().NoError(s.other.ShipStats(s.ctx, s.base.Add(time.Hour)))

	peak, err := s.store.CustomerConcurrencyPeak(s.ctx, "app", s.base.Add(-time.Hour), s.base.Add(time.Hour))
	s.Require().NoError(err)
	s.EqualValues(2, peak.Sessions)
	s.Require().NotNil(peak.At)
	s.True(peak.At.Equal(s.base.Add(5 * time.Minute)))
}

// Defect: an export opens the other shard's database, or folds its rows into this one's.
func (s *ShardedStoreSuite) TestAnExportCarriesThisShardsRowsAlone() {
	latency := 100.0
	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{Modality: "stt", CustomerID: "app", Provider: "deepgram", Model: "flux", StartedAt: s.base, AudioMs: 1000, LatencyMs: &latency, Success: true}))
	slower := 300.0
	s.Require().NoError(insertRequest(s.ctx, s.other, &Request{Modality: "stt", CustomerID: "app", Provider: "deepgram", Model: "flux", StartedAt: s.base, AudioMs: 500, LatencyMs: &slower, Success: true}))
	_, err := s.store.Buckets.Rollup(s.ctx, Hourly, s.base, s.base.Add(time.Hour))
	s.Require().NoError(err)
	_, err = s.other.Buckets.Rollup(s.ctx, Hourly, s.base, s.base.Add(time.Hour))
	s.Require().NoError(err)

	var hours []map[string]any
	requests := 0
	_, err = s.store.ExportCustomer(s.ctx, "app", func(table string, row json.RawMessage) error {
		switch table {
		case "stats_hourly":
			var hour map[string]any
			s.Require().NoError(json.Unmarshal(row, &hour))
			hours = append(hours, hour)
		case "requests":
			requests++
		}
		return nil
	})
	s.Require().NoError(err)
	s.Equal(1, requests, "this shard's request")
	s.Require().Len(hours, 1)
	s.EqualValues(1000, hours[0]["audio_ms_total"])
	s.EqualValues(1, hours[0]["request_count"])
	s.EqualValues(100, hours[0]["latency_p50_ms"])
}
