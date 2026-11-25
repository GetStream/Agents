//go:build integration

package store

import (
	"context"
	"strconv"
	"time"

	"github.com/uptrace/bun"
)

func (s *StoreSuite) TestAgentLogsPageFilterAndResume() {
	for i := 0; i < 520; i++ {
		entry := AgentLog{CustomerID: "log-test", ConfigID: "agent-a", SessionID: "session-a", UserID: "user-a", Source: "user", Severity: "info", EventType: "test", Message: "same timestamp", OccurredAt: s.base}
		if i%2 == 0 {
			entry.Severity = "error"
		}
		s.Require().NoError(s.store.AgentLogs.Insert(s.ctx, &entry))
	}
	other := AgentLog{CustomerID: "other", Source: "system", Severity: "error", EventType: "test", Message: "not yours", OccurredAt: s.base}
	s.Require().NoError(s.store.AgentLogs.Insert(s.ctx, &other))
	high, err := s.store.AgentLogs.HighWater(s.ctx, "log-test")
	s.Require().NoError(err)
	f := AgentLogFilter{Limit: 250, Before: high.Above()}
	first, err := s.store.AgentLogs.Select(s.ctx, "log-test", f)
	s.Require().NoError(err)
	s.Require().Len(first, 250)
	f.Before = first[len(first)-1].Mark
	second, err := s.store.AgentLogs.Select(s.ctx, "log-test", f)
	s.Require().NoError(err)
	s.Require().Len(second, 250)
	s.Less(second[0].ID, first[len(first)-1].ID)
	f.Before = second[len(second)-1].Mark
	third, err := s.store.AgentLogs.Select(s.ctx, "log-test", f)
	s.Require().NoError(err)
	s.Len(third, 20)
	fresh := AgentLog{CustomerID: "log-test", Source: "tool", Severity: "error", EventType: "tool_failed", Message: "Fresh", OccurredAt: s.base}
	s.Require().NoError(s.store.AgentLogs.Insert(s.ctx, &fresh))
	rows, err := s.store.AgentLogs.Select(s.ctx, "log-test", AgentLogFilter{After: high, Forward: true, Limit: 250})
	s.Require().NoError(err)
	s.Require().Len(rows, 1)
	s.Equal(fresh.ID, rows[0].ID)
	_, err = s.store.AgentLogs.Get(s.ctx, "other", fresh.ID)
	s.Error(err)
	rows, err = s.store.AgentLogs.Select(s.ctx, "log-test", AgentLogFilter{Severity: "error", Sources: []string{"user"}, UserID: "user-a", ConfigID: "agent-a", SessionID: "session-a", Limit: 250})
	s.Require().NoError(err)
	s.Len(rows, 250)
	for _, row := range rows {
		s.Equal("error", row.Severity)
		s.Equal("user", row.Source)
		s.Nil(row.Details)
	}
}

func (s *StoreSuite) TestAgentLogRedactsBeforeStorage() {
	entry := AgentLog{CustomerID: "redaction-test", Source: "agent", Severity: "info", EventType: "test", Message: "Bearer private-token api_key=secret-value", Details: map[string]any{"authorization": "secret", "tool": "lookup", "arguments": map[string]string{"password": "private"}}}
	s.Require().NoError(s.store.AgentLogs.Insert(s.ctx, &entry))
	row, err := s.store.AgentLogs.Get(s.ctx, "redaction-test", entry.ID)
	s.Require().NoError(err)
	s.NotContains(row.Message, "private-token")
	s.NotContains(row.Message, "secret-value")
	s.Equal(map[string]any{"tool": "lookup"}, row.Details)
}

func (s *StoreSuite) TestAgentLogProviderFailureDetails() {
	request := &Request{Modality: "search", CustomerID: "failure-details", AgentID: "test-agent", Provider: "perplexity", Model: "search", StartedAt: time.Now(), Success: false, ErrorCode: "build_failed", ErrorMessage: "perplexity: PERPLEXITY_API_KEY is required; api_key=private-key-value"}
	s.Require().NoError(insertRequest(s.ctx, s.store, request))
	rows, err := s.store.AgentLogs.Select(s.ctx, request.CustomerID, AgentLogFilter{Limit: 1})
	s.Require().NoError(err)
	s.Require().Len(rows, 1)
	detail, err := s.store.AgentLogs.Get(s.ctx, request.CustomerID, rows[0].ID)
	s.Require().NoError(err)
	s.Equal("build_failed", detail.Details["error_code"])
	s.Contains(detail.Details["error_message"], "PERPLEXITY_API_KEY is required")
	s.NotContains(detail.Details["error_message"], "private-key-value")
	_, err = s.store.AgentLogs.Get(s.ctx, "another-customer", rows[0].ID)
	s.Require().Error(err)
}

func (s *StoreSuite) TestAMovedLogRowNeverShowsTheErrorOfTheRequestHoldingItsNumberHere() {
	// The destination's own failed request, which a moved log row's number happens to name.
	own := &Request{Modality: "llm", CustomerID: "moved-logs", AgentID: "agent", Provider: "openai", Model: "gpt",
		StartedAt: s.base, Success: false, ErrorCode: "upstream_error", ErrorMessage: "the destination's own failure"}
	s.Require().NoError(insertRequest(s.ctx, s.store, own))
	// A row written as an import writes it, details as they came, naming own's number.
	logged := func(at time.Time, details map[string]any) AgentLog {
		details["request_id"] = strconv.FormatInt(own.ID, 10)
		entry := &AgentLog{CustomerID: "moved-logs", AgentID: "agent", Source: "system", Severity: "info",
			EventType: "provider_request", Message: "llm request completed", OccurredAt: at, IngestedAt: at, Details: details}
		_, err := s.store.GlobalDB().NewInsert().Model(entry).Exec(s.ctx)
		s.Require().NoError(err)
		read, err := s.store.AgentLogs.Get(s.ctx, "moved-logs", entry.ID)
		s.Require().NoError(err)
		return read
	}

	carried := logged(s.base.Add(time.Hour), map[string]any{"error_code": nil, "error_message": nil})
	older := logged(s.base.Add(time.Hour), map[string]any{})
	ownOlder := logged(s.base, map[string]any{})

	s.Contains(carried.Details, "error_code")
	s.Nil(carried.Details["error_code"], "a row carrying its error is its own answer")
	s.NotContains(older.Details, "error_code", "an older row's number names a request that started at another time")
	if !s.store.Sharded() {
		s.Equal("upstream_error", ownOlder.Details["error_code"], "an older row of this deployment still finds its request")
	}
}

func (s *StoreSuite) TestAgentLogQueryBudgets() {
	entry := func() *AgentLog {
		return &AgentLog{CustomerID: "acme", AgentID: "agent", SessionID: "session", Source: "agent", Severity: "info",
			EventType: "turn", Message: "said hello", OccurredAt: s.base}
	}
	for range 5 {
		s.Require().NoError(s.store.AgentLogs.Insert(s.ctx, entry()))
	}
	last := entry()

	// The transaction, the customer's lock and the insert.
	s.Equal(4, s.countQueries(func() { s.Require().NoError(s.store.AgentLogs.Insert(s.ctx, last)) }))
	holders := len(s.store.holders())
	s.Equal(holders, s.countQueries(func() {
		high, err := s.store.AgentLogs.HighWater(s.ctx, "acme")
		s.Require().NoError(err)
		s.Equal(last.ID, high[logRange(last.ID)])
	}))
	s.Equal(holders, s.countQueries(func() {
		listed, err := s.store.AgentLogs.Select(s.ctx, "acme", AgentLogFilter{SessionID: "session", Limit: 3})
		s.Require().NoError(err)
		s.Len(listed, 3)
	}))
	s.Equal(1, s.countQueries(func() {
		found, err := s.store.AgentLogs.Get(s.ctx, "acme", last.ID)
		s.Require().NoError(err)
		s.Equal("said hello", found.Message)
	}))
}

// logsIn is how many log rows of customer db holds.
func (s *StoreSuite) logsIn(db bun.IDB, customer string) int {
	count, err := db.NewSelect().Model((*AgentLog)(nil)).Where("customer_id = ?", customer).Count(s.ctx)
	s.Require().NoError(err)
	return count
}

// legacyLog writes a log row where a router from before the logs moved to the shards wrote it:
// in the shared database.
func (s *StoreSuite) legacyLog(customer, message string) AgentLog {
	entry := &AgentLog{CustomerID: customer, Source: "agent", Severity: "info", EventType: "turn", Message: message,
		OccurredAt: s.base, IngestedAt: s.base}
	_, err := s.store.global.NewInsert().Model(entry).Exec(s.ctx)
	s.Require().NoError(err)
	return *entry
}

// logged writes a log row of customer through into's store.
func (s *StoreSuite) logged(into *Store, customer, message string) AgentLog {
	entry := &AgentLog{CustomerID: customer, Source: "agent", Severity: "info", EventType: "turn", Message: message}
	s.Require().NoError(into.AgentLogs.Insert(s.ctx, entry))
	return *entry
}

func logMessages(rows []AgentLog) []string {
	messages := make([]string, len(rows))
	for i, row := range rows {
		messages[i] = row.Message
	}
	return messages
}

func (s *StoreSuite) TestARequestAndItsLogAreWrittenTogetherOrNotAtAll() {
	s.onShard("ALTER TABLE agent_logs ADD CONSTRAINT refuses_requests CHECK (event_type <> 'provider_request') NOT VALID")
	s.T().Cleanup(func() { s.onShard("ALTER TABLE agent_logs DROP CONSTRAINT refuses_requests") })

	_, err := s.store.Requests.InsertMany(s.ctx, []Request{{Modality: "llm", CustomerID: "atomic", AgentID: "agent",
		Provider: "openai", Model: "gpt", StartedAt: s.base, Success: true}})

	s.Require().Error(err, "the log row was refused")
	var requests int
	s.Require().NoError(s.store.shard.NewSelect().Model((*Request)(nil)).ColumnExpr("count(*)").
		Where("customer_id = 'atomic'").Scan(s.ctx, &requests))
	s.Zero(requests, "the request went with the log row it could not write")
}

func (s *ShardedStoreSuite) TestALogIsWrittenInThisShardAndNotTheSharedDatabase() {
	s.logged(s.store, "app", "said hello")

	s.Equal(1, s.logsIn(s.store.shard, "app"))
	s.Zero(s.logsIn(s.store.global, "app"))
}

func (s *ShardedStoreSuite) TestLogsOfTheSharedDatabaseAndEveryShardAreListedAndFoundOnceEach() {
	written := []AgentLog{s.legacyLog("app", "before the move"), s.logged(s.store, "app", "here"), s.logged(s.other, "app", "there")}
	ranges := map[int64]bool{}
	for _, row := range written {
		ranges[logRange(row.ID)] = true
	}
	s.Len(ranges, 3, "each database draws ids of its own range")

	all, err := s.store.AgentLogs.Select(s.ctx, "app", AgentLogFilter{Limit: 10})
	s.Require().NoError(err)
	s.Equal([]string{"there", "here", "before the move"}, logMessages(all), "newest first, whichever database holds it")

	high, err := s.store.AgentLogs.HighWater(s.ctx, "app")
	s.Require().NoError(err)
	var paged []AgentLog
	filter := AgentLogFilter{Limit: 1, Before: high.Above()}
	for range 5 {
		page, err := s.store.AgentLogs.Select(s.ctx, "app", filter)
		s.Require().NoError(err)
		if len(page) == 0 {
			break
		}
		paged = append(paged, page...)
		filter.Before, err = ParseAgentLogCursor(page[0].Cursor)
		s.Require().NoError(err)
	}
	s.Equal(logMessages(all), logMessages(paged), "one to a page, each once")

	for _, row := range written {
		found, err := s.store.AgentLogs.Get(s.ctx, "app", row.ID)
		s.Require().NoError(err, row.Message)
		s.Equal(row.Message, found.Message)
	}
}

func (s *ShardedStoreSuite) TestReadingOnFromAMarkMissesNoRowOfADatabaseNumberingBelowAnother() {
	s.logged(s.other, "app", "read before")
	high, err := s.store.AgentLogs.HighWater(s.ctx, "app")
	s.Require().NoError(err)

	s.logged(s.other, "app", "there")
	s.logged(s.store, "app", "here")
	s.legacyLog("app", "late in the shared database")

	rows, err := s.store.AgentLogs.Select(s.ctx, "app", AgentLogFilter{After: high, Forward: true, Limit: 10})
	s.Require().NoError(err)
	s.ElementsMatch([]string{"there", "here", "late in the shared database"}, logMessages(rows))
}

func (s *ShardedStoreSuite) TestAPeerWithoutTheLogTableYetHoldsNoneOfTheLogs() {
	_, err := s.other.shard.ExecContext(s.ctx, "ALTER TABLE agent_logs RENAME TO agent_logs_unmigrated")
	s.Require().NoError(err)
	s.T().Cleanup(func() {
		_, err := s.other.shard.ExecContext(context.WithoutCancel(s.ctx), "ALTER TABLE agent_logs_unmigrated RENAME TO agent_logs")
		s.NoError(err)
	})
	here := s.logged(s.store, "app", "here")
	s.legacyLog("app", "before the move")

	high, err := s.store.AgentLogs.HighWater(s.ctx, "app")
	s.Require().NoError(err)
	s.Equal(here.ID, high[logRange(here.ID)])
	rows, err := s.store.AgentLogs.Select(s.ctx, "app", AgentLogFilter{Limit: 10})
	s.Require().NoError(err)
	s.Equal([]string{"here", "before the move"}, logMessages(rows))
	_, err = s.store.AgentLogs.Get(s.ctx, "app", here.ID+1)
	s.ErrorIs(err, ErrNoAgentLog)
}

func (s *ShardedStoreSuite) TestMigratingAgainKeepsDrawingLogIdsWhereTheShardLeftOff() {
	// Numbered again: SetupTest empties shard_numbers.
	s.Require().NoError(s.store.Migrate(s.ctx))
	first := s.logged(s.store, "app", "first")
	s.Require().NoError(s.store.Migrate(s.ctx))

	second := s.logged(s.store, "app", "second")
	s.Greater(second.ID, first.ID)
	s.Equal(logRange(first.ID), logRange(second.ID))
}
