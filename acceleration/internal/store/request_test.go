//go:build integration

package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// record stores a speech-to-text request, defaulting the fields a test does not care about.
func (s *StoreSuite) record(customerID string, at time.Time, audioMs int64, latencyMs float64, success bool) {
	request := &Request{
		Modality:   "stt",
		CustomerID: customerID,
		Provider:   "deepgram",
		Model:      "flux-general-en",
		StartedAt:  at,
		AudioMs:    audioMs,
		LatencyMs:  &latencyMs,
		Success:    success,
	}
	if !success {
		request.ErrorCode = "upstream_error"
	}
	s.Require().NoError(insertRequest(s.ctx, s.store, request))
}

func (s *StoreSuite) TestRecordRequestDefaultsStartedAt() {
	request := &Request{
		Modality: "stt", CustomerID: "acme",
		Provider: "deepgram", Model: "flux-general-en", Success: true,
	}

	s.Require().NoError(insertRequest(s.ctx, s.store, request))

	s.False(request.StartedAt.IsZero(), "a request with no timestamp should be stamped now")
	s.Positive(request.ID, "the row should come back with its generated id")
}

func (s *StoreSuite) TestModelRequestsCountEveryCustomerSinceTheWindowOpened() {
	s.record("acme", s.base.Add(1*time.Minute), 1000, 100, true)
	s.record("globex", s.base.Add(2*time.Minute), 1000, 100, false)
	s.record("acme", s.base.Add(-time.Hour), 1000, 100, true)
	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{
		Modality: "llm", CustomerID: "acme", Provider: "openai", Model: "gpt-5.6-sol", StartedAt: s.base, Success: true,
	}))
	s.shipStats(s.base.Add(time.Hour))

	counts, err := s.store.Requests.CountByModel(s.ctx, "stt", s.base)
	s.Require().NoError(err)
	s.Equal(map[string]int64{"deepgram/flux-general-en": 2}, counts)
}

func (s *StoreSuite) TestARequestItsCallerCancelledIsLoggedAsNoError() {
	s.sessionCall(s.store, Call{ID: "call", SessionID: "session-cancel-log", CustomerID: "cancel-log", AgentID: "agent", StartedAt: s.base}, "cfg")
	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{Modality: "llm", CustomerID: "cancel-log", AgentID: "agent", Provider: "openai", Model: "gpt",
		StartedAt: s.base.Add(time.Second), OutputTokens: 4, Success: false, ErrorCode: ErrorCancelled}))
	logs, err := s.store.AgentLogs.Select(s.ctx, "cancel-log", AgentLogFilter{Limit: 10})
	s.Require().NoError(err)
	s.Require().Len(logs, 1)

	s.Equal("llm request cancelled", logs[0].Message)
	s.Equal("info", logs[0].Severity)
}

// usageRow is a request with a usage id of its own, defaulting what a test does not care about.
func (s *StoreSuite) usageRow(customerID string, change func(*Request)) Request {
	request := Request{
		UsageID: uuid.Must(uuid.NewV7()), Modality: "llm", CustomerID: customerID,
		Provider: "openai", Model: "gpt", StartedAt: s.base, OutputTokens: 10, CostMicros: 25, Success: true,
	}
	if change != nil {
		change(&request)
	}
	return request
}

// tooBigToIndex is an id longer than a btree index row may be, even compressed.
func tooBigToIndex() string {
	raw := make([]byte, 10_000)
	_, _ = rand.Read(raw)
	return hex.EncodeToString(raw)
}

// requestsOf is every row a customer has in this shard, by usage id.
func (s *StoreSuite) requestsOf(customerID string) map[uuid.UUID]Request {
	var rows []Request
	s.Require().NoError(s.store.ShardDB().NewSelect().Model(&rows).Where("customer_id = ?", customerID).Scan(s.ctx))
	byUsage := map[uuid.UUID]Request{}
	for _, row := range rows {
		byUsage[row.UsageID] = row
	}
	return byUsage
}

func (s *StoreSuite) TestRecordRequestsSkipsWrittenUsageIDs() {
	first, second := s.usageRow("batch", nil), s.usageRow("batch", nil)
	written, err := s.store.Requests.InsertMany(s.ctx, []Request{first})
	s.Require().NoError(err)
	s.Equal([]uuid.UUID{first.UsageID}, written)

	written, err = s.store.Requests.InsertMany(s.ctx, []Request{first, second})

	s.Require().NoError(err)
	s.Equal([]uuid.UUID{second.UsageID}, written, "a row already written is not written again")
	s.Len(s.requestsOf("batch"), 2)
}

func (s *StoreSuite) TestTwoRowsWithOneUsageIDInOneBatchWriteOne() {
	first := s.usageRow("twice", nil)
	second := s.usageRow("twice", func(request *Request) { request.UsageID = first.UsageID })

	written, err := s.store.Requests.InsertMany(s.ctx, []Request{first, second})

	s.Require().NoError(err)
	s.Equal([]uuid.UUID{first.UsageID}, written)
	s.Len(s.requestsOf("twice"), 1)
}

func (s *StoreSuite) TestARowWithoutAUsageIDIsRefused() {
	written, err := s.store.Requests.InsertMany(s.ctx, []Request{
		s.usageRow("refused", nil),
		s.usageRow("refused", func(request *Request) { request.UsageID = uuid.Nil }),
	})

	var invalid *ValidationError
	s.Require().ErrorAs(err, &invalid)
	s.Equal("usage_id", invalid.Field)
	s.True(Unwritable(err), "a row with no usage id never becomes writable")
	s.Empty(written)
	s.Empty(s.requestsOf("refused"), "the batch is one transaction")
}

func (s *StoreSuite) TestTheTableRefusesARowWhoseUsageIDIsNull() {
	_, err := s.store.ShardDB().ExecContext(s.ctx, `INSERT INTO requests (usage_id, modality, customer_id, provider, model, started_at, success)
VALUES (NULL, 'llm', 'bare', 'openai', 'gpt', now(), true)`)

	s.Require().Error(err, "a writer that skips RecordRequests still cannot write a row with no usage id")
	s.True(Unwritable(err))
}

func (s *StoreSuite) TestARowWrittenWithoutAUsageIDGetsOneFromTheTable() {
	request := s.usageRow("bare", func(request *Request) { request.UsageID = uuid.Nil })

	_, err := s.store.ShardDB().NewInsert().Model(&request).Exec(s.ctx)

	s.Require().NoError(err, "as the router from before usage ids writes")
	s.Require().Len(s.requestsOf("bare"), 1)
	for usageID := range s.requestsOf("bare") {
		s.NotEqual(uuid.Nil, usageID)
	}
}

func (s *StoreSuite) TestARowPostgresRefusesIsUnwritable() {
	_, err := s.store.Requests.InsertMany(s.ctx, []Request{
		s.usageRow("too-big", func(request *Request) { request.AgentID = tooBigToIndex() }),
	})

	s.Require().Error(err)
	s.True(Unwritable(err), "an id too big for its index is refused however often it is sent: %v", err)
}

// With one database or many, the router writes the log row: a second writer would make it two.
func (s *StoreSuite) TestABatchWritesOneLogRowPerNewRow() {
	s.sessionCall(s.store, Call{ID: "call", SessionID: "session-batch-log", CustomerID: "batch-log", AgentID: "agent", StartedAt: s.base}, "cfg")
	batch := []Request{
		s.usageRow("batch-log", func(request *Request) { request.AgentID = "agent" }),
		s.usageRow("batch-log", func(request *Request) { request.AgentID = "agent" }),
		s.usageRow("batch-log", nil),
	}

	_, err := s.store.Requests.InsertMany(s.ctx, batch)
	s.Require().NoError(err)
	_, err = s.store.Requests.InsertMany(s.ctx, batch)
	s.Require().NoError(err)

	logs, err := s.store.AgentLogs.Select(s.ctx, "batch-log", AgentLogFilter{Limit: 10})
	s.Require().NoError(err)
	s.Len(logs, 2, "one log row per agent row, and none for a row written again")
	for _, log := range logs {
		s.Equal("session-batch-log", log.SessionID)
	}
}

func (s *StoreSuite) TestABatchLogsEachRowInItsOwnCall() {
	for config, call := range map[string]Call{
		"cfg-1": {ID: "call-first", SessionID: "first", CallID: "stream-1", CustomerID: "calls-a", AgentID: "agent", StartedAt: s.base},
		"cfg-2": {ID: "call-second", SessionID: "second", CallID: "stream-2", CustomerID: "calls-a", AgentID: "agent", StartedAt: s.base.Add(time.Minute)},
		"cfg-3": {ID: "call-other", SessionID: "other", CallID: "stream-3", CustomerID: "calls-b", AgentID: "agent", StartedAt: s.base},
	} {
		s.sessionCall(s.store, call, config)
	}
	at := func(customer, callID string, after time.Duration) Request {
		return s.usageRow(customer, func(request *Request) {
			request.AgentID, request.CallID, request.StartedAt = "agent", callID, s.base.Add(after)
		})
	}
	batch := []Request{
		at("calls-a", "stream-1", 2*time.Minute),
		at("calls-b", "", time.Second),
		at("calls-a", "", 2*time.Minute),
		at("calls-a", "", 30*time.Second),
	}

	_, err := s.store.Requests.InsertMany(s.ctx, batch)
	s.Require().NoError(err)

	sessions := func(customer string) map[string]string {
		logs, err := s.store.AgentLogs.Select(s.ctx, customer, AgentLogFilter{Limit: 10})
		s.Require().NoError(err)
		byRequest := map[string]string{}
		for _, log := range logs {
			logged, err := s.store.AgentLogs.Get(s.ctx, customer, log.ID)
			s.Require().NoError(err)
			byRequest[logged.Details["request_id"].(string)] = logged.SessionID + "/" + logged.ConfigID
		}
		return byRequest
	}
	id := func(request Request) string { return strconv.FormatInt(request.ID, 10) }
	s.Equal(map[string]string{
		id(batch[0]): "first/cfg-1",
		id(batch[2]): "second/cfg-2",
		id(batch[3]): "first/cfg-1",
	}, sessions("calls-a"), "the call a row names, else the agent's latest call started by it")
	s.Equal(map[string]string{id(batch[1]): "other/cfg-3"}, sessions("calls-b"))
}

// A row's log names the agent config of the call it happened in, which was read off whichever
// customer's session had the call's session id.
func (s *StoreSuite) TestARowsLogTakesNoAgentConfigFromAnotherCustomersSession() {
	s.opened("borrowed", "calls-b", s.base, func(session *AgentSession) { session.ConfigID = "cfg-b" })
	s.Require().NoError(s.store.Calls.Start(s.ctx, &Call{ID: "call-borrowed", SessionID: "borrowed", CallID: "stream-1",
		CustomerID: "calls-a", AgentID: "agent", StartedAt: s.base}))
	row := s.usageRow("calls-a", func(request *Request) {
		request.AgentID, request.CallID, request.StartedAt = "agent", "stream-1", s.base.Add(time.Minute)
	})

	_, err := s.store.Requests.InsertMany(s.ctx, []Request{row})
	s.Require().NoError(err)

	logs, err := s.store.AgentLogs.Select(s.ctx, "calls-a", AgentLogFilter{Limit: 10})
	s.Require().NoError(err)
	s.Require().Len(logs, 1)
	logged, err := s.store.AgentLogs.Get(s.ctx, "calls-a", logs[0].ID)
	s.Require().NoError(err)
	s.Equal("borrowed", logged.SessionID)
	s.Empty(logged.ConfigID, "calls-b's agent config is not calls-a's")
}

func (s *StoreSuite) TestBatchesNamingTheSameCustomersInAnotherOrderDoNotDeadlock() {
	// The agent log trigger locks each customer it logs a row for until the batch commits.
	batch := func(first, second string) []Request {
		var rows []Request
		for range 50 {
			rows = append(rows, s.usageRow(first, func(request *Request) { request.AgentID = "agent" }))
		}
		return append(rows, s.usageRow(second, func(request *Request) { request.AgentID = "agent" }))
	}

	for range 5 {
		one, other := batch("order-a", "order-b"), batch("order-b", "order-a")
		failed := make(chan error, 2)
		go func() { _, err := s.store.Requests.InsertMany(s.ctx, one); failed <- err }()
		go func() { _, err := s.store.Requests.InsertMany(s.ctx, other); failed <- err }()
		s.Require().NoError(<-failed)
		s.Require().NoError(<-failed)
	}

	s.Len(s.requestsOf("order-a"), 5*51)
	s.Len(s.requestsOf("order-b"), 5*51)
}

func (s *StoreSuite) TestARequestIDADataMoveTookIsWrittenOnTheNextTry() {
	var next int64
	s.Require().NoError(s.store.ShardDB().QueryRowContext(s.ctx,
		"SELECT nextval(pg_get_serial_sequence('requests', 'id'))").Scan(&next))
	// What a data move does: a row carried in with its own id, ahead of the sequence.
	moved := s.usageRow("moved-id", func(request *Request) { request.ID = next + 1 })
	_, err := s.store.ShardDB().NewInsert().Model(&moved).Exec(s.ctx)
	s.Require().NoError(err)
	row := s.usageRow("moved-id", nil)

	_, err = s.store.Requests.InsertMany(s.ctx, []Request{row})
	s.Require().Error(err, "the sequence's next id is the moved row's")
	s.True(IDTaken(err), "%v", err)
	s.False(Unwritable(err), "the next try draws another id: %v", err)

	written, err := s.store.Requests.InsertMany(s.ctx, []Request{row})
	s.Require().NoError(err)
	s.Equal([]uuid.UUID{row.UsageID}, written)
}

func (s *StoreSuite) TestManyMovedRequestIDsCostOneTry() {
	var next int64
	s.Require().NoError(s.store.ShardDB().QueryRowContext(s.ctx,
		"SELECT nextval(pg_get_serial_sequence('requests', 'id'))").Scan(&next))
	moved := make([]Request, 500)
	for i := range moved {
		moved[i] = s.usageRow("many-moved-ids", func(request *Request) { request.ID = next + 1 + int64(i) })
	}
	_, err := s.store.ShardDB().NewInsert().Model(&moved).Exec(s.ctx)
	s.Require().NoError(err)
	rows := []Request{s.usageRow("many-moved-ids", nil), s.usageRow("many-moved-ids", nil)}

	_, err = s.store.Requests.InsertMany(s.ctx, rows)
	s.Require().True(IDTaken(err), "%v", err)
	written, err := s.store.Requests.InsertMany(s.ctx, rows)

	s.Require().NoError(err, "the failed try moved the sequence past every moved id")
	s.Len(written, 2)
	s.Greater(rows[0].ID, next+500)
}

func (s *StoreSuite) TestTheRequestIDSequenceIsNeverMovedBack() {
	var ahead int64
	s.Require().NoError(s.store.ShardDB().QueryRowContext(s.ctx,
		"SELECT setval(pg_get_serial_sequence('requests', 'id'), nextval(pg_get_serial_sequence('requests', 'id')) + 1000)").Scan(&ahead))

	s.Require().NoError(s.store.Requests.advanceIDs(s.ctx))

	rows := []Request{s.usageRow("sequence-ahead", nil)}
	_, err := s.store.Requests.InsertMany(s.ctx, rows)
	s.Require().NoError(err)
	s.Equal(ahead+1, rows[0].ID, "a sequence ahead of every row is left where it is")
}

func (s *StoreSuite) TestABatchLeavesItsRowsInTheOrderSentWithTheirIDs() {
	sent := []Request{s.usageRow("order-z", nil), s.usageRow("order-y", nil), s.usageRow("order-z", nil)}

	_, err := s.store.Requests.InsertMany(s.ctx, sent)

	s.Require().NoError(err)
	s.Equal([]string{"order-z", "order-y", "order-z"}, []string{sent[0].CustomerID, sent[1].CustomerID, sent[2].CustomerID})
	stored := s.requestsOf("order-z")
	s.Equal(sent[0].ID, stored[sent[0].UsageID].ID, "each row sent has the id it was written under")
	s.Equal(sent[2].ID, stored[sent[2].UsageID].ID)
	s.Less(sent[0].ID, sent[2].ID, "one customer's rows are written in the order they were sent")
}

func (s *StoreSuite) TestTheIdentityAndShippingColumnsAreStored() {
	shipped := s.base.Add(time.Minute)
	request := s.usageRow("identity", func(request *Request) {
		request.OrganizationID, request.OrganizationTier, request.APIKey = "org", "tier-a", "key"
		request.UserID, request.UsageCategories, request.SessionID = "ada", []string{"support", "team:core"}, "session"
		request.ToShip, request.ShippedAt, request.ShipError = true, &shipped, "refused"
	})
	_, err := s.store.Requests.InsertMany(s.ctx, []Request{request})
	s.Require().NoError(err)

	stored := s.requestsOf("identity")[request.UsageID]

	s.Equal("org", stored.OrganizationID)
	s.Equal("tier-a", stored.OrganizationTier)
	s.Equal("key", stored.APIKey)
	s.Equal("ada", stored.UserID)
	s.Equal([]string{"support", "team:core"}, stored.UsageCategories)
	s.Equal("session", stored.SessionID)
	s.True(stored.ToShip)
	s.Require().NotNil(stored.ShippedAt)
	s.True(shipped.Equal(*stored.ShippedAt))
	s.Equal("refused", stored.ShipError)
}

func (s *StoreSuite) TestMarkingARowAnotherSessionHoldsFailsWithinTheLockWait() {
	request := &Request{UsageID: uuid.Must(uuid.NewV7()), Modality: "llm", CustomerID: "lock-" + uuid.NewString(),
		Provider: "openai", Model: "gpt", StartedAt: time.Now().UTC(), Success: true, ToShip: true}
	s.Require().NoError(insertRequest(s.ctx, s.store, request))
	// A router that went silent with the row locked.
	tx, err := s.store.ShardDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(s.ctx, "SELECT 1 FROM requests WHERE usage_id = ? FOR UPDATE", request.UsageID)
	s.Require().NoError(err)

	started := time.Now()
	err = s.store.Requests.Ship(s.ctx, []uuid.UUID{request.UsageID}, nil)

	s.Require().ErrorContains(err, "lock timeout")
	s.Less(time.Since(started), shippingLockWait+3*time.Second)
	s.GreaterOrEqual(time.Since(started), shippingLockWait-time.Second)
	err = s.store.Requests.SetOrganization(s.ctx, []uuid.UUID{request.UsageID}, "org")
	s.ErrorContains(err, "lock timeout", "the sweeper's other write waits no longer")
}

// shippable records a row of organization's started at at, still to be shipped.
func (s *StoreSuite) shippable(organization string, at time.Time) uuid.UUID {
	request := &Request{UsageID: uuid.Must(uuid.NewV7()), Modality: "llm", CustomerID: "acme", OrganizationID: organization,
		Provider: "openai", Model: "gpt", StartedAt: at, Success: true, ToShip: true}
	s.Require().NoError(insertRequest(s.ctx, s.store, request))
	return request.UsageID
}

// shipping is the row's to_ship and ship_error.
func (s *StoreSuite) shipping(usageID uuid.UUID) (bool, string) {
	var request Request
	s.Require().NoError(s.store.ShardDB().NewSelect().Model(&request).Where("usage_id = ?", usageID).Scan(s.ctx))
	return request.ToShip, request.ShipError
}

func (s *StoreSuite) TestUnshippedCountsTheRowsWithAnOrganizationStillToShip() {
	now := time.Now().UTC()
	s.shippable("org", now.Add(-2*time.Hour))
	s.shippable("org", now.Add(-time.Hour))
	s.shippable("", now.Add(-3*time.Hour))
	shipped := s.shippable("org", now.Add(-4*time.Hour))
	s.Require().NoError(s.store.Requests.Ship(s.ctx, []uuid.UUID{shipped}, nil))

	rows, oldest, err := s.store.Requests.Unshipped(s.ctx)

	s.Require().NoError(err)
	s.EqualValues(2, rows, "neither the shipped row nor the one with no organization")
	s.InDelta((2 * time.Hour).Seconds(), oldest.Seconds(), 60)
}

func (s *StoreSuite) TestReshipQueuesTheShippedRowsStartedSinceAgain() {
	since := time.Now().UTC().Add(-time.Hour)
	recent := s.shippable("org", since.Add(time.Minute))
	older := s.shippable("org", since.Add(-time.Minute))
	refused := s.shippable("org", since.Add(2*time.Minute))
	waiting := s.shippable("org", since.Add(3*time.Minute))
	s.Require().NoError(s.store.Requests.Ship(s.ctx, []uuid.UUID{recent, older}, map[uuid.UUID]string{refused: "unknown price"}))

	queued, err := s.store.Requests.Reship(s.ctx, since, false)

	s.Require().NoError(err)
	s.EqualValues(1, queued)
	toShip, _ := s.shipping(recent)
	s.True(toShip)
	toShip, _ = s.shipping(older)
	s.False(toShip, "started before since")
	toShip, reason := s.shipping(refused)
	s.False(toShip, "the service refused it, it never had it")
	s.Equal("unknown price", reason)
	toShip, _ = s.shipping(waiting)
	s.True(toShip, "still queued, as it was")
}

func (s *StoreSuite) TestReshipOfRejectedRowsQueuesTheRefusedOnesAndClearsWhy() {
	since := time.Now().UTC().Add(-time.Hour)
	shipped := s.shippable("org", since.Add(time.Minute))
	refused := s.shippable("org", since.Add(2*time.Minute))
	s.Require().NoError(s.store.Requests.Ship(s.ctx, []uuid.UUID{shipped}, map[uuid.UUID]string{refused: "unknown price"}))

	queued, err := s.store.Requests.Reship(s.ctx, since, true)

	s.Require().NoError(err)
	s.EqualValues(1, queued)
	toShip, reason := s.shipping(refused)
	s.True(toShip)
	s.Empty(reason)
	toShip, _ = s.shipping(shipped)
	s.False(toShip, "the service has it")
}

// spent records a request that cost money, which is what the spend paths aggregate.
func (s *StoreSuite) spent(
	customerID, modality string,
	at time.Time,
	costMicros int64,
	tags map[string]string,
) {
	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{
		Modality:   modality,
		CustomerID: customerID,
		Provider:   "openai",
		Model:      "gpt-5.6-sol",
		Tags:       tags,
		StartedAt:  at,
		CostMicros: costMicros,
		Success:    true,
	}))
}

func (s *StoreSuite) TestSpendCoversEveryModalityAtOnce() {
	s.spent("acme", "llm", s.base.Add(time.Minute), 1200, nil)
	s.spent("acme", "tts", s.base.Add(2*time.Minute), 800, nil)
	s.spent("acme", "llm", s.base.Add(26*time.Hour), 400, nil)
	s.shipStats(s.base.Add(48 * time.Hour))

	buckets, err := s.store.Requests.Spend(
		s.ctx, "acme", "modality", Daily, s.base, s.base.Add(48*time.Hour), 6, nil, UsageFilter{})
	s.Require().NoError(err)
	s.Require().Len(buckets, 3)

	firstDay := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	s.Equal(firstDay, buckets[0].Bucket.UTC())
	s.Equal("llm", buckets[0].Value, "the biggest group of the day comes first")
	s.EqualValues(1200, buckets[0].CostMicrosTotal)
	s.Equal(firstDay, buckets[1].Bucket.UTC())
	s.Equal("tts", buckets[1].Value)
	s.EqualValues(800, buckets[1].CostMicrosTotal)
	s.Equal(firstDay.Add(24*time.Hour), buckets[2].Bucket.UTC())
	s.EqualValues(400, buckets[2].CostMicrosTotal)
}

func (s *StoreSuite) TestSpendKeepsTheBiggestGroupsAndSumsTheRestIntoOther() {
	// A label naming an end customer has as many values as the customer has customers, so
	// a chart of every one of them says nothing. What is left has to still add up.
	for i, cost := range []int64{5000, 4000, 3000, 2000, 1000} {
		s.spent("acme", "llm", s.base.Add(time.Duration(i)*time.Minute), cost,
			map[string]string{"customer_id": fmt.Sprintf("c-%d", i)})
	}
	s.spent("acme", "llm", s.base.Add(10*time.Minute), 900, nil)
	s.shipStats(s.base.Add(24 * time.Hour))

	buckets, err := s.store.Requests.Spend(
		s.ctx, "acme", "customer_id", Daily, s.base, s.base.Add(24*time.Hour), 2, nil, UsageFilter{})
	s.Require().NoError(err)

	byValue := map[string]int64{}
	for _, bucket := range buckets {
		byValue[bucket.Value] += bucket.CostMicrosTotal
	}
	s.Equal(map[string]int64{"c-0": 5000, "c-1": 4000, "other": 6000, "": 900}, byValue,
		"the two biggest keep a group, the rest are other, and the unlabelled spend is its own")
}

func (s *StoreSuite) TestRuntimeIsSpendAndNotRequests() {
	support := map[string]string{"product": "support"}
	s.spent("acme", "llm", s.base.Add(time.Minute), 1000, support)
	s.spent("acme", RuntimeModality, s.base.Add(2*time.Minute), 600, support)
	s.spent("acme", RuntimeModality, s.base.Add(3*time.Minute), 600, nil)
	s.shipStats(s.base.Add(24 * time.Hour))

	buckets, err := s.store.Requests.Spend(
		s.ctx, "acme", "modality", Daily, s.base, s.base.Add(24*time.Hour), 6, nil, UsageFilter{})
	s.Require().NoError(err)
	byModality := map[string]SpendBucket{}
	for _, bucket := range buckets {
		byModality[bucket.Value] = bucket
	}
	s.EqualValues(1200, byModality[RuntimeModality].CostMicrosTotal, "a session's time is spend")
	s.Zero(byModality[RuntimeModality].RequestCount, "and asks no provider anything")
	s.EqualValues(1, byModality["llm"].RequestCount)

	keys, err := s.store.Requests.TagKeys(s.ctx, "acme", s.base, s.base.Add(24*time.Hour), nil)
	s.Require().NoError(err)
	s.Require().Len(keys, 1)
	s.EqualValues(1600, keys[0].CostMicrosTotal)
	s.EqualValues(1, keys[0].RequestCount)
	s.InDelta(1.0, keys[0].Coverage, 0.001, "the one request carries the label; runtime is not a request")
}

func (s *StoreSuite) TestTagKeysOfRuntimeAloneAreSpendWithNoRequests() {
	s.spent("acme", RuntimeModality, s.base.Add(time.Minute), 600, map[string]string{"product": "support"})
	s.shipStats(s.base.Add(24 * time.Hour))

	keys, err := s.store.Requests.TagKeys(s.ctx, "acme", s.base, s.base.Add(24*time.Hour), nil)
	s.Require().NoError(err)
	s.Require().Len(keys, 1)
	s.EqualValues(600, keys[0].CostMicrosTotal)
	s.Zero(keys[0].RequestCount)
	s.Zero(keys[0].Coverage)
	s.Require().Len(keys[0].TopValues, 1)
	s.InDelta(1.0, keys[0].TopValues[0].Share, 0.001)
}

func (s *StoreSuite) TestSpendCanBeNarrowedToOneLabel() {
	s.spent("acme", "llm", s.base.Add(time.Minute), 1000,
		map[string]string{"product": "support", "environment": "production"})
	s.spent("acme", "llm", s.base.Add(2*time.Minute), 9000,
		map[string]string{"product": "support", "environment": "staging"})
	s.shipStats(s.base.Add(24 * time.Hour))

	buckets, err := s.store.Requests.Spend(s.ctx, "acme", "product", Daily,
		s.base, s.base.Add(24*time.Hour), 6, map[string]string{"environment": "production"}, UsageFilter{})
	s.Require().NoError(err)

	s.Require().Len(buckets, 1)
	s.Equal("support", buckets[0].Value)
	s.EqualValues(1000, buckets[0].CostMicrosTotal, "only the production request counts")
}

func (s *StoreSuite) TestSpendIsOnlyTheCallingCustomers() {
	s.spent("acme", "llm", s.base.Add(time.Minute), 1000, nil)
	s.spent("globex", "llm", s.base.Add(time.Minute), 9000, nil)
	s.shipStats(s.base.Add(24 * time.Hour))

	buckets, err := s.store.Requests.Spend(
		s.ctx, "acme", "modality", Daily, s.base, s.base.Add(24*time.Hour), 6, nil, UsageFilter{})
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(1000, buckets[0].CostMicrosTotal)
}

func (s *StoreSuite) TestTagKeysTellABreakdownFromAPieceOfContext() {
	// environment is on everything with one value, which is context. product is what the
	// spend actually splits by, and only one of the two is worth charting.
	s.spent("acme", "llm", s.base.Add(time.Minute), 6000,
		map[string]string{"product": "support", "environment": "production"})
	s.spent("acme", "llm", s.base.Add(2*time.Minute), 2000,
		map[string]string{"product": "sales", "environment": "production"})
	s.spent("acme", "llm", s.base.Add(3*time.Minute), 1000, nil)
	s.shipStats(s.base.Add(24 * time.Hour))

	keys, err := s.store.Requests.TagKeys(s.ctx, "acme", s.base, s.base.Add(24*time.Hour), nil)
	s.Require().NoError(err)
	s.Require().Len(keys, 2)

	byKey := map[string]TagKeySummary{}
	for _, key := range keys {
		byKey[key.Key] = key
	}

	product := byKey["product"]
	s.EqualValues(2, product.ValueCount)
	s.EqualValues(8000, product.CostMicrosTotal)
	s.EqualValues(2, product.RequestCount)
	s.InDelta(2.0/3.0, product.Coverage, 0.001, "one of the three requests carries no label at all")
	s.Require().Len(product.TopValues, 2)
	s.Equal("support", product.TopValues[0].Value, "biggest spend first")
	s.InDelta(0.75, product.TopValues[0].Share, 0.001)

	s.EqualValues(1, byKey["environment"].ValueCount, "one value is context, not a breakdown")
}

func (s *StoreSuite) TestTagKeysReportOnlyTheLargestValues() {
	for i := range 15 {
		s.spent("acme", "llm", s.base.Add(time.Duration(i)*time.Minute), int64(100*(i+1)),
			map[string]string{"customer_id": fmt.Sprintf("c-%02d", i)})
	}
	s.shipStats(s.base.Add(24 * time.Hour))

	keys, err := s.store.Requests.TagKeys(s.ctx, "acme", s.base, s.base.Add(24*time.Hour), nil)
	s.Require().NoError(err)
	s.Require().Len(keys, 1)
	s.EqualValues(15, keys[0].ValueCount, "the count is of every value, not of the ones listed")
	s.Len(keys[0].TopValues, 10)
	s.Equal("c-14", keys[0].TopValues[0].Value)
}

func (s *StoreSuite) TestTagKeysAreEmptyWhenNothingWasRecorded() {
	keys, err := s.store.Requests.TagKeys(s.ctx, "acme", s.base, s.base.Add(24*time.Hour), nil)
	s.Require().NoError(err)
	s.Empty(keys)
}

func (s *StoreSuite) TestRequestQueryBudgets() {
	single := s.usageRow("acme", func(request *Request) { request.AgentID = "agent"; request.UserID = "sam" })
	batch := []Request{
		s.usageRow("acme", func(request *Request) { request.AgentID = "agent" }),
		s.usageRow("acme", nil),
		s.usageRow("globex", func(request *Request) { request.AgentID = "agent" }),
	}

	// The rows, the grants they used, their calls read, the log rows under the customers' lock, and
	// the one transaction around them: the same for one row as for a batch.
	s.Equal(6, s.countQueries(func() { s.Require().NoError(insertRequest(s.ctx, s.store, &single)) }), "InsertMany of one row")
	s.Equal(6, s.countQueries(func() {
		_, err := s.store.Requests.InsertMany(s.ctx, batch)
		s.Require().NoError(err)
	}), "InsertMany")
	ids := []uuid.UUID{batch[0].UsageID, batch[1].UsageID}
	// The transaction, the shipping lock's bound and the update.
	s.Equal(4, s.countQueries(func() {
		s.Require().NoError(s.store.Requests.SetOrganization(s.ctx, ids, "org"))
	}), "SetOrganization")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.Requests.ToShip(s.ctx, 0, nil, 10)
		s.Require().NoError(err)
	}), "ToShip")
	// The transaction, the bound, the shipped rows and the refused ones.
	s.Equal(5, s.countQueries(func() {
		s.Require().NoError(s.store.Requests.Ship(s.ctx, ids[:1], map[uuid.UUID]string{ids[1]: "unknown price"}))
	}), "Ship")
	s.Equal(1, s.countQueries(func() {
		shipped, err := s.store.Requests.Shipped(s.ctx, ids)
		s.Require().NoError(err)
		s.Len(shipped, 1, "a refused row is not shipped")
	}), "Shipped")
	s.Equal(1, s.countQueries(func() {
		_, _, err := s.store.Requests.Unshipped(s.ctx)
		s.Require().NoError(err)
	}), "Unshipped")
	// Where to start, then one batch.
	s.Equal(2, s.countQueries(func() {
		queued, err := s.store.Requests.Reship(s.ctx, s.base.Add(-time.Hour), true)
		s.Equal(int64(1), queued)
		s.Require().NoError(err)
	}), "Reship")
	s.Equal(1, s.countQueries(func() {
		count, err := s.store.Requests.CountByUser(s.ctx, "acme", "sam")
		s.Require().NoError(err)
		s.Equal(int64(1), count)
	}), "CountByUser")
	// What is left of rows and of changes, and one batch.
	s.Equal(3, s.countQueries(func() {
		renamed, err := s.store.Requests.RenameUser(s.ctx, "acme", "sam", "someone")
		s.Require().NoError(err)
		s.Equal(int64(1), renamed)
	}), "RenameUser")
	s.shipStats(s.base.Add(time.Hour))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.Requests.Spend(s.ctx, "acme", "modality", Daily, s.base, s.base.Add(time.Hour), 6, nil, UsageFilter{})
		s.Require().NoError(err)
	}), "Spend")
	// The labels' values are read only when the customer has requests.
	s.Equal(2, s.countQueries(func() {
		_, err := s.store.Requests.TagKeys(s.ctx, "acme", s.base, s.base.Add(time.Hour), nil)
		s.Require().NoError(err)
	}), "TagKeys")
	s.Equal(1, s.countQueries(func() {
		counts, err := s.store.Requests.CountByModel(s.ctx, "llm", s.base.Add(-time.Hour))
		s.Require().NoError(err)
		s.Equal(int64(4), counts["openai/gpt"])
	}), "CountByModel")
	// A read-only transaction with its own statement timeout, on this shard alone.
	s.Equal(4, s.countQueries(func() {
		_, err := s.store.Requests.VendorCostGaps(s.ctx, s.base.Add(-time.Hour), 3, time.Minute)
		s.Require().NoError(err)
	}), "VendorCostGaps")
}

func (s *StoreSuite) TestRenamingAUserWithMoreRequestsThanOneStatementTakesRenamesThemAll() {
	s.inSmallBatches()
	for i := range 3 {
		s.Require().NoError(insertRequest(s.ctx, s.store, &Request{
			Modality: "stt", CustomerID: "acme", UserID: "jlahey", Provider: "deepgram", Model: "flux-general-en",
			StartedAt: s.base.Add(time.Duration(i) * time.Minute), Success: true,
		}))
	}

	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{
		Modality: "stt", CustomerID: "other-app", UserID: "jlahey", Provider: "deepgram", Model: "flux-general-en", StartedAt: s.base, Success: true,
	}))

	var renamed int64
	var err error
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
	updates := s.statementsLike(`WITH "changes" AS (UPDATE`, func() { renamed, err = s.store.Requests.RenameUser(ctx, "acme", "jlahey", "forgotten") })

	s.Require().NoError(err)
	s.EqualValues(3, renamed)
	s.Equal(3, updates, "a row per statement")
	theirs, err := s.store.Requests.CountByUser(s.ctx, "other-app", "jlahey")
	s.Require().NoError(err)
	s.EqualValues(1, theirs, "the same id under another app is another person")
	left, err := s.store.Requests.CountByUser(s.ctx, "acme", "jlahey")
	s.Require().NoError(err)
	s.Zero(left)
	moved, err := s.store.Requests.CountByUser(s.ctx, "acme", "forgotten")
	s.Require().NoError(err)
	s.EqualValues(3, moved)
}

func (s *StoreSuite) TestRenamingAUserToTheirOwnIdRewritesNothing() {
	s.inSmallBatches()
	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{
		Modality: "stt", CustomerID: "acme", UserID: "jlahey", Provider: "deepgram", Model: "flux-general-en", StartedAt: s.base, Success: true,
	}))
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()

	renamed, err := s.store.Requests.RenameUser(ctx, "acme", "jlahey", "jlahey")

	s.Require().NoError(err)
	s.Zero(renamed)
}
