//go:build integration

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// benchDSN is the database the benchmarks share, its schema dropped and migrated once per
// process so every run starts from the same empty tables.
var benchDSN = sync.OnceValues(func() (string, error) {
	own := testenv.Database(os.Getenv(DSNEnvVar), "bench")
	db, err := Open(own)
	if err != nil {
		return "", err
	}
	defer db.Close()
	ctx := context.Background()
	var database string
	if err := db.GlobalDB().QueryRowContext(ctx, "SELECT current_database()").Scan(&database); err != nil {
		return "", err
	}
	if !strings.HasSuffix(database, "_test") {
		return "", fmt.Errorf("refusing to drop the schema of %s, which is not a test database", database)
	}
	if _, err := db.GlobalDB().ExecContext(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public"); err != nil {
		return "", err
	}
	return own, db.Migrate(ctx)
})

// bench is a store on the benchmarks' database and Redis, counting what it sends each as
// budget_test.go's countQueries and countRedis do.
type bench struct {
	store      *Store
	ctx        context.Context
	customer   string
	queries    queryCount
	roundTrips roundTrips
}

// newBench opens a store for one benchmark, closed when it ends, with a customer of its own.
func newBench(b *testing.B) *bench {
	b.Helper()
	if os.Getenv(DSNEnvVar) == "" {
		b.Skipf("%s not set", DSNEnvVar)
	}
	if os.Getenv(redisAddressVar) == "" {
		b.Skipf("%s not set", redisAddressVar)
	}
	dsn, err := benchDSN()
	require.NoError(b, err)
	h := &bench{ctx: context.Background(), customer: "bench-" + newID()}
	h.store, err = OpenShards(Options{DSN: dsn, Redis: Redis{Address: os.Getenv(redisAddressVar)}, redisHook: &h.roundTrips})
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, h.store.Close()) })
	for _, db := range append([]*bun.DB{h.store.global}, h.store.peers...) {
		db.AddQueryHook(&h.queries)
	}
	return h
}

// run times op b.N times and reports the Postgres statements and Redis round trips each
// sent. prepare, when given, runs before each op off the clock and out of the counts.
func (h *bench) run(b *testing.B, prepare, op func(i int) error) {
	b.Helper()
	b.ReportAllocs()
	var skippedQueries, skippedTrips int64
	b.ResetTimer()
	queries, trips := h.queries.sent.Load(), h.roundTrips.sent.Load()
	for i := range b.N {
		if prepare != nil {
			b.StopTimer()
			q, t := h.queries.sent.Load(), h.roundTrips.sent.Load()
			if err := prepare(i); err != nil {
				b.Fatal(err)
			}
			skippedQueries += h.queries.sent.Load() - q
			skippedTrips += h.roundTrips.sent.Load() - t
			b.StartTimer()
		}
		if err := op(i); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(h.queries.sent.Load()-queries-skippedQueries)/float64(b.N), "queries/op")
	b.ReportMetric(float64(h.roundTrips.sent.Load()-trips-skippedTrips)/float64(b.N), "round-trips/op")
}

// forgetting is a prepare that drops a cached row, so the op after it reads Postgres.
func (h *bench) forgetting(name string) func(int) error {
	return func(int) error {
		h.store.forget(h.ctx, name)
		return nil
	}
}

// app stores an organization and an app in it under the bench's customer id.
func (h *bench) app(b *testing.B) App {
	org := Organization{Name: "bench"}
	require.NoError(b, h.store.Organizations.Insert(h.ctx, &org))
	app := App{ID: h.customer, OrganizationID: org.ID, Name: "bench"}
	require.NoError(b, h.store.Apps.Insert(h.ctx, &app))
	return app
}

// config stores an agent config of the bench's customer with instructions of a typical size.
func (h *bench) config(b *testing.B) AgentConfig {
	config := AgentConfig{
		CustomerID:   h.customer,
		Name:         "agent-" + newID(),
		Instructions: strings.Repeat("Answer the caller's questions about their order briefly. ", 40),
		Skills:       []string{"refunds", "shipping"},
	}
	require.NoError(b, h.store.AgentConfigs.Insert(h.ctx, &config))
	return config
}

// deleteAfter deletes the bench customer's rows of table once the benchmark ends, for the
// queues a claim reads across customers.
func (h *bench) deleteAfter(b *testing.B, table string) {
	b.Cleanup(func() {
		_, err := h.store.GlobalDB().ExecContext(h.ctx, "DELETE FROM "+table+" WHERE customer_id = ?", h.customer)
		require.NoError(b, err)
	})
}

func BenchmarkAPIKeysGetLive_Cold(b *testing.B) {
	h := newBench(b)
	id := "vak_live_" + newID()
	key := APIKey{ID: id, AppID: h.app(b).ID, Name: "ci", Env: "live", Sealed: []byte("sealed"), KEKVersion: 1, Last4: "cret"}
	require.NoError(b, h.store.APIKeys.Insert(h.ctx, &key))
	h.run(b, h.forgetting(cacheKey("key", id)), func(int) error {
		_, err := h.store.APIKeys.GetLive(h.ctx, id)
		return err
	})
}

func BenchmarkAPIKeysGetLive_Cached(b *testing.B) {
	h := newBench(b)
	id := "vak_live_" + newID()
	key := APIKey{ID: id, AppID: h.app(b).ID, Name: "ci", Env: "live", Sealed: []byte("sealed"), KEKVersion: 1, Last4: "cret"}
	require.NoError(b, h.store.APIKeys.Insert(h.ctx, &key))
	_, err := h.store.APIKeys.GetLive(h.ctx, id)
	require.NoError(b, err)
	h.run(b, nil, func(int) error {
		_, err := h.store.APIKeys.GetLive(h.ctx, id)
		return err
	})
}

func BenchmarkAppsGetSettings_Cached(b *testing.B) {
	h := newBench(b)
	app := h.app(b)
	_, err := h.store.Apps.GetSettings(h.ctx, app.ID)
	require.NoError(b, err)
	h.run(b, nil, func(int) error {
		_, err := h.store.Apps.GetSettings(h.ctx, app.ID)
		return err
	})
}

func BenchmarkPoliciesGet_Cached(b *testing.B) {
	h := newBench(b)
	app := h.app(b)
	training := false
	require.NoError(b, h.store.Policies.Upsert(h.ctx, &Policy{Scope: ScopeApp, ScopeID: app.ID, Document: PolicyDocument{
		DataPolicy: PolicyDataPolicy{AllowTraining: &training},
		Tags:       map[string]string{"team": "support"},
	}}))
	_, err := h.store.Policies.Get(h.ctx, ScopeApp, app.ID)
	require.NoError(b, err)
	h.run(b, nil, func(int) error {
		_, err := h.store.Policies.Get(h.ctx, ScopeApp, app.ID)
		return err
	})
}

func BenchmarkProviderKeysSelectAll_Cached(b *testing.B) {
	h := newBench(b)
	for _, provider := range []string{"anthropic", "deepgram", "openai"} {
		_, _, err := h.store.ProviderKeys.Upsert(h.ctx, &ProviderKey{
			CustomerID: h.customer, Provider: provider, Region: "us", APIKeySealed: []byte("sealed-" + provider), KEKVersion: 1, KeyHint: "abcd",
		})
		require.NoError(b, err)
	}
	_, err := h.store.ProviderKeys.SelectAll(h.ctx, h.customer)
	require.NoError(b, err)
	h.run(b, nil, func(int) error {
		_, err := h.store.ProviderKeys.SelectAll(h.ctx, h.customer)
		return err
	})
}

func BenchmarkAgentConfigsGet_Cold(b *testing.B) {
	h := newBench(b)
	config := h.config(b)
	h.run(b, h.forgetting(agentConfigKey(h.customer, config.ID)), func(int) error {
		_, err := h.store.AgentConfigs.Get(h.ctx, h.customer, config.ID)
		return err
	})
}

func BenchmarkAgentConfigsGet_Cached(b *testing.B) {
	h := newBench(b)
	config := h.config(b)
	_, err := h.store.AgentConfigs.Get(h.ctx, h.customer, config.ID)
	require.NoError(b, err)
	h.run(b, nil, func(int) error {
		_, err := h.store.AgentConfigs.Get(h.ctx, h.customer, config.ID)
		return err
	})
}

func BenchmarkAgentConfigsGetByName_Cold(b *testing.B) {
	h := newBench(b)
	config := h.config(b)
	h.run(b, h.forgetting(agentConfigNameKey(h.customer, config.Name)), func(int) error {
		_, err := h.store.AgentConfigs.GetByName(h.ctx, h.customer, config.Name)
		return err
	})
}

func BenchmarkAgentConfigsGetByName_Cached(b *testing.B) {
	h := newBench(b)
	config := h.config(b)
	_, err := h.store.AgentConfigs.GetByName(h.ctx, h.customer, config.Name)
	require.NoError(b, err)
	h.run(b, nil, func(int) error {
		_, err := h.store.AgentConfigs.GetByName(h.ctx, h.customer, config.Name)
		return err
	})
}

func BenchmarkSkillsGetByNames_Cached(b *testing.B) {
	h := newBench(b)
	config := h.config(b)
	for _, name := range []string{"refunds", "shipping", "returns", "warranty", "billing"} {
		require.NoError(b, h.store.Skills.Insert(h.ctx, &Skill{
			CustomerID: h.customer, ConfigID: config.ID, Name: name, Description: "Handles " + name,
			Instructions: strings.Repeat("Look the order up and explain what happens next. ", 20),
			Files:        map[string]string{"reference.md": "# " + name},
		}))
	}
	names := []string{"refunds", "shipping"}
	_, err := h.store.Skills.GetByNames(h.ctx, h.customer, config.ID, names)
	require.NoError(b, err)
	h.run(b, nil, func(int) error {
		_, err := h.store.Skills.GetByNames(h.ctx, h.customer, config.ID, names)
		return err
	})
}

func BenchmarkUsersSeen_Held(b *testing.B) {
	h := newBench(b)
	require.NoError(b, h.store.Users.Seen(h.ctx, h.customer, "jlahey", UserKindAuthenticated))
	h.run(b, nil, func(int) error {
		return h.store.Users.Seen(h.ctx, h.customer, "jlahey", UserKindAuthenticated)
	})
}

// BenchmarkUsersSeen_OtherReplica is a user another replica wrote down: in Redis, not in this
// process's LRU.
func BenchmarkUsersSeen_OtherReplica(b *testing.B) {
	h := newBench(b)
	client := h.store.sharedRedis()
	for start := 0; start < b.N; start += 1000 {
		commands := make(rueidis.Commands, 0, 1000)
		for i := start; i < min(start+1000, b.N); i++ {
			commands = append(commands, client.B().Set().Key(userKey(h.customer, fmt.Sprint("user-", i))).
				Value(UserKindAuthenticated).Ex(userTTL).Build())
		}
		for _, reply := range client.DoMulti(h.ctx, commands...) {
			require.NoError(b, reply.Error())
		}
	}
	h.run(b, nil, func(i int) error {
		return h.store.Users.Seen(h.ctx, h.customer, fmt.Sprint("user-", i), UserKindAuthenticated)
	})
}

func BenchmarkUsersSeen_New(b *testing.B) {
	h := newBench(b)
	h.run(b, nil, func(i int) error {
		return h.store.Users.Seen(h.ctx, h.customer, fmt.Sprint("user-", i), UserKindAuthenticated)
	})
}

// request is a model request an agent's session made, as the router records one.
func (h *bench) request() Request {
	latency := 412.0
	return Request{
		UsageID: uuid.Must(uuid.NewV7()), Modality: "llm", CustomerID: h.customer, AgentID: "agent-1", SessionID: "session-1",
		Provider: "openai", Model: "gpt-5.6-sol", StartedAt: time.Now().UTC(), LatencyMs: &latency,
		InputTokens: 1200, CachedInputTokens: 800, OutputTokens: 90, CostMicros: 1400, Success: true,
	}
}

func BenchmarkRequestsInsert(b *testing.B) {
	h := newBench(b)
	h.run(b, nil, func(int) error {
		request := h.request()
		return insertRequest(h.ctx, h.store, &request)
	})
}

func BenchmarkRequestsInsertMany_50(b *testing.B) {
	h := newBench(b)
	h.run(b, nil, func(int) error {
		requests := make([]Request, 50)
		for i := range requests {
			requests[i] = h.request()
		}
		_, err := h.store.Requests.InsertMany(h.ctx, requests)
		return err
	})
}

// BenchmarkAgentLogsInsert_Batch50 writes 50 of a session's log rows the way its recorder
// drains them: one Insert each, as there is no batch write.
func BenchmarkAgentLogsInsert_Batch50(b *testing.B) {
	h := newBench(b)
	h.run(b, nil, func(int) error {
		for i := range 50 {
			err := h.store.AgentLogs.Insert(h.ctx, &AgentLog{
				CustomerID: h.customer, ConfigID: "config-1", AgentID: "agent-1", SessionID: "session-1",
				Source: "agent", Severity: "info", EventType: "agent_message",
				Message: "Your order shipped yesterday and should arrive on Friday.",
				Details: map[string]any{"turn_id": fmt.Sprint("turn-", i)},
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// session is a session of the bench's customer, as the manager opens one.
func (h *bench) session(id string) *AgentSession {
	return &AgentSession{
		ID: id, CustomerID: h.customer, AgentID: "agent-" + id, AgentName: "support", ConfigID: "config-1",
		UserID: "jlahey", CallerKind: "user", Title: "Where is my order",
	}
}

func BenchmarkSessionsUpsert_New(b *testing.B) {
	h := newBench(b)
	h.run(b, nil, func(int) error {
		return h.store.Sessions.Upsert(h.ctx, h.session(newID()))
	})
}

func BenchmarkSessionsUpsert_Existing(b *testing.B) {
	h := newBench(b)
	id := newID()
	require.NoError(b, h.store.Sessions.Upsert(h.ctx, h.session(id)))
	h.run(b, nil, func(int) error {
		return h.store.Sessions.Upsert(h.ctx, h.session(id))
	})
}

func BenchmarkSessionsGet(b *testing.B) {
	h := newBench(b)
	id := newID()
	require.NoError(b, h.store.Sessions.Upsert(h.ctx, h.session(id)))
	h.run(b, nil, func(int) error {
		_, err := h.store.Sessions.Get(h.ctx, h.customer, id)
		return err
	})
}

// call is a call of the bench's customer on Stream call callID.
func (h *bench) call(sessionID, callID string, at time.Time) *Call {
	return &Call{
		ID: newID(), SessionID: sessionID, CustomerID: h.customer, CallType: "default", CallID: callID,
		AgentID: "agent-" + callID, StartedAt: at, STT: "deepgram/flux-general-en", TTS: "cartesia/sonic-3",
		LLM: "openai/gpt-5.6-sol", Skills: []string{"refunds"},
	}
}

func BenchmarkCallsStart(b *testing.B) {
	h := newBench(b)
	h.run(b, nil, func(int) error {
		return h.store.Calls.Start(h.ctx, h.call("", newID(), time.Now()))
	})
}

// BenchmarkCallsLatest reads the latest call of a page of 20 sessions, two calls each.
func BenchmarkCallsLatest(b *testing.B) {
	h := newBench(b)
	ids := make([]string, 20)
	for i := range ids {
		ids[i] = newID()
		require.NoError(b, h.store.Sessions.Upsert(h.ctx, h.session(ids[i])))
		for c := range 2 {
			require.NoError(b, h.store.Calls.Start(h.ctx, h.call(ids[i], newID(), time.Now().Add(time.Duration(c)*time.Minute))))
		}
	}
	h.run(b, nil, func(int) error {
		_, err := h.store.Calls.Latest(h.ctx, h.customer, ids)
		return err
	})
}

// BenchmarkCallsSelect reads the newest page of a customer holding 200 calls.
func BenchmarkCallsSelect(b *testing.B) {
	h := newBench(b)
	for i := range 200 {
		require.NoError(b, h.store.Calls.Start(h.ctx, h.call("", newID(), time.Now().Add(-time.Duration(i)*time.Minute))))
	}
	h.run(b, nil, func(int) error {
		_, err := h.store.Calls.Select(h.ctx, h.customer, CallFilter{})
		return err
	})
}

func BenchmarkCallLegsOpen(b *testing.B) {
	h := newBench(b)
	h.run(b, nil, func(int) error {
		id := newID()
		_, err := h.store.CallLegs.Open(h.ctx, &CallLeg{
			CustomerID: h.customer, OrganizationID: "org", APIKey: "key", CallType: "default", CallID: "phone-" + id,
			StreamSessionID: id, Vendor: "telnyx", Direction: Inbound, LocalNumber: "+15125550100",
			RemoteNumber: "+15125550199", Country: "US",
		})
		return err
	})
}

// BenchmarkCallLegsSelectPlaced reads the placed leg of one call, as Calls.Start does, from a
// customer that placed 20.
func BenchmarkCallLegsSelectPlaced(b *testing.B) {
	h := newBench(b)
	for i := range 20 {
		placed := time.Now().UTC()
		require.NoError(b, h.store.CallLegs.Place(h.ctx, &CallLeg{
			CustomerID: h.customer, OrganizationID: "org", APIKey: "key", CallType: "default", CallID: fmt.Sprint("call-", i),
			Vendor: "telnyx", Direction: Outbound, LocalNumber: "+15125550100", RemoteNumber: "+15125550199",
			Country: "US", Token: newID(), PlacedAt: &placed,
		}))
	}
	calls := []string{"call-7"}
	h.run(b, nil, func(int) error {
		_, err := h.store.CallLegs.SelectPlaced(h.ctx, h.customer, calls)
		return err
	})
}

// turn is one of an agent's turns, timed as a cascaded pipeline reports one.
func (h *bench) turn(agentID string, at time.Time) Turn {
	ms := func(v float64) *float64 { return &v }
	return Turn{
		CustomerID: h.customer, AgentID: agentID, CallID: "call-1", TurnID: newID(), StartedAt: at,
		STTLatencyMs: ms(180), LLMTTFTMs: ms(320), TTSTTFBMs: ms(140), RoundtripMs: ms(690),
		SpeechEndToAudioMs: ms(910), AudioOutMs: ms(4200),
	}
}

func BenchmarkTurnsInsert(b *testing.B) {
	h := newBench(b)
	h.run(b, nil, func(int) error {
		return h.store.Turns.Insert(h.ctx, h.turn("agent-1", time.Now()))
	})
}

// BenchmarkTurnsSelect reads the 50 turns of a call.
func BenchmarkTurnsSelect(b *testing.B) {
	h := newBench(b)
	start := time.Now().Add(-30 * time.Minute)
	for i := range 50 {
		require.NoError(b, h.store.Turns.Insert(h.ctx, h.turn("agent-1", start.Add(time.Duration(i)*10*time.Second))))
	}
	h.run(b, nil, func(int) error {
		_, err := h.store.Turns.Select(h.ctx, h.customer, "agent-1", start, nil)
		return err
	})
}

// callEvents are n judgements made on one call.
func (h *bench) callEvents(n int) []CallEvent {
	events := make([]CallEvent, n)
	for i := range events {
		events[i] = CallEvent{
			CustomerID: h.customer, CallID: "call-1", AgentID: "agent-1", At: time.Now().UTC(),
			Kind: "waited", Reason: "the caller paused mid-sentence", TurnID: fmt.Sprint("turn-", i), Said: "I wanted to ask about",
		}
	}
	return events
}

func BenchmarkCallEventsInsertMany_10(b *testing.B) {
	h := newBench(b)
	h.run(b, nil, func(int) error {
		return h.store.CallEvents.InsertMany(h.ctx, h.callEvents(10))
	})
}

// BenchmarkCallEventsSelect reads a call's 200 judgements.
func BenchmarkCallEventsSelect(b *testing.B) {
	h := newBench(b)
	require.NoError(b, h.store.CallEvents.InsertMany(h.ctx, h.callEvents(200)))
	from := time.Now().Add(-time.Hour)
	h.run(b, nil, func(int) error {
		_, err := h.store.CallEvents.Select(h.ctx, h.customer, "call-1", from, nil, 0)
		return err
	})
}

// BenchmarkScheduledTasksClaim claims the 10 tasks due, with the ones claimed before finished
// off the clock, as a worker settles them.
func BenchmarkScheduledTasksClaim(b *testing.B) {
	h := newBench(b)
	h.deleteAfter(b, "scheduled_tasks")
	due := func(int) error {
		_, err := h.store.GlobalDB().NewUpdate().Model((*ScheduledTask)(nil)).
			Set("state = ?", ScheduledTaskCompleted).Set("next_run_at = NULL").
			Where("customer_id = ?", h.customer).Where("state = ?", ScheduledTaskActive).
			Exec(h.ctx)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		next := now.Add(-time.Minute)
		tasks := make([]ScheduledTask, 10)
		for i := range tasks {
			tasks[i] = ScheduledTask{
				ID: newID(), CustomerID: h.customer, ConfigID: "config-1", UserID: "jlahey", CallerKind: "authenticated",
				Title: "Stretch", Instructions: "Remind them to stretch.", StartLocal: next, Timezone: "UTC",
				NextRunAt: &next, State: ScheduledTaskActive, Channels: []string{ScheduledTaskDeliveredConversation},
				CreatedBy: "api", CreatedAt: now, UpdatedAt: now,
			}
		}
		_, err = h.store.GlobalDB().NewInsert().Model(&tasks).Exec(h.ctx)
		return err
	}
	h.run(b, due, func(int) error {
		_, err := h.store.ScheduledTasks.Claim(h.ctx, 10, time.Minute, ShardClaim{})
		return err
	})
}

// BenchmarkChatInboundsClaim claims 10 of 100 queued messages. Each claim's lease ends as it
// is taken, so the next claim finds the queue as deep.
func BenchmarkChatInboundsClaim(b *testing.B) {
	h := newBench(b)
	h.deleteAfter(b, "chat_inbound")
	for i := range 100 {
		id := newID()
		queued, err := h.store.ChatInbounds.Queue(h.ctx, ChatInbound{
			StreamAppPK: 1257545, MessageID: id, CustomerID: h.customer, APIKey: "key-one",
			Body:          []byte(`{"type":"message.new","message":{"id":"` + id + `","text":"where is my order"}}`),
			NextAttemptAt: time.Now().Add(-time.Duration(i) * time.Second),
		})
		require.NoError(b, err)
		require.True(b, queued)
	}
	h.run(b, nil, func(int) error {
		_, err := h.store.ChatInbounds.Claim(h.ctx, 10, 0)
		return err
	})
}

// BenchmarkImageJobsClaim claims 10 of 100 jobs to draw, leased as BenchmarkChatInboundsClaim's.
func BenchmarkImageJobsClaim(b *testing.B) {
	h := newBench(b)
	h.deleteAfter(b, "image_jobs")
	attribution, err := json.Marshal(map[string]string{"api_key": "key-one"})
	require.NoError(b, err)
	for i := range 100 {
		id := newID()
		queued, err := h.store.ImageJobs.Queue(h.ctx, &ImageJob{
			ID: id, CustomerID: h.customer, StreamAppPK: 1257545, CID: "messaging:bench", AgentUser: "agent",
			SessionID: "session-1", TurnID: "turn-1", ToolCallID: id, RequestKey: id, AgentID: "agent-1",
			Target: "openai/gpt-image-2", Attribution: attribution, Prompt: "a red bicycle", AspectRatio: "1:1",
			NextAttemptAt: time.Now().Add(-time.Duration(i) * time.Second),
		})
		require.NoError(b, err)
		require.True(b, queued)
	}
	h.run(b, nil, func(int) error {
		_, err := h.store.ImageJobs.Claim(h.ctx, 10, 0, false)
		return err
	})
}

// contact maps a phone number of the bench's customer to an omni-channel.
func (h *bench) contact(b *testing.B, address string) ContactMapEntry {
	entry := ContactMapEntry{
		CustomerID: h.customer, ConfigID: "config-1", Kind: ContactPhone, Address: address,
		ConversationID: "agent:omni-" + newID(),
	}
	_, err := h.store.ContactMapEntries.Map(h.ctx, &entry)
	require.NoError(b, err)
	return entry
}

// threadEpisode is the episode a message on an SMS thread opens.
func (h *bench) threadEpisode(contact ContactMapEntry, thread string) *Episode {
	return &Episode{CustomerID: h.customer, ContactID: contact.ID, Source: EpisodeSMS, ThreadChannel: thread}
}

// BenchmarkEpisodesOpen_Existing is a message on a thread whose episode is open.
func BenchmarkEpisodesOpen_Existing(b *testing.B) {
	h := newBench(b)
	contact := h.contact(b, "+15125550199")
	_, err := h.store.Episodes.Open(h.ctx, h.threadEpisode(contact, "agent:thread-1"))
	require.NoError(b, err)
	h.run(b, nil, func(int) error {
		_, err := h.store.Episodes.Open(h.ctx, h.threadEpisode(contact, "agent:thread-1"))
		return err
	})
}

func BenchmarkEpisodesOpen_New(b *testing.B) {
	h := newBench(b)
	contact := h.contact(b, "+15125550199")
	h.run(b, nil, func(int) error {
		_, err := h.store.Episodes.Open(h.ctx, h.threadEpisode(contact, "agent:thread-"+newID()))
		return err
	})
}

func BenchmarkContactMapEntriesMap_Existing(b *testing.B) {
	h := newBench(b)
	h.contact(b, "+15125550199")
	h.run(b, nil, func(int) error {
		_, err := h.store.ContactMapEntries.Map(h.ctx, &ContactMapEntry{
			CustomerID: h.customer, ConfigID: "config-1", Kind: ContactPhone, Address: "+15125550199",
			ConversationID: "agent:omni-" + newID(),
		})
		return err
	})
}

func BenchmarkContactMapEntriesMap_New(b *testing.B) {
	h := newBench(b)
	h.run(b, nil, func(i int) error {
		_, err := h.store.ContactMapEntries.Map(h.ctx, &ContactMapEntry{
			CustomerID: h.customer, ConfigID: "config-1", Kind: ContactPhone, Address: fmt.Sprintf("+1512%07d", i),
			ConversationID: "agent:omni-" + newID(),
		})
		return err
	})
}

// BenchmarkChannelThreadsLink_Existing is a message on a linked thread, which touches its
// last inbound time.
func BenchmarkChannelThreadsLink_Existing(b *testing.B) {
	h := newBench(b)
	thread := func() *ChannelThread {
		return &ChannelThread{
			ChannelID: "thread-" + newID(), CustomerID: h.customer, ConnectorID: "slack", ProviderUnitID: "T0001",
			ThreadKey: "C0001:1700000000.000100", ConnectionID: "connection-1",
			ThreadParts: map[string]string{"channel": "C0001", "ts": "1700000000.000100"},
		}
	}
	_, err := h.store.ChannelThreads.Link(h.ctx, thread())
	require.NoError(b, err)
	h.run(b, nil, func(int) error {
		_, err := h.store.ChannelThreads.Link(h.ctx, thread())
		return err
	})
}

func BenchmarkConnectorConnectionsGet(b *testing.B) {
	h := newBench(b)
	require.NoError(b, h.store.ConnectorDefinitions.Seed(h.ctx, fstest.MapFS{"acme.yaml": {Data: []byte(acmeManifest)}}))
	connection := appConnection()
	connection.CustomerID = h.customer
	require.NoError(b, h.store.ConnectorConnections.Insert(h.ctx, testSchemes, connection))
	h.run(b, nil, func(int) error {
		_, err := h.store.ConnectorConnections.Get(h.ctx, h.customer, connection.ID)
		return err
	})
}
