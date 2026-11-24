//go:build integration

package store

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

// seedRows is how many rows each hot table is seeded with, spread over 200 customers, so a
// plan that reads a whole table costs more than one that reads a customer's slice of it.
const seedRows = 100_000

// hotTables are the tables that grow with traffic, which no statement may read in full.
var hotTables = regexp.MustCompile(`\b(requests|agent_sessions|calls|call_legs|agent_logs|image_jobs)\b`)

// seqScans finds a sequential scan of one of hotTables in a plan.
var seqScans = regexp.MustCompile(`Seq Scan on (requests|agent_sessions|calls|call_legs|agent_logs|image_jobs)\b`)

// seededDatabase fills a database of its own with seedRows of each hot table, once per run of the
// suite: the suite's databases are emptied before every test, and seeding them for each would
// take minutes. Row n belongs to customer n % 200, as do agent n % 2000, user n % 20000 and
// session n, so the rows that point at each other agree on whose they are. They are spread
// over the 90 days before s.base.
func (s *StoreSuite) seededDatabase() string {
	if s.seededDSN != "" {
		return s.seededDSN
	}
	dsn := s.emptyDatabase("store_explain")
	// Seeding is setup, not the store's own work: its inserts of seedRows may run past the
	// serving statement limit and the driver's read timeout, which the DSN lifts for it.
	parsed, err := url.Parse(dsn)
	s.Require().NoError(err)
	query := parsed.Query()
	query.Set("statement_timeout", "0")
	query.Set("read_timeout", "2m")
	parsed.RawQuery = query.Encode()
	store, err := Open(parsed.String())
	s.Require().NoError(err)
	defer store.Close()
	s.Require().NoError(store.Migrate(s.ctx))
	s.Require().NoError(store.global.RunInTx(s.ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// Neither the change log's triggers nor the request log's, which would double the
		// rows written, and no foreign key checks: every row seeded points at a seeded one.
		for _, statement := range append([]string{"SET LOCAL session_replication_role = replica"}, seedStatements...) {
			if _, err := tx.ExecContext(ctx, statement, s.base, seedRows); err != nil {
				return err
			}
		}
		// Statistics read off every row rather than a sample, so the planner breaks a tie
		// between two indexes the same way every run.
		_, err := tx.ExecContext(ctx, "SET LOCAL default_statistics_target = 1000; "+
			"ANALYZE agent_sessions, calls, call_legs, requests, agent_logs, image_jobs")
		return err
	}))
	s.seededDSN = dsn
	return dsn
}

// seedStatements each take the time the rows end at and how many to write.
var seedStatements = []string{
	`INSERT INTO agent_sessions (id, customer_id, config_id, agent_name, agent_id, conversation_id, user_id,
		caller_kind, api_key, team, title, project, state, modality, created_at, updated_at, closed_at,
		last_response_at, pinned_at, review_score, stream_app_pk)
	SELECT 'seeded-session-' || n, 'customer-' || (n % 200), 'config-' || (n % 2000), 'agent ' || (n % 2000),
		'agent-' || (n % 2000), 'conversation-' || (n % 50000), 'user-' || (n % 20000),
		CASE WHEN n % 10 = 0 THEN 'anonymous' ELSE 'user' END, 'key-' || (n % 1000), 'team-' || (n % 400),
		'call ' || n, 'project-' || (n % 600), CASE WHEN n % 100 = 0 THEN 'running' ELSE 'closed' END,
		(ARRAY['text', 'voice', 'video'])[n % 3 + 1], at, at + interval '10 minutes',
		CASE WHEN n % 100 <> 0 THEN at + interval '10 minutes' END, at + interval '9 minutes',
		CASE WHEN n % 500 = 0 THEN at END, CASE WHEN n % 20 = 0 THEN n % 5 + 1 END,
		CASE WHEN n % 7 <> 0 THEN 1 + n % 3 END
	FROM generate_series(1, ?1) AS n, LATERAL (SELECT ?0::timestamptz - (?1 - n) * interval '77 seconds' AS at) AS t`,

	`INSERT INTO calls (id, session_id, customer_id, stream_app_pk, call_type, call_id, agent_id, conversation_id,
		campaign_id, direction, started_at, ended_at, llm, review_score)
	SELECT 'seeded-call-' || n, 'seeded-session-' || n, 'customer-' || (n % 200),
		CASE WHEN n % 7 <> 0 THEN 1 + n % 3 END, 'default', CASE WHEN n % 3 = 0 THEN '' ELSE 'stream-' || n END,
		'agent-' || (n % 2000), 'conversation-' || (n % 50000), CASE WHEN n % 10 = 0 THEN 'campaign-' || (n % 30) END,
		CASE WHEN n % 2 = 0 THEN 'outbound' ELSE 'inbound' END, at, CASE WHEN n % 100 <> 0 THEN at + interval '5 minutes' END,
		'gpt', CASE WHEN n % 20 = 0 THEN n % 5 + 1 END
	FROM generate_series(1, ?1) AS n, LATERAL (SELECT ?0::timestamptz - (?1 - n) * interval '77 seconds' AS at) AS t`,

	`INSERT INTO call_legs (customer_id, organization_id, api_key, user_id, stream_app_pk, call_type, call_id,
		stream_session_id, participant_session, vendor, vendor_call_id, direction, local_number, remote_number,
		agent_id, dialed_at, started_at, ended_at, end_reason, token_hash, placed_at, outcome, decided_at)
	SELECT 'customer-' || (n % 200), 'org-' || (n % 100), 'key-' || (n % 1000), 'user-' || (n % 20000),
		CASE WHEN n % 7 <> 0 THEN 1 + n % 3 END, 'default', 'stream-' || n, 'sfu-' || n,
		CASE WHEN n % 50 = 1 OR n % 100 BETWEEN 2 AND 9 THEN '' ELSE 'phone-' || n END, 'telnyx',
		CASE WHEN n % 2 = 0 THEN 'vendor-' || n END, CASE WHEN n % 2 = 0 THEN 'outbound' ELSE 'inbound' END,
		'+1555' || lpad((n % 1000)::text, 7, '0'), '+1666' || lpad(n::text, 7, '0'), 'agent-' || (n % 2000),
		at, CASE WHEN n % 50 <> 1 AND n % 100 NOT BETWEEN 2 AND 9 THEN at + interval '5 seconds' END,
		CASE WHEN n % 100 NOT IN (0, 1) THEN at + interval '5 minutes' END,
		CASE WHEN n % 100 IN (0, 1) THEN '' WHEN n % 100 < 10 THEN 'unanswered' ELSE 'left' END,
		CASE WHEN n % 2 = 0 AND n % 100 IN (0, 2) THEN encode(sha256(convert_to('token-' || n, 'UTF8')), 'hex') END, CASE WHEN n % 2 = 0 THEN at END,
		CASE WHEN n % 2 = 0 AND n % 100 > 10 THEN 'completed' END, CASE WHEN n % 2 = 0 AND n % 100 > 10 THEN at + interval '5 minutes' END
	FROM generate_series(1, ?1) AS n, LATERAL (SELECT ?0::timestamptz - (?1 - n) * interval '77 seconds' AS at) AS t`,

	`INSERT INTO requests (modality, customer_id, organization_id, api_key, user_id, agent_id, session_id, call_id,
		turn_id, provider, model, started_at, success, latency_ms, input_tokens, output_tokens, cost_micros, billable,
		usage_categories, limit_grants, to_ship, shipped_at, tags, usage_id)
	SELECT (ARRAY['llm', 'stt', 'tts', 'runtime'])[n % 4 + 1], 'customer-' || (s % 200), 'org-' || (s % 100),
		'key-' || (s % 1000), 'user-' || (s % 20000), 'agent-' || (s % 2000), 'seeded-session-' || s,
		'stream-' || s, 'turn-' || n, 'provider-' || (n % 5), 'model-' || (n % 20), at, n % 50 <> 0, 100 + n % 900,
		n % 1000, n % 300, n % 5000, n % 9 <> 0, CASE WHEN n % 5 = 0 THEN ARRAY['team:team-' || (s % 400)] ELSE '{}' END,
		CASE WHEN n % 100 = 0 THEN ARRAY[1 + n % 10]::bigint[] ELSE '{}' END, n % 1000 = 0,
		CASE WHEN n % 1000 <> 0 THEN at + interval '1 minute' END,
		CASE WHEN n % 3 = 0 THEN jsonb_build_object('project', 'project-' || (s % 600)) ELSE '{}' END,
		md5('usage-' || n)::uuid
	FROM generate_series(1, ?1) AS n,
		LATERAL (SELECT n % 25000 + 1 AS s, ?0::timestamptz - (?1 - n) * interval '77 seconds' AS at) AS t`,

	`INSERT INTO agent_logs (customer_id, config_id, agent_id, session_id, user_id, source, severity, event_type,
		message, occurred_at, ingested_at, source_id)
	SELECT 'customer-' || (n % 200), 'config-' || (n % 2000), 'agent-' || (n % 2000), 'seeded-session-' || n,
		'user-' || (n % 20000), (ARRAY['system', 'agent', 'tool'])[n % 3 + 1],
		(ARRAY['info', 'info', 'info', 'warn', 'error'])[n % 5 + 1], 'provider_request', 'request ' || n, at, at,
		CASE WHEN n % 50 = 0 THEN n END
	FROM generate_series(1, ?1) AS n, LATERAL (SELECT ?0::timestamptz - (?1 - n) * interval '77 seconds' AS at) AS t`,

	`INSERT INTO image_jobs (id, customer_id, cid, agent_user, session_id, tool_call_id, request_key, target,
		attribution, prompt, state, message, created_at, next_attempt_at, finished_at)
	SELECT 'seeded-image-' || n, 'customer-' || (n % 200), 'messaging:channel-' || (n % 50000), 'agent-' || (n % 2000),
		'seeded-session-' || n, 'tool-call-' || n, 'request-' || n, 'model-' || (n % 5), '{}', 'draw ' || n,
		CASE WHEN n % 100 = 0 THEN 'queued' WHEN n % 50 = 0 THEN 'failed' ELSE 'done' END,
		CASE WHEN n % 200 = 0 THEN 'drawn' ELSE '' END, at, at, CASE WHEN n % 100 <> 0 THEN at + interval '1 minute' END
	FROM generate_series(1, ?1) AS n, LATERAL (SELECT ?0::timestamptz - (?1 - n) * interval '77 seconds' AS at) AS t`,

	// Two apps seen under org-7, whose requests the seed names it for, so a read of an
	// organization's apps sums more than one.
	`INSERT INTO app_organizations (app_id, organization_id, seen_at)
	VALUES ('customer-7', 'org-7', ?0), ('customer-107', 'org-7', ?0)`,
}

// explained runs read on a store of the seeded database and answers the plan of each
// statement it sent that names a hot table, on either handle.
func (s *StoreSuite) explained(read func(*Store)) []string {
	return s.explainedOn(Options{DSN: s.seededDatabase()}, read)
}

// explainedSharded is explained on a store whose shard is a handle of its own on the same
// database, for the reads only a sharded store makes.
func (s *StoreSuite) explainedSharded(read func(*Store)) []string {
	dsn := s.seededDatabase()
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	return s.explainedOn(Options{DSN: dsn, ShardDSN: dsn + separator + "application_name=shard"}, read)
}

// sentQueries keeps every statement sent, from the goroutines everyShard reads shards on too.
type sentQueries struct {
	mu   sync.Mutex
	sent []string
}

func (*sentQueries) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context { return ctx }
func (q *sentQueries) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.sent = append(q.sent, event.Query)
}

func (s *StoreSuite) explainedOn(options Options, read func(*Store)) []string {
	watched, err := OpenShards(options)
	s.Require().NoError(err)
	defer watched.Close()
	sent := &sentQueries{}
	watched.global.AddQueryHook(sent)
	if watched.Sharded() {
		watched.shard.AddQueryHook(sent)
	}
	read(watched)

	var plans []string
	for _, query := range sent.sent {
		if !hotTables.MatchString(query) {
			continue
		}
		var plan []string
		s.Require().NoError(watched.global.NewRaw("EXPLAIN "+query).Scan(context.Background(), &plan), query)
		plans = append(plans, strings.Join(plan, "\n"))
	}
	s.Require().NotEmpty(plans, "nothing read a hot table")
	return plans
}

// usesIndex checks no plan reads a hot table in full, and that one reads index.
func (s *StoreSuite) usesIndex(plans []string, index string) {
	for _, plan := range plans {
		s.NotRegexp(seqScans, plan, plan)
	}
	s.Regexp(`\b`+index+`\b`, strings.Join(plans, "\n"), plans)
}

// explainedRead is one repository method that reads, updates or deletes a hot table, run on
// rows the seed wrote, and the index its statements read that table by.
type explainedRead struct {
	name, index string
	// sharded runs it on a store with a shard of its own, for what only such a store reads.
	sharded bool
	// inOrder says the index hands the rows over in the order asked for, with no sort: a queue
	// claim that sorts reads every due row, and every dead one, to take its first few.
	inOrder bool
	run     func(ctx context.Context, store *Store) error
}

// seededUsage is the usage id the seed gave request n.
func seededUsage(n int) uuid.UUID {
	return uuid.UUID(md5.Sum([]byte(fmt.Sprintf("usage-%d", n))))
}

// Every method that reads, updates or deletes a hot table reads it by an index, on tables
// large enough that the planner reads a whole one only when no index fits.
//
// Two are left out, both operator commands of router stream-apps that read a whole table on
// purpose: BackfillStreamPins pins every unpinned session and call, and LegacyStreamPins counts
// every customer's sessions and calls still in the deployment's app. An index for either holds
// every row of a deployment that never pins one, written on every insert for a command run once.
func (s *StoreSuite) TestEveryHotTableIsReadByIndex() {
	since := s.base.Add(-24 * time.Hour)
	until := s.base
	score := 2
	app := AppScope{App: 2}
	reads := []explainedRead{
		{name: "Requests.InsertMany", index: "calls_customer_agent_idx", run: func(ctx context.Context, store *Store) error {
			return insertRequest(ctx, store, &Request{Modality: "llm", CustomerID: "customer-7", AgentID: "agent-7",
				CallID: "stream-7", Provider: "provider-1", Model: "model-1", StartedAt: s.base, Success: true})
		}},
		{name: "Requests.advanceIDs", index: "stt_requests_pkey", run: func(ctx context.Context, store *Store) error {
			return store.Requests.advanceIDs(ctx)
		}},
		{name: "Requests.ToShip", index: "requests_to_ship_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Requests.ToShip(ctx, time.Minute, &Request{ID: 1000, StartedAt: since}, 100)
			return err
		}},
		{name: "Requests.Shipped", index: "requests_usage_id_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Requests.Shipped(ctx, []uuid.UUID{seededUsage(7), seededUsage(8)})
			return err
		}},
		{name: "Requests.Ship", index: "requests_usage_id_idx", run: func(ctx context.Context, store *Store) error {
			return store.Requests.Ship(ctx, []uuid.UUID{seededUsage(1000)}, map[uuid.UUID]string{seededUsage(2000): "refused"})
		}},
		{name: "Requests.SetOrganization", index: "requests_usage_id_idx", run: func(ctx context.Context, store *Store) error {
			return store.Requests.SetOrganization(ctx, []uuid.UUID{seededUsage(3000)}, "org-0")
		}},
		{name: "Requests.Unshipped", index: "requests_to_ship_idx", run: func(ctx context.Context, store *Store) error {
			_, _, err := store.Requests.Unshipped(ctx)
			return err
		}},
		{name: "Requests.Reship", index: "requests_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Requests.Reship(ctx, s.base.Add(-time.Hour), false)
			return err
		}},
		{name: "Requests.Reship rejected", index: "requests_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Requests.Reship(ctx, s.base.Add(-time.Hour), true)
			return err
		}},
		{name: "Requests.CountByUser", index: "requests_customer_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Requests.CountByUser(ctx, "customer-7", "user-7")
			return err
		}},
		{name: "Requests.RenameUser", index: "requests_customer_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Requests.RenameUser(ctx, "customer-7", "user-207", "user-207-renamed")
			return err
		}},
		{name: "Requests.Spend", index: "requests_customer_spend_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Requests.Spend(ctx, "customer-7", "modality", Hourly, since, until, 10, nil, UsageFilter{})
			return err
		}},
		{name: "Requests.Spend by key", index: "requests_customer_spend_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Requests.Spend(ctx, "customer-7", "modality", Hourly, since, until, 10, nil, UsageFilter{APIKey: "key-7"})
			return err
		}},
		{name: "Requests.Spend by user", index: "requests_customer_spend_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Requests.Spend(ctx, "customer-7", "modality", Hourly, since, until, 10, nil, UsageFilter{UserID: "user-7"})
			return err
		}},
		{name: "Requests.Spend by category", index: "requests_customer_spend_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Requests.Spend(ctx, "customer-7", "modality", Hourly, since, until, 10, nil,
				UsageFilter{Categories: []string{"team:team-7"}})
			return err
		}},
		{name: "Requests.Spend across apps", index: "requests_customer_spend_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Requests.Spend(ctx, "customer-7", "project", Daily, since, until, 10, nil,
				UsageFilter{OrganizationID: "org-7", Apps: []string{"customer-107"}})
			return err
		}},
		{name: "Requests.TagKeys", index: "requests_customer_spend_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Requests.TagKeys(ctx, "customer-7", since, until, nil)
			return err
		}},
		{name: "Requests.CountByModel", index: "requests_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Requests.CountByModel(ctx, "llm", s.base.Add(-7*24*time.Hour))
			return err
		}},
		{name: "Requests.VendorCostGaps", index: "requests_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Requests.VendorCostGaps(ctx, s.base.Add(-7*24*time.Hour), 5, 10*time.Second)
			return err
		}},
		{name: "Store.UserOrganizations", index: "requests_customer_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.UserOrganizations(ctx, "customer-7", "user-7")
			return err
		}},

		{name: "Calls.Start", index: "call_legs_placed_call_idx", run: func(ctx context.Context, store *Store) error {
			return store.Calls.Start(ctx, &Call{ID: "explained-call", SessionID: "seeded-session-7", CustomerID: "customer-7",
				AgentID: "agent-7", CallType: "default", CallID: "stream-7", StartedAt: s.base})
		}},
		{name: "Calls.Finish", index: "calls_pkey", run: func(ctx context.Context, store *Store) error {
			return store.Calls.Finish(ctx, "customer-100", "seeded-call-100", s.base)
		}},
		{name: "Calls.ChangeModels", index: "calls_pkey", run: func(ctx context.Context, store *Store) error {
			return store.Calls.ChangeModels(ctx, &Call{ID: "seeded-call-7", CustomerID: "customer-7", LLM: "gpt"})
		}},
		{name: "Calls.Review", index: "calls_pkey", run: func(ctx context.Context, store *Store) error {
			return store.Calls.Review(ctx, "customer-7", "seeded-call-7", "fine", &score, "")
		}},
		{name: "Calls.Get", index: "calls_pkey", run: func(ctx context.Context, store *Store) error {
			_, err := store.Calls.Get(ctx, "customer-7", "seeded-call-7")
			return err
		}},
		{name: "Calls.Latest", index: "calls_session_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Calls.Latest(ctx, "customer-7", []string{"seeded-session-7", "seeded-session-207"})
			return err
		}},
		{name: "Calls.LastVoiced", index: "calls_session_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Calls.LastVoiced(ctx, "customer-7", "seeded-session-7")
			return err
		}},
		{name: "Calls.Select", index: "calls_customer_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Calls.Select(ctx, "customer-7", CallFilter{From: since, To: until})
			return err
		}},
		{name: "Calls.Select running", index: "calls_customer_agent_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Calls.Select(ctx, "customer-7", CallFilter{Running: true, MaxReviewScore: 3})
			return err
		}},
		{name: "Calls.Select by session", index: "calls_session_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Calls.Select(ctx, "customer-7", CallFilter{SessionID: "seeded-session-7"})
			return err
		}},
		{name: "Calls.Select by agent", index: "calls_customer_agent_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Calls.Select(ctx, "customer-7", CallFilter{AgentID: "agent-7"})
			return err
		}},
		{name: "Calls.Select by campaign", index: "calls_customer_campaign_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Calls.Select(ctx, "customer-10", CallFilter{CampaignID: "campaign-10"})
			return err
		}},
		{name: "Calls.ModelCalls", index: "requests_customer_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Calls.ModelCalls(ctx, "customer-7", "agent-7", "stream-7", since, &until)
			return err
		}},
		{name: "Calls.UsedModels", index: "requests_agent_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Calls.UsedModels(ctx, "customer-7", "agent-7", since, &until)
			return err
		}},
		{name: "Calls.Usage", index: "requests_agent_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Calls.Usage(ctx, "customer-7", "agent-7", "", since, &until)
			return err
		}},
		{name: "Calls.Usage of a session", index: "requests_agent_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Calls.Usage(ctx, "customer-7", "agent-7", "seeded-session-7", since, &until)
			return err
		}},
		{name: "Calls.Tokens", index: "requests_agent_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Calls.Tokens(ctx, "customer-7", "agent-7", "", since, &until)
			return err
		}},
		{name: "Store.CallByAgentInApp", index: "calls_agent_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.CallByAgentInApp(ctx, app, "agent-7")
			if errors.Is(err, ErrNoCall) {
				return nil
			}
			return err
		}},

		{name: "CallLegs.Open", index: "call_legs_participant_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.CallLegs.Open(ctx, &CallLeg{CustomerID: "customer-7", OrganizationID: "org-7", CallType: "default",
				CallID: "stream-7", Vendor: "telnyx", Direction: Inbound, LocalNumber: "+15550000007",
				RemoteNumber: "+16660000007", StreamSessionID: "explained-sfu", DialedAt: s.base})
			return err
		}},
		{name: "CallLegs.Open placed", index: "call_legs_token_hash_key", run: func(ctx context.Context, store *Store) error {
			_, err := store.CallLegs.Open(ctx, &CallLeg{Token: "token-2", CustomerID: "customer-2", CallType: "default",
				CallID: "stream-2", Vendor: "telnyx", Direction: Outbound, LocalNumber: "+15550000002",
				RemoteNumber: "+16660000002", DialedAt: s.base})
			if errors.Is(err, ErrNoPlacedCall) {
				return nil
			}
			return err
		}},
		{name: "CallLegs.Connect", index: "call_legs_unanswered_idx", run: func(ctx context.Context, store *Store) error {
			_, _, err := store.CallLegs.Connect(ctx, AppScope{App: 3}, "default", "stream-8", "sfu-8", "explained-phone",
				"+16660000008", s.base, nil)
			return err
		}},
		{name: "CallLegs.Ringing", index: "call_legs_call_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.CallLegs.Ringing(ctx, AppScope{App: 3}, "default", "stream-101")
			return err
		}},
		{name: "CallLegs.HasParticipant", index: "call_legs_participant_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.CallLegs.HasParticipant(ctx, app, "default", "stream-100", "phone-100")
			return err
		}},
		{name: "CallLegs.EndParticipant", index: "call_legs_call_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.CallLegs.EndParticipant(ctx, app, "default", "stream-200", "phone-200", s.base)
			return err
		}},
		{name: "CallLegs.EndCall", index: "call_legs_call_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.CallLegs.EndCall(ctx, app, "default", "stream-300", s.base)
			return err
		}},
		{name: "CallLegs.CountOpen", index: "call_legs_open_started_idx", run: func(ctx context.Context, store *Store) error {
			_, _, err := store.CallLegs.CountOpen(ctx, "org-1", s.base.Add(-time.Minute), "default", "stream-101")
			return err
		}},
		{name: "CallLegs.Sweep", index: "call_legs_open_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.CallLegs.Sweep(ctx, s.base.Add(-60*24*time.Hour), 4*time.Hour, 100)
			return err
		}},
		{name: "CallLegs.SelectEnded", index: "call_legs_ended_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.CallLegs.SelectEnded(ctx, s.base.Add(-2*time.Hour), s.base.Add(-5*time.Minute), 100)
			return err
		}},
		{name: "CallLegs.Placed", index: "call_legs_vendor_call_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.CallLegs.Placed(ctx, "customer-8", "telnyx", "vendor-8")
			return err
		}},
		{name: "CallLegs.Unplace", index: "call_legs_token_hash_key", run: func(ctx context.Context, store *Store) error {
			return store.CallLegs.Unplace(ctx, "token-102")
		}},
		{name: "CallLegs.ChangeOutcome", index: "call_legs_token_hash_key", run: func(ctx context.Context, store *Store) error {
			_, err := store.CallLegs.ChangeOutcome(ctx, PlacedWhere{Token: "token-100"}, func(*CallLeg) bool { return false })
			return err
		}},
		{name: "CallLegs.ChangeOutcome by call", index: "call_legs_placed_call_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.CallLegs.ChangeOutcome(ctx, PlacedWhere{CustomerID: "customer-100", StreamAppPK: 2, CallType: "default",
				CallID: "stream-100", LocalNumber: "+15550000100", RemoteNumber: "+16660000100"}, func(*CallLeg) bool { return false })
			return err
		}},
		{name: "CallLegs.GetByToken", index: "call_legs_token_hash_key", run: func(ctx context.Context, store *Store) error {
			_, err := store.CallLegs.GetByToken(ctx, "token-100")
			return err
		}},
		{name: "CallLegs.SelectPlaced", index: "call_legs_placed_call_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.CallLegs.SelectPlaced(ctx, "customer-8", []string{"stream-8", "stream-208"})
			return err
		}},
		{name: "CallLegs.ClearTokens", index: "call_legs_token_hash_placed_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.CallLegs.ClearTokens(ctx, s.base.Add(-60*24*time.Hour))
			return err
		}},

		{name: "Sessions.Upsert", index: "agent_sessions_pkey", run: func(ctx context.Context, store *Store) error {
			return store.Sessions.Upsert(ctx, &AgentSession{ID: "seeded-session-7", CustomerID: "customer-7", AgentName: "agent 7",
				UserID: "user-7", CallerKind: "user", CreatedAt: s.base})
		}},
		{name: "Sessions.Supersede", index: "agent_sessions_pkey", run: func(ctx context.Context, store *Store) error {
			return store.Sessions.Supersede(ctx, "customer-8", "seeded-session-8", s.base)
		}},
		{name: "Sessions.SetToolTimeout", index: "agent_sessions_pkey", run: func(ctx context.Context, store *Store) error {
			return store.Sessions.SetToolTimeout(ctx, "customer-7", "seeded-session-7", "search", 1000)
		}},
		{name: "Sessions.Close", index: "agent_sessions_pkey", run: func(ctx context.Context, store *Store) error {
			return store.Sessions.Close(ctx, "customer-100", "seeded-session-100", s.base, s.base, "left")
		}},
		{name: "Sessions.SawVideo", index: "agent_sessions_pkey", run: func(ctx context.Context, store *Store) error {
			return store.Sessions.SawVideo(ctx, "customer-7", "seeded-session-7")
		}},
		{name: "Sessions.Voiced", index: "agent_sessions_pkey", run: func(ctx context.Context, store *Store) error {
			return store.Sessions.Voiced(ctx, "customer-7", "seeded-session-7")
		}},
		{name: "Sessions.Describe", index: "agent_sessions_pkey", run: func(ctx context.Context, store *Store) error {
			return store.Sessions.Describe(ctx, "customer-7", "seeded-session-7", "call 7", "", nil)
		}},
		{name: "Sessions.ChooseConnection", index: "agent_sessions_pkey", run: func(ctx context.Context, store *Store) error {
			return store.Sessions.ChooseConnection(ctx, "customer-7", "seeded-session-7", "github", "connection")
		}},
		{name: "Sessions.Exists", index: "agent_sessions_pkey", run: func(ctx context.Context, store *Store) error {
			_, err := store.Sessions.Exists(ctx, "seeded-session-7")
			return err
		}},
		{name: "Sessions.Get", index: "agent_sessions_pkey", run: func(ctx context.Context, store *Store) error {
			_, err := store.Sessions.Get(ctx, "customer-7", "seeded-session-7")
			return err
		}},
		{name: "Sessions.ConversationPin", index: "agent_sessions_conversation_idx", run: func(ctx context.Context, store *Store) error {
			_, _, err := store.Sessions.ConversationPin(ctx, "customer-7", "conversation-7")
			return err
		}},
		{name: "Sessions.HeldConversations", index: "agent_sessions_conversation_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Sessions.HeldConversations(ctx, "customer-7", HeldConversation{}, 500)
			return err
		}},
		{name: "Sessions.HeldConversations of every customer", index: "agent_sessions_conversation_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Sessions.HeldConversations(ctx, "", HeldConversation{CustomerID: "customer-7"}, 500)
			return err
		}},
		{name: "Sessions.CallerKinds", index: "agent_sessions_user_last_message_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Sessions.CallerKinds(ctx, "customer-7", "conversation-7", "user-7")
			return err
		}},
		{name: "Sessions.HeldAsGuest", index: "agent_sessions_user_last_message_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Sessions.HeldAsGuest(ctx, "customer-7", "user-7")
			return err
		}},
		{name: "Sessions.Delete", index: "agent_sessions_pkey", run: func(ctx context.Context, store *Store) error {
			return store.Sessions.Delete(ctx, "customer-199", "seeded-session-99999")
		}},
		// What calls_session_id_fkey runs on the calls of a session deleted, which no hook sees.
		{name: "Sessions.Delete frees its calls", index: "calls_session_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.shard.NewRaw("UPDATE calls SET session_id = NULL WHERE session_id = ?", "seeded-session-99998").Exec(ctx)
			return err
		}},
		{name: "ImageJobs.Claim", index: "image_jobs_undecided", inOrder: true, run: func(ctx context.Context, store *Store) error {
			_, err := store.ImageJobs.Claim(ctx, 10, 0, false)
			return err
		}},
		{name: "ImageJobs.Claim decided", index: "image_jobs_decided", inOrder: true, run: func(ctx context.Context, store *Store) error {
			_, err := store.ImageJobs.Claim(ctx, 10, 0, true)
			return err
		}},
		{name: "ImageJobs.NextDue", index: "image_jobs_undecided", run: func(ctx context.Context, store *Store) error {
			_, _, err := store.ImageJobs.NextDue(ctx, false)
			return err
		}},
		{name: "Sessions.Select", index: "agent_sessions_last_message_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Sessions.Select(ctx, "customer-7", SessionFilter{})
			return err
		}},
		{name: "Sessions.Select oldest first", index: "agent_sessions_last_message_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Sessions.Select(ctx, "customer-7", SessionFilter{Order: SessionOrder{Ascending: true}})
			return err
		}},
		{name: "Sessions.Select pinned first", index: "agent_sessions_pinned_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Sessions.Select(ctx, "customer-7", SessionFilter{Order: SessionOrder{PinnedFirst: true}})
			return err
		}},
		{name: "Sessions.Select by review score", index: "agent_sessions_conversation_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Sessions.Select(ctx, "customer-7", SessionFilter{Order: SessionOrder{By: SessionSortReviewScore}})
			return err
		}},
		{name: "Sessions.Select of a user", index: "agent_sessions_user_last_message_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Sessions.Select(ctx, "customer-7", SessionFilter{UserID: "user-7", CallerKinds: []string{"user"}})
			return err
		}},
		{name: "Sessions.Select of an agent", index: "agent_sessions_agent_id_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Sessions.Select(ctx, "customer-7", SessionFilter{AgentID: "agent-7"})
			return err
		}},
		{name: "Sessions.Select of a project", index: "agent_sessions_project_last_message_idx", run: func(ctx context.Context, store *Store) error {
			where := SessionIs(SessionFieldProject, "project-7")
			_, err := store.Sessions.Select(ctx, "customer-7", SessionFilter{Where: &where})
			return err
		}},
		{name: "Sessions.Select filtered", index: "agent_sessions_agent_idx", run: func(ctx context.Context, store *Store) error {
			where := AllOf(SessionIs(SessionFieldConfig, "config-7"), SessionIs(SessionFieldState, SessionClosed),
				SessionIs(SessionFieldAgent, "agent 7"), SessionExpr{Field: SessionFieldCreatedAt, Op: SessionGte, Time: since})
			_, err := store.Sessions.Select(ctx, "customer-7", SessionFilter{Where: &where})
			return err
		}},
		{name: "Sessions.Search", index: "agent_sessions_conversation_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Sessions.Search(ctx, "customer-7", "call 7", SessionFilter{})
			return err
		}},
		{name: "Sessions.Pin", index: "agent_sessions_pkey", run: func(ctx context.Context, store *Store) error {
			_, err := store.Sessions.Pin(ctx, "customer-7", "seeded-session-7", false, s.base)
			return err
		}},
		{name: "Sessions.Endings", index: "agent_sessions_customer_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Sessions.Endings(ctx, []string{"customer-7", "customer-8"}, since)
			return err
		}},
		{name: "Sessions.Latest", index: "agent_sessions_customer_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Sessions.Latest(ctx, []string{"customer-7", "customer-8"}, since, 10)
			return err
		}},
		{name: "Sessions.Ends", index: "agent_sessions_pkey", run: func(ctx context.Context, store *Store) error {
			_, err := store.Sessions.Ends(ctx, []SessionKey{{"customer-7", "seeded-session-7"}, {"customer-7", "seeded-session-207"}}, since)
			return err
		}},
		{name: "Users.ClaimGuest", index: "agent_sessions_user_last_message_idx", run: func(ctx context.Context, store *Store) error {
			if err := store.Users.InsertGuest(ctx, &User{ID: "guest-7", CustomerID: "customer-7", CreatedAt: s.base}); err != nil {
				return err
			}
			_, err := store.Users.ClaimGuest(ctx, "customer-7", "guest-7", "user-7")
			return err
		}},
		{name: "CallLegs.RenameUser", index: "call_legs_user_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.CallLegs.RenameUser(ctx, "customer-7", "user-207", "user-207-renamed")
			return err
		}},
		{name: "CallLegs.CountUser", index: "call_legs_user_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.CallLegs.CountUser(ctx, "customer-7", "user-207")
			return err
		}},
		{name: "AgentLogs.DeleteUser", index: "agent_logs_customer_order", run: func(ctx context.Context, store *Store) error {
			_, err := store.AgentLogs.DeleteUser(ctx, "customer-7", "user-207")
			return err
		}},
		{name: "AgentLogs.CountUser", index: "agent_logs_customer_order", run: func(ctx context.Context, store *Store) error {
			_, err := store.AgentLogs.CountUser(ctx, "customer-7", "user-207")
			return err
		}},
		{name: "BranchMessages.CountUser", index: "agent_sessions_user_last_message_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.BranchMessages.CountUser(ctx, "customer-7", "user-7")
			return err
		}},
		{name: "BranchMessages.DeleteUser", index: "agent_sessions_user_last_message_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.BranchMessages.DeleteUser(ctx, "customer-7", "user-7")
			return err
		}},
		{name: "Responses.Finish", index: "agent_sessions_pkey", run: func(ctx context.Context, store *Store) error {
			return store.Responses.Finish(ctx, "customer-7", "explained-response", ResponseCompleted, "", s.base)
		}},
		{name: "Responses.Replies", index: "agent_sessions_conversation_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Responses.Replies(ctx, "customer-7", "conversation-7", []string{"turn-7"})
			return err
		}},
		{name: "Responses.Opening", index: "agent_sessions_conversation_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Responses.Opening(ctx, "customer-7", "conversation-7")
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		}},
		{name: "Episodes.closeCardsWithSessions", index: "agent_sessions_pkey", run: func(ctx context.Context, store *Store) error {
			cards := []EpisodeCard{{Episode: Episode{SessionID: "seeded-session-7"}}, {Episode: Episode{SessionID: "seeded-session-207"}}}
			return store.Episodes.closeCardsWithSessions(ctx, "customer-7", cards)
		}},

		{name: "AgentLogs.HighWater", index: "agent_logs_customer_order", run: func(ctx context.Context, store *Store) error {
			_, err := store.AgentLogs.HighWater(ctx, "customer-7")
			return err
		}},
		{name: "AgentLogs.Select", index: "agent_logs_customer_order", run: func(ctx context.Context, store *Store) error {
			_, err := store.AgentLogs.Select(ctx, "customer-7", AgentLogFilter{From: since, To: until})
			return err
		}},
		{name: "AgentLogs.Select of a config", index: "agent_logs_config_order", run: func(ctx context.Context, store *Store) error {
			_, err := store.AgentLogs.Select(ctx, "customer-7", AgentLogFilter{ConfigID: "config-7", Before: AgentLogMark{0: 90000}})
			return err
		}},
		{name: "AgentLogs.Select errors", index: "agent_logs_error_order", run: func(ctx context.Context, store *Store) error {
			_, err := store.AgentLogs.Select(ctx, "customer-7", AgentLogFilter{Severity: "error", After: AgentLogMark{0: 1000}, Forward: true})
			return err
		}},
		{name: "AgentLogs.Select of a session", index: "agent_logs_session_order", run: func(ctx context.Context, store *Store) error {
			_, err := store.AgentLogs.Select(ctx, "customer-7", AgentLogFilter{SessionID: "seeded-session-7", UserID: "user-7",
				Sources: []string{"agent"}, Search: "request"})
			return err
		}},
		{name: "AgentLogs.Get", index: "agent_logs_pkey", run: func(ctx context.Context, store *Store) error {
			_, err := store.AgentLogs.Get(ctx, "customer-7", 7)
			return err
		}},

		{name: "Store.CustomerActivity", index: "agent_sessions_customer_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.CustomerActivity(ctx, "customer-7", ActivityDaily, since, until)
			return err
		}},
		{name: "Store.CustomerConcurrencyPeak", index: "agent_sessions_agent_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.CustomerConcurrencyPeak(ctx, "customer-7", since, until)
			return err
		}},
		{name: "Store.CustomerToolUsage", index: "agent_sessions_pkey", run: func(ctx context.Context, store *Store) error {
			_, _, err := store.CustomerToolUsage(ctx, "customer-7", "config-7", since, until)
			return err
		}},
		{name: "Store.CustomerTurns", index: "agent_sessions_pkey", run: func(ctx context.Context, store *Store) error {
			_, err := store.CustomerTurns(ctx, "customer-7", "config-7", since, until)
			return err
		}},
		{name: "Store.RecordedUsage", index: "requests_usage_id_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.RecordedUsage(ctx, []uuid.UUID{seededUsage(7), seededUsage(8)})
			return err
		}},
		{name: "Store.ExportCustomer", index: "requests_customer_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.ExportCustomer(ctx, "customer-7", func(string, json.RawMessage) error { return nil })
			return err
		}},
		{name: "Store.ApplyChanges", index: "requests_usage_id_idx", run: func(ctx context.Context, store *Store) error {
			key := func(id string) json.RawMessage { return json.RawMessage(fmt.Sprintf(`{"id": %s}`, id)) }
			usage := json.RawMessage(fmt.Sprintf(`{"id": 1, "usage_id": %q}`, seededUsage(4007)))
			return store.ApplyChanges(ctx, "customer-7", []DataChange{
				{Table: "requests", Op: ChangeDelete, Key: usage, At: s.base},
				{Table: "agent_logs", Op: ChangeDelete, Key: key("50"), At: s.base},
				{Table: "calls", Op: ChangeDelete, Key: key(`"seeded-call-99807"`), At: s.base},
				{Table: "agent_sessions", Op: ChangeDelete, Key: key(`"seeded-session-99807"`), At: s.base},
			})
		}},
		{name: "Store.ImportRow", index: "agent_logs_source_idx", run: func(ctx context.Context, store *Store) error {
			return store.ImportRow(ctx, "customer-7", "agent_logs", json.RawMessage(`{"id": 7, "customer_id": "customer-7",
				"config_id": "config-7", "agent_id": "agent-7", "session_id": "seeded-session-7", "user_id": "user-7", "source": "agent", "severity": "info", "event_type": "provider_request", "message": "imported", "details": {},
				"occurred_at": "2026-03-01T10:00:00Z", "ingested_at": "2026-03-01T10:00:00Z"}`))
		}},
		{name: "Store.ImportRow of a turn", index: "agent_sessions_pkey", run: func(ctx context.Context, store *Store) error {
			return store.ImportRow(ctx, "customer-7", "agent_responses", json.RawMessage(`{"id": "imported-turn", "session_id": "seeded-session-7",
				"said": "hello", "status": "completed", "error": "", "created_at": "2026-03-01T10:00:00Z"}`))
		}},
		{name: "Store.HoldingTable", index: "agent_logs_customer_order", run: func(ctx context.Context, store *Store) error {
			_, err := store.HoldingTable(ctx, "customer-123")
			return err
		}},

		{name: "Spend.Since", index: "requests_customer_spend_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Spend.Since(ctx, "customer-7", since)
			return err
		}},
		{name: "Spend.Teams", index: "requests_customer_spend_idx", run: func(ctx context.Context, store *Store) error {
			// Of an app in no organization, as the category sum below is.
			_, err := store.Spend.Teams(ctx, "customer-7", []time.Time{since}, s.base.AddDate(0, 0, -1), false)
			return err
		}},
		{name: "Spend.Teams of an organization", index: "requests_customer_spend_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Spend.Teams(ctx, "org-7", []time.Time{since}, s.base.AddDate(0, 0, -1), true)
			return err
		}},
		{name: "Spend.Subject", index: "requests_customer_spend_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Spend.Subject(ctx, usage.SubjectRef{OrganizationID: "org-7", Subject: usage.SubjectApp, AppID: "customer-7"}, since, true)
			return err
		}},
		{name: "Spend.Subject of a user", index: "requests_user_spend_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Spend.Subject(ctx, usage.SubjectRef{OrganizationID: "org-7", Subject: usage.SubjectUser, AppID: "customer-7",
				UserID: "user-7"}, since, true)
			return err
		}},
		{name: "Spend.Subject of a key", index: "requests_key_spend_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Spend.Subject(ctx, usage.SubjectRef{OrganizationID: "org-7", Subject: usage.SubjectAPIKey, AppID: "customer-7",
				APIKey: "key-7"}, since, true)
			return err
		}},
		// Of an app in no organization, customer-7.
		{name: "Spend.Subject of a category", index: "requests_category_spend_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Spend.Subject(ctx, usage.SubjectRef{OrganizationID: "customer-7", Subject: usage.SubjectCategory,
				Category: "team:team-7"}, since, false)
			return err
		}},
		{name: "Spend.Daily", index: "requests_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Spend.Daily(ctx, s.base.AddDate(0, 0, -7), s.base)
			return err
		}},
		{name: "Spend.Meters", index: "requests_customer_spend_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Spend.Meters(ctx, []string{"customer-7", "customer-8"}, since)
			return err
		}},
		{name: "Buckets.Rollup", index: "requests_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Buckets.Rollup(ctx, Hourly, s.base.Add(-time.Hour), s.base)
			return err
		}},
		{name: "Buckets.Rollup daily", index: "requests_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Buckets.Rollup(ctx, Daily, s.base.Add(-24*time.Hour), s.base)
			return err
		}},
		{name: "Buckets.Select", index: "requests_customer_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Buckets.Select(ctx, "customer-7", BucketFilter{Modality: "llm", Granularity: Hourly, From: since, To: until})
			return err
		}},
		{name: "TagBuckets.Select", index: "requests_customer_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.TagBuckets.Select(ctx, "customer-7", TagBucketFilter{Modality: "llm", Key: "project", Granularity: Hourly, From: since, To: until})
			return err
		}},
		{name: "Sandboxes.Usage", index: "requests_started_idx", run: func(ctx context.Context, store *Store) error {
			_, err := store.Sandboxes.Usage(ctx, "deployment", []string{"sandbox-7"}, s.base.Add(-48*time.Hour), s.base)
			return err
		}},
		{name: "LimitGrantUses.Reconcile", index: "requests_limit_grants_started_idx", sharded: true, run: func(ctx context.Context, store *Store) error {
			_, err := store.LimitGrantUses.Reconcile(ctx)
			return err
		}},
	}
	for _, read := range reads {
		s.Run(read.name, func() {
			explain := s.explained
			if read.sharded {
				explain = s.explainedSharded
			}
			plans := explain(func(store *Store) { s.Require().NoError(read.run(s.ctx, store)) })
			s.usesIndex(plans, read.index)
			if read.inOrder {
				s.NotContains(strings.Join(plans, "\n"), "Sort", plans)
			}
		})
	}
}
