//go:build integration

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/uptrace/bun"
)

// recordedFor is every change recorded for a customer, as following their move reads them.
func (s *StoreSuite) recordedFor(customerID string) []DataChange {
	changes, _, err := s.store.Changes(s.ctx, customerID, 0, 1000)
	s.Require().NoError(err)
	return changes
}

// payloadsNaming are the recorded changes of a customer whose row names a user.
func (s *StoreSuite) payloadsNaming(customerID, userID string) []DataChange {
	var naming []DataChange
	for _, change := range s.recordedFor(customerID) {
		if strings.Contains(string(change.Payload), `"`+userID+`"`) {
			naming = append(naming, change)
		}
	}
	return naming
}

// recordedOf are the recorded changes of one table of a customer's.
func (s *StoreSuite) recordedOf(customerID, table string) []DataChange {
	return slices.DeleteFunc(s.recordedFor(customerID), func(change DataChange) bool { return change.Table != table })
}

// seqs are the numbers of changes.
func seqs(changes []DataChange) []int64 {
	numbers := make([]int64, 0, len(changes))
	for _, change := range changes {
		numbers = append(numbers, change.Seq)
	}
	return numbers
}

// forget runs the shared database's step of forgetting jlahey of app.
func (s *StoreSuite) forget() {
	_, err := s.store.ForgetUserSettings(s.ctx, "app", "jlahey", "forgotten", &AuditEntry{
		CustomerID: "app", ResourceType: AuditEndUser, ResourceID: "forgotten", Action: AuditUpdated, Source: AuditSourceCLI, ActorID: "ops",
	})
	s.Require().NoError(err)
}

func (s *StoreSuite) TestForgettingAUserRenamesThemInTheChangesRecordedOfTheirCampaigns() {
	for _, app := range []string{"app", "app2"} {
		s.Require().NoError(s.store.StartDataCapture(s.ctx, app, time.Hour, time.Second))
	}
	reused := Campaign{CustomerID: "app2", Name: "Renewals", ConfigID: "config-1", FromNumber: "+15550100", UserID: "jlahey"}
	s.Require().NoError(s.store.Campaigns.Insert(s.ctx, &reused))
	// The other app's campaign is gone and the app's holds its id, so only the customer tells
	// their changes apart: an id is unique among the rows that exist, not among those recorded.
	s.Require().NoError(s.store.Campaigns.Delete(s.ctx, "app2", reused.ID))
	_, err := s.store.global.ExecContext(s.ctx,
		"INSERT INTO campaigns (id, customer_id, name, config_id, from_number, user_id) VALUES (?, 'app', 'Renewals', 'config-1', '+15550100', 'jlahey')", reused.ID)
	s.Require().NoError(err)
	other := Campaign{CustomerID: "app", Name: "Winback", ConfigID: "config-1", FromNumber: "+15550100", UserID: "rlafleur"}
	s.Require().NoError(s.store.Campaigns.Insert(s.ctx, &other))
	before := seqs(s.recordedFor("app"))
	s.Require().NotEmpty(s.payloadsNaming("app", "jlahey"), "the campaign's insert was recorded")

	s.forget()

	s.Empty(s.payloadsNaming("app", "jlahey"))
	s.Len(s.payloadsNaming("app", "forgotten"), 2, "the insert renamed, and the rewrite recorded under the replacement")
	s.NotEmpty(s.payloadsNaming("app", "rlafleur"), "another user's changes stay")
	s.NotEmpty(s.payloadsNaming("app2", "jlahey"), "the same user id of another app is another user")
	s.Subset(seqs(s.recordedFor("app")), before, "every change keeps its number")
}

func (s *StoreSuite) TestForgettingAUserOfOneAppLeavesAnotherAppsFollowerWhereItWas() {
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "app", time.Hour, time.Second))
	theirs := Campaign{CustomerID: "app", Name: "Renewals", ConfigID: "config-1", FromNumber: "+15550100", UserID: "jlahey"}
	s.Require().NoError(s.store.Campaigns.Insert(s.ctx, &theirs))
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "other", time.Hour, time.Second))
	cursor, err := s.store.ExportCustomer(s.ctx, "other", func(string, json.RawMessage) error { return nil })
	s.Require().NoError(err)
	// The user's changes are then on both sides of the follower's cursor, and the oldest kept.
	later := Campaign{CustomerID: "app", Name: "Winback", ConfigID: "config-1", FromNumber: "+15550100", UserID: "jlahey"}
	s.Require().NoError(s.store.Campaigns.Insert(s.ctx, &later))
	unread := Campaign{CustomerID: "other", Name: "Winback", ConfigID: "config-1", FromNumber: "+15550101", UserID: "rlafleur"}
	s.Require().NoError(s.store.Campaigns.Insert(s.ctx, &unread))

	s.forget()

	changes, _, err := s.store.Changes(s.ctx, "other", cursor, 100)
	s.Require().NoError(err, "forgetting a user of another app takes no change of this one's")
	s.Len(changes, 1)
}

func (s *StoreSuite) TestForgettingAUserRenamesThemInTheChangesOfTheRequestsRewrittenAlone() {
	if s.store.Sharded() {
		s.T().Skip("a shard's own tables record no changes (watchDataChanges)")
	}
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "app", time.Hour, time.Second))
	request := func(user string) *Request {
		recorded := &Request{Modality: "stt", CustomerID: "app", UserID: user, Provider: "deepgram", Model: "flux-general-en", StartedAt: s.base, Success: true}
		s.Require().NoError(insertRequest(s.ctx, s.store, recorded))
		return recorded
	}
	request("jlahey")
	request("rlafleur")
	s.opened("one", "app", s.base, nil)

	_, err := s.store.Requests.RenameUser(s.ctx, "app", "jlahey", "forgotten")
	s.Require().NoError(err)
	// A request in flight when its rows were rewritten, which forget-user waits out and rewrites
	// on a later run: until then its row names the user, and so does its change.
	late := request("jlahey")
	s.forget()

	var naming []string
	for _, change := range s.recordedOf("app", "requests") {
		if strings.Contains(string(change.Payload), `"jlahey"`) {
			naming = append(naming, string(change.Key))
		}
	}
	s.Require().Len(naming, 1, "only the late request's change names the user")
	s.Contains(naming[0], late.UsageID.String())
	s.Len(slices.DeleteFunc(s.recordedOf("app", "requests"), func(change DataChange) bool {
		return !strings.Contains(string(change.Payload), `"forgotten"`)
	}), 2, "the rewritten request's insert renamed, and its rewrite")
	s.NotEmpty(s.payloadsNaming("app", "rlafleur"), "another user's changes stay")
	s.NotEmpty(s.recordedOf("app", "agent_sessions")[0].Payload, "a table forget leaves alone keeps its changes")
	s.Contains(string(s.recordedOf("app", "agent_sessions")[0].Payload), `"jlahey"`)
}

func (s *StoreSuite) TestForgettingAUserRenamesThemInTheChangesOfACampaignDeletedBefore() {
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "app", time.Hour, time.Second))
	gone := Campaign{CustomerID: "app", Name: "Renewals", ConfigID: "config-1", FromNumber: "+15550100", UserID: "jlahey"}
	s.Require().NoError(s.store.Campaigns.Insert(s.ctx, &gone))
	s.Require().NoError(s.store.Campaigns.Delete(s.ctx, "app", gone.ID))
	s.Require().NotEmpty(s.payloadsNaming("app", "jlahey"), "the campaign's insert was recorded")

	s.forget()

	s.Empty(s.payloadsNaming("app", "jlahey"))
}

// What RenameUser sends: a read of a user's requests, and a rewrite of a batch and its changes.
const (
	readsRequests  = `SELECT "r"."id" FROM "requests"`
	rewritesABatch = "jsonb_set(payload, '{user_id}'"
)

// afterStatements runs after each statement that holds match, while it is active.
type afterStatements struct {
	active atomic.Bool
	match  string
	run    func(query string)
}

func (*afterStatements) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}
func (a *afterStatements) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	if a.active.Load() && strings.Contains(event.Query, a.match) {
		a.run(event.Query)
	}
}

// afterEach runs run after each statement on the shared database that holds match, until the test ends.
func (s *StoreSuite) afterEach(match string, run func(query string)) {
	watch := &afterStatements{match: match, run: run}
	s.store.global.AddQueryHook(watch)
	watch.active.Store(true)
	s.T().Cleanup(func() { watch.active.Store(false) })
}

// requestOf records a request of a user of app, and answers it.
func (s *StoreSuite) requestOf(user string) *Request {
	recorded := &Request{Modality: "stt", CustomerID: "app", UserID: user, Provider: "deepgram", Model: "flux-general-en", StartedAt: s.base, Success: true}
	s.Require().NoError(insertRequest(s.ctx, s.store, recorded))
	return recorded
}

// renaming starts recording app's changes, for a test that renames its users a row per statement.
func (s *StoreSuite) renaming() {
	if s.store.Sharded() {
		s.T().Skip("a shard's own tables record no changes (watchDataChanges)")
	}
	s.inSmallBatches()
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "app", time.Hour, time.Second))
}

// leftOf is how many of app's requests, and of the changes recorded of them, name a user.
func (s *StoreSuite) leftOf(user string) (requests int64, changes int) {
	requests, err := s.store.Requests.CountByUser(s.ctx, "app", user)
	s.Require().NoError(err)
	return requests, len(s.payloadsNaming("app", user))
}

func (s *StoreSuite) TestRenamingAUserRewritesEachChangeOfTheirRequestsWithItsOwnBatch() {
	s.renaming()
	for range 3 {
		s.requestOf("jlahey")
	}
	var after []int
	s.afterEach(rewritesABatch, func(string) {
		inserts := 0
		for _, change := range s.recordedOf("app", "requests") {
			if change.Op == "insert" && strings.Contains(string(change.Payload), `"forgotten"`) {
				inserts++
			}
		}
		after = append(after, inserts)
	})

	renamed, err := s.store.Requests.RenameUser(s.ctx, "app", "jlahey", "forgotten")

	s.Require().NoError(err)
	s.EqualValues(3, renamed)
	s.Equal([]int{1, 2, 3}, after, "a request's insert renamed by its own batch")
	s.Empty(s.payloadsNaming("app", "jlahey"))
}

// Requests recorded once the batches are done, more than one statement takes.
func (s *StoreSuite) TestRenamingAUserRenamesTheRequestsRecordedSinceTheLastBatch() {
	s.renaming()
	s.requestOf("jlahey")
	reads := 0
	s.afterEach(readsRequests, func(string) {
		if reads++; reads == 2 {
			s.requestOf("jlahey")
			s.requestOf("jlahey")
		}
	})

	renamed, err := s.store.Requests.RenameUser(s.ctx, "app", "jlahey", "forgotten")

	s.Require().NoError(err)
	s.EqualValues(3, renamed)
	requests, changes := s.leftOf("jlahey")
	s.Zero(requests)
	s.Zero(changes)
}

func (s *StoreSuite) TestARenameCutShortLeavesEachRequestAndItsChangesNamingOneUserAndARunAgainEndsIt() {
	s.renaming()
	// Cut after the first batch, and after the first statement on what was recorded since.
	for rewrites, left := range map[int]int{1: 2, 4: 1} {
		for range 3 {
			s.requestOf("jlahey")
		}
		ctx, cancel := context.WithCancel(s.ctx)
		reads := 0
		s.afterEach(readsRequests, func(string) {
			if reads++; ctx.Err() == nil && reads == 4 {
				s.requestOf("jlahey")
				s.requestOf("jlahey")
			}
		})
		sent := 0
		s.afterEach(rewritesABatch, func(string) {
			if sent++; sent == rewrites {
				cancel()
			}
		})

		_, err := s.store.Requests.RenameUser(ctx, "app", "jlahey", "forgotten")

		s.Require().ErrorIs(err, context.Canceled)
		requests, changes := s.leftOf("jlahey")
		s.EqualValues(left, requests)
		s.Equal(left, changes, "the changes of the requests left, and no others")

		renamed, err := s.store.Requests.RenameUser(s.ctx, "app", "jlahey", "forgotten")

		s.Require().NoError(err)
		s.EqualValues(left, renamed)
		requests, changes = s.leftOf("jlahey")
		s.Zero(requests)
		s.Zero(changes)
		cancel()
	}
}

// A request given to another user between a batch's read and its rewrite is theirs.
func (s *StoreSuite) TestRenamingAUserLeavesARequestThatIsNoLongerTheirs() {
	s.renaming()
	request := s.requestOf("jlahey")
	reads := 0
	s.afterEach(readsRequests, func(string) {
		if reads++; reads == 1 {
			_, err := s.store.shard.ExecContext(s.ctx, "UPDATE requests SET user_id = 'rlafleur' WHERE usage_id = ?", request.UsageID)
			s.Require().NoError(err)
		}
	})

	renamed, err := s.store.Requests.RenameUser(s.ctx, "app", "jlahey", "forgotten")

	s.Require().NoError(err)
	s.Zero(renamed)
	requests, _ := s.leftOf("rlafleur")
	s.EqualValues(1, requests)
	s.Empty(s.payloadsNaming("app", "jlahey"), "the change from when it was theirs is renamed")
}

// A change left names a request by its id alone, which may be another app's by then, and that
// app's own changes of the same user id are not left to rename.
func (s *StoreSuite) TestRenamingAUserLeavesAnotherAppsRequestOfTheIdAChangeNames() {
	s.renaming()
	theirs := &Request{Modality: "stt", CustomerID: "other-app", UserID: "jlahey", Provider: "deepgram", Model: "flux-general-en", StartedAt: s.base, Success: true}
	s.Require().NoError(insertRequest(s.ctx, s.store, theirs))
	_, err := s.store.global.ExecContext(s.ctx, `INSERT INTO data_changes (customer_id, table_name, op, key, payload)
SELECT app, 'requests', 'insert', '{}', jsonb_build_object('id', id, 'user_id', 'jlahey')
FROM requests, unnest(ARRAY['other-app', 'app']) WITH ORDINALITY AS a (app, n) WHERE usage_id = ? ORDER BY n`, theirs.UsageID)
	s.Require().NoError(err)
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()

	renamed, err := s.store.Requests.RenameUser(ctx, "app", "jlahey", "forgotten")

	s.Require().NoError(err)
	s.Zero(renamed)
	s.Empty(s.payloadsNaming("app", "jlahey"))
	s.Len(s.payloadsNaming("other-app", "jlahey"), 1)
	left, err := s.store.Requests.CountByUser(s.ctx, "other-app", "jlahey")
	s.Require().NoError(err)
	s.EqualValues(1, left)
}

// A batch finds its changes by their ids: by the index, or before the statistics know the user's
// changes by a filter on values the planner sees, which it looks up in a hash. Comparing each
// change with every id of a batch outgrows the statement limit.
func (s *StoreSuite) TestRenamingAUserReadsTheChangesOfABatchByTheirRequests() {
	if s.store.Sharded() {
		s.T().Skip("a shard's own tables record no changes (watchDataChanges)")
	}
	const requests = `
INSERT INTO requests (usage_id, modality, customer_id, user_id, provider, model, started_at, success)
SELECT gen_random_uuid(), 'stt', '%[1]s', CASE WHEN n %% 3 = 0 THEN 'rlafleur' ELSE 'jlahey' END, 'deepgram', 'flux-general-en', now() - n * interval '1 second', true
FROM generate_series(1, 60000) AS n;
INSERT INTO data_changes (customer_id, table_name, op, key, payload)
SELECT customer_id, 'requests', 'insert', jsonb_build_object('usage_id', usage_id), to_jsonb(r) FROM requests AS r WHERE customer_id = '%[1]s';`
	plan := func(app string) string {
		ctx, cancel := context.WithCancel(s.ctx)
		defer cancel()
		var sent string
		s.afterEach(rewritesABatch, func(query string) {
			if sent == "" {
				sent = query
				cancel()
			}
		})
		_, err := s.store.Requests.RenameUser(ctx, app, "jlahey", "forgotten")
		s.Require().ErrorIs(err, context.Canceled)
		var plan []string
		s.Require().NoError(s.store.global.NewRaw("EXPLAIN "+sent).Scan(s.ctx, &plan))
		return strings.Join(plan, "\n")
	}
	_, err := s.store.shard.ExecContext(s.ctx, fmt.Sprintf(requests, "known")+"ANALYZE requests, data_changes")
	s.Require().NoError(err)

	read := plan("known")
	s.Contains(read, "Index Scan using data_changes_user_idx on data_changes", "with statistics")
	s.Regexp(`Index Cond: .*\(\(payload ->> 'user_id'::text\) = 'jlahey'::text\) AND \(\(payload ->> 'id'::text\) = ANY \('{`, read, "with statistics")

	_, err = s.store.shard.ExecContext(s.ctx, fmt.Sprintf(requests, "new"))
	s.Require().NoError(err)

	s.Contains(plan("new"), `((payload ->> 'id'::text) = ANY ('{`, "with statistics from before the app's changes")
}

func (s *StoreSuite) TestRenamingAUserRenamesThemInTheChangesOfARequestDeletedBefore() {
	if s.store.Sharded() {
		s.T().Skip("a shard's own tables record no changes (watchDataChanges)")
	}
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "app", time.Hour, time.Second))
	gone := &Request{Modality: "stt", CustomerID: "app", UserID: "jlahey", Provider: "deepgram", Model: "flux-general-en", StartedAt: s.base, Success: true}
	s.Require().NoError(insertRequest(s.ctx, s.store, gone))
	_, err := s.store.shard.ExecContext(s.ctx, "DELETE FROM requests WHERE usage_id = ?", gone.UsageID)
	s.Require().NoError(err)
	s.Require().NotEmpty(s.payloadsNaming("app", "jlahey"), "the request's insert was recorded")

	_, err = s.store.Requests.RenameUser(s.ctx, "app", "jlahey", "forgotten")
	s.Require().NoError(err)

	s.Empty(s.payloadsNaming("app", "jlahey"))
}

func (s *StoreSuite) TestAUsersPolicyForgottenByACommitThatLandsAfterItGaveUpIsForgottenAgain() {
	proxy, dsn := s.pgProxy(true)
	slow, err := OpenShards(Options{DSN: dsn, ShardDSN: s.shardDSN, Redis: s.redis(), writeTimeout: 300 * time.Millisecond})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(slow.Close()) })
	app := "app-" + newID()
	user := UserScopeID(app, "jlahey")
	yes := true
	s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeUser, ScopeID: user, Document: PolicyDocument{PromptInjection: &yes}}))

	_, err = slow.ForgetUserSettings(s.ctx, app, "jlahey", "forgotten", &AuditEntry{
		CustomerID: app, ResourceType: AuditEndUser, ResourceID: "forgotten", Action: AuditUpdated, Source: AuditSourceCLI, ActorID: "ops",
	})
	s.Require().Error(err, "the COMMIT is held past the read timeout")
	document, err := s.store.Policies.Get(s.ctx, ScopeUser, user)
	s.Require().NoError(err)
	s.Require().NotNil(document.PromptInjection, "the policy is still there while the COMMIT is held")
	has, err := s.store.Policies.HasUsers(s.ctx, app)
	s.Require().NoError(err)
	s.Require().True(has, "and the app still has a user with a policy")
	// Past the first deletion that follows, so the COMMIT lands between two of them.
	for deadline := time.Now().Add(2 * forgetBackoff); time.Now().Before(deadline); {
		document, err := s.store.Policies.Get(s.ctx, ScopeUser, user)
		s.Require().NoError(err)
		s.Require().NotNil(document.PromptInjection, "the policy is still there while the COMMIT is held")
	}
	proxy.let()
	s.Require().Eventually(func() bool {
		held, err := s.store.global.NewSelect().Model((*Policy)(nil)).
			Where("scope = ? AND scope_id = ?", ScopeUser, user).Exists(s.ctx)
		s.Require().NoError(err)
		return !held
	}, 5*time.Second, 10*time.Millisecond, "the held COMMIT lands")

	s.Eventually(func() bool {
		document, err := s.store.Policies.Get(s.ctx, ScopeUser, user)
		s.Require().NoError(err)
		has, err := s.store.Policies.HasUsers(s.ctx, app)
		s.Require().NoError(err)
		return document.PromptInjection == nil && !has
	}, 5*time.Second, 10*time.Millisecond, "the copies cached before the COMMIT landed are dropped again")
}
