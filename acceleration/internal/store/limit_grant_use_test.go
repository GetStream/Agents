//go:build integration

package store

import (
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

// grantRow is a request row that names grants, written as the writer writes it: of the
// organization of the first grant it names.
func (s *StoreSuite) grantRow(store *Store, grants []int64, cost int64, billable bool) Request {
	row := Request{
		UsageID: uuid.Must(uuid.NewV7()), Modality: "llm", CustomerID: "app-1", OrganizationID: "org-1", Provider: "openai", Model: "m",
		StartedAt: s.base, CostMicros: cost, Billable: billable, Success: true, LimitGrants: grants,
	}
	if len(grants) > 0 {
		grant, err := s.store.LimitGrants.Get(s.ctx, "", grants[0])
		s.Require().NoError(err)
		row.OrganizationID = grant.OrganizationID
	}
	s.writeAsTheWriter(store, row)
	return row
}

// writeAsTheWriter writes the rows and counts the ones written toward their grants, as the
// writer does: RecordRequests counts them with one database, the writer's grant counter apart
// with shards.
func (s *StoreSuite) writeAsTheWriter(store *Store, rows ...Request) {
	written, err := store.Requests.InsertMany(s.ctx, rows)
	s.Require().NoError(err)
	if store.Sharded() {
		_, err = store.LimitGrantUses.InsertMany(s.ctx, GrantUsesOf(rows, written))
		s.Require().NoError(err)
	}
}

// used is what the grant has paid for, as its row says.
func (s *StoreSuite) used(id int64) int64 {
	grant, err := s.store.LimitGrants.Get(s.ctx, "", id)
	s.Require().NoError(err)
	return grant.UsedMicros
}

func (s *StoreSuite) TestAWrittenRowCountsTowardEachGrantItNames() {
	a, b := s.grant(LimitGrant{AmountMicros: 10_000}), s.grant(LimitGrant{AmountMicros: 10_000})

	s.grantRow(s.store, []int64{a.ID}, 100, true)
	s.grantRow(s.store, []int64{a.ID, b.ID}, 200, true)
	s.grantRow(s.store, []int64{a.ID}, 400, false)
	s.grantRow(s.store, nil, 800, true)

	s.EqualValues(300, s.used(a.ID), "billable rows only")
	s.EqualValues(200, s.used(b.ID), "a row naming two counts for both")
}

func (s *StoreSuite) TestAWrittenRowCountsOnlyTowardItsOwnOrganizationsGrant() {
	grant := s.grant(LimitGrant{AmountMicros: 10_000})
	written := func(organization string, cost int64) Request {
		return Request{
			UsageID: uuid.Must(uuid.NewV7()), Modality: "llm", CustomerID: "app-2", OrganizationID: organization, Provider: "openai",
			Model: "m", StartedAt: s.base, CostMicros: cost, Billable: true, Success: true, LimitGrants: []int64{grant.ID},
		}
	}

	s.writeAsTheWriter(s.store, written("org-2", 300), written("org-1", 50), written("", 700))

	s.EqualValues(50, s.used(grant.ID), "a row of another organization, or of none, naming the grant counts nothing")
}

// unresolved is a request row of the app naming the grant, written without its organization,
// as an app in none has it and as the writer leaves a row it could not look the app up for.
func (s *StoreSuite) unresolved(app string, cost int64, grant int64) Request {
	return Request{
		UsageID: uuid.Must(uuid.NewV7()), Modality: "llm", CustomerID: app, Provider: "openai", Model: "m",
		StartedAt: s.base, CostMicros: cost, Billable: true, Success: true, LimitGrants: []int64{grant},
	}
}

// member records the app as seen under the organization.
func (s *StoreSuite) member(app, organization string) {
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: app, OrganizationID: organization}))
}

func (s *StoreSuite) TestARowWithoutItsOrganizationCountsTowardItsAppsGrants() {
	own := s.grant(LimitGrant{OrganizationID: "app-alone", AmountMicros: 10_000})
	theirs := s.grant(LimitGrant{AmountMicros: 10_000})
	s.member("app-1", "org-1")
	s.member("org-1", "org-2")

	s.writeAsTheWriter(s.store, s.unresolved("app-alone", 100, own.ID), s.unresolved("app-1", 20, theirs.ID),
		s.unresolved("app-2", 3_000, own.ID), s.unresolved("app-2", 3_000, theirs.ID), s.unresolved("org-1", 3_000, theirs.ID))

	s.EqualValues(100, s.used(own.ID), "an app in no organization keeps its grants under its own id")
	s.EqualValues(20, s.used(theirs.ID), "a row of the organization's app counts; one of another app, or of an app of "+
		"another organization whose id is this one's, does not")
}

func (s *StoreSuite) TestAGrantScopedToAnAppCountsOnlyThatAppsRows() {
	app := s.grant(LimitGrant{Subject: usage.SubjectApp, AppID: "app-1", AmountMicros: 10_000})
	key := s.grant(LimitGrant{Subject: usage.SubjectAPIKey, AppID: "app-1", APIKey: "k", AmountMicros: 10_000})
	written := func(app string, cost int64, grant int64) Request {
		return Request{
			UsageID: uuid.Must(uuid.NewV7()), Modality: "llm", CustomerID: app, OrganizationID: "org-1", Provider: "openai",
			Model: "m", StartedAt: s.base, CostMicros: cost, Billable: true, Success: true, LimitGrants: []int64{grant},
		}
	}

	s.writeAsTheWriter(s.store, written("app-1", 40, app.ID), written("app-3", 900, app.ID), written("app-1", 5, key.ID), written("app-3", 900, key.ID))

	s.EqualValues(40, s.used(app.ID), "a row of another app of the organization counts nothing toward the app's grant")
	s.EqualValues(5, s.used(key.ID), "nor toward a grant of the app's key")
}

func (s *StoreSuite) TestAUseRefusedOnWhoseTheGrantIsIsCountedOnMetrics() {
	grant := s.grant(LimitGrant{AmountMicros: 10_000})
	moved := uuid.Must(uuid.NewV7())
	_, err := s.store.LimitGrantUses.InsertMany(s.ctx, []GrantUse{{GrantID: grant.ID, CustomerID: "app-1", OrganizationID: "org-1", UsageID: moved, Micros: 10}})
	s.Require().NoError(err)
	before := testutil.ToFloat64(grantUsesRefused)

	_, err = s.store.LimitGrantUses.InsertMany(s.ctx, []GrantUse{
		{GrantID: grant.ID, CustomerID: "app-1", OrganizationID: "org-1", UsageID: uuid.Must(uuid.NewV7()), Micros: 10},
		{GrantID: grant.ID, CustomerID: "app-2", OrganizationID: "org-2", UsageID: uuid.Must(uuid.NewV7()), Micros: 10},
		{GrantID: grant.ID, CustomerID: "app-2", OrganizationID: "org-2", UsageID: moved, Micros: 10},
		{GrantID: grant.ID + 1_000_000, CustomerID: "app-2", OrganizationID: "org-2", UsageID: uuid.Must(uuid.NewV7()), Micros: 10},
	})
	s.Require().NoError(err)

	s.InDelta(1, testutil.ToFloat64(grantUsesRefused)-before, 0, "only the use of another organization the grant had not counted; "+
		"a use it counts, one it counted before and one of a grant that is not there are not refusals")
	s.EqualValues(20, s.used(grant.ID))
}

func (s *StoreSuite) TestARowWrittenTwiceCountsOnceTowardItsGrant() {
	grant := s.grant(LimitGrant{AmountMicros: 10_000})
	row := s.grantRow(s.store, []int64{grant.ID}, 300, true)

	again := row
	again.ID = 0
	written, err := s.store.Requests.InsertMany(s.ctx, []Request{again, again})
	s.Require().NoError(err)
	s.Empty(written, "the shard has it")
	counted, err := s.store.LimitGrantUses.InsertMany(s.ctx, []GrantUse{{GrantID: grant.ID, OrganizationID: "org-1", UsageID: row.UsageID, Micros: 300}})
	s.Require().NoError(err)
	s.Zero(counted, "nor does counting it again")

	s.EqualValues(300, s.used(grant.ID))
	fresh := Request{
		UsageID: uuid.Must(uuid.NewV7()), Modality: "llm", CustomerID: "app-1", OrganizationID: "org-1", Provider: "openai", Model: "m",
		StartedAt: s.base, CostMicros: 50, Billable: true, Success: true, LimitGrants: []int64{grant.ID},
	}
	s.writeAsTheWriter(s.store, fresh, fresh)
	s.EqualValues(350, s.used(grant.ID), "a row sent twice in one batch counts once")
}

func (s *StoreSuite) TestGrantUsesCountedIsHowManyGrantsEachRowCountsToward() {
	a, b := s.grant(LimitGrant{AmountMicros: 10_000}), s.grant(LimitGrant{AmountMicros: 10_000})
	both, one, other, unasked := uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7())
	_, err := s.store.LimitGrantUses.InsertMany(s.ctx, []GrantUse{
		{GrantID: a.ID, OrganizationID: "org-1", UsageID: both, Micros: 10},
		{GrantID: b.ID, OrganizationID: "org-1", UsageID: both, Micros: 10},
		{GrantID: a.ID, OrganizationID: "org-1", UsageID: one, Micros: 10},
		{GrantID: b.ID, OrganizationID: "org-1", UsageID: unasked, Micros: 10},
	})
	s.Require().NoError(err)

	counted, err := s.store.LimitGrantUses.Count(s.ctx, []uuid.UUID{both, one, other})
	s.Require().NoError(err)
	s.Equal(map[uuid.UUID]int{both: 2, one: 1}, counted, "a row counted toward nothing, or not asked about, is left out")
}

func (s *StoreSuite) TestTheUsesOfAGrantEndedLongAgoArePruned() {
	ended := func(column string, ago time.Duration) LimitGrant {
		grant := s.grant(LimitGrant{AmountMicros: 10_000})
		s.grantRow(s.store, []int64{grant.ID}, 300, true)
		_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE limit_grants SET created_at = now() - make_interval(secs => ?), "+
			column+" = now() - make_interval(secs => ?) WHERE id = ?", (ago + time.Hour).Seconds(), ago.Seconds(), grant.ID)
		s.Require().NoError(err)
		return grant
	}
	expired, revoked := ended("expires_at", GrantUsesKept+time.Hour), ended("revoked_at", GrantUsesKept+time.Hour)
	recent := ended("revoked_at", GrantUsesKept-time.Hour)
	live := s.grant(LimitGrant{AmountMicros: 10_000})
	s.grantRow(s.store, []int64{live.ID}, 300, true)
	uses := func(id int64) (n int) {
		s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx, "SELECT count(*) FROM limit_grant_uses WHERE grant_id = ?", id).Scan(&n))
		return n
	}

	pruned, err := s.store.LimitGrantUses.Prune(s.ctx)
	s.Require().NoError(err)
	s.EqualValues(2, pruned)
	s.Zero(uses(expired.ID))
	s.Zero(uses(revoked.ID))
	s.Equal(1, uses(recent.ID), "a grant ended inside GrantUsesKept keeps them")
	s.Equal(1, uses(live.ID))
	s.EqualValues(300, s.used(expired.ID), "what it paid for stays")
	counted, err := s.store.LimitGrantUses.InsertMany(s.ctx, []GrantUse{{GrantID: revoked.ID, OrganizationID: "org-1", UsageID: uuid.Must(uuid.NewV7()), Micros: 5}})
	s.Require().NoError(err)
	s.Zero(counted, "nor does it count anything more")
	s.EqualValues(300, s.used(revoked.ID))
}

func (s *ShardedStoreSuite) TestRowsInEveryShardCountTowardTheirGrant() {
	grant := s.grant(LimitGrant{AmountMicros: 10_000})
	for _, store := range []*Store{s.store, s.other} {
		s.grantRow(store, []int64{grant.ID}, 300, true)
	}

	s.EqualValues(600, s.used(grant.ID))
}

// uncounted writes a row naming the grants to the shard as a writer that dropped its count,
// or stopped before it, leaves it.
func (s *StoreSuite) uncounted(store *Store, startedAt time.Time, cost int64, grants ...int64) {
	lost := Request{
		UsageID: uuid.Must(uuid.NewV7()), Modality: "llm", CustomerID: "app-1", OrganizationID: "org-1", Provider: "openai", Model: "m",
		StartedAt: startedAt, CostMicros: cost, Billable: true, Success: true, LimitGrants: grants,
	}
	s.Require().NoError(prepareRequest(&lost))
	_, err := store.ShardDB().NewInsert().Model(&lost).Exec(s.ctx)
	s.Require().NoError(err)
}

func (s *ShardedStoreSuite) reconcile() int {
	added, err := s.store.LimitGrantUses.Reconcile(s.ctx)
	s.Require().NoError(err)
	return added
}

func (s *ShardedStoreSuite) TestAReconcileCountsARowItsWriterDidNot() {
	grant := s.grant(LimitGrant{AmountMicros: 10_000})
	s.grantRow(s.store, []int64{grant.ID}, 300, true)
	s.uncounted(s.other, s.base, 70, grant.ID)
	s.EqualValues(300, s.used(grant.ID))

	s.Equal(1, s.reconcile(), "the row the writer counted is not counted again")
	s.EqualValues(370, s.used(grant.ID))
	s.Zero(s.reconcile())
	s.EqualValues(370, s.used(grant.ID))
}

func (s *ShardedStoreSuite) TestAReconcileCountsARowOnlyTowardItsOwnOrganizationsGrant() {
	grant := s.grant(LimitGrant{AmountMicros: 10_000})
	s.uncounted(s.other, s.base, 70, grant.ID)
	foreign := Request{
		UsageID: uuid.Must(uuid.NewV7()), Modality: "llm", CustomerID: "app-2", OrganizationID: "org-2", Provider: "openai", Model: "m",
		StartedAt: s.base, CostMicros: 9_000, Billable: true, Success: true, LimitGrants: []int64{grant.ID},
	}
	s.Require().NoError(prepareRequest(&foreign))
	_, err := s.other.ShardDB().NewInsert().Model(&foreign).Exec(s.ctx)
	s.Require().NoError(err)

	s.Equal(1, s.reconcile(), "only the row of the grant's own organization")
	s.EqualValues(70, s.used(grant.ID))
}

func (s *ShardedStoreSuite) TestAReconcileCountsARowWithoutItsOrganizationTowardItsAppsGrants() {
	own := s.grant(LimitGrant{OrganizationID: "app-alone", AmountMicros: 10_000})
	theirs := s.grant(LimitGrant{AmountMicros: 10_000})
	s.member("app-1", "org-1")
	for _, row := range []Request{s.unresolved("app-alone", 100, own.ID), s.unresolved("app-1", 20, theirs.ID), s.unresolved("app-2", 3_000, own.ID)} {
		s.Require().NoError(prepareRequest(&row))
		_, err := s.other.ShardDB().NewInsert().Model(&row).Exec(s.ctx)
		s.Require().NoError(err)
	}

	s.Equal(2, s.reconcile(), "the rows of the grants' own apps")
	s.EqualValues(100, s.used(own.ID))
	s.EqualValues(20, s.used(theirs.ID))
}

func (s *ShardedStoreSuite) TestAReconcileReadsOnlyTheRowsSinceTheLastLessItsOverlap() {
	grant := s.grant(LimitGrant{AmountMicros: 1_000_000})
	for range grantReconcilePage + 1 {
		s.uncounted(s.store, s.base, 1, grant.ID)
	}
	s.Equal(grantReconcilePage+1, s.reconcile(), "the first reads every row, page by page")
	var shards []string
	s.Require().NoError(s.store.GlobalDB().NewRaw("SELECT shard FROM limit_grant_reconciles ORDER BY shard").Scan(s.ctx, &shards))
	s.Equal([]string{"here", "there"}, shards, "where it got in each shard is kept")

	now := time.Now().UTC()
	s.uncounted(s.store, s.base.Add(time.Minute), 10, grant.ID)
	s.uncounted(s.store, now.Add(-GrantReconcileOverlap/2), 100, grant.ID)
	s.uncounted(s.other, now.Add(-GrantReconcileOverlap/2), 1_000, grant.ID)
	s.Equal(2, s.reconcile(), "a row started before the last reconcile less its overlap is not read again")
	s.EqualValues(grantReconcilePage+1+100+1_000, s.used(grant.ID))
}

func (s *ShardedStoreSuite) TestOneRouterReconcilesAtATime() {
	grant := s.grant(LimitGrant{AmountMicros: 10_000})
	s.uncounted(s.store, s.base, 70, grant.ID)
	other, err := s.store.GlobalDB().Conn(s.ctx)
	s.Require().NoError(err)
	defer other.Close()
	_, err = other.ExecContext(s.ctx, "SELECT pg_advisory_lock(hashtextextended('limit_grant_reconciles', 0))")
	s.Require().NoError(err)

	added, err := s.store.LimitGrantUses.Reconcile(s.ctx)
	s.ErrorIs(err, ErrReconcileBusy, "another router is at it")
	s.Zero(added)
	s.Zero(s.used(grant.ID))

	_, err = other.ExecContext(s.ctx, "SELECT pg_advisory_unlock(hashtextextended('limit_grant_reconciles', 0))")
	s.Require().NoError(err)
	s.Equal(1, s.reconcile())
	s.EqualValues(70, s.used(grant.ID))
}

func (s *StoreSuite) TestWithOneDatabaseThereIsNothingToReconcile() {
	if s.store.Sharded() {
		return
	}
	grant := s.grant(LimitGrant{AmountMicros: 10_000})
	s.uncounted(s.store, s.base, 70, grant.ID)
	added, err := s.store.LimitGrantUses.Reconcile(s.ctx)
	s.Require().NoError(err)
	s.Zero(added, "the writer counts every row in the transaction that writes it")
}

func (s *StoreSuite) TestLimitGrantUseQueryBudgets() {
	a, b := s.grant(LimitGrant{AmountMicros: 10_000}), s.grant(LimitGrant{AmountMicros: 10_000})
	first, second := uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7())
	s.Equal(4, s.countQueries(func() {
		counted, err := s.store.LimitGrantUses.InsertMany(s.ctx, []GrantUse{
			{GrantID: a.ID, OrganizationID: "org-1", UsageID: first, Micros: 1}, {GrantID: b.ID, OrganizationID: "org-1", UsageID: first, Micros: 1}, {GrantID: a.ID, OrganizationID: "org-1", UsageID: second, Micros: 1},
		})
		s.Require().NoError(err)
		s.Equal(3, counted)
	}), "begin, the grants locked, every use counted in one statement, commit")
	s.Equal(1, s.countQueries(func() {
		counted, err := s.store.LimitGrantUses.Count(s.ctx, []uuid.UUID{first, second})
		s.Require().NoError(err)
		s.Equal(map[uuid.UUID]int{first: 2, second: 1}, counted)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.LimitGrantUses.Prune(s.ctx)
		s.Require().NoError(err)
	}), "one delete while fewer than a batch are due")
	shards := len(s.store.shards())
	reconcile := 0
	if s.store.Sharded() {
		reconcile = 3 + 3*shards
	}
	s.Equal(reconcile, s.countQueries(func() {
		_, err := s.store.LimitGrantUses.Reconcile(s.ctx)
		s.Require().NoError(err)
	}), "begin, the lock, and for each shard where it got, one page and where it gets to, then the end")
}

// countedByID writes the row to the shard and counts it toward each grant it names by id
// alone, as the writer and the reconcile did before the owner check.
func (s *StoreSuite) countedByID(store *Store, row Request) {
	s.Require().NoError(prepareRequest(&row))
	_, err := store.ShardDB().NewInsert().Model(&row).Exec(s.ctx)
	s.Require().NoError(err)
	for _, grant := range row.LimitGrants {
		_, err = s.store.GlobalDB().NewInsert().Model(&grantUse{GrantID: grant, UsageID: row.UsageID, Micros: row.CostMicros}).Exec(s.ctx)
		s.Require().NoError(err)
	}
}

// namingGrants is a billable row of the app and organization naming the grants.
func (s *StoreSuite) namingGrants(app, organization string, cost int64, grants ...int64) Request {
	return Request{
		UsageID: uuid.Must(uuid.NewV7()), Modality: "llm", CustomerID: app, OrganizationID: organization, Provider: "openai", Model: "m",
		StartedAt: s.base, CostMicros: cost, Billable: true, Success: true, LimitGrants: grants,
	}
}

func (s *StoreSuite) foreign() []ForeignGrantUses {
	foreign, err := s.store.LimitGrantUses.Foreign(s.ctx)
	s.Require().NoError(err)
	return foreign
}

func (s *StoreSuite) TestForeignIsTheRowsCountedTowardAGrantNotTheirs() {
	organization := s.grant(LimitGrant{AmountMicros: 10_000})
	app := s.grant(LimitGrant{Subject: usage.SubjectApp, AppID: "app-1", AmountMicros: 10_000})
	alone := s.grant(LimitGrant{OrganizationID: "app-alone", AmountMicros: 10_000})
	s.member("app-1", "org-1")
	for _, row := range []Request{
		s.namingGrants("app-1", "org-1", 1, organization.ID, app.ID),
		s.namingGrants("app-9", "org-1", 2, organization.ID),
		s.namingGrants("app-1", "", 3, organization.ID),
		s.namingGrants("app-alone", "", 4, alone.ID),
		s.namingGrants("app-2", "org-2", 900, organization.ID),
		s.namingGrants("app-3", "org-1", 50, app.ID),
		s.namingGrants("app-2", "org-2", 4_000, organization.ID, app.ID, alone.ID),
	} {
		s.countedByID(s.store, row)
	}
	s.uncounted(s.store, s.base, 70_000, organization.ID)
	notCounted := s.namingGrants("app-2", "org-2", 80_000, organization.ID)
	s.Require().NoError(prepareRequest(&notCounted))
	_, err := s.store.ShardDB().NewInsert().Model(&notCounted).Exec(s.ctx)
	s.Require().NoError(err)

	s.Equal([]ForeignGrantUses{
		{GrantID: organization.ID, OrganizationID: "org-1", Rows: 2, Micros: 4_900},
		{GrantID: app.ID, OrganizationID: "org-1", Rows: 2, Micros: 4_050},
		{GrantID: alone.ID, OrganizationID: "app-alone", Rows: 1, Micros: 4_000},
	}, s.foreign(), "a row of the grant's organization, of its app or of an app alone counts toward it; one of another "+
		"organization or another app does not, and one the grant never counted is not there to take off")
}

func (s *StoreSuite) TestForeignReadsEveryPageOfAShard() {
	grant := s.grant(LimitGrant{AmountMicros: 1_000_000})
	own := make([]Request, grantReconcilePage)
	for i := range own {
		own[i] = s.namingGrants("app-1", "org-1", 1, grant.ID)
	}
	_, err := s.store.Requests.InsertMany(s.ctx, own)
	s.Require().NoError(err)
	late := s.namingGrants("app-2", "org-2", 900, grant.ID)
	late.StartedAt = s.base.Add(time.Minute)
	s.countedByID(s.store, late)

	s.Equal([]ForeignGrantUses{{GrantID: grant.ID, OrganizationID: "org-1", Rows: 1, Micros: 900}}, s.foreign(),
		"the row after the first page")
}

func (s *ShardedStoreSuite) TestForeignCountsEveryShardAndEachUseOnce() {
	grant := s.grant(LimitGrant{AmountMicros: 10_000})
	here := s.namingGrants("app-2", "org-2", 900, grant.ID)
	s.countedByID(s.store, here)
	s.countedByID(s.other, s.namingGrants("app-3", "org-3", 50, grant.ID))
	copied := here
	s.Require().NoError(prepareRequest(&copied))
	_, err := s.other.ShardDB().NewInsert().Model(&copied).Exec(s.ctx)
	s.Require().NoError(err)

	s.Equal([]ForeignGrantUses{{GrantID: grant.ID, OrganizationID: "org-1", Rows: 2, Micros: 950}}, s.foreign(),
		"a row in another shard counts; one held by two shards was counted once")
}
