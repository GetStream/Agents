//go:build integration

package store

import (
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

// spend stores a request that cost the given amount, some minutes before the suite's base.
func (s *StoreSuite) spend(customerID string, costMicros int64, minutesAgo int) {
	request := &Request{
		Modality:   "llm",
		CustomerID: customerID,
		Provider:   "openai",
		Model:      "gpt-5.6-luna",
		StartedAt:  s.base.Add(-time.Duration(minutesAgo) * time.Minute),
		CostMicros: costMicros,
		Success:    true,
	}
	s.Require().NoError(insertRequest(s.ctx, s.store, request))
}

func (s *StoreSuite) TestSpendSinceCountsOnlyTheCustomerAndTheWindow() {
	s.spend("app-1", 1_000_000, 10)
	s.spend("app-1", 250_000, 90)
	s.spend("app-2", 7_000_000, 10)

	spent, err := s.store.Spend.Since(s.ctx, "app-1", s.base.Add(-time.Hour))

	s.Require().NoError(err)
	s.Equal(int64(1_000_000), spent)
}

func (s *StoreSuite) TestASubjectsSpendIsItsBillableRowsAlone() {
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: "app-1", OrganizationID: "org-1"}))
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: "app-2", OrganizationID: "org-1"}))
	row := func(customer, key, user string, categories []string, cost int64, billable bool) {
		s.Require().NoError(insertRequest(s.ctx, s.store, &Request{
			UsageID: uuid.Must(uuid.NewV7()), Modality: "llm", CustomerID: customer, Provider: "openai", Model: "m",
			StartedAt: s.base.Add(-time.Minute), APIKey: key, UserID: user, UsageCategories: categories,
			CostMicros: cost, Billable: billable, Success: true,
		}))
	}
	row("app-1", "k1", "u1", []string{"team:finance"}, 100, true)
	row("app-1", "k2", "u2", nil, 200, true)
	row("app-2", "k1", "u1", []string{"team:finance"}, 400, true)
	row("app-1", "k1", "u1", []string{"team:finance"}, 800, false)
	since := s.base.Add(-time.Hour)

	for ref, want := range map[usage.SubjectRef]int64{
		{OrganizationID: "org-1", Subject: usage.SubjectOrganization}:                                          700,
		{OrganizationID: "org-1", Subject: usage.SubjectApp, AppID: "app-1"}:                                   300,
		{OrganizationID: "org-1", Subject: usage.SubjectAPIKey, AppID: "app-1", APIKey: "k1"}:                  100,
		{OrganizationID: "org-1", Subject: usage.SubjectUser, AppID: "app-2", UserID: "u1"}:                    400,
		{OrganizationID: "org-1", Subject: usage.SubjectCategory, Category: "team:finance"}:                    500,
		{OrganizationID: "org-1", Subject: usage.SubjectAppCategory, AppID: "app-1", Category: "team:finance"}: 100,
	} {
		spent, err := s.store.Spend.Subject(s.ctx, ref, since, true)
		s.Require().NoError(err)
		s.Equal(want, spent, "%+v", ref)
	}
}

// orgSpend writes one request to store's shard, started at at, for organization and costing cost.
func (s *StoreSuite) orgSpend(store *Store, organization string, at time.Time, cost int64, change ...func(*Request)) {
	request := &Request{
		Modality: "llm", CustomerID: "app-" + organization, OrganizationID: organization, Provider: "openai", Model: "m",
		StartedAt: at, CostMicros: cost, Billable: true, Success: true,
	}
	for _, apply := range change {
		apply(request)
	}
	s.Require().NoError(insertRequest(s.ctx, store, request))
}

func (s *StoreSuite) TestDailySpendIsEachOrganizationsBillableSpendPerUTCDay() {
	day := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	s.orgSpend(s.store, "org-a", day, 300)
	s.orgSpend(s.store, "org-a", day.Add(23*time.Hour+59*time.Minute), 200)
	s.orgSpend(s.store, "org-a", day.Add(24*time.Hour), 7)
	s.orgSpend(s.store, "org-a", day.Add(2*time.Hour), 1000, func(r *Request) { r.Billable = false })
	s.orgSpend(s.store, "org-a", day.Add(-time.Second), 50)
	s.orgSpend(s.store, "org-b", day.Add(3*time.Hour), 40)
	s.orgSpend(s.store, "", day.Add(3*time.Hour), 9000, func(r *Request) { r.CustomerID = "app-unowned" })
	s.orgSpend(s.store, "org-a", day.Add(48*time.Hour), 11)

	spent, err := s.store.Spend.Daily(s.ctx, day, day.AddDate(0, 0, 2))

	s.Require().NoError(err)
	s.Equal([]DaySpend{
		{OrganizationID: "org-a", Day: day, CostMicros: 500},
		{OrganizationID: "org-a", Day: day.AddDate(0, 0, 1), CostMicros: 7},
		{OrganizationID: "org-b", Day: day, CostMicros: 40},
	}, spent, "free work, a row with no organization and rows outside the range are left out; the first and "+
		"last minutes of a day are that day's")
}

// A day is the UTC day whatever time zone the database session is in: without AT TIME ZONE 'UTC'
// a session in Los Angeles files a request made at 03:00 UTC under the day before.
func (s *StoreSuite) TestDailySpendIsUTCWhateverTheSessionTimeZone() {
	day := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	la, err := OpenShards(Options{DSN: s.dsn + "&timezone=America/Los_Angeles", ShardDSN: s.shardDSN + "&timezone=America/Los_Angeles"})
	s.Require().NoError(err)
	defer la.Close()
	var zone string
	s.Require().NoError(la.GlobalDB().QueryRowContext(s.ctx, "SHOW timezone").Scan(&zone))
	s.Require().Equal("America/Los_Angeles", zone, "the session is not in UTC")
	s.orgSpend(la, "org-tz", day.Add(3*time.Hour), 300)

	spent, err := la.Spend.Daily(s.ctx, day, day.AddDate(0, 0, 1))

	s.Require().NoError(err)
	s.Equal([]DaySpend{{OrganizationID: "org-tz", Day: day, CostMicros: 300}}, spent)
}

func (s *ShardedStoreSuite) TestDailySpendSumsEveryShard() {
	day := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	s.orgSpend(s.store, "org-a", day.Add(time.Hour), 300)
	s.orgSpend(s.other, "org-a", day.Add(2*time.Hour), 200)

	spent, err := s.store.Spend.Daily(s.ctx, day, day.AddDate(0, 0, 1))

	s.Require().NoError(err)
	s.Equal([]DaySpend{{OrganizationID: "org-a", Day: day, CostMicros: 500}}, spent)
}

// usedBy is a request of app's of modality at at, its quantities set by change.
func (s *StoreSuite) usedBy(appID, modality string, at time.Time, change func(*Request)) {
	request := &Request{Modality: modality, CustomerID: appID, Provider: "stub", Model: "stub", StartedAt: at, Success: true}
	change(request)
	s.Require().NoError(insertRequest(s.ctx, s.store, request))
}

// Defect: a meter summed for one app of the organization or for every app, a quantity left
// out or summed from another column, a request before since counted, or the sliced time of
// runtime and sandboxes counted as requests.
func (s *StoreSuite) TestMeterUsesSumTheAppsAskedForByModality() {
	took := func(ms float64) *float64 { return &ms }
	s.usedBy("one", "llm", s.base, func(r *Request) { r.InputTokens, r.OutputTokens, r.CostMicros = 10, 20, 5 })
	s.usedBy("two", "llm", s.base.Add(time.Hour), func(r *Request) { r.InputTokens, r.OutputTokens, r.CostMicros = 1, 2, 1 })
	s.usedBy("one", "tts", s.base, func(r *Request) { r.AudioMs, r.Characters, r.CostMicros = 1500, 42, 3 })
	s.usedBy("two", "sandbox", s.base, func(r *Request) { r.DurationMs, r.CostMicros = took(60_000), 7 })
	s.usedBy("one", "runtime", s.base, func(r *Request) { r.DurationMs, r.CostMicros = took(30_000.4), 2 })
	s.usedBy("stranger", "llm", s.base, func(r *Request) { r.InputTokens, r.CostMicros = 1000, 1000 })
	s.usedBy("one", "llm", s.base.Add(-time.Second), func(r *Request) { r.InputTokens, r.CostMicros = 1000, 1000 })

	uses, err := s.store.Spend.Meters(s.ctx, []string{"one", "two"}, s.base)
	s.Require().NoError(err)

	s.Equal([]MeterUse{
		{Modality: "llm", Requests: 2, InputTokens: 11, OutputTokens: 22, CostMicros: 6},
		{Modality: "runtime", DurationMs: 30_000, CostMicros: 2},
		{Modality: "sandbox", DurationMs: 60_000, CostMicros: 7},
		{Modality: "tts", Requests: 1, AudioMs: 1500, Characters: 42, CostMicros: 3},
	}, uses)
}

// Defect: the requests of another shard left out, a shard's answer replacing another's, or one
// modality listed once per shard.
func (s *ShardedStoreSuite) TestMeterUsesSumEveryShard() {
	took := 1000.0
	s.usedBy("one", "llm", s.base, func(r *Request) {
		r.InputTokens, r.OutputTokens, r.AudioMs, r.Characters, r.DurationMs, r.CostMicros = 10, 20, 30, 40, &took, 5
	})
	s.usedBy("one", "tts", s.base, func(r *Request) { r.Characters, r.CostMicros = 9, 1 })
	request := &Request{Modality: "llm", CustomerID: "two", Provider: "stub", Model: "stub", StartedAt: s.base,
		InputTokens: 1, OutputTokens: 2, AudioMs: 3, Characters: 4, DurationMs: &took, CostMicros: 1, Success: true}
	s.Require().NoError(insertRequest(s.ctx, s.other, request))

	uses, err := s.store.Spend.Meters(s.ctx, []string{"one", "two"}, s.base)
	s.Require().NoError(err)
	s.Equal([]MeterUse{{Modality: "llm", Requests: 2, InputTokens: 11, OutputTokens: 22, AudioMs: 33, Characters: 44,
		DurationMs: 2000, CostMicros: 6}, {Modality: "tts", Requests: 1, Characters: 9, CostMicros: 1}}, uses)
}

// Defect: the shards the meters were read from not named, or named in another order than the
// reads take them, this shard's first.
func (s *ShardedStoreSuite) TestMeterShardsNameEveryShardThisOneFirst() {
	s.Equal([]string{"here", "there"}, s.store.Spend.Shards())
	s.Equal([]string{"there", "here"}, s.other.Spend.Shards())
}

func (s *StoreSuite) TestSpendQueryBudgets() {
	organization := "org-" + uuid.NewString()
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: "app-1", OrganizationID: organization}))
	shards := len(s.store.shards())

	s.Equal(shards, s.countQueries(func() {
		_, err := s.store.Spend.Since(s.ctx, "app-1", s.base)
		s.Require().NoError(err)
	}), "one sum a shard")
	s.Equal(shards, s.countQueries(func() {
		_, err := s.store.Spend.Subject(s.ctx, usage.SubjectRef{Subject: usage.SubjectApp, OrganizationID: organization, AppID: "app-1"}, s.base, true)
		s.Require().NoError(err)
	}))
	s.Equal(1+shards, s.countQueries(func() {
		_, err := s.store.Spend.Subject(s.ctx, usage.SubjectRef{Subject: usage.SubjectOrganization, OrganizationID: organization}, s.base, true)
		s.Require().NoError(err)
	}), "the organization's apps, then one sum a shard")
	s.Equal(1+shards, s.countQueries(func() {
		_, err := s.store.Spend.Teams(s.ctx, organization, []time.Time{s.base}, s.base)
		s.Require().NoError(err)
	}), "the organization's apps, then one sum a shard")
	s.Equal(shards, s.countQueries(func() {
		_, err := s.store.Spend.Daily(s.ctx, s.base, s.base.AddDate(0, 0, 1))
		s.Require().NoError(err)
	}))
	s.Equal(shards, s.countQueries(func() {
		_, err := s.store.Spend.Meters(s.ctx, []string{"app-1"}, s.base)
		s.Require().NoError(err)
	}))
}
