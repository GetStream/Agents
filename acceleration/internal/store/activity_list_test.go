//go:build integration

package store

import (
	"time"
)

// on is a delta of organization's app in area from client on the UTC day n days from s.base's,
// refused of its requests refused.
func (s *StoreSuite) on(n int, organization, app, area, client string, requests, refused int64) ActivityDelta {
	day := s.base.UTC().Truncate(24*time.Hour).AddDate(0, 0, n)
	return ActivityDelta{
		OrganizationID: organization, AppID: app, Area: area, Client: client,
		Day: day, Requests: requests, Refused: refused, FirstAt: day.Add(time.Hour), LastAt: day.Add(2 * time.Hour),
	}
}

// listed is a row as a test names it: organization, day, app, area and client.
type listed struct{ organization, day, app, area, client string }

func listedOf(rows []ActivityRow) []listed {
	names := make([]listed, 0, len(rows))
	for _, row := range rows {
		names = append(names, listed{row.OrganizationID, row.Day.Format(time.DateOnly), row.AppID, row.Area, row.Client})
	}
	return names
}

// listFixture writes rows that tie at every key of List's order but the last, on the days
// around the range of s.base's day and the next.
func (s *StoreSuite) listFixture() {
	s.Require().NoError(s.store.Activities.Add(s.ctx, []ActivityDelta{
		s.on(0, "org-b", "app-3", "agent_setup", AuditSourceCLI, 1, 0),
		s.on(1, "org-a", "app-2", "conversations", AuditSourceSDK, 1, 0),
		s.on(0, "org-a", "app-2", "agent_setup", AuditSourceSDK, 1, 0),
		s.on(0, "org-a", "app-1", "agent_setup", AuditSourceSDK, 1, 0),
		s.on(0, "org-a", "app-1", "conversations", AuditSourceCLI, 1, 0),
		s.on(0, "org-a", "app-1", "agent_setup", AuditSourceCLI, 4, 1),
		s.on(-1, "org-a", "app-1", "agent_setup", AuditSourceCLI, 1, 0),
		s.on(2, "org-a", "app-1", "agent_setup", AuditSourceCLI, 1, 0),
		s.on(0, "", "app-9", "agent_setup", AuditSourceCLI, 1, 0),
	}))
}

// Defect: rows sorted by app before day, or by any key in another order, so a cursor built on
// the order skips or repeats rows; a day outside the range read, either end left out; or a
// row with no organization listed.
func (s *StoreSuite) TestActivityListIsTheRangesRowsByOrganizationDayAppAreaAndClient() {
	s.listFixture()
	day := s.base.UTC().Format(time.DateOnly)
	next := s.base.UTC().AddDate(0, 0, 1).Format(time.DateOnly)

	rows, err := s.store.Activities.List(s.ctx, s.base, s.base.AddDate(0, 0, 1), nil, 100)
	s.Require().NoError(err)

	s.Equal([]listed{
		{"org-a", day, "app-1", "agent_setup", AuditSourceCLI},
		{"org-a", day, "app-1", "agent_setup", AuditSourceSDK},
		{"org-a", day, "app-1", "conversations", AuditSourceCLI},
		{"org-a", day, "app-2", "agent_setup", AuditSourceSDK},
		{"org-a", next, "app-2", "conversations", AuditSourceSDK},
		{"org-b", day, "app-3", "agent_setup", AuditSourceCLI},
	}, listedOf(rows))
	s.Equal(int64(4), rows[0].Requests)
	s.Equal(int64(1), rows[0].Refused)
	s.True(s.base.UTC().Truncate(24*time.Hour).Add(time.Hour).Equal(rows[0].FirstAt), "first at %s", rows[0].FirstAt)
}

// Defect: a position that compares fewer keys than the order sorts by, or compares them the
// wrong way, so a page that ends between two rows tying on the first keys loses or repeats one;
// or a limit not kept.
func (s *StoreSuite) TestActivityListGoesOnAfterAPositionWithNoRowLostOrRepeated() {
	s.listFixture()
	from, to := s.base, s.base.AddDate(0, 0, 1)
	every, err := s.store.Activities.List(s.ctx, from, to, nil, 100)
	s.Require().NoError(err)

	var walked []ActivityRow
	var after *ActivityPosition
	for range len(every) {
		rows, err := s.store.Activities.List(s.ctx, from, to, after, 2)
		s.Require().NoError(err)
		s.Require().LessOrEqual(len(rows), 2)
		if len(rows) == 0 {
			break
		}
		walked = append(walked, rows...)
		last := rows[len(rows)-1]
		after = &ActivityPosition{
			OrganizationID: last.OrganizationID, Day: last.Day.Format(time.DateOnly), AppID: last.AppID, Area: last.Area, Client: last.Client,
		}
	}

	s.Len(every, 6)
	s.Equal(listedOf(every), listedOf(walked))
}

// Defect: the counts of days outside the range summed, the first answered day bounded by the
// range or read off a day of refusals only, clients not summed into one area, or an
// organization not asked for read.
func (s *StoreSuite) TestOrganizationAreasSumTheRangeAndTakeTheFirstDayEverAnswered() {
	s.Require().NoError(s.store.Activities.Add(s.ctx, []ActivityDelta{
		s.on(-5, "org-a", "app-1", "agent_setup", AuditSourceCLI, 2, 1),
		s.on(0, "org-a", "app-1", "agent_setup", AuditSourceCLI, 1, 1),
		s.on(0, "org-a", "app-1", "conversations", AuditSourceCLI, 3, 1),
		s.on(1, "org-a", "app-2", "conversations", AuditSourceSDK, 2, 0),
		s.on(0, "org-a", "app-1", "voice_video", AuditSourceSDK, 2, 2),
		s.on(3, "org-a", "app-1", "workers", AuditSourceAPI, 1, 0),
		s.on(0, "org-b", "app-3", "phone_sms", AuditSourceAPI, 5, 0),
		s.on(0, "org-c", "app-4", "agent_setup", AuditSourceCLI, 9, 0),
	}))
	day := func(n int) *time.Time {
		d := s.base.UTC().Truncate(24*time.Hour).AddDate(0, 0, n)
		return &d
	}

	areas, err := s.store.Activities.OrganizationAreas(s.ctx, []string{"org-a", "org-b"}, s.base, s.base.AddDate(0, 0, 1))
	s.Require().NoError(err)

	s.Equal([]OrganizationArea{
		{OrganizationID: "org-a", Area: "agent_setup", Requests: 1, Refused: 1, FirstAnswered: day(-5)},
		{OrganizationID: "org-a", Area: "conversations", Requests: 5, Refused: 1, FirstAnswered: day(0)},
		{OrganizationID: "org-a", Area: "voice_video", Requests: 2, Refused: 2},
		{OrganizationID: "org-a", Area: "workers", FirstAnswered: day(3)},
		{OrganizationID: "org-b", Area: "phone_sms", Requests: 5, FirstAnswered: day(0)},
	}, s.inUTC(areas))
}

// inUTC is areas with their days in UTC, as a test names them.
func (s *StoreSuite) inUTC(areas []OrganizationArea) []OrganizationArea {
	for i := range areas {
		if day := areas[i].FirstAnswered; day != nil {
			utc := day.UTC()
			areas[i].FirstAnswered = &utc
		}
	}
	return areas
}

// Defect: a day counted twice for two areas or two apps, a day of refusals only, of another
// area or outside the range counted, or an organization not asked for read.
func (s *StoreSuite) TestOrganizationDaysCountEachAnsweredDayOfTheAreasInTheRangeOnce() {
	s.Require().NoError(s.store.Activities.Add(s.ctx, []ActivityDelta{
		s.on(0, "org-a", "app-1", "conversations", AuditSourceSDK, 1, 0),
		s.on(0, "org-a", "app-2", "model_api", AuditSourceAPI, 1, 0),
		s.on(1, "org-a", "app-1", "model_api", AuditSourceAPI, 2, 2),
		s.on(1, "org-a", "app-1", "agent_setup", AuditSourceCLI, 1, 0),
		s.on(-1, "org-a", "app-1", "conversations", AuditSourceSDK, 1, 0),
		s.on(2, "org-a", "app-1", "conversations", AuditSourceSDK, 1, 0),
		s.on(1, "org-b", "app-3", "conversations", AuditSourceSDK, 1, 0),
		s.on(1, "org-c", "app-4", "conversations", AuditSourceSDK, 1, 0),
	}))

	days, err := s.store.Activities.OrganizationDays(s.ctx, []string{"org-a", "org-b", "org-d"}, s.base, s.base.AddDate(0, 0, 1),
		[]string{"conversations", "model_api"})
	s.Require().NoError(err)

	s.Equal(map[string]int{"org-a": 1, "org-b": 1}, days)
}
