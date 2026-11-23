//go:build integration

package api

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/activity"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// OpsActivitySuite is Stream staff reading every organization's activity by day. Every test
// has organizations of its own, and reads only its own out of what every suite wrote.
type OpsActivitySuite struct {
	RouterSuite
	// staff is Stream's own tool, holding the ops key and no app's credentials.
	staff *testClient
}

func TestOpsActivitySuite(t *testing.T) {
	runSuite(t, new(OpsActivitySuite))
}

func (s *OpsActivitySuite) SetupTest() {
	s.useApp(s.data.createApp())
	s.staff = &testClient{suite: &s.RouterSuite, header: http.Header{opsKeyHeader: {suiteOpsKey}}, kind: noCredential}
}

// daysAgo is the UTC day n days before today's, with up to spread days more picked at random,
// so suites running at once seldom write the same day.
func daysAgo(n, spread int) time.Time {
	return time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -n-rand.IntN(spread+1))
}

// on is a day of requests of app's, in its organization, in area from client, refused of them
// refused.
func (s *OpsActivitySuite) on(app testApp, appID, area, client string, day time.Time, requests, refused int64) store.ActivityDelta {
	return store.ActivityDelta{
		OrganizationID: app.organization.ID, AppID: appID, Area: area, Client: client,
		Day: day, Requests: requests, Refused: refused, FirstAt: day.Add(time.Hour), LastAt: day.Add(2 * time.Hour),
	}
}

func activityPath(from, to time.Time, limit int) string {
	path := "/v1/ops/activity?from=" + from.Format(time.DateOnly) + "&to=" + to.Format(time.DateOnly)
	if limit > 0 {
		path += fmt.Sprintf("&limit=%d", limit)
	}
	return path
}

// walk is every page from from to to, limit items at a time, following next_cursor.
func (s *OpsActivitySuite) walk(from, to time.Time, limit int) []ActivityPage {
	var pages []ActivityPage
	path := activityPath(from, to, limit)
	for {
		var page ActivityPage
		s.Require().Equal(http.StatusOK, s.staff.do(http.MethodGet, path, nil, &page))
		pages = append(pages, page)
		if !page.HasMore {
			s.Nil(page.NextCursor)
			return pages
		}
		s.Require().NotNil(page.NextCursor)
		s.Require().Less(len(pages), 10000, "the cursor does not move on")
		path = activityPath(from, to, limit) + "&cursor=" + url.QueryEscape(*page.NextCursor)
	}
}

// itemsOf is the items of pages that are organization's.
func itemsOf(pages []ActivityPage, organization string) []ActivityDay {
	var items []ActivityDay
	for _, page := range pages {
		for _, item := range page.Items {
			if item.OrganizationId == organization {
				items = append(items, item)
			}
		}
	}
	return items
}

// Defect: a page's rows that do not add up to what the organization's own read says for the
// day: a row of another day, organization or app summed in, or one lost or read twice between
// pages.
func (s *OpsActivitySuite) TestTheRequestsOfEachOrganizationOnADayAddUpToItsOwnRead() {
	day := daysAgo(60, 20)
	one, two, three := s.app, s.data.createApp(), s.data.createApp()
	second := store.App{OrganizationID: one.organization.ID, Name: "app-" + s.utils.uuid()}
	s.Require().NoError(s.store.Apps.Insert(context.Background(), &second))
	s.Require().NoError(s.store.Activities.Add(context.Background(), []store.ActivityDelta{
		s.on(one, one.app.ID, activity.AreaAgentSetup, store.AuditSourceCLI, day, 3, 1),
		s.on(one, one.app.ID, activity.AreaConversations, store.AuditSourceSDK, day, 5, 0),
		s.on(one, second.ID, activity.AreaConversations, store.AuditSourceAPI, day, 7, 2),
		s.on(one, one.app.ID, activity.AreaConversations, store.AuditSourceSDK, day.AddDate(0, 0, -1), 100, 0),
		s.on(two, two.app.ID, activity.AreaModelAPI, store.AuditSourceSDK, day, 11, 0),
		s.on(three, three.app.ID, activity.AreaDashboardReads, store.AuditSourceDashboard, day, 2, 0),
		s.on(three, three.app.ID, activity.AreaWorkers, store.AuditSourceCLI, day, 1, 1),
	}))

	pages := s.walk(day, day, 2)

	for organization, requests := range map[string]int64{one.organization.ID: 15, two.organization.ID: 11, three.organization.ID: 3} {
		var listed int64
		for _, item := range itemsOf(pages, organization) {
			listed += item.Requests
		}
		var own OrganizationActivity
		path := "/v1/ops/orgs/" + organization + "/activity?since=" + url.QueryEscape(day.Format(time.RFC3339))
		s.Require().Equal(http.StatusOK, s.staff.do(http.MethodGet, path, nil, &own))
		var read int64
		for _, area := range own.Areas {
			read += area.Requests
		}
		s.Equal(requests, listed, organization)
		s.Equal(read, listed, organization)
	}
}

// Defect: a cursor that keeps fewer keys than the order, or the page's first row rather than its
// last, so a page ending inside an organization loses or repeats one of its rows; or an
// organization spanning two pages told differently on each.
func (s *OpsActivitySuite) TestAPageEndingInsideAnOrganizationLosesAndRepeatsNoRow() {
	day := daysAgo(100, 50)
	s.Require().NoError(s.store.Activities.Add(context.Background(), []store.ActivityDelta{
		s.on(s.app, s.customerID(), activity.AreaAgentSetup, store.AuditSourceCLI, day, 1, 0),
		s.on(s.app, s.customerID(), activity.AreaAgentSetup, store.AuditSourceSDK, day, 2, 0),
		s.on(s.app, s.customerID(), activity.AreaConversations, store.AuditSourceSDK, day, 3, 0),
	}))

	pages := s.walk(day, day, 2)

	var areas []string
	for _, item := range itemsOf(pages, s.app.organization.ID) {
		areas = append(areas, item.Area+"/"+item.Client)
		s.Equal(day.Format(time.DateOnly), item.Day)
		s.Equal(s.customerID(), item.AppId)
	}
	s.Equal([]string{"agent_setup/cli", "agent_setup/sdk", "conversations/sdk"}, areas)
	var told []OrganizationProgress
	for _, page := range pages {
		for _, progress := range page.Organizations {
			if progress.OrganizationId == s.app.organization.ID {
				told = append(told, progress)
			}
		}
	}
	s.Require().Len(told, 2, "three rows at two a page end a page inside the organization")
	s.Equal(told[0], told[1])
}

// Defect: furthest_step read over every day rather than from and to, or steps bounded by them
// rather than every day recorded, or the days of steady use counted outside the range.
func (s *OpsActivitySuite) TestTheFurthestStepIsTheRangesAndTheStepsEveryDays() {
	day := daysAgo(150, 50)
	at := func(n int) time.Time { return day.AddDate(0, 0, n) }
	s.Require().NoError(s.store.Activities.Add(context.Background(), []store.ActivityDelta{
		s.on(s.app, s.customerID(), activity.AreaPhoneSMS, store.AuditSourceAPI, at(-3), 1, 0),
		s.on(s.app, s.customerID(), activity.AreaAgentSetup, store.AuditSourceCLI, at(0), 1, 0),
		s.on(s.app, s.customerID(), activity.AreaConversations, store.AuditSourceSDK, at(0), 1, 0),
		s.on(s.app, s.customerID(), activity.AreaVoiceVideo, store.AuditSourceSDK, at(0), 2, 2),
		s.on(s.app, s.customerID(), activity.AreaConversations, store.AuditSourceSDK, at(1), 1, 0),
		s.on(s.app, s.customerID(), activity.AreaModelAPI, store.AuditSourceSDK, at(2), 1, 0),
	}))
	progressOf := func(from, to time.Time) OrganizationProgress {
		for _, page := range s.walk(from, to, 1000) {
			for _, progress := range page.Organizations {
				if progress.OrganizationId == s.app.organization.ID {
					return progress
				}
			}
		}
		s.FailNow("the organization is not listed")
		return OrganizationProgress{}
	}

	s.Equal(OrganizationProgress{
		OrganizationId: s.app.organization.ID, FurthestStep: activityStepConversations,
		Steps: []ActivityStepDay{
			{Step: activity.AreaAgentSetup, FirstAnswered: at(0).Format(time.DateOnly)},
			{Step: activity.AreaConversations, FirstAnswered: at(0).Format(time.DateOnly)},
			{Step: activity.AreaPhoneSMS, FirstAnswered: at(-3).Format(time.DateOnly)},
		},
	}, progressOf(at(0), at(1)))
	s.Equal(activityStepSteadyUse, progressOf(at(0), at(2)).FurthestStep)
}

// Defect: a range longer than the 400 days kept, or one ending before it starts, read rather
// than refused, or the longest range kept refused.
func (s *OpsActivitySuite) TestFromAndToSpanAtMostFourHundredDaysAndToIsNotBeforeFrom() {
	from := daysAgo(30, 0)

	status, payload := s.staff.call(http.MethodGet, activityPath(from, from.AddDate(0, 0, 400), 0), nil)
	s.Equal(http.StatusBadRequest, status, string(payload))
	s.Contains(string(payload), "400 days")
	status, payload = s.staff.call(http.MethodGet, activityPath(from, from.AddDate(0, 0, -1), 0), nil)
	s.Equal(http.StatusBadRequest, status, string(payload))
	s.Contains(string(payload), "before from")
	s.Equal(http.StatusOK, s.staff.do(http.MethodGet, activityPath(from, from.AddDate(0, 0, 399), 1), nil, nil))
	s.Equal(http.StatusOK, s.staff.do(http.MethodGet, activityPath(from, from, 1), nil, nil))
}

// Defect: a cursor this list never handed out, or one whose day is no day, answered with a 500
// or read as the first page.
func (s *OpsActivitySuite) TestACursorThisListDidNotHandOutIsRefused() {
	from := daysAgo(30, 0)
	notADay := base64.RawURLEncoding.EncodeToString([]byte(`{"o":"org","d":"yesterday","a":"","r":"","c":""}`))

	for _, cursor := range []string{"not base64!", notADay} {
		status, payload := s.staff.call(http.MethodGet, activityPath(from, from, 0)+"&cursor="+url.QueryEscape(cursor), nil)
		s.Equal(http.StatusBadRequest, status, string(payload))
		s.Contains(string(payload), "cursor")
	}
}

// Defect: a page with no limit named holding another number of items than 500, or a limit over
// 1000 read.
func (s *OpsActivitySuite) TestAPageHoldsFiveHundredItemsUnlessALimitUpToAThousandIsNamed() {
	day := daysAgo(250, 100)
	deltas := make([]store.ActivityDelta, 0, defaultActivityPage+1)
	for i := range defaultActivityPage + 1 {
		deltas = append(deltas, s.on(s.app, s.customerID(), fmt.Sprintf("area-%04d", i), store.AuditSourceAPI, day, 1, 0))
	}
	s.Require().NoError(s.store.Activities.Add(context.Background(), deltas))

	var page ActivityPage
	s.Require().Equal(http.StatusOK, s.staff.do(http.MethodGet, activityPath(day, day, 0), nil, &page))
	s.Len(page.Items, 500)
	s.True(page.HasMore)
	s.Require().Equal(http.StatusOK, s.staff.do(http.MethodGet, activityPath(day, day, 1000), nil, &page))
	s.GreaterOrEqual(len(page.Items), 501)
	s.Equal(http.StatusBadRequest, s.staff.do(http.MethodGet, activityPath(day, day, 1001), nil, nil))
}

// Defect: the operation reached with an app's credentials, even its backend's, or with a key
// that is not the ops key.
func (s *OpsActivitySuite) TestOnlyTheOpsKeyListsEveryOrganizationsActivity() {
	path := activityPath(daysAgo(1, 0), daysAgo(1, 0), 1)
	wrong := &testClient{suite: &s.RouterSuite, header: http.Header{opsKeyHeader: {"not-" + suiteOpsKey}}, kind: noCredential}

	s.Equal(http.StatusUnauthorized, s.serverClient.do(http.MethodGet, path, nil, nil))
	s.Equal(http.StatusForbidden, s.client.do(http.MethodGet, path, nil, nil))
	s.Equal(http.StatusUnauthorized, s.unauthenticatedClient.do(http.MethodGet, path, nil, nil))
	s.Equal(http.StatusUnauthorized, wrong.do(http.MethodGet, path, nil, nil))
	s.Equal(http.StatusOK, s.staff.do(http.MethodGet, path, nil, nil))
}
