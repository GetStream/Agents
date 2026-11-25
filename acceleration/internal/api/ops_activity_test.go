//go:build integration

package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
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
	return s.walkFiltered(from, to, limit, "")
}

// walkFiltered is walk with filter, a query string such as "&org_id=x", sent with every page.
func (s *OpsActivitySuite) walkFiltered(from, to time.Time, limit int, filter string) []ActivityPage {
	var pages []ActivityPage
	path := activityPath(from, to, limit) + filter
	for {
		var page ActivityPage
		s.Require().Equal(http.StatusOK, s.staff.do(http.MethodGet, path, nil, &page))
		pages = append(pages, page)
		if !page.HasMore {
			s.Nil(page.NextCursor)
			return pages
		}
		s.Require().NotNil(page.NextCursor)
		s.Require().Less(len(pages), 500, "the cursor does not move on")
		path = activityPath(from, to, limit) + filter + "&cursor=" + url.QueryEscape(*page.NextCursor)
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
// pages; or a page not telling every organization its items name, or telling one the furthest
// step of another on the page.
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

	for _, page := range pages {
		var named, told []string
		for _, item := range page.Items {
			if len(named) == 0 || named[len(named)-1] != item.OrganizationId {
				named = append(named, item.OrganizationId)
			}
		}
		for _, progress := range page.Organizations {
			told = append(told, progress.OrganizationId)
		}
		s.Equal(named, told, "a page tells every organization its items name, once")
	}
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
	for organization, step := range map[string]ActivityStep{
		one.organization.ID: activityStepConversations, two.organization.ID: activityStepNone, three.organization.ID: activityStepDashboardReads,
	} {
		for _, page := range pages {
			for _, progress := range page.Organizations {
				if progress.OrganizationId == organization {
					s.Equal(step, progress.FurthestStep, "furthest step of "+organization+" is its own, not another organization's on the page")
				}
			}
		}
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

// Defect: a range with no row answered with a 500, as the organizations of no row are read, or
// with null lists.
func (s *OpsActivitySuite) TestARangeWithNoRowIsAnEmptyPage() {
	future := time.Now().UTC().AddDate(1, 0, rand.IntN(100))

	status, payload := s.staff.call(http.MethodGet, activityPath(future, future, 0), nil)

	s.Require().Equal(http.StatusOK, status, string(payload))
	var page map[string]json.RawMessage
	s.Require().NoError(json.Unmarshal(payload, &page))
	s.JSONEq(`[]`, string(page["items"]))
	s.JSONEq(`[]`, string(page["organizations"]))
	s.JSONEq(`false`, string(page["has_more"]))
}

// Defect: a cursor this list never handed out, or one whose day is no day, answered with a 500
// or read as the first page.
func (s *OpsActivitySuite) TestACursorThisListDidNotHandOutIsRefused() {
	from := daysAgo(30, 0)
	notADay := base64.RawURLEncoding.EncodeToString([]byte(
		`{"o":"org","d":"yesterday","a":"","r":"","c":"","q":{"f":"` + from.Format(time.DateOnly) + `","t":"` + from.Format(time.DateOnly) + `"}}`))

	for _, cursor := range []string{"not base64!", notADay} {
		status, payload := s.staff.call(http.MethodGet, activityPath(from, from, 0)+"&cursor="+url.QueryEscape(cursor), nil)
		s.Equal(http.StatusBadRequest, status, string(payload))
		s.Contains(string(payload), "cursor")
	}
}

// Defect: an org_id or app_id that is no id (a NUL, bytes that are not UTF-8) reaching the
// query, where bun drops the NUL, so "<org>\x00" read the rows of <org>, and answering 200
// rather than a 400 that names the field.
func (s *OpsActivitySuite) TestAnOrgIdOrAppIdThatIsNoIdIsRefused() {
	day := daysAgo(30, 0)
	s.Require().NoError(s.store.Activities.Add(context.Background(), []store.ActivityDelta{
		s.on(s.app, s.app.app.ID, activity.AreaConversations, store.AuditSourceSDK, day, 3, 0),
	}))

	for _, field := range []string{"org_id", "app_id"} {
		own := s.app.organization.ID
		if field == "app_id" {
			own = s.app.app.ID
		}
		for _, value := range []string{"%00", "a%00b", "%ff", "org-%c3%28", own + "%00", "%00" + own} {
			status, payload := s.staff.call(http.MethodGet, activityPath(day, day, 0)+"&"+field+"="+value, nil)
			s.Equal(http.StatusBadRequest, status, field+"="+value+" "+string(payload))
			s.Contains(string(payload), field)
		}
	}
}

// Defect: a cursor whose position holds a NUL, which this list never handed out, reaching the
// query and answered with a 500, not a 400.
func (s *OpsActivitySuite) TestACursorWithAPositionPostgresCannotStoreIsRefused() {
	day := daysAgo(30, 0)
	for _, key := range []string{"o", "a", "r", "c"} {
		position := map[string]any{
			"o": "org", "d": day.Format(time.DateOnly), "a": "app", "r": "area", "c": "client",
			"q": map[string]string{"f": day.Format(time.DateOnly), "t": day.Format(time.DateOnly)},
		}
		position[key] = "x\x00y"
		cursor, err := json.Marshal(position)
		s.Require().NoError(err)
		encoded := base64.RawURLEncoding.EncodeToString(cursor)

		status, payload := s.staff.call(http.MethodGet, activityPath(day, day, 0)+"&cursor="+url.QueryEscape(encoded), nil)
		s.Equal(http.StatusBadRequest, status, key+" "+string(payload))
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

// Defect: a router that never set an activity key reading activity for an X-Activity-Key, an
// empty one above all, which an unset key would equal.
func (s *OpsActivitySuite) TestAnUnsetActivityKeyReadsNothing() {
	paths := []string{activityPath(daysAgo(1, 0), daysAgo(1, 0), 1), "/v1/ops/orgs/" + s.app.organization.ID + "/activity"}
	for _, key := range []string{"", "suite-activity-key"} {
		caller := &testClient{suite: &s.RouterSuite, header: http.Header{activityKeyHeader: {key}}, kind: noCredential}
		for _, path := range paths {
			s.Equal(http.StatusUnauthorized, caller.do(http.MethodGet, path, nil, nil), path)
		}
	}
	s.Equal(http.StatusOK, s.staff.do(http.MethodGet, paths[0], nil, nil), "the ops key alone still reads it")
}

// listedOf is the app, area and client of every item of pages, and the organizations they tell.
func listedOf(pages []ActivityPage) (items []string, organizations []OrganizationProgress) {
	for _, page := range pages {
		for _, item := range page.Items {
			items = append(items, item.OrganizationId+"/"+item.AppId+"/"+item.Area)
		}
		organizations = append(organizations, page.Organizations...)
	}
	return items, organizations
}

// Defect: org_id or app_id not narrowing the rows, either one dropped when both are set, a
// filter lost after the first page so paging under it lists other rows, a row lost or repeated
// between filtered pages, or with app_id an organization's progress read from that app alone.
func (s *OpsActivitySuite) TestOrgIdAndAppIdEachAloneAndTogetherListOnlyTheirRows() {
	day := daysAgo(200, 50)
	one, two := s.app, s.data.createApp()
	second := store.App{OrganizationID: one.organization.ID, Name: "app-" + s.utils.uuid()}
	s.Require().NoError(s.store.Apps.Insert(context.Background(), &second))
	org, first, other := one.organization.ID, one.app.ID, two.organization.ID
	s.Require().NoError(s.store.Activities.Add(context.Background(), []store.ActivityDelta{
		s.on(one, first, activity.AreaAgentSetup, store.AuditSourceCLI, day, 1, 0),
		s.on(one, first, activity.AreaWorkers, store.AuditSourceCLI, day, 1, 0),
		s.on(one, second.ID, activity.AreaConversations, store.AuditSourceSDK, day, 1, 0),
		s.on(two, two.app.ID, activity.AreaModelAPI, store.AuditSourceSDK, day, 1, 0),
	}))

	for _, test := range []struct {
		filter string
		items  []string
		told   []string
	}{
		{"&org_id=" + org, []string{org + "/" + first + "/agent_setup", org + "/" + first + "/workers", org + "/" + second.ID + "/conversations"}, []string{org, org, org}},
		{"&app_id=" + first, []string{org + "/" + first + "/agent_setup", org + "/" + first + "/workers"}, []string{org, org}},
		{"&app_id=" + two.app.ID, []string{other + "/" + two.app.ID + "/model_api"}, []string{other}},
		{"&org_id=" + org + "&app_id=" + second.ID, []string{org + "/" + second.ID + "/conversations"}, []string{org}},
		{"&org_id=" + other + "&app_id=" + first, nil, nil},
	} {
		items, organizations := listedOf(s.walkFiltered(day, day, 1, test.filter))
		// The apps' ids are random, so their order is; ElementsMatch still counts a repeated row.
		s.ElementsMatch(test.items, items, test.filter)
		var told []string
		for _, progress := range organizations {
			told = append(told, progress.OrganizationId)
			if progress.OrganizationId == org {
				s.Equal(activityStepConversations, progress.FurthestStep, "%s: the organization's own, of all its apps", test.filter)
			}
		}
		s.Equal(test.told, told, test.filter)
	}
}

// Defect: an organization or app with no row answered with a 404 or a 500 rather than an empty
// page.
func (s *OpsActivitySuite) TestAnUnknownOrgIdOrAppIdIsAnEmptyPage() {
	day := daysAgo(30, 0)

	for _, filter := range []string{"&org_id=org-" + s.utils.uuid(), "&app_id=app-" + s.utils.uuid()} {
		status, payload := s.staff.call(http.MethodGet, activityPath(day, day, 0)+filter, nil)

		s.Require().Equal(http.StatusOK, status, string(payload))
		var page map[string]json.RawMessage
		s.Require().NoError(json.Unmarshal(payload, &page))
		s.JSONEq(`[]`, string(page["items"]), filter)
		s.JSONEq(`[]`, string(page["organizations"]), filter)
	}
}

// Defect: an id check that takes only ASCII for an id, refusing (400) or never matching an
// organization or app whose id holds other UTF-8, through the filters and through a cursor.
func (s *OpsActivitySuite) TestAnOrgIdOrAppIdWithNonASCIIUTF8IsStillAccepted() {
	day := daysAgo(300, 50)
	org := store.Organization{ID: "org-é日本-" + s.utils.uuid()}
	app := "app-ü😀-" + s.utils.uuid()
	owner := testApp{organization: org}
	s.Require().NoError(s.store.Activities.Add(context.Background(), []store.ActivityDelta{
		s.on(owner, app, activity.AreaAgentSetup, store.AuditSourceCLI, day, 1, 0),
		s.on(owner, app, activity.AreaWorkers, store.AuditSourceCLI, day, 1, 0),
		s.on(owner, app, activity.AreaConversations, store.AuditSourceSDK, day, 1, 0),
	}))

	pages := s.walkFiltered(day, day, 1, "&org_id="+url.QueryEscape(org.ID)+"&app_id="+url.QueryEscape(app))

	items, _ := listedOf(pages)
	s.Len(pages, 3)
	s.ElementsMatch([]string{
		org.ID + "/" + app + "/agent_setup", org.ID + "/" + app + "/workers", org.ID + "/" + app + "/conversations",
	}, items)
}

// Defect: a cursor read under another org_id, app_id, from or to than the page it came from,
// which goes on from a position in rows the new filters do not read, answered rather than
// refused; or one read under the same refused.
func (s *OpsActivitySuite) TestACursorSentWithOtherFiltersIsRefused() {
	day := daysAgo(300, 50)
	app := s.customerID()
	s.Require().NoError(s.store.Activities.Add(context.Background(), []store.ActivityDelta{
		s.on(s.app, app, activity.AreaAgentSetup, store.AuditSourceCLI, day, 1, 0),
		s.on(s.app, app, activity.AreaWorkers, store.AuditSourceCLI, day, 1, 0),
	}))
	filter := "&org_id=" + s.app.organization.ID + "&app_id=" + app
	var page ActivityPage
	s.Require().Equal(http.StatusOK, s.staff.do(http.MethodGet, activityPath(day, day, 1)+filter, nil, &page))
	s.Require().NotNil(page.NextCursor)
	cursor := "&cursor=" + url.QueryEscape(*page.NextCursor)

	for _, path := range []string{
		activityPath(day, day, 1) + "&org_id=" + s.app.organization.ID + cursor,
		activityPath(day, day, 1) + "&app_id=" + app + cursor,
		activityPath(day, day, 1) + "&org_id=org-" + s.utils.uuid() + "&app_id=" + app + cursor,
		activityPath(day, day, 1) + cursor,
		activityPath(day.AddDate(0, 0, -1), day, 1) + filter + cursor,
		activityPath(day, day.AddDate(0, 0, 1), 1) + filter + cursor,
	} {
		status, payload := s.staff.call(http.MethodGet, path, nil)
		s.Equal(http.StatusBadRequest, status, path+": "+string(payload))
		s.Contains(string(payload), "cursor", path)
	}
	s.Equal(http.StatusOK, s.staff.do(http.MethodGet, activityPath(day, day, 1)+filter+cursor, nil, nil))
}
