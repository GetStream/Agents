package api

import (
	"cmp"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/danielgtaylor/huma/v2"

	"github.com/GetStream/Vision-Agents/acceleration/internal/activity"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// defaultActivityPage is how many rows a page of every organization's activity holds when the
// caller names no limit: a day of a few hundred organizations in a few pages. A judgement, not
// a measurement.
const defaultActivityPage = 500

var (
	errActivityReversed = invalidRequest("to may not be before from")
	errActivitySpan     = invalidRequest(fmt.Sprintf("from and to may span at most %d days, what is kept", store.ActivityKeepDays))
	errActivityFilters  = invalidRequest("cursor was handed out for another from, to, org_id or app_id")
	errActivityText     = func(field string) error { return invalidRequest(field + " is UTF-8 text without a NUL") }
)

// isActivityText is whether text is one the query reads as it is: bun drops a NUL from a value
// and Postgres refuses bytes that are not UTF-8, so such an id would read another one's rows.
func isActivityText(text string) bool {
	return utf8.ValidString(text) && !strings.ContainsRune(text, 0)
}

// activityFilters is what a page of listActivity was read with, kept in its cursor so that the
// next page is read with the same.
type activityFilters struct {
	From           string `json:"f"`
	To             string `json:"t"`
	OrganizationID string `json:"fo,omitempty"`
	AppID          string `json:"fa,omitempty"`
}

// activityCursor is where a page of listActivity ended, and what it was read with.
type activityCursor struct {
	store.ActivityPosition
	Filters activityFilters `json:"q"`
}

// ActivityDay is what one app of an organization did in one area of the API from one client on
// one UTC day.
type ActivityDay struct {
	OrganizationId string `json:"organization_id" doc:"The organization the app was in when the row was last written."`
	AppId          string `json:"app_id" doc:"The app."`
	Day            string `json:"day" format:"date" doc:"The UTC day, YYYY-MM-DD."`
	ActivityArea
}

// OrganizationProgress is how far one organization got with Agents.
type OrganizationProgress struct {
	OrganizationId string            `json:"organization_id" doc:"The organization."`
	FurthestStep   ActivityStep      `json:"furthest_step" doc:"The furthest step from from to to, read from the rows that name the organization, of all its apps even when app_id is set. For apps that stayed in it, as getOrganizationActivity reads it from since."`
	Steps          []ActivityStepDay `json:"steps" nullable:"false" doc:"Every step the organization's apps ever had a request answered in, whenever, not only from from to to, of all its apps even when app_id is set, in furthest_step's order, with the day of the first, read from the rows that name the organization. For apps that stayed in it, as getOrganizationActivity lists them."`
	Funded         *bool             `json:"funded,omitempty" doc:"The organization has credit that is not trial credit now, after refunds and disputes, as the usage service holds it: the rule that lets it buy numbers. false for one with only trial credit, none, or no account. Read when the page is, not as of to. Absent, on every organization of the page, when this deployment has no usage service or it did not answer."`
}

// ActivityPage is a page of every organization's activity.
type ActivityPage struct {
	Items         []ActivityDay          `json:"items" nullable:"false" doc:"One item per organization, day, app, area and client, sorted in that order."`
	Organizations []OrganizationProgress `json:"organizations" nullable:"false" doc:"Every organization items names, by id. An organization whose items span two pages is on both, the same, unless a row was written between the two reads."`
	HasMore       bool                   `json:"has_more"`
	NextCursor    *string                `json:"next_cursor,omitempty" doc:"Pass as cursor for the next page, with the same from, to, org_id and app_id. Absent on the last."`
}

type listActivityRequest struct {
	From   time.Time             `query:"from" timeFormat:"2006-01-02" required:"true" doc:"The first UTC day read, YYYY-MM-DD."`
	To     time.Time             `query:"to" timeFormat:"2006-01-02" required:"true" doc:"The last UTC day read, YYYY-MM-DD, included. At most 400 days from from on, both included, and not before it."`
	OrgId  optionalParam[string] `query:"org_id" doc:"Narrow to the rows that name this organization. Empty is no filter. One with no row is an empty page."`
	AppId  optionalParam[string] `query:"app_id" doc:"Narrow to this app's rows. Empty is no filter. One with no row is an empty page."`
	Limit  int                   `query:"limit" minimum:"1" maximum:"1000" doc:"Up to 1000. Omitted is 500."`
	Cursor string                `query:"cursor" doc:"The next_cursor of the previous page, sent with the same from, to, org_id and app_id. A cursor sent with others is refused."`
}

type listActivityResponse struct{ Body ActivityPage }

// listActivity returns a page of what every organization's apps did over a range of UTC days,
// with how far each organization on the page got.
func (s *Server) listActivity(ctx context.Context, request *listActivityRequest) (*listActivityResponse, error) {
	if s.store == nil {
		return nil, errNoActivity
	}
	if request.To.Before(request.From) {
		return nil, errActivityReversed
	}
	if request.To.Sub(request.From) >= store.ActivityKeepDays*24*time.Hour {
		return nil, errActivitySpan
	}
	if !isActivityText(request.OrgId.Value) {
		return nil, errActivityText("org_id")
	}
	if !isActivityText(request.AppId.Value) {
		return nil, errActivityText("app_id")
	}
	filters := activityFilters{
		From: request.From.Format(time.DateOnly), To: request.To.Format(time.DateOnly),
		OrganizationID: request.OrgId.Value, AppID: request.AppId.Value,
	}
	cursor, err := decodeCursor[activityCursor](&request.Cursor)
	if err == nil && cursor != nil {
		_, err = time.Parse(time.DateOnly, cursor.Day)
		if !isActivityText(cursor.OrganizationID) || !isActivityText(cursor.AppID) ||
			!isActivityText(cursor.Area) || !isActivityText(cursor.Client) {
			err = errBadCursor
		}
	}
	if err != nil {
		return nil, invalidRequest(errBadCursor.Error())
	}
	var after *store.ActivityPosition
	if cursor != nil {
		if cursor.Filters != filters {
			return nil, errActivityFilters
		}
		after = &cursor.ActivityPosition
	}

	limit := cmp.Or(request.Limit, defaultActivityPage)
	rows, err := s.store.Activities.List(ctx, store.ActivityFilter{
		From: request.From, To: request.To, OrganizationID: filters.OrganizationID, AppID: filters.AppID,
	}, after, limit+1)
	if err != nil {
		return nil, err
	}
	kept, more := page(rows, limit)
	answer := ActivityPage{Items: make([]ActivityDay, 0, len(kept)), HasMore: more}
	var organizations []string
	for _, row := range kept {
		answer.Items = append(answer.Items, ActivityDay{
			OrganizationId: row.OrganizationID, AppId: row.AppID, Day: row.Day.Format(time.DateOnly), ActivityArea: ActivityArea(row.ActivityArea),
		})
		if len(organizations) == 0 || organizations[len(organizations)-1] != row.OrganizationID {
			organizations = append(organizations, row.OrganizationID)
		}
	}
	if answer.Organizations, err = s.organizationProgress(ctx, organizations, request.From, request.To); err != nil {
		return nil, err
	}
	s.markFunded(ctx, answer.Organizations, organizations)
	if more {
		last := kept[len(kept)-1]
		answer.NextCursor = encodeCursor(activityCursor{ActivityPosition: store.ActivityPosition{
			OrganizationID: last.OrganizationID, Day: last.Day.Format(time.DateOnly), AppID: last.AppID, Area: last.Area, Client: last.Client,
		}, Filters: filters})
	}
	return &listActivityResponse{Body: answer}, nil
}

// organizationProgress is how far each of organizations, sorted, got: the furthest step from
// from to to, and every step it ever reached with its first day.
func (s *Server) organizationProgress(ctx context.Context, organizations []string, from, to time.Time) ([]OrganizationProgress, error) {
	areas, err := s.store.Activities.OrganizationAreas(ctx, organizations, from, to)
	if err != nil {
		return nil, err
	}
	days, err := s.store.Activities.OrganizationDays(ctx, organizations, from, to, []string{activity.AreaConversations, activity.AreaModelAPI})
	if err != nil {
		return nil, err
	}
	sums := make(map[string][]store.ActivityArea, len(organizations))
	firsts := make(map[string][]store.ActivityAreaDay, len(organizations))
	for _, area := range areas {
		sums[area.OrganizationID] = append(sums[area.OrganizationID], store.ActivityArea{Area: area.Area, Requests: area.Requests, Refused: area.Refused})
		if area.FirstAnswered != nil {
			firsts[area.OrganizationID] = append(firsts[area.OrganizationID], store.ActivityAreaDay{Area: area.Area, Day: *area.FirstAnswered})
		}
	}
	progress := make([]OrganizationProgress, 0, len(organizations))
	for _, organization := range organizations {
		progress = append(progress, OrganizationProgress{
			OrganizationId: organization, FurthestStep: furthestStep(sums[organization], days[organization]),
			Steps: reachedSteps(firsts[organization]),
		})
	}
	return progress, nil
}

// markFunded sets funded on each organization of progress, from one read of the usage service
// for organizations, the ids progress holds. A deployment without one, or a service that did not answer, leaves it unset: how far
// the organizations got is still worth answering.
func (s *Server) markFunded(ctx context.Context, progress []OrganizationProgress, organizations []string) {
	funded, ok, err := s.policies.Funded(ctx, organizations)
	if err != nil {
		s.logger.Error("could not read whether organizations are funded from the usage service", "organizations", len(organizations), "error", err)
	}
	if !ok {
		return
	}
	for i := range progress {
		value := funded[progress[i].OrganizationId]
		progress[i].Funded = &value
	}
}

// registerActivity declares Stream staff's read of every organization's activity, which takes
// the ops key.
func (s *Server) registerActivity(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID: "listActivity",
		Method:      http.MethodGet,
		Path:        "/v1/ops/activity",
		Summary:     "List every organization's Agents activity by day",
		Description: "What every organization's apps did in each area of the API, from each client, on each UTC " +
			"day from from to to, one item per organization, day, app, area and client, with how far each " +
			"organization on the page got, read from the rows that name the organization; for apps that stayed in it, " +
			"as getOrganizationActivity tells it. org_id and app_id narrow the items; with app_id, how far an " +
			"organization got is still read from all its apps. Send from, to, org_id and app_id again with cursor " +
			"for the next page. Rows are " +
			"kept 400 days. A row of an app whose organization was not known when it was written names none and " +
			"is left out. Today's counts keep rising until the UTC day ends: pull with to = yesterday.\n\n" +
			"These definitions are the router's; count a dashboard by them:\n\n" +
			"- Tried: at least one request answered in agent_setup or a later step, a step in steps other than " +
			"dashboard_reads. dashboard_reads alone is opening the dashboard's Agents tab.\n" +
			"- API calls: answered requests, requests less refused, of every area but dashboard_reads, hooks_in " +
			"and webhooks_out. Those are the dashboard's own reads, vendors' hooks and the router's own deliveries.\n" +
			"- Production: funded true on the organization, and conversations answered, an item of " +
			"conversations with requests above refused, on 5 of the last 7 UTC days. The 5 of 7 is a judgement, " +
			"not a measurement.\n" +
			"- Churned: was in production, then had no request answered in conversations or model_api for 30 " +
			"days.\n\n" +
			"Stream staff only: it needs the ops key.",
		Tags:      []string{staffTag},
		Security:  opsSecurity,
		Responses: map[string]*huma.Response{"200": {Description: "A page of every organization's activity"}},
		Errors:    []int{http.StatusBadRequest, http.StatusUnauthorized},
	}, s.listActivity)
}
