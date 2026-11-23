package api

import (
	"cmp"
	"context"
	"fmt"
	"net/http"
	"time"

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
)

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
	FurthestStep   ActivityStep      `json:"furthest_step" doc:"The furthest step from from to to, as getOrganizationActivity reads it from since."`
	Steps          []ActivityStepDay `json:"steps" nullable:"false" doc:"Every step the organization's apps ever had a request answered in, whenever, not only from from to to, in furthest_step's order, with the day of the first, as getOrganizationActivity lists them."`
}

// ActivityPage is a page of every organization's activity.
type ActivityPage struct {
	Items         []ActivityDay          `json:"items" nullable:"false" doc:"One item per organization, day, app, area and client, sorted in that order."`
	Organizations []OrganizationProgress `json:"organizations" nullable:"false" doc:"Every organization items names, by id. An organization whose items span two pages is on both, the same."`
	HasMore       bool                   `json:"has_more"`
	NextCursor    *string                `json:"next_cursor,omitempty" doc:"Pass as cursor for the next page, with the same from and to. Absent on the last."`
}

type listActivityRequest struct {
	From   time.Time `query:"from" timeFormat:"2006-01-02" required:"true" doc:"The first UTC day read, YYYY-MM-DD."`
	To     time.Time `query:"to" timeFormat:"2006-01-02" required:"true" doc:"The last UTC day read, YYYY-MM-DD, included. At most 400 days from from on, both included, and not before it."`
	Limit  int       `query:"limit" minimum:"1" maximum:"1000" doc:"Up to 1000. Omitted is 500."`
	Cursor string    `query:"cursor" doc:"The next_cursor of the previous page, sent with the same from and to."`
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
	after, err := decodeCursor[store.ActivityPosition](&request.Cursor)
	if err == nil && after != nil {
		_, err = time.Parse(time.DateOnly, after.Day)
	}
	if err != nil {
		return nil, invalidRequest(errBadCursor.Error())
	}

	limit := cmp.Or(request.Limit, defaultActivityPage)
	rows, err := s.store.Activities.List(ctx, request.From, request.To, after, limit+1)
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
	if more {
		last := kept[len(kept)-1]
		answer.NextCursor = encodeCursor(store.ActivityPosition{
			OrganizationID: last.OrganizationID, Day: last.Day.Format(time.DateOnly), AppID: last.AppID, Area: last.Area, Client: last.Client,
		})
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
			"organization on the page got, as getOrganizationActivity tells it. Send from and to again with " +
			"cursor for the next page. Rows are kept 400 days. A row of an app whose organization was not known " +
			"when it was written names none and is left out.\n\n" +
			"These definitions are the router's, so a dashboard and the activity digest count the same thing:\n\n" +
			"- Tried: at least one request answered in agent_setup or a later step, a step in steps other than " +
			"dashboard_reads. dashboard_reads alone is opening the dashboard's Agents tab.\n" +
			"- API calls: answered requests, requests less refused, of every area but dashboard_reads, hooks_in " +
			"and webhooks_out. Those are the dashboard's own reads, vendors' hooks and the router's own deliveries.\n" +
			"- Production: funded, and conversations answered, an item of conversations with requests above " +
			"refused, on 5 of the last 7 UTC days. Funded is credit that is not trial credit: the usage service " +
			"holds it, and the router does not compute it, so join it from there. The 5 of 7 is a judgement, " +
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
