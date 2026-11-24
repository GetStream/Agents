//go:build integration

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// seen records app as last seen under organization.
func (s *SelfHostedLimitsSuite) seen(app, organization string) {
	s.Require().NoError(s.store.AppOrganizations.Upsert(context.Background(), &store.AppOrganization{AppID: app, OrganizationID: organization}))
}

// sharedID is an app in no organization and an app of the organization of the same id, as their
// backends.
func (s *SelfHostedLimitsSuite) sharedID() (own, organization *testClient, id string) {
	id = s.utils.uuid()
	member := s.utils.uuid()
	s.seen(member, id)
	return s.backend(id, ""), s.backend(member, id), id
}

func (s *SelfHostedLimitsSuite) limitAs(client *testClient, body map[string]any) Limit {
	var created Limit
	status, payload := client.call(http.MethodPost, "/v1/spend/limits", body)
	s.Require().Equal(http.StatusCreated, status, string(payload))
	s.Require().NoError(json.Unmarshal(payload, &created))
	return created
}

func (s *SelfHostedLimitsSuite) grantAs(client *testClient) LimitGrant {
	var made LimitGrant
	s.Require().Equal(http.StatusCreated, client.do(http.MethodPost, "/v1/spend/limits/grants", teamGrant(), &made))
	return made
}

func (s *SelfHostedLimitsSuite) limitsAs(client *testClient) []Limit {
	var page LimitPage
	s.Require().Equal(http.StatusOK, client.do(http.MethodGet, "/v1/spend/limits", nil, &page))
	return page.Items
}

func (s *SelfHostedLimitsSuite) grantsAs(client *testClient) []LimitGrant {
	var page LimitGrantPage
	s.Require().Equal(http.StatusOK, client.do(http.MethodGet, "/v1/spend/limits/grants", nil, &page))
	return page.Items
}

func (s *SelfHostedLimitsSuite) usageAs(client *testClient) SubjectUsage {
	var read []SubjectUsage
	s.Require().Equal(http.StatusOK, client.do(http.MethodGet, "/v1/spend/limits/usage?subject=team&team=finance", nil, &read))
	s.Require().Len(read, 1)
	return read[0]
}

func teamLimit(micros int64) map[string]any {
	return map[string]any{"subject": "team", "team": "finance", "interval": "daily", "limit_micros": micros}
}

func teamGrant() map[string]any {
	return map[string]any{"subject": "team", "team": "finance", "amount_micros": 5, "reason": "launch"}
}

func teamReset() map[string]any {
	return map[string]any{"subject": "team", "team": "finance", "reason": "x"}
}

// refusedAsManaged checks an answer is the one every limits operation of an app with a shared id
// gets.
func (s *SelfHostedLimitsSuite) refusedAsManaged(status int, payload []byte) {
	s.Equal(http.StatusForbidden, status, string(payload))
	var failure ErrorResponse
	s.Require().NoError(json.Unmarshal(payload, &failure))
	s.Equal("forbidden", failure.Error.Code)
	s.Equal(ErrorTypePermission, failure.Error.Type)
	s.Equal("the spend limits of this app are managed by support", failure.Error.Message)
}

// The limits of an organization are its apps': an app in no organization whose id is the
// organization's reads and changes none, whichever operation it asks, and whether or not what
// it names is there.

func (s *SelfHostedLimitsSuite) TestAnAppWithAnOrganizationsIdDoesNotListLimits() {
	own, organization, _ := s.sharedID()
	s.limitAs(organization, teamLimit(4242))

	s.refusedAsManaged(own.call(http.MethodGet, "/v1/spend/limits", nil))
	s.Len(s.limitsAs(organization), 1)
}

func (s *SelfHostedLimitsSuite) TestAnAppWithAnOrganizationsIdDoesNotReadALimit() {
	own, organization, _ := s.sharedID()
	limit := s.limitAs(organization, teamLimit(4242))

	s.refusedAsManaged(own.call(http.MethodGet, fmt.Sprintf("/v1/spend/limits/%d", limit.Id), nil))
	s.refusedAsManaged(own.call(http.MethodGet, "/v1/spend/limits/1", nil))
	s.Equal(http.StatusOK, organization.do(http.MethodGet, fmt.Sprintf("/v1/spend/limits/%d", limit.Id), nil, nil))
}

func (s *SelfHostedLimitsSuite) TestAnAppWithAnOrganizationsIdDoesNotCreateALimit() {
	own, organization, _ := s.sharedID()

	s.refusedAsManaged(own.call(http.MethodPost, "/v1/spend/limits", teamLimit(4242)))

	s.Empty(s.limitsAs(organization))
	s.limitAs(organization, teamLimit(4242))
	s.refusedAsManaged(own.call(http.MethodPost, "/v1/spend/limits", teamLimit(4242)))
}

func (s *SelfHostedLimitsSuite) TestAnAppWithAnOrganizationsIdDoesNotChangeALimit() {
	own, organization, _ := s.sharedID()
	path := fmt.Sprintf("/v1/spend/limits/%d", s.limitAs(organization, teamLimit(4242)).Id)

	s.refusedAsManaged(own.call(http.MethodPatch, path, map[string]any{"limit_micros": 1}))

	s.Equal(int64(4242), s.limitsAs(organization)[0].LimitMicros)
	s.Equal(http.StatusOK, organization.do(http.MethodPatch, path, map[string]any{"limit_micros": 7}, nil))
}

func (s *SelfHostedLimitsSuite) TestAnAppWithAnOrganizationsIdDoesNotDeleteALimit() {
	own, organization, _ := s.sharedID()
	path := fmt.Sprintf("/v1/spend/limits/%d", s.limitAs(organization, teamLimit(4242)).Id)

	s.refusedAsManaged(own.call(http.MethodDelete, path, nil))

	s.Len(s.limitsAs(organization), 1)
	s.Equal(http.StatusNoContent, organization.do(http.MethodDelete, path, nil, nil))
}

func (s *SelfHostedLimitsSuite) TestAnAppWithAnOrganizationsIdDoesNotReadLimitUsage() {
	own, organization, _ := s.sharedID()
	s.limitAs(organization, teamLimit(4242))

	s.refusedAsManaged(own.call(http.MethodGet, "/v1/spend/limits/usage?subject=team&team=finance", nil))
	s.Equal(int64(4242), s.usageAs(organization).Windows[0].Limit.LimitMicros)
}

func (s *SelfHostedLimitsSuite) TestAnAppWithAnOrganizationsIdDoesNotResetUsage() {
	own, organization, id := s.sharedID()
	s.limitAs(organization, teamLimit(4242))

	s.refusedAsManaged(own.call(http.MethodPost, "/v1/spend/limits/usage/reset", teamReset()))

	resets, err := s.store.UsageResets.Select(context.Background(), []string{id}, time.Time{})
	s.Require().NoError(err)
	s.Empty(resets)
	s.Equal(http.StatusOK, organization.do(http.MethodPost, "/v1/spend/limits/usage/reset", teamReset(), nil))
}

func (s *SelfHostedLimitsSuite) TestAnAppWithAnOrganizationsIdDoesNotCreateAGrant() {
	own, organization, _ := s.sharedID()
	s.limitAs(organization, teamLimit(4242))

	s.refusedAsManaged(own.call(http.MethodPost, "/v1/spend/limits/grants", teamGrant()))

	s.Empty(s.grantsAs(organization))
	s.grantAs(organization)
}

func (s *SelfHostedLimitsSuite) TestAnAppWithAnOrganizationsIdDoesNotListGrants() {
	own, organization, _ := s.sharedID()
	s.limitAs(organization, teamLimit(4242))
	s.grantAs(organization)

	s.refusedAsManaged(own.call(http.MethodGet, "/v1/spend/limits/grants", nil))
	s.Len(s.grantsAs(organization), 1)
}

func (s *SelfHostedLimitsSuite) TestAnAppWithAnOrganizationsIdDoesNotRevokeOrUnrevokeAGrant() {
	own, organization, _ := s.sharedID()
	s.limitAs(organization, teamLimit(4242))
	path := fmt.Sprintf("/v1/spend/limits/grants/%d", s.grantAs(organization).Id)
	reason := map[string]any{"reason": "x"}

	s.refusedAsManaged(own.call(http.MethodPost, path+"/revoke", reason))
	s.Equal(GrantLive, s.grantsAs(organization)[0].State)
	s.Require().Equal(http.StatusOK, organization.do(http.MethodPost, path+"/revoke", reason, nil))

	s.refusedAsManaged(own.call(http.MethodPost, path+"/unrevoke", reason))
	s.Equal(GrantRevoked, s.grantsAs(organization)[0].State)
	s.Equal(http.StatusOK, organization.do(http.MethodPost, path+"/unrevoke", reason, nil))
}

func (s *SelfHostedLimitsSuite) TestAnAppWithAnOrganizationsIdDoesNotListTeams() {
	own, organization, _ := s.sharedID()
	s.limitAs(organization, teamLimit(4242))

	s.refusedAsManaged(own.call(http.MethodGet, "/v1/teams", nil))

	var teams []Team
	s.Require().Equal(http.StatusOK, organization.do(http.MethodGet, "/v1/teams", nil, &teams))
	s.Require().Len(teams, 1)
	s.Len(teams[0].Limits, 1)
}

// An organization one of whose apps is registered, and not yet seen, is one too.
func (s *SelfHostedLimitsSuite) TestAnAppWithTheIdOfARegisteredAppsOrganizationDoesNotListLimits() {
	s.refusedAsManaged(s.backend(s.app.organization.ID, "").call(http.MethodGet, "/v1/spend/limits", nil))
}

// The organization may have any number of apps.
func (s *SelfHostedLimitsSuite) TestAnAppWithTheIdOfAnOrganizationOfTwoAppsDoesNotListLimits() {
	own, _, id := s.sharedID()
	s.seen(s.utils.uuid(), id)

	s.refusedAsManaged(own.call(http.MethodGet, "/v1/spend/limits", nil))
}

// What is recorded now is read, not what a cache holds.
func (s *SelfHostedLimitsSuite) TestAnAppNoLongerInTheOrganizationOfItsOwnIdDoesNotListLimits() {
	own, _, id := s.sharedID()
	s.seen(id, id)
	cached, err := s.store.AppOrganizations.Get(context.Background(), id)
	s.Require().NoError(err)
	s.Require().Equal(id, cached.OrganizationID)
	_, err = s.store.GlobalDB().NewDelete().Model((*store.AppOrganization)(nil)).Where("app_id = ?", id).Exec(context.Background())
	s.Require().NoError(err)

	s.refusedAsManaged(own.call(http.MethodGet, "/v1/spend/limits", nil))
}

// An app last seen under another organization is in none of its id's.
func (s *SelfHostedLimitsSuite) TestAnAppSeenElsewhereWithAnOrganizationsIdDoesNotListLimits() {
	own, _, id := s.sharedID()
	s.seen(id, s.utils.uuid())

	s.refusedAsManaged(own.call(http.MethodGet, "/v1/spend/limits", nil))
}

// An app recorded in the organization of its own id is one of its apps, named or not.
func (s *SelfHostedLimitsSuite) TestAnAppInTheOrganizationOfItsOwnIdKeepsItsLimits() {
	own, organization, id := s.sharedID()
	s.seen(id, id)

	s.limitAs(own, map[string]any{"subject": "organization", "interval": "daily", "limit_micros": 0})

	s.Len(s.limitsAs(own), 1)
	s.Len(s.limitsAs(organization), 1)
	s.refusedSearch(own)
	s.refusedSearch(organization)
}

// An app's first request in the organization of its own id can come before the router recorded
// it there, and a later one that names none is answered once it has.
func (s *SelfHostedLimitsSuite) TestAnAppIsNotRefusedOnceRecordedInTheOrganizationOfItsOwnId() {
	own, _, id := s.sharedID()
	s.refusedAsManaged(own.call(http.MethodGet, "/v1/spend/limits", nil))

	s.Require().Equal(http.StatusOK, s.backend(id, id).do(http.MethodGet, "/v1/spend/limits", nil, nil))

	s.Eventually(func() bool {
		return own.do(http.MethodGet, "/v1/spend/limits", nil, nil) == http.StatusOK
	}, 5*time.Second, 20*time.Millisecond)
}

// memberRecordedElsewhere is an app its router recorded under id once, as its backend, recorded
// under another organization since.
func (s *SelfHostedLimitsSuite) memberRecordedElsewhere(id string) (organization *testClient, member string) {
	member = s.utils.uuid()
	organization = s.backend(member, id)
	// Any call but one on limits, which records the app before it is answered.
	organization.call(http.MethodGet, "/v1/account", nil)
	s.Require().Eventually(func() bool { return s.recorded(member) == id }, 5*time.Second, 20*time.Millisecond)
	s.seen(member, s.utils.uuid())
	return organization, member
}

// recorded is the organization an app is recorded under.
func (s *SelfHostedLimitsSuite) recorded(app string) string {
	owners, err := s.store.AppOrganizations.OrganizationsOf(context.Background(), []string{app})
	s.Require().NoError(err)
	return owners[app]
}

// A router records an app under an organization once, so the record can be of one the app named
// since. The organization a call on limits names is its organization all the same.
func (s *SelfHostedLimitsSuite) TestAnOrganizationIsKnownOnceItHasALimit() {
	id := s.utils.uuid()
	organization, member := s.memberRecordedElsewhere(id)

	s.limitAs(organization, teamLimit(4242))

	s.refusedAsManaged(s.backend(id, "").call(http.MethodGet, "/v1/spend/limits", nil))
	s.Equal(id, s.recorded(member))
}

func (s *SelfHostedLimitsSuite) TestAnOrganizationIsKnownOnceItHasAGrant() {
	id := s.utils.uuid()
	organization, member := s.memberRecordedElsewhere(id)
	limit := s.limitAs(organization, teamLimit(4242))
	s.seen(member, s.utils.uuid())

	grant := s.grantAs(organization)

	own := s.backend(id, "")
	s.refusedAsManaged(own.call(http.MethodPost, fmt.Sprintf("/v1/spend/limits/grants/%d/revoke", grant.Id), map[string]any{"reason": "x"}))
	s.refusedAsManaged(own.call(http.MethodDelete, fmt.Sprintf("/v1/spend/limits/%d", limit.Id), nil))
	s.Equal(GrantLive, s.grantsAs(organization)[0].State)
	s.Len(s.limitsAs(organization), 1)
}

// A call on limits by an app recorded in the organization it names writes no record.
func (s *SelfHostedLimitsSuite) TestAnAppRecordedInItsOrganizationIsNotRecordedAgain() {
	id := s.utils.uuid()
	organization, member := s.memberRecordedElsewhere(id)
	s.seen(member, id)
	s.failQueries(`INSERT INTO "app_organizations"`)

	s.limitAs(organization, teamLimit(4242))
}

// Staff's write records no app, whatever its headers name.
func (s *SelfHostedLimitsSuite) TestStaffsLimitRecordsNoApp() {
	id, elsewhere := s.utils.uuid(), s.utils.uuid()
	organization, member := s.memberRecordedElsewhere(id)
	s.seen(member, elsewhere)
	staff := organization.with(manageKeyHeader, suiteManageKey).with(actorIDHeader, "staff:"+s.utils.uuid())
	made := teamLimit(5)
	made["organization_id"] = id

	s.Require().Equal(http.StatusCreated, staff.do(http.MethodPost, "/v1/manage/spend/limits", made, nil))

	s.Equal(elsewhere, s.recorded(member))
}

// An organization seen after the app made its limits has them too, and they hold both.
func (s *SelfHostedLimitsSuite) TestAnOrganizationThatComesLaterHasTheLimits() {
	id, member := s.utils.uuid(), s.utils.uuid()
	own, organization := s.backend(id, ""), s.backend(member, id)
	limit := s.limitAs(own, map[string]any{"subject": "organization", "interval": "daily", "limit_micros": 0})

	s.seen(member, id)

	s.refusedAsManaged(own.call(http.MethodGet, "/v1/spend/limits", nil))
	s.Require().Len(s.limitsAs(organization), 1)
	s.Equal(limit.Id, s.limitsAs(organization)[0].Id)
	s.refusedSearch(own)
	s.refusedSearch(organization)
}

// A limit of the organization holds the app's work as it holds its own, both ways round.
func (s *SelfHostedLimitsSuite) TestALimitOfASharedIdHoldsTheAppsWorkAndTheOrganizations() {
	own, organization, _ := s.sharedID()
	s.limitAs(organization, map[string]any{"subject": "organization", "interval": "daily", "limit_micros": 0})

	s.refusedSearch(own)
	s.refusedSearch(organization)
}

// Staff read and change the limits an app with a shared id does not.
func (s *SelfHostedLimitsSuite) TestStaffManageTheLimitsOfASharedId() {
	_, organization, id := s.sharedID()
	limit := s.limitAs(organization, teamLimit(4242))
	staff := &testClient{suite: &s.RouterSuite, kind: noCredential, header: http.Header{
		manageKeyHeader: {suiteManageKey}, actorIDHeader: {"staff:" + s.utils.uuid()},
	}}
	named := "?organization_id=" + url.QueryEscape(id)

	var page LimitPage
	s.Require().Equal(http.StatusOK, staff.do(http.MethodGet, "/v1/manage/spend/limits"+named, nil, &page))
	s.Len(page.Items, 1)
	var teams []Team
	s.Require().Equal(http.StatusOK, staff.do(http.MethodGet, "/v1/manage/teams"+named, nil, &teams))
	s.Len(teams, 1)
	s.Equal(http.StatusOK, staff.do(http.MethodPatch, fmt.Sprintf("/v1/manage/spend/limits/%d", limit.Id), map[string]any{"limit_micros": 9}, nil))
	s.Equal(http.StatusNoContent, staff.do(http.MethodDelete, fmt.Sprintf("/v1/manage/spend/limits/%d", limit.Id), nil, nil))
	made := teamLimit(5)
	made["organization_id"] = id
	s.Equal(http.StatusCreated, staff.do(http.MethodPost, "/v1/manage/spend/limits", made, nil))
}

// What cannot be looked up is the router's failure, and no limit is read or written on it.
func (s *SelfHostedLimitsSuite) TestLimitsAreNotReadWhileAnOrganizationsAppsCannotBe() {
	own, organization, _ := s.sharedID()
	s.limitAs(organization, teamLimit(4242))
	s.failQueries("AS member")

	s.Equal(http.StatusInternalServerError, own.do(http.MethodGet, "/v1/spend/limits", nil, nil))
}

func (s *SelfHostedLimitsSuite) TestALimitIsNotWrittenUnlessItsOrganizationIsRecorded() {
	id, unread := s.utils.uuid(), s.utils.uuid()
	for member, query := range map[string]string{
		unread:         "app_id IN ('" + unread + "')",
		s.utils.uuid(): `INSERT INTO "app_organizations"`,
	} {
		s.failQueries(query)
		s.Equal(http.StatusInternalServerError, s.backend(member, id).do(http.MethodPost, "/v1/spend/limits", teamLimit(1), nil), query)
	}

	limits, err := s.store.Limits.SelectOf(context.Background(), []string{id})
	s.Require().NoError(err)
	s.Empty(limits)
}

// refusedSearch waits for client's search to be refused at a spent limit.
func (s *SelfHostedLimitsSuite) refusedSearch(client *testClient) {
	s.Eventually(func() bool {
		status, payload := client.call(http.MethodPost, "/v1/search", SearchRequest{Query: "what is the time"})
		var failure ErrorResponse
		if status != http.StatusPaymentRequired {
			return false
		}
		s.Require().NoError(json.Unmarshal(payload, &failure))
		return failure.Error.Code == "spend_limit_reached"
	}, 10*time.Second, 50*time.Millisecond)
}
