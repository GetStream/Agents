//go:build integration

package api

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

// sameID is an app in no organization and an app of the organization whose id is that app's, as
// their backends: an owner of limits each.
func (s *SelfHostedLimitsSuite) sameID() (own, organization *testClient, id string) {
	id = s.utils.uuid()
	return s.backend(id, ""), s.backend(s.utils.uuid(), id), id
}

// eachOwner runs check with each of the two owners of one id as the one that writes.
func (s *SelfHostedLimitsSuite) eachOwner(check func(writer, other *testClient)) {
	own, organization, _ := s.sameID()
	check(own, organization)
	own, organization, _ = s.sameID()
	check(organization, own)
}

func (s *SelfHostedLimitsSuite) limitAs(client *testClient, micros int64) Limit {
	var created Limit
	s.Require().Equal(http.StatusCreated, client.do(http.MethodPost, "/v1/spend/limits",
		map[string]any{"subject": "team", "team": "finance", "interval": "daily", "limit_micros": micros}, &created))
	return created
}

func (s *SelfHostedLimitsSuite) limitsAs(client *testClient, path string) []Limit {
	var page LimitPage
	s.Require().Equal(http.StatusOK, client.do(http.MethodGet, path, nil, &page))
	return page.Items
}

func (s *SelfHostedLimitsSuite) grantAs(client *testClient) LimitGrant {
	var made LimitGrant
	s.Require().Equal(http.StatusCreated, client.do(http.MethodPost, "/v1/spend/limits/grants",
		map[string]any{"subject": "team", "team": "finance", "amount_micros": 5, "reason": "launch"}, &made))
	return made
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

func (s *SelfHostedLimitsSuite) TestLimitsAreListedToTheirOwnerAlone() {
	s.eachOwner(func(writer, other *testClient) {
		made := s.limitAs(writer, 4242)

		s.Empty(s.limitsAs(other, "/v1/spend/limits"))
		s.Empty(s.teamsOf(other))
		s.Equal(http.StatusNotFound, other.do(http.MethodGet, fmt.Sprintf("/v1/spend/limits/%d", made.Id), nil, nil))
		s.Nil(s.usageAs(other).Windows[0].Limit)

		s.Len(s.limitsAs(writer, "/v1/spend/limits"), 1)
		s.Require().Len(s.teamsOf(writer), 1)
		s.Len(s.teamsOf(writer)[0].Limits, 1)
		s.Equal(http.StatusOK, writer.do(http.MethodGet, fmt.Sprintf("/v1/spend/limits/%d", made.Id), nil, nil))
		s.Equal(int64(4242), s.usageAs(writer).Windows[0].Limit.LimitMicros)
	})
}

func (s *SelfHostedLimitsSuite) teamsOf(client *testClient) []Team {
	var teams []Team
	s.Require().Equal(http.StatusOK, client.do(http.MethodGet, "/v1/teams", nil, &teams))
	return teams
}

func (s *SelfHostedLimitsSuite) TestALimitIsChangedAndDeletedByItsOwnerAlone() {
	s.eachOwner(func(writer, other *testClient) {
		made := s.limitAs(writer, 4242)
		path := fmt.Sprintf("/v1/spend/limits/%d", made.Id)

		s.Equal(http.StatusNotFound, other.do(http.MethodPatch, path, map[string]any{"limit_micros": 1}, nil))
		s.Equal(http.StatusNotFound, other.do(http.MethodDelete, path, nil, nil))

		s.Equal(int64(4242), s.limitsAs(writer, "/v1/spend/limits")[0].LimitMicros)
		s.Equal(http.StatusOK, writer.do(http.MethodPatch, path, map[string]any{"limit_micros": 7}, nil))
		s.Equal(http.StatusNoContent, writer.do(http.MethodDelete, path, nil, nil))
	})
}

func (s *SelfHostedLimitsSuite) TestEachOwnerHasALimitOfItsOwnAtOnePlace() {
	s.eachOwner(func(writer, other *testClient) {
		first, second := s.limitAs(writer, 100), s.limitAs(other, 900)
		s.NotEqual(first.Id, second.Id)

		status, body := writer.call(http.MethodPost, "/v1/spend/limits",
			map[string]any{"subject": "team", "team": "finance", "interval": "daily", "limit_micros": 5})
		s.Equal(http.StatusConflict, status)
		s.Contains(string(body), fmt.Sprintf("limit %d;", first.Id))
		s.Equal(int64(100), s.usageAs(writer).Windows[0].Limit.LimitMicros)
		s.Equal(int64(900), s.usageAs(other).Windows[0].Limit.LimitMicros)
	})
}

func (s *SelfHostedLimitsSuite) TestALimitIsHeldToItsOwnersParentsAlone() {
	s.eachOwner(func(writer, other *testClient) {
		s.Require().Equal(http.StatusCreated, writer.do(http.MethodPost, "/v1/spend/limits",
			map[string]any{"subject": "organization", "interval": "daily", "limit_micros": 10}, nil))

		made := s.limitAs(other, 4242)

		s.Equal(int64(4242), made.EffectiveLimitMicros)
		s.Equal(int64(4242), s.limitsAs(other, "/v1/spend/limits")[0].EffectiveLimitMicros)
		status, _ := writer.call(http.MethodPost, "/v1/spend/limits",
			map[string]any{"subject": "team", "team": "finance", "interval": "daily", "limit_micros": 4242})
		s.Equal(http.StatusBadRequest, status)
	})
}

func (s *SelfHostedLimitsSuite) TestGrantsAreTheirOwnersAlone() {
	s.eachOwner(func(writer, other *testClient) {
		s.limitAs(writer, 4242)
		s.Equal(http.StatusNotFound, other.do(http.MethodPost, "/v1/spend/limits/grants",
			map[string]any{"subject": "team", "team": "finance", "amount_micros": 5, "reason": "x"}, nil), "it has no limit")
		s.limitAs(other, 4242)
		made := s.grantAs(writer)
		path := fmt.Sprintf("/v1/spend/limits/grants/%d", made.Id)

		s.Empty(s.grantsAs(other))
		s.Empty(s.usageAs(other).Grants)
		s.Equal(http.StatusNotFound, other.do(http.MethodPost, path+"/revoke", map[string]any{"reason": "x"}, nil))
		s.Len(s.usageAs(writer).Grants, 1)
		s.Equal(GrantLive, s.grantsAs(writer)[0].State)
		s.Equal(http.StatusOK, writer.do(http.MethodPost, path+"/revoke", map[string]any{"reason": "x"}, nil))
		s.Equal(http.StatusNotFound, other.do(http.MethodPost, path+"/unrevoke", map[string]any{"reason": "x"}, nil))
		s.Equal(GrantRevoked, s.grantsAs(writer)[0].State)
		s.Equal(http.StatusOK, writer.do(http.MethodPost, path+"/unrevoke", map[string]any{"reason": "x"}, nil))
	})
}

func (s *SelfHostedLimitsSuite) TestGrantsOfOneOwnerTakeNoPlaceOfAnothers() {
	s.eachOwner(func(writer, other *testClient) {
		s.limitAs(writer, 4242)
		s.limitAs(other, 4242)
		kept := s.grantAs(other)
		path := fmt.Sprintf("/v1/spend/limits/grants/%d", kept.Id)
		s.Require().Equal(http.StatusOK, other.do(http.MethodPost, path+"/revoke", map[string]any{"reason": "x"}, nil))
		for range 20 {
			s.grantAs(writer)
		}

		s.grantAs(other)
		s.Equal(http.StatusOK, other.do(http.MethodPost, path+"/unrevoke", map[string]any{"reason": "x"}, nil))
		s.Equal(http.StatusConflict, writer.do(http.MethodPost, "/v1/spend/limits/grants",
			map[string]any{"subject": "team", "team": "finance", "amount_micros": 5, "reason": "x"}, nil))
	})
}

func (s *SelfHostedLimitsSuite) TestAResetIsItsOwnersAlone() {
	own, organization, id := s.sameID()
	reset := map[string]any{"subject": "organization", "reason": "x"}
	s.Require().Equal(http.StatusCreated, own.do(http.MethodPost, "/v1/spend/limits",
		map[string]any{"subject": "organization", "interval": "daily", "limit_micros": 1_000_000}, nil))
	s.Equal(http.StatusNotFound, organization.do(http.MethodPost, "/v1/spend/limits/usage/reset", reset, nil), "it has no limit")
	s.Require().Equal(http.StatusCreated, organization.do(http.MethodPost, "/v1/spend/limits",
		map[string]any{"subject": "organization", "interval": "daily", "limit_micros": 1_000_000}, nil))
	s.spent(id, 300_000)

	s.Require().Equal(http.StatusOK, own.do(http.MethodPost, "/v1/spend/limits/usage/reset", reset, nil))

	s.Equal(int64(300_000), s.organizationWindow(own).BaselineMicros)
	s.Zero(s.organizationWindow(organization).BaselineMicros)

	// The other way round, with a baseline of the organization's own.
	member := s.utils.uuid()
	s.Require().NoError(s.store.AppOrganizations.Upsert(context.Background(), &store.AppOrganization{AppID: member, OrganizationID: id}))
	s.spent(member, 700_000)
	s.Require().Equal(http.StatusOK, s.backend(member, id).do(http.MethodPost, "/v1/spend/limits/usage/reset", reset, nil))
	s.Equal(int64(700_000), s.organizationWindow(organization).BaselineMicros)
	s.Equal(int64(300_000), s.organizationWindow(own).BaselineMicros)
}

func (s *SelfHostedLimitsSuite) organizationWindow(client *testClient) LimitWindow {
	var read []SubjectUsage
	s.Require().Equal(http.StatusOK, client.do(http.MethodGet, "/v1/spend/limits/usage?subject=organization", nil, &read))
	return read[0].Windows[0]
}

// legacy writes a limit as the release before limits said whose they are does, made by creator.
func (s *SelfHostedLimitsSuite) legacy(id, creator, source string, micros int64) store.Limit {
	limit := store.Limit{Subject: usage.SubjectCategory, Level: store.LevelOrganization, OrganizationID: id,
		Category: "team:finance", Interval: usage.Daily, LimitMicros: micros, UpdatedBy: "api"}
	s.Require().NoError(s.store.Limits.Insert(context.Background(), &limit, store.LimitAudit{CustomerID: creator, Source: source}))
	_, err := s.store.ShardDB().NewRaw("UPDATE limits SET billed = NULL, created_at = now() - interval '1 hour' WHERE id = ?", limit.ID).Exec(context.Background())
	s.Require().NoError(err)
	return limit
}

// One of nobody known is enforced on both, and shown to and changed by neither.
func (s *SelfHostedLimitsSuite) TestALimitOfNobodyKnownIsShownToNeitherOwnerAndToStaff() {
	own, organization, id := s.sameID()
	limit := s.legacy(id, id, store.AuditSourceManage, 4242)
	path := fmt.Sprintf("/v1/spend/limits/%d", limit.ID)
	for _, client := range []*testClient{own, organization} {
		s.Empty(s.limitsAs(client, "/v1/spend/limits"))
		s.Empty(s.teamsOf(client))
		s.Nil(s.usageAs(client).Windows[0].Limit)
		s.Equal(http.StatusNotFound, client.do(http.MethodGet, path, nil, nil))
		s.Equal(http.StatusNotFound, client.do(http.MethodPatch, path, map[string]any{"limit_micros": 1}, nil))
		s.Equal(http.StatusNotFound, client.do(http.MethodDelete, path, nil, nil))
		status, body := client.call(http.MethodPost, "/v1/spend/limits",
			map[string]any{"subject": "team", "team": "finance", "interval": "daily", "limit_micros": 5})
		s.Equal(http.StatusConflict, status)
		s.Contains(string(body), "support can change")
		s.NotContains(string(body), fmt.Sprintf("limit %d", limit.ID))
	}

	staff := s.staff()
	listed := s.limitsAs(staff, "/v1/manage/spend/limits?organization_id="+url.QueryEscape(id))
	s.Require().Len(listed, 1)
	s.Equal(http.StatusOK, staff.do(http.MethodPatch, "/v1/manage/spend/limits/"+fmt.Sprint(limit.ID), map[string]any{"limit_micros": 9}, nil))
	s.Equal(http.StatusNoContent, staff.do(http.MethodDelete, "/v1/manage/spend/limits/"+fmt.Sprint(limit.ID), nil, nil))
}

// nobodys makes a row of table's one of nobody known.
func (s *SelfHostedLimitsSuite) nobodys(table string, id int64) {
	_, err := s.store.ShardDB().NewRaw("UPDATE ? SET billed = NULL WHERE id = ?", bun.Ident(table), id).Exec(context.Background())
	s.Require().NoError(err)
}

func (s *SelfHostedLimitsSuite) TestAGrantOfNobodyKnownIsShownToNeitherOwnerAndToStaff() {
	own, organization, id := s.sameID()
	s.limitAs(own, 4242)
	s.limitAs(organization, 4242)
	made := s.grantAs(own)
	s.nobodys("limit_grants", made.Id)
	path := fmt.Sprintf("/spend/limits/grants/%d", made.Id)
	for _, client := range []*testClient{own, organization} {
		s.Empty(s.grantsAs(client))
		s.Empty(s.usageAs(client).Grants)
		s.Equal(http.StatusNotFound, client.do(http.MethodPost, "/v1"+path+"/revoke", map[string]any{"reason": "x"}, nil))
	}

	staff := s.staff()
	var listed LimitGrantPage
	s.Require().Equal(http.StatusOK, staff.do(http.MethodGet, "/v1/manage/spend/limits/grants?organization_id="+url.QueryEscape(id), nil, &listed))
	s.Require().Len(listed.Items, 1)
	s.Equal(http.StatusOK, staff.do(http.MethodPost, "/v1/manage"+path+"/revoke", map[string]any{"reason": "x"}, nil))
	s.Equal(http.StatusOK, staff.do(http.MethodPost, "/v1/manage"+path+"/unrevoke", map[string]any{"reason": "x"}, nil))
}

func (s *SelfHostedLimitsSuite) TestAResetOfNobodyKnownIsShownToNeitherOwner() {
	own, _, id := s.sameID()
	s.Require().Equal(http.StatusCreated, own.do(http.MethodPost, "/v1/spend/limits",
		map[string]any{"subject": "organization", "interval": "daily", "limit_micros": 1_000_000}, nil))
	s.spent(id, 300_000)
	s.Require().Equal(http.StatusOK, own.do(http.MethodPost, "/v1/spend/limits/usage/reset", map[string]any{"subject": "organization", "reason": "x"}, nil))
	resets, err := s.store.UsageResets.Select(context.Background(), []string{id}, time.Time{})
	s.Require().NoError(err)
	for _, reset := range resets {
		s.nobodys("usage_resets", reset.ID)
	}

	s.Zero(s.organizationWindow(own).BaselineMicros)
}

// A limit of nobody known still refuses the work of both owners of its id.
func (s *SelfHostedLimitsSuite) TestALimitOfNobodyKnownRefusesTheWorkOfBothOwners() {
	own, organization, id := s.sameID()
	limit := store.Limit{Subject: usage.SubjectOrganization, Level: store.LevelOrganization, OrganizationID: id, Interval: usage.Daily, UpdatedBy: "api"}
	s.Require().NoError(s.store.Limits.Insert(context.Background(), &limit, store.LimitAudit{CustomerID: id, Source: store.AuditSourceManage}))
	s.nobodys("limits", limit.ID)

	s.True(s.refusedSearch(own))
	s.True(s.refusedSearch(organization))
}

func (s *SelfHostedLimitsSuite) staff() *testClient {
	return &testClient{suite: &s.RouterSuite, kind: noCredential, header: http.Header{
		manageKeyHeader: {suiteManageKey}, actorIDHeader: {"staff:" + s.utils.uuid()},
	}}
}

func (s *SelfHostedLimitsSuite) TestAnAttributedLimitIsItsWritersAgain() {
	own, organization, id := s.sameID()
	mine := s.legacy(id, id, store.AuditSourceAPI, 4242)
	s.Require().NotZero(mine.ID)
	_, err := s.store.AttributeLimits(context.Background())
	s.Require().NoError(err)
	s.Len(s.limitsAs(own, "/v1/spend/limits"), 1)
	s.Empty(s.limitsAs(organization, "/v1/spend/limits"))

	own, organization, id = s.sameID()
	s.legacy(id, s.utils.uuid(), store.AuditSourceAPI, 4242)
	_, err = s.store.AttributeLimits(context.Background())
	s.Require().NoError(err)
	s.Empty(s.limitsAs(own, "/v1/spend/limits"))
	s.Len(s.limitsAs(organization, "/v1/spend/limits"), 1)
}

// searched is whether client's searches come to be refused at its limit within a while.
func (s *SelfHostedLimitsSuite) refusedSearch(client *testClient) bool {
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if status, _ := client.call(http.MethodPost, "/v1/search", SearchRequest{Query: "what is the time"}); status == http.StatusPaymentRequired {
			return true
		}
	}
	return false
}

func (s *SelfHostedLimitsSuite) TestALimitWrittenOverHTTPRefusesItsOwnersWorkAlone() {
	s.eachOwner(func(writer, other *testClient) {
		s.Require().Equal(http.StatusCreated, writer.do(http.MethodPost, "/v1/spend/limits",
			map[string]any{"subject": "organization", "interval": "daily", "limit_micros": 0}, nil))

		s.True(s.refusedSearch(writer))
		s.False(s.refusedSearch(other))
	})
}

// What an app in no organization writes of its limits holds its next admission.
func (s *SelfHostedLimitsSuite) TestAnAppsOwnWritesHoldItsNextAdmission() {
	own, _, id := s.sameID()
	searched := func() int {
		status, _ := own.call(http.MethodPost, "/v1/search", SearchRequest{Query: "what is the time"})
		return status
	}
	s.spent(id, 1_000_000)
	s.Require().Equal(http.StatusOK, searched())

	var limit Limit
	s.Require().Equal(http.StatusCreated, own.do(http.MethodPost, "/v1/spend/limits",
		map[string]any{"subject": "organization", "interval": "daily", "limit_micros": 1_000_000}, &limit))
	s.Equal(http.StatusPaymentRequired, searched(), "created")
	path := fmt.Sprintf("/v1/spend/limits/%d", limit.Id)
	s.Require().Equal(http.StatusOK, own.do(http.MethodPatch, path, map[string]any{"limit_micros": 9_000_000}, nil))
	s.Equal(http.StatusOK, searched(), "raised")
	s.Require().Equal(http.StatusOK, own.do(http.MethodPatch, path, map[string]any{"limit_micros": 1_000_000}, nil))
	s.Equal(http.StatusPaymentRequired, searched(), "lowered")
	var grant LimitGrant
	s.Require().Equal(http.StatusCreated, own.do(http.MethodPost, "/v1/spend/limits/grants",
		map[string]any{"subject": "organization", "amount_micros": 9_000_000, "reason": "x"}, &grant))
	s.Equal(http.StatusOK, searched(), "granted")
	s.Require().Equal(http.StatusOK, own.do(http.MethodPost, fmt.Sprintf("/v1/spend/limits/grants/%d/revoke", grant.Id), map[string]any{"reason": "x"}, nil))
	s.Equal(http.StatusPaymentRequired, searched(), "revoked")
	s.Require().Equal(http.StatusOK, own.do(http.MethodPost, fmt.Sprintf("/v1/spend/limits/grants/%d/unrevoke", grant.Id), map[string]any{"reason": "x"}, nil))
	s.Equal(http.StatusOK, searched(), "unrevoked")
	s.Require().Equal(http.StatusOK, own.do(http.MethodPost, fmt.Sprintf("/v1/spend/limits/grants/%d/revoke", grant.Id), map[string]any{"reason": "x"}, nil))
	s.Require().Equal(http.StatusOK, own.do(http.MethodPost, "/v1/spend/limits/usage/reset", map[string]any{"subject": "organization", "reason": "x"}, nil))
	s.Equal(http.StatusOK, searched(), "reset")
	s.Require().Equal(http.StatusOK, own.do(http.MethodPatch, path, map[string]any{"limit_micros": 0}, nil))
	s.Equal(http.StatusPaymentRequired, searched(), "lowered to nothing")
	s.Require().Equal(http.StatusNoContent, own.do(http.MethodDelete, path, nil, nil))
	s.Equal(http.StatusOK, searched(), "deleted")
}
