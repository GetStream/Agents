//go:build integration

package api

import (
	"encoding/json"
	"net/http"
	"testing"
)

// suiteActivityKey and suitePreviousActivityKey are what ActivityKeySuite's router takes in
// X-Activity-Key: the current key and the one being rotated out.
const (
	suiteActivityKey         = "suite-activity-key"
	suitePreviousActivityKey = "suite-previous-activity-key"
)

// ActivityKeySuite is a job reading activity with the activity key, on a router that has one
// and the key it replaces: the two activity paths answer it, and nothing else does.
type ActivityKeySuite struct {
	RouterSuite
}

func TestActivityKeySuite(t *testing.T) {
	runSuite(t, new(ActivityKeySuite))
}

func (s *ActivityKeySuite) SetupSuite() {
	s.activityKey, s.previousActivityKey = suiteActivityKey, suitePreviousActivityKey
	s.RouterSuite.SetupSuite()
}

func (s *ActivityKeySuite) SetupTest() {
	s.useApp(s.data.createApp())
}

// activityPaths are the two operations under activitySecurity.
func (s *ActivityKeySuite) activityPaths() []string {
	day := daysAgo(1, 0)
	return []string{activityPath(day, day, 1), "/v1/ops/orgs/" + s.app.organization.ID + "/activity"}
}

// holding is a caller with no app's credentials sending key in header.
func (s *ActivityKeySuite) holding(header, key string) *testClient {
	return &testClient{suite: &s.RouterSuite, header: http.Header{header: {key}}, kind: noCredential}
}

// Defect: the activity key, or the one it replaces, refused on the routes it is for.
func (s *ActivityKeySuite) TestEitherActivityKeyReadsBothActivityRoutes() {
	for _, key := range []string{suiteActivityKey, suitePreviousActivityKey} {
		for _, path := range s.activityPaths() {
			status, body := s.holding(activityKeyHeader, key).call(http.MethodGet, path, nil)
			s.Equal(http.StatusOK, status, key+" "+path+": "+string(body))
		}
	}
}

// Defect: the ops key no longer reading activity once an activity key is set.
func (s *ActivityKeySuite) TestTheOpsKeyStillReadsBothActivityRoutes() {
	for _, path := range s.activityPaths() {
		s.Equal(http.StatusOK, s.holding(opsKeyHeader, suiteOpsKey).do(http.MethodGet, path, nil, nil), path)
	}
}

// Defect: a key that is not one of the two, or one the router took from another header,
// reading activity. The 401 names both headers the routes take.
func (s *ActivityKeySuite) TestOnlyAnActivityKeyInItsOwnHeaderReadsActivity() {
	for _, caller := range map[string]*testClient{
		"another key":                 s.holding(activityKeyHeader, "not-"+suiteActivityKey),
		"the activity key as ops key": s.holding(opsKeyHeader, suiteActivityKey),
		"the ops key as activity key": s.holding(activityKeyHeader, suiteOpsKey),
		"the manage key":              s.holding(activityKeyHeader, suiteManageKey),
		"no key":                      s.holding(activityKeyHeader, ""),
		"the previous key as ops key": s.holding(opsKeyHeader, suitePreviousActivityKey),
		"the activity key, an app's":  s.serverClient.with(activityKeyHeader, suiteActivityKey).with(opsKeyHeader, ""),
	} {
		for _, path := range s.activityPaths() {
			s.refused(caller, http.MethodGet, path, "X-Ops-Key or X-Activity-Key")
		}
	}
}

// Defect: the activity key reaching a staff route that is not an activity read, a write
// above all, or the management API, or a customer's route.
func (s *ActivityKeySuite) TestTheActivityKeyReachesNoOtherRoute() {
	for _, key := range []string{suiteActivityKey, suitePreviousActivityKey} {
		caller := s.holding(activityKeyHeader, key).with(actorIDHeader, "staff:"+s.utils.uuid())
		s.refused(caller, http.MethodGet, "/v1/ops/use-cases", "it needs X-Ops-Key")
		s.refused(caller, http.MethodPost, "/v1/ops/stats/rollup", "it needs X-Ops-Key")
		s.refused(caller, http.MethodGet, "/v1/manage/status", "it needs X-Manage-Key")
		s.refused(caller, http.MethodGet, "/v1/phone/use-cases", "")
	}
	// Control: the routes it was refused on answer their own key.
	s.Equal(http.StatusOK, s.holding(opsKeyHeader, suiteOpsKey).do(http.MethodGet, "/v1/ops/use-cases", nil, nil))
}

// refused checks caller is answered 401 on method path, with a message holding asked for,
// and no key in the answer.
func (s *ActivityKeySuite) refused(caller *testClient, method, path, asked string) {
	status, body := caller.call(method, path, nil)
	s.Require().Equal(http.StatusUnauthorized, status, method+" "+path+": "+string(body))
	var answered ErrorResponse
	s.Require().NoError(json.Unmarshal(body, &answered), string(body))
	s.Contains(answered.Error.Message, asked, path)
	if asked == "it needs X-Ops-Key" {
		s.NotContains(answered.Error.Message, activityKeyHeader, "only the activity routes take it: "+path)
	}
	for _, key := range []string{suiteActivityKey, suitePreviousActivityKey, suiteOpsKey, suiteManageKey} {
		s.NotContains(string(body), key, path)
	}
}
