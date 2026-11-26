//go:build integration

package api

import (
	"net/http"
	"testing"
)

// ReservedActingForSuite covers a backend acting for a user id Stream keeps for itself (!),
// named in X-Stream-User-Id: every write that would store or act as that user is refused,
// before anything about the request is looked up.
type ReservedActingForSuite struct {
	RouterSuite
}

func TestReservedActingForSuite(t *testing.T) {
	runSuite(t, new(ReservedActingForSuite))
}

func (s *ReservedActingForSuite) SetupTest() {
	s.useApp(s.data.createApp())
	s.T().Cleanup(reservedIDsSwitch.EnableForApp(s.customerID()))
}

// reserved is the backend acting for a user id Stream keeps for itself.
func (s *ReservedActingForSuite) reserved() *testClient {
	return s.serverClient.actingFor(&testClient{userID: "!anon"})
}

func (s *ReservedActingForSuite) TestNoSessionIsOpenedForAReservedUserID() {
	status, failure := s.reserved().failure(http.MethodPost, "/v1/agents/sessions",
		CreateSessionRequest{})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "may not start with !")
}

func (s *ReservedActingForSuite) TestNoSessionIsForkedOrResumedForAReservedUserID() {
	for _, path := range []string{"/fork", "/resume"} {
		status, failure := s.reserved().failure(http.MethodPost, "/v1/agents/sessions/"+s.utils.uuid()+path, nil)

		s.Equal(http.StatusBadRequest, status, path)
		s.Contains(failure, "may not start with !", path)
	}
}

func (s *ReservedActingForSuite) TestNoTaskIsScheduledForAReservedUserID() {
	status, failure := s.reserved().failure(http.MethodPost, "/v1/agents/scheduled-tasks", taskRequest(s.utils.uuid()))

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "may not start with !")
}

func (s *ReservedActingForSuite) TestNoConnectionIsCreatedForAReservedUserID() {
	status, failure := s.reserved().failure(http.MethodPost, "/v1/agents/connections", appOwned("linear"))

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "may not start with !")
}

// Reading and deleting stay open, so whatever is already kept under such an id can still be
// found and erased.
func (s *ReservedActingForSuite) TestAReservedUserIDStillListsItsTasks() {
	s.Equal(http.StatusOK, s.reserved().do(http.MethodGet, "/v1/agents/scheduled-tasks", nil, nil))
}

// Until its switch is on, a reserved acting-for id is let through as before.
func (s *ReservedActingForSuite) TestWithTheSwitchOffAReservedUserIDIsLetThrough() {
	s.useApp(s.data.createApp()) // an app the switch is not on for

	status, _ := s.reserved().failure(http.MethodPost, "/v1/agents/sessions/"+s.utils.uuid()+"/fork", nil)

	s.Equal(http.StatusNotFound, status)
}
