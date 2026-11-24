//go:build integration

package api

import (
	"net/http"
)

// A resume of an ended session that cannot be carried on, the credit being spent, is refused for
// money as a message to it is: a 402 that names the credit, not the 500 of a failure that is ours.
func (s *TelnyxUnpaidSuite) TestAResumeOfAnEndedSessionOfAnOrganizationWithNoCreditIsRefusedForMoney() {
	user, opened := s.spentAfterOpening()
	status, body := user.call(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/stop", nil)
	s.Require().Equal(http.StatusNoContent, status, string(body))

	status, body = s.serverClient.actingFor(user).call(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/resume",
		ResumeSessionRequest{})

	s.Equal(http.StatusPaymentRequired, status, string(body))
	s.Contains(string(body), `"code":"insufficient_credits"`)
}
